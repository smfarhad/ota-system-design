# Flight Booking System (OTA) — Multi-Supplier Design

A flight booking platform that aggregates inventory from several suppliers
(Sabre, Amadeus, Travelport/Galileo, NDC airline-direct, LCC direct, consolidators)
behind one canonical API.

---

## 1. Goals and non-goals

**Goals**
- One search/price/book/ticket/servicing API for web, mobile, B2B/agents.
- Add a new supplier by writing one adapter — no change to core services.
- Search p95 < 4 s even when slow suppliers are involved (partial/streamed results).
- Never double-charge, never lose a booking: every money/PNR step is idempotent and recoverable.
- Supplier outages degrade results, not availability.

**Non-goals (v1)**: hotels/cars, loyalty, corporate travel policy engine, own GDS/CRS.

---

## 2. High-level architecture

```
 Web / Mobile / B2B partners
            │  HTTPS
      ┌─────▼──────┐
      │ API Gateway │  authN/Z, rate limit, WAF
      └─────┬──────┘
            │
 ┌──────────┼───────────────────────────────────────────────┐
 │          ▼          Core domain services                  │
 │  Search Svc ─► Offer/Pricing Svc ─► Booking Svc ─► Ticketing Svc
 │     │   ▲            │                 │  ▲            │
 │     │   │            │                 │  │ Saga       │
 │     │  Cache         │            Payment Svc ◄────────┘
 │     │  (Redis)       │                 │
 │     ▼                ▼                 ▼
 │  ┌────────────── Supplier Orchestrator ──────────────┐
 │  │ fan-out · timeouts · circuit breakers · hedging   │
 │  │ routing rules · normalisation · dedupe · ranking  │
 │  └───┬────────┬──────────┬─────────┬─────────┬───────┘
 └──────┼────────┼──────────┼─────────┼─────────┼──────────┘
        ▼        ▼          ▼         ▼         ▼
   ┌───────┐ ┌────────┐ ┌─────────┐ ┌─────┐ ┌─────────┐
   │ Sabre │ │Amadeus │ │Travelport│ │ NDC │ │ LCC/…   │   Provider Adapters
   │adapter│ │adapter │ │ (Galileo)│ │adapt│ │ adapters│   (anti-corruption layer)
   └───┬───┘ └───┬────┘ └────┬────┘ └──┬──┘ └────┬────┘
       ▼         ▼           ▼         ▼         ▼
     Sabre     Amadeus    Travelport  Airline   LCC APIs
     APIs      APIs       APIs        NDC APIs

 Cross-cutting: Postgres · Redis · Kafka · Object store (raw payload archive)
                Notification Svc (email/SMS/WhatsApp) · Admin/Backoffice
                Observability (OTel, Prometheus, Grafana, Loki) · Vault/KMS
```

Style: modular services around an **event bus (Kafka)**; synchronous gRPC/REST on the
hot path (search → price → book), async events for ticketing, notifications, reconciliation.
Start as a modular monolith with the adapter layer as a hard boundary; split Search and
Orchestrator out first (they scale differently).

---

## 3. Supplier landscape

| Supplier | Content / protocol | Notes |
|---|---|---|
| **Sabre** | REST (Bargain Finder Max, Create PNR, Ticketing), legacy SOAP/SabreCommand | OAuth token per PCC; PCC/pseudo-city based commercial setup |
| **Amadeus** | Enterprise: SOAP/EDIFACT web services (Fare_MasterPricer, PNR_AddMultiElements, DocIssuance); Self-Service: REST (Flight Offers Search/Price, Flight Orders) | Office ID based; Self-Service is good for MVP/test |
| **Travelport (Galileo)** | Travelport+ REST/JSON; legacy Universal API (SOAP/XML: `AirLowFareSearch`, `AirCreateReservation`) | Galileo = GDS (1G) inside Travelport; PCC + branch |
| **NDC (airline direct)** | NDC XML/JSON (AirShopping, OfferPrice, OrderCreate, OrderChange) — AA, BA, LH group, AF-KL, etc. | Offer/Order model, not PNR; richer ancillaries |
| **LCC direct** | Proprietary REST/SOAP | Often no ticket — a PNR is the product; pay-at-booking |
| **Aggregators** | Duffel, Kiwi/Tequila, etc. | Fast onboarding for long tail; higher cost |

Key difference to hide behind the adapter: **GDS = PNR + ticket** (book, then issue e-ticket
before the time limit) vs **NDC/LCC = Offer → Order** (payment often at order creation).

---

## 4. Provider adapter layer (anti-corruption layer)

Every supplier implements the same port. Core never sees supplier DTOs.

```ts
interface FlightSupplier {
  readonly code: "SABRE" | "AMADEUS" | "TRAVELPORT" | "NDC_BA" | ...;
  capabilities(): Capabilities;            // ticketing, ancillaries, seatmap, split-pay, void window…

  search(req: SearchRequest, ctx: Ctx): Promise<Offer[]>;
  price(offerRef: SupplierOfferRef, pax: Pax[], ctx): Promise<PricedOffer>;   // revalidate fare
  fareRules(offerRef, ctx): Promise<FareRules>;
  seatMap(offerRef, ctx): Promise<SeatMap>;
  ancillaries(offerRef, ctx): Promise<Ancillary[]>;

  book(priced: PricedOffer, pax: PaxDetails[], contact, ctx): Promise<SupplierBooking>; // PNR / Order
  retrieve(ref: SupplierBookingRef, ctx): Promise<SupplierBooking>;
  issueTicket(ref, payment: FormOfPayment, ctx): Promise<Ticket[]>;
  cancel(ref, ctx): Promise<CancelResult>;       // void / refund quote + execute
  change(ref, change: ChangeRequest, ctx): Promise<ChangeQuote>;
  health(): Promise<Health>;
}
```

Each adapter package contains:
- **Client**: HTTP/SOAP transport, auth/token refresh (per PCC/Office ID), connection pooling, retries on idempotent calls only.
- **Mapper**: canonical ⇄ supplier (IATA codes, cabin, RBD/booking class, fare basis, baggage, pax types ADT/CHD/INF, taxes by code, currency).
- **Error translator**: supplier errors → canonical error taxonomy (`FARE_CHANGED`, `SOLD_OUT`, `INVALID_PAX_DATA`, `TIMEOUT`, `SUPPLIER_DOWN`, `PAYMENT_DECLINED`…) with `retryable` flag.
- **Contract tests** against recorded fixtures + sandbox (VCR-style), run in CI.
- **Raw payload archive**: every request/response stored (PII-masked, encrypted) in object storage with correlation ID — mandatory for disputes and debugging.

**Supplier account config** (DB, not code): per-supplier credentials ref (Vault), PCC/Office ID, markets, currencies, enabled carriers, commission/net-fare agreements, rate limits, timeouts. One supplier can have many accounts (e.g. separate PCC for B2C vs B2B, per country).

---

## 5. Canonical domain model

```
SearchRequest { legs[{origin,dest,date}], pax{adt,chd,inf}, cabin, flexDates, filters }
Offer         { offerId (ours), supplier, supplierAccount, itineraries[segments[]],
                fareBreakdown{base,taxes[],fees,total,currency}, validatingCarrier,
                fareFamily, baggage, refundable/changeable, ttl, supplierRef(opaque, signed) }
Segment       { marketing/operating carrier, flightNo, dep/arr airport+terminal+time(UTC+local),
                aircraft, rbd, cabin, duration }
Booking       { bookingId, status, supplierRef (PNR/OrderId), pax[], contact,
                price snapshot, ticketTimeLimit, tickets[], payments[], events[] }
```

- **Money**: integer minor units + ISO-4217; never floats. Keep supplier currency and customer currency + FX rate snapshot.
- **Time**: store local time + airport tz + UTC.
- **Opaque `supplierRef`** is signed and encrypted so clients can't tamper; offers expire (TTL 10–30 min).

---

## 6. Core flows

### 6.1 Search (fan-out, streaming)

1. Gateway → Search Svc: validate, normalise (city→airports, date flex).
2. **Cache check** (Redis, key = route+dates+pax+cabin, TTL 3–10 min; "recent searches" are indicative only, always re-price before book).
3. **Orchestrator routing rules** pick suppliers per request (market, carrier, cabin, cost, health): e.g. don't call NDC_BA for non-BA routes; skip suppliers with open circuit breakers.
4. Parallel fan-out with **per-supplier deadline** (e.g. 6 s hard, 3 s soft), bulkheads (separate thread/connection pools & rate limiter per supplier), circuit breaker, optional hedged request.
5. Normalise → **dedupe** (same itinerary + fare family from several suppliers → keep cheapest net/best margin, keep alternates for failover) → **apply pricing rules** (markup, discounts, fees, promo) → rank.
6. Return via **SSE/WebSocket streaming** (results appear as suppliers answer) or a `searchId` + polling; final event marks completion with `partial: true` if some suppliers timed out.

### 6.2 Price / revalidate (mandatory before booking)
User picks offer → Orchestrator calls the same supplier's `price` with fresh session → compare to displayed total; if changed beyond tolerance, return `PRICE_CHANGED` and require confirmation. Return fare rules, baggage, ancillaries, seat map.

### 6.3 Booking (saga, idempotent)

```
Client ──POST /bookings (Idempotency-Key)──► Booking Svc
 1 validate pax data (names as per passport, DOB for infants/children, APIS/doc info)
 2 re-price/lock offer            ──► supplier.price
 3 create supplier booking (hold) ──► supplier.book  → PNR/Order, time limit (TTL)
 4 authorise payment              ──► Payment Svc (card auth / wallet / bank / mobile banking)
 5 issue ticket                   ──► supplier.issueTicket  (GDS)  /  order paid (NDC/LCC)
 6 capture payment, persist tickets, emit BookingConfirmed
 7 notify (email/SMS), generate itinerary PDF
Failure compensation:
  step 4 fails  → cancel PNR (release inventory)
  step 5 fails  → void auth, cancel PNR, or retry ticketing (if time limit remains)
  unknown state (timeout) → mark PENDING_RECONCILIATION, background job calls
                           supplier.retrieve to determine truth, then complete or compensate
```

- State machine: `CREATED → PRICED → HELD → PAID(AUTHORISED) → TICKETING → TICKETED | FAILED | CANCELLED | EXPIRED`.
- **Authorise first, capture after ticketing**: avoids charging for un-ticketed bookings.
- **Hold flows** (pay later): GDS PNR with ticket time limit; reminder jobs; auto-cancel at TTL to avoid ADM/duplicate-booking penalties.
- **Duplicate booking guard**: same pax + same flights within window → warn/block (airlines & GDS penalise).
- Orchestrator uses **orchestration-style saga** persisted in Postgres (outbox pattern → Kafka) so a crash resumes from the last step.

### 6.4 Post-booking servicing
Retrieve/sync PNR changes (schedule changes, cancellations by airline — poll + webhook/queue where supported, e.g. GDS queue/ NDC notifications), void (same-day), refund quote & execute, exchange/reissue, add ancillaries/seats, name correction, refund to original FoP, accounting entries.

---

## 7. Data stores

| Store | Use |
|---|---|
| **PostgreSQL** | bookings, passengers, tickets, payments, ledger, supplier accounts, pricing rules, outbox. ACID is essential here |
| **Redis** | search cache, offer snapshots (TTL), rate-limit counters, circuit-breaker state, distributed locks, idempotency keys |
| **Kafka** | domain events: `SearchPerformed`, `BookingCreated`, `TicketIssued`, `PaymentCaptured`, `ScheduleChanged` … |
| **Object store (S3)** | raw supplier XML/JSON archive, e-ticket PDFs, invoices |
| **OpenSearch/ClickHouse** | search analytics, look-to-book, supplier performance, logs |
| **Static data** | airports, airlines, aircraft, countries, currencies, FX rates, baggage/fare family maps (cached in memory) |

Core tables (sketch): `supplier_account`, `booking`, `booking_segment`, `passenger`,
`ticket`, `payment`, `refund`, `ledger_entry`, `pricing_rule`, `markup_profile`,
`agency/customer`, `audit_log`, `outbox`, `idempotency_key`.

---

## 8. Pricing, commercials and fees

- **Pricing engine** (rules by priority): supplier/carrier/route/cabin/channel/agent-tier → markup (% or fixed), discount, service fee, payment gateway fee pass-through, promo codes, currency rounding.
- Keep **net (supplier) price vs sell price vs margin** on every offer & booking.
- Handle **commission/incentive agreements**, private/negotiated fares (per account), tax breakdown (YQ/YR vs government taxes — matters for refunds).
- FX: rates service with timestamp snapshot stored on booking.

---

## 9. Reliability and performance

- **Timeouts everywhere**, deadlines propagated; never block UI on slowest supplier.
- **Circuit breaker + bulkhead + rate limiter per supplier account** (GDS contracts cap transactions/sec; also "look-to-book" ratio limits — throttle/cache aggressively to avoid penalty fees).
- **Retries**: only idempotent reads (search/price/retrieve). Never blind-retry `book`/`issueTicket`; reconcile via `retrieve` first.
- **Session handling**: some GDS APIs are stateful (Sabre session tokens, Amadeus Soap Header sessions) → session pool inside adapter; stateless REST variants preferred.
- **Failover**: if booking with Supplier A fails with a retryable/`SOLD_OUT` error and an equivalent alternate offer exists from Supplier B, offer re-price rather than silent switch.
- **Graceful degradation**: cached/indicative fares, hide failing suppliers, status page, kill-switch per supplier/carrier via feature flags.
- Autoscale Search/Orchestrator on CPU & in-flight requests; Kubernetes with HPA; multi-AZ DB with PITR.

---

## 10. Security and compliance

- **PCI DSS**: never touch raw PAN — use gateway hosted fields/tokenisation (Stripe/Adyen/SSLCommerz etc.). Use IATA/BSP or card form-of-payment per supplier rules; virtual cards for LCC.
- **PII/GDPR-style**: encrypt passport/DOB at rest (field-level, KMS), mask in logs, retention policies, access audit. APIS/Secure Flight data only to carriers that need it.
- Secrets in Vault/KMS; supplier credentials rotated; per-environment accounts (cert vs prod).
- AuthN: OAuth2/OIDC for users, API keys + HMAC for B2B; RBAC in backoffice; full audit log.
- Anti-fraud: velocity checks, 3-D Secure, device fingerprint, risk scoring before ticketing (chargebacks are costly).
- Bot protection on search endpoints (scrapers inflate look-to-book).

---

## 11. Observability and operations

- Correlation ID from gateway → orchestrator → each supplier call → raw payload archive.
- Metrics per supplier: latency p50/p95/p99, error rate by canonical code, timeout rate, result count, win-rate (cheapest), look-to-book, booking success, ticketing success, time-to-ticket.
- Alerts: supplier error spike, ticketing failures, bookings stuck in `PENDING_RECONCILIATION`, PNRs near time-limit without ticket, payment/ticket mismatch.
- **Daily reconciliation**: bookings ↔ supplier reports ↔ payment settlements ↔ BSP/ARC billing. Finance ledger is double-entry.
- Backoffice: manual ticketing/queue handling, refund approval, supplier toggle, rule editing, booking timeline with raw payload viewer.

---

## 12. Suggested tech stack

- **Language**: Java/Kotlin (Spring Boot) or Go or TypeScript (NestJS) — XML/SOAP-heavy adapters are comfortable in Java/Kotlin; Go for high-concurrency orchestrator.
- **Infra**: Kubernetes, PostgreSQL, Redis, Kafka, S3, Terraform, GitLab CI/GitHub Actions, OpenTelemetry + Prometheus + Grafana + Loki.
- **Frontend**: Next.js/React (SSR for SEO landing pages), mobile via React Native/Flutter.
- **Resilience libs**: Resilience4j / gobreaker; Temporal (or Camunda) for durable booking sagas — recommended over hand-rolled.

---

## 13. API surface (public, canonical)

```
POST /v1/flights/search                → { searchId }   (+ GET /v1/flights/search/{id}/stream  SSE)
GET  /v1/offers/{offerId}              → priced offer, rules, baggage
POST /v1/offers/{offerId}/price        → revalidate (may return PRICE_CHANGED)
GET  /v1/offers/{offerId}/seatmap
GET  /v1/offers/{offerId}/ancillaries
POST /v1/bookings                      (Idempotency-Key header)
GET  /v1/bookings/{id}
POST /v1/bookings/{id}/pay | /ticket | /cancel | /refund-quote | /exchange
GET  /v1/bookings/{id}/documents       → e-ticket / invoice
Webhooks: booking.ticketed, booking.schedule_changed, booking.cancelled, payment.*
```

---

## 14. Rollout plan

1. **Phase 0** — Canonical model, adapter interface, one supplier (Amadeus Self-Service sandbox) end-to-end: search → price → book (test).
2. **Phase 1** — Add Sabre + Travelport adapters, orchestrator (fan-out, dedupe, breakers), pricing engine, payments, ticketing saga, e-ticket email.
3. **Phase 2** — NDC carriers + LCC, ancillaries & seat maps, servicing (void/refund/exchange), backoffice.
4. **Phase 3** — Reconciliation/finance, B2B agent portal & API, fraud scoring, smart routing (cost/conversion-based supplier selection), ML ranking.

---

## 15. Key risks and mitigations

| Risk | Mitigation |
|---|---|
| Price changes between search and book | Mandatory re-price, tolerance + user confirmation |
| Look-to-book penalties from GDS | Caching, request shaping, route/carrier-based supplier routing |
| Partial failures (booked but not ticketed / charged but no PNR) | Saga + outbox + reconciliation jobs, auth-then-capture |
| Duplicate content from many suppliers | Itinerary+fare-family fingerprint dedupe, best-net selection |
| Supplier schema/version change | Contract tests, version-pinned adapters, raw payload archive for replay |
| Slow supplier hurts UX | Deadlines, streaming results, circuit breakers |
| Onboarding cost per supplier | Strict port/adapter boundary + test kit + config-driven accounts |
