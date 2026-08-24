# Synthetic Company Fixture v0

## Contract and scope

Fixture v0 is the immutable initial-state contract for the shipment/refund
benchmark. Its schema version is `chaosagent.fixture/v0`; future versions
require new schema and loader dispatch rather than reinterpretation of stored v0
data.

The contract contains an identity, revision, human-readable metadata, explicit
`reference_time`, and only six V1 entity collections:

- customers;
- orders;
- shipments;
- payments;
- refunds; and
- support tickets.

Objects are strict and reject unknown properties. Status vocabularies are
closed. Monetary values use positive integer minor units plus an explicit
three-letter currency; floating-point money is rejected even when a JSON Schema
implementation would treat a mathematically integral float as an integer.

## Relationships and business model

Fixture validation enforces unique IDs and the relationships required by the
four planned V1 tools:

- every order belongs to an existing customer;
- V0 permits at most one shipment and one payment per order;
- shipments and payments reference existing orders;
- payment currency matches its order and cannot exceed the order total;
- refunds name both an existing payment and that payment's order;
- non-failed refunds cannot collectively exceed the payment amount; and
- a support ticket's customer must own its referenced order.

These constraints are intentionally not a generic commerce or accounting system.
Later tool behavior may add valid state transitions, but it must not silently
broaden the frozen Fixture v0 input contract.

## Determinism and canonical digest

All timestamps are explicit, timezone-bearing values no later than the fixture
`reference_time`. Loading and initialization use no wall clock, randomness,
generated IDs, or implicit business defaults.

Canonical bytes use RFC 8785/JCS after validation. Object key order, JSON
whitespace, and the order of entity collections do not affect the digest;
entities are sorted by their collection-specific stable ID. Field values and
array order outside those explicitly set-like entity collections are not
silently normalized. Duplicate JSON object keys are rejected while parsing. The
digest is lowercase `sha256:<hex>` over those canonical bytes.

## Immutable revision to isolated Run state

The relationship is:

```text
Fixture revision --canonical copy--> Run-local synthetic company state
```

`fixture_revisions` stores the validated canonical JSON value and digest and is
protected from UPDATE/DELETE by the persistence immutability trigger. A newly
created Run resolves the Fixture reference embedded in its immutable Scenario
and freezes the exact Fixture ID, revision, and digest through a composite
foreign key.

Initialization locks the Run and is allowed only while it is `queued` with
`attempt = 0`. It atomically creates the Run state marker and relational rows
for all entities inside the caller-owned transaction. Concurrent initialization
serializes on the Run row. A repeated call returns the existing state unchanged;
it does not reset later mutations. If initialization rolls back, no partial
state remains.

Pre-Issue-7 Runs remain explicitly unbound because their referenced Fixture
content cannot be reconstructed honestly during migration. They cannot be
initialized. No lease or lifecycle state is fabricated during upgrade.

All mutable company tables include `run_id` in their primary and foreign keys.
Consequently, Run A and Run B receive equivalent initial values but distinct
rows, and cross-Run business references fail database integrity checks.

## Persistence representation

The immutable Fixture contract remains JSONB because it is loaded and verified
as one versioned document. Mutable authoritative business state is relational:

- `run_company_state` records initialization and the frozen Fixture binding;
- `company_customers`;
- `company_orders`;
- `company_shipments`;
- `company_payments`;
- `company_refunds`; and
- `company_support_tickets`.

This representation supports later authoritative state evidence and effect
verification without embedding mutable state in an opaque blob. It does not
itself emit `state.evidence_recorded`; that remains a later execution/tool
responsibility.

## Golden fixture and deferred behavior

The golden fixture is
`benchmarks/shipment-refund/fixtures/failed-shipment.v0.json`. It contains
captured payment `PAY-1007`, failed shipment `SHP-1007`, open ticket `TKT-204`,
and no initial refund for order `ORD-1007`. The Scenario example now references
its real canonical digest.

Deferred work includes agent-facing tools and schemas, business mutation APIs,
effect/idempotency ledgers, fault injection, retry behavior, evaluators,
campaigns, telemetry, and UI.
