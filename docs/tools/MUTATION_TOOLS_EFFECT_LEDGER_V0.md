# Mutation tools and effect ledger v0

## Scope and contracts

Issue #9 adds only the two approved synthetic-company mutations:

- `payments.refund` → `chaosagent.tool/payments.refund/v0`
- `support.update_ticket` → `chaosagent.tool/support.update_ticket/v0`

Their input and output contracts are strict JSON Schema Draft 2020-12 package
resources. Inputs reject unknown fields and U+0000. Money is an integer number
of minor units. Because Python JSON Schema considers mathematically integral
floats to be JSON Schema integers, the runtime persistence profile additionally
requires `type(amount_minor) is int`; booleans, `5000.0`, and exponent spellings
parsed as floats are rejected before evidence or database access. Scenario v0's
frozen tool mapping now includes these exact versions. Registering a future Tool
v1 cannot change the meaning of a stored Scenario v0.

`payments.refund` requires `order_id`, `payment_id`, positive `amount_minor`, a
bounded `reason`, and `idempotency_key`. It verifies the Run-local relationship,
requires a captured or partially refunded payment, derives currency from that
payment, and prevents cumulative successful refunds from exceeding its captured
amount. A successful refund is immediately `succeeded`; V0 does not model an
external processor or pending settlement.

`support.update_ticket` requires `ticket_id`, one Fixture v0 status, a nonempty
replacement `note`, and `idempotency_key`. Fixture v0 has one current note and
no note-history contract, so V0 replaces status/note atomically and records the
effect in the immutable ledger rather than inventing a helpdesk history model.

## Capability and authorization boundary

Read handlers retain Issue #8's `ReadOnlyCompanyState`. Mutation handlers
receive only the already validated argument mapping and return a frozen pure
mutation intent. They receive no repository, Session, SQL, lifecycle,
transaction, or event capability. The gateway alone translates a matching intent
into a repository operation and independently verifies the resulting
ledger/business projection before successful evidence.

Handlers registered in this in-process catalog remain trusted application code,
not a sandbox or security boundary: arbitrary Python loaded into the process can
import modules or introspect process state. The pure-intent API prevents
accidental capability retention; deployment/plugin isolation is deferred.

The gateway validates the exact tool/version and input, locks the Run, proves
owner/token/attempt and database-time lease validity, requires `running`,
resolves the frozen Scenario digest and allowlist, and requires initialized
Run-local state before a mutation handler runs. It rechecks that same lease
after mutation/output validation. A stale identity fails and rolls back the
effect/evidence. The transaction can commit after wall-clock expiry if its final
check passed just before expiry; recovery cannot acquire the Run row until that
transaction releases it. This is database-local serialization, not fencing of
external side effects.

## Idempotency identity and fingerprint

The durable idempotency identity is:

`(run_id, tool_id, contract_version, SHA-256-JCS(idempotency_key))`.

The request fingerprint is SHA-256 over RFC 8785/JCS serialization of the JSON
object `{tool_id, contract_version, arguments}`. It therefore covers every
strict V0 argument—including the key itself—plus exact tool identity/version.
Object key order and insignificant JSON formatting do not affect it; values and
array order remain meaningful. The raw idempotency key is neither stored in the
ledger nor emitted in evidence.

Same identity plus the same fingerprint returns the established result with
`application: already_applied` and the same effect/subject identity. Same
identity plus a different fingerprint returns `idempotency_conflict`. A
different key is a new potential effect and must pass current business rules.

## Ledger, locking, and effect identity

`public.company_effects` is an immutable relational ledger. Its composite
primary key is the full idempotency identity; `(run_id, effect_id)` is also
unique. Each row stores the request digest, deterministic effect ID, fact and
subject identities, applied state, established result document, database
timestamp, first logical/physical attempt provenance, and lease attempt. The
existing database immutability trigger rejects UPDATE and DELETE through normal
DML. As elsewhere, a database owner can bypass triggers; deployment roles are
outside this issue.

Effect IDs are deterministic SHA-256 identifiers over the full idempotency
identity, not random acknowledgements. Refund rows reference their ledger
effect; a support ticket records its last effect. Foreign keys prevent those
business rows from naming a nonexistent effect.

Mutation-created refunds are marked `origin = mutation` and must have a non-null
effect reference; fixture refunds are marked `origin = fixture` and must not.
The database enforces that direction plus the effect foreign key. The reverse
claim—every ledger row has the correct business projection and output—is checked
by the trusted repository after independently reloading both within the same
transaction. PostgreSQL constraints do not encode the full tool contract. A
partial `(run_id, payment_id)` index for succeeded refunds supports the locked
cumulative-refund query.

The gateway's required Run lock serializes authorization/fencing and event
sequence allocation for a Run. Public repository mutation APIs also acquire the
Run lock in their own savepoint, so direct callers receive the same
serialization and recoverable integrity failures; those APIs do not perform
worker lease authorization. Repository mutations additionally lock the specific
payment or ticket row. The payment lock protects the cumulative-refund read and
insert from write skew; two different keys cannot over-refund one payment. This
is not database-global serialization, and different Runs remain independent. The
protocol assumes PostgreSQL `READ COMMITTED` and short caller-owned
transactions.

## Atomic evidence and replay

For a newly applied effect, one caller-owned transaction/savepoint contains:

1. `tool.requested` with arguments and idempotency-key digests;
2. immutable ledger insertion;
3. refund insertion/payment status update or ticket state update;
4. validated successful `tool.result`; and
5. `state.evidence_recorded` with `business_effect`, the real ledger effect ID,
   `refund.created` or `support_ticket.updated`, the real subject, and links to
   request/result events.

Any failure in business mutation, output validation, result evidence, or
authoritative effect evidence rolls back the new effect. The gateway never
commits; the caller must commit before treating the result as durable. A caller
rollback removes the ledger, business change, and all attempt evidence.

Business-rule, missing-entity, and idempotency-conflict attempts preserve a
coherent requested/failed-result pair but no business effect. Infrastructure
failure never appears as successful business state. Stable codes are
`invalid_request`, `unsupported_tool`, `tool_not_allowed`, `run_not_ready`,
`stale_lease`, `entity_not_found`, `business_rule_violation`,
`idempotency_conflict`, and `infrastructure_error`; SQL details are not exposed.

An identical replay emits a fresh request/result pair because it is a new
physical observation, but no second `state.evidence_recorded`: the existing
ledger row is already the authoritative fact, and the replay result explicitly
says `already_applied`. Historical replay is non-reverting: after effects A then
B on the same ticket/payment, replaying A returns A's established result without
reapplying A, changing current state, or emitting new state evidence. Stored
results are fully shape/type-validated on replay; corruption fails as a
sanitized infrastructure error. One invocation is one physical attempt; positive
Event v0 attempt numbers are accepted, but Issue #9 defines no retry policy.

## Guarantee and limitations

Within one PostgreSQL database, committed operations under the same identity
apply exactly one synthetic business effect. This supports the later ambiguous
acknowledgement case: if a commit succeeded but its acknowledgement was lost,
the same request/key resolves the existing refund.

This is not universal exactly-once delivery. A committed effect cannot be
uncommitted because a caller later loses its lease. Issue #9 does not inject an
ambiguous timeout, schedule retries, make external payment calls, run agents,
evaluate outcomes, enforce approvals/policy beyond existing structural checks,
or add faults, campaigns, telemetry, SSE, or UI. Real external APIs require a
distributed idempotency agreement with the provider and reconciliation; the
local PostgreSQL guarantee alone would be insufficient.

Structural examples are in
`benchmarks/shipment-refund/tools/v0/mutation-calls.json`. They use Fixture v0
identities but are not claimed execution traces.
