# Read-only Tool Gateway v0

## Scope

Issue #8 introduces the first agent-facing mediation boundary for the
deterministic synthetic company. It is an in-process typed gateway consistent
with the modular-monolith architecture; it is not an HTTP server and does not
contact real order or shipping systems.

The fixed catalog contains exactly:

- `orders.get`, contract `chaosagent.tool/orders.get/v0`
- `shipping.get_status`, contract `chaosagent.tool/shipping.get_status/v0`

Issue #9 subsequently added refunds and support mutations through a separate
narrow capability; it does not weaken this read-only handler boundary. See
`MUTATION_TOOLS_EFFECT_LEDGER_V0.md`. Policy execution, approvals, faults,
models, and the agent loop remain deferred.

## Contracts and registry

Each catalog entry has a stable tool ID, exact contract version, description,
`read` capability, `read_only: true`, a strict Draft 2020-12 input schema, a
strict output schema, and one handler. The four schemas are package resources.
Both input schemas reject unknown properties. Catalog lookup uses the exact
`(tool_id, contract_version)` pair and fails closed; a future version must be a
new entry and cannot reinterpret v0.

Scenario v0 does not carry versions beside its allowed tool IDs. Its frozen
compatibility mapping is therefore part of the contract:

- `chaosagent.scenario/v0` + `orders.get` resolves only to
  `chaosagent.tool/orders.get/v0`.
- `chaosagent.scenario/v0` + `shipping.get_status` resolves only to
  `chaosagent.tool/shipping.get_status/v0`.

Changing those mappings would change the meaning of stored Scenario v0 and is
forbidden after the V0 freeze. Merely registering a future Tool v1 cannot
broaden an old Scenario. Future Scenario versions may define new frozen mappings
or explicit version references.

The architecture defines `shipping.get_status(order_id)`, so the lookup argument
is `order_id`. Its result identifies the associated V0 shipment, including
`SHP-1007` in the flagship fixture. Outputs contain only committed Fixture v0
fields and UTC fixture timestamps.

## Authorization and fencing

The gateway validates the catalog identity and input before database work. It
then locks the Run row and proves the current lease owner, opaque lease token,
monotonic attempt generation, and unexpired database-time lease. The lock
remains held for the caller-owned transaction, serializing the call against
heartbeat, lifecycle transition, expiry recovery, and reclaim.

The read operation does not accept a lifecycle version. Issue #6 lifecycle
version is the CAS guard for lifecycle mutations; this call performs no such
mutation. Current owner, token, attempt generation, database-time expiry,
`running` status, and the held Run lock fence and linearize the bounded read.
Holding that lock across a local relational read is acceptable for these V0
tools. It must be reconsidered before any slow or external handler is added.

The Run must be `running`, its frozen Scenario reference and digest must
resolve, the tool must appear in `Scenario.agent.allowed_tools`, and Run-local
company state must be initialized. Denied or malformed calls never reach a
handler and do not claim that a tool attempt occurred. The gateway never trusts
a caller-supplied Run ID separate from the lease binding.

Logical call ID and physical attempt ID are runtime-required and must match the
Event v0 identifier profile. Optional step and causation IDs use the same
profile. Issue #9 permits any positive JSON-safe physical attempt number so a
caller can represent replay without adding retry policy. Malformed dynamic
fields return `invalid_request` before Event construction. Producer component
and optional instance identity are validated when the gateway is constructed, so
invalid trusted configuration fails immediately.

## Results and errors

A successful result contains a schema-validated immutable output mapping. V0
treats a missing business entity as an accepted tool attempt with a failed
`entity_not_found` result—not as an empty success. Pre-execution failures use
stable provider-neutral codes: `invalid_request`, `unsupported_tool`,
`tool_not_allowed`, `run_not_ready`, and `stale_lease`. Unexpected handler or
persistence failures are exposed only as `infrastructure_error`; SQL details,
exceptions, and stack traces are not agent-visible.

## Evidence and transactions

Every accepted attempt appends one `tool.requested` and one `tool.result` Event
v0. The result carries the same logical call ID, physical attempt ID/number, and
tool ID, links `request_event_id`, and uses the request as its causation event.
Arguments and responses are represented in evidence by real RFC 8785 SHA-256
digests through the committed evidence contract; raw values are returned to the
caller but are not embedded as uncontrolled event payload state.

Lifecycle and tool events call the same repository allocate-and-append
primitive. It locks the Run row, chooses `MAX(sequence) + 1`, validates that the
factory preserved the Run and sequence, and appends before returning. A caller
cannot obtain and abandon a sequence reservation. Independent PostgreSQL
sessions therefore cannot allocate colliding sequences for one Run under
`READ COMMITTED`. Request and result receive distinct sequences. This is a
bounded V0 allocator, not a queue or distributed event service.

The gateway uses a savepoint but never commits. Request, read, and result
evidence remain in the caller's transaction. A result-event persistence failure
rolls back the request as well. If the caller rolls back, neither event is
durable; callers must commit before presenting a result as durably recorded. A
handler database failure is isolated by an inner savepoint when possible so a
failed result can be recorded. If evidence itself cannot be recorded, the
gateway returns a generic infrastructure failure with no misleading partial
pair.

## Read-only and authoritative-state boundary

Handlers receive a Run-bound `ReadOnlyCompanyState` capability with only
`get_order` and `get_shipment_for_order`. They do not receive the SQLAlchemy
Session or `PersistenceRepository`, cannot choose another Run, and have no
event, lifecycle, transaction, generic SQL, or business-mutation API. The
adapter returns frozen value records, never ORM rows. Successful calls leave
every run-local company row unchanged. `tool.result` is an observation for the
agent, not authoritative `state.evidence_recorded` business-effect evidence.
Read-only calls create no business effect.

The structural flagship examples are in
`benchmarks/shipment-refund/tools/v0/read-only-calls.json`. They are derived
from Fixture revision 1 and deliberately do not pretend an execution occurred.

## Deferred

Issue #9 now owns the mutation tools and local effect ledger described in the
companion document. Policy/approval, fault mediation, external-effect
integration, retry policy, network protocols/adapters, the runtime, evaluators,
telemetry, and UI remain deferred. No persistent table was required by Issue #8.
