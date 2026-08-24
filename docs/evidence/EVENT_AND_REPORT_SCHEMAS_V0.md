# Run Event and Run Report schemas v0

Issue #4 defines two immutable data contracts. It does not define the systems
that emit, store, evaluate, stream, or display them.

## Contract boundary

Run Events are authoritative product evidence: lossless domain facts selected
for durable recording and later evaluation. Operational logs, metrics, traces,
and OpenTelemetry signals are diagnostic telemetry. They may correlate with an
event through trace fields, but may be sampled or dropped and are not an
authoritative substitute for the event stream. Large or sensitive bodies belong
in separately governed artifacts; events carry structural metadata, digests, and
artifact references.

A Run Report is a final structured result over a bounded event stream. It
records classifications, gate outputs, diagnostics, totals, and evidence
references. It does not decide those values. Evaluator algorithms, gate
calculation, artifact resolution, and verification of referenced external
revisions are deferred.

The Scenario/Campaign boundary from Scenario v0 remains unchanged. The report
records the immutable Scenario and Agent Configuration revisions used by one
run. It contains no Campaign sampling, trial orchestration, pass@k, pass^k, or
aggregate statistics.

## Run Event v0

The schema version is `chaosagent.run-event/v0`. Every event has an opaque
`event_id`, `run_id`, positive `sequence`, `occurred_at`, `recorded_at`, typed
`event_type`/`payload`, producer identity, `correlation_id`, optional causation
and trace context, and a `payload_digest`.

`occurred_at` is when the domain occurrence was observed by its producer.
`recorded_at` is when the event envelope was recorded. Both use UTC RFC 3339
timestamps. Neither timestamp orders events. `sequence` is the authoritative
order within one run. A stream must be strictly increasing in supplied order;
gaps are allowed. A complete run stream begins at sequence 1, while a loaded
subset may begin later. `(run_id, sequence)` uniqueness is an event-store
responsibility, while `validate_run_event_stream_v0` detects duplicates and
ordering violations in an in-memory stream.

For tool evidence, `logical_call_id` identifies the agent's logical action and
`attempt_id` identifies one physical invocation. `attempt_number` is descriptive
evidence only. A tool result explicitly names its request event, and complete
stream validation requires request/result identity fields to agree. V0 defines
no retry policy, attempt limit, or rule for when another attempt occurs.

Payload types are:

- `run.lifecycle`
- `agent.step`
- `tool.requested` and `tool.result`
- `fault.not_matched`, `fault.matched`, `fault.applied`, and `fault.observed`
- `state.evidence_recorded`
- `policy.decision`
- `approval.requested` and `approval.resolved`
- `evaluation.started` and `evaluation.result_recorded`
- `run.error`

The lifecycle payload names architecture-approved states but does not validate
state transitions. Fault payloads preserve observable lifecycle evidence but do
not define matching or injection behavior. `state.evidence_recorded` is an
assertion emitted by an authoritative synthetic-state producer that a named
state snapshot or business effect exists; it does not define storage, business
logic, or evaluator interpretation. Evaluation events record only lifecycle,
evaluator revision, outcome, and evidence boundary. They do not store or compute
evaluation results.

## Run Report v0

The schema version is `chaosagent.run-report/v0`. A report contains report/run
identity, generation time, Scenario and Agent Configuration revision references,
terminal run status, final classification, an evidence boundary,
fault-observation summary, critical gate results, diagnostic metrics,
usage/latency/cost totals, and provenance.

Revision references reuse Scenario v0's `{id, revision, digest}` syntax. The
contract checks their shape and internal provenance links only; catalog
resolution and digest verification remain external responsibilities.

`run_status` describes execution termination. `classification` is an independent
evaluation dimension: `pass`, `fail`, `invalid`, or `not_evaluated`.
Infrastructure failure is represented only by `run_status: infra_error` and
optional run-error evidence. A completed execution may still be evaluator
invalid. A passing evaluation requires completed execution and cannot contain a
failed/error gate; a failing evaluation must contain a failed gate. A gate error
requires an invalid classification. These are contradiction checks, not
evaluator algorithms or evaluator-completeness rules. In particular, schema
validity does not require a passing report to contain any minimum gate suite.

When evaluation did not run, classification is `not_evaluated`, and gate and
evaluator-revision collections are empty.

The report's complete `evidence_boundary` may extend beyond the evaluator input
because evaluation lifecycle and finalization events occur afterward.
`provenance.evaluated_through_sequence` records the frozen evaluator input
boundary and is required for pass/fail classifications; it must lie within the
complete report boundary.

Totals contain only known observations. Any usage, token, latency, or cost field
may be omitted when unavailable; omission means unknown. A present numeric zero
means measured or otherwise authoritatively known zero. An empty `totals` object
means no totals were available. This avoids provider-specific assumptions and
never substitutes zero for missing provider usage.

Evidence references are discriminated as one event, an inclusive event range, or
an artifact. A range includes the existing events whose sequence falls between
its endpoints; it does not imply that every integer sequence exists. Persisted
sequence numbers must never be renumbered. Event references are scoped by the
report's `run_id`. Artifact IDs are opaque external-store identifiers and are
also interpreted in the report/run context unless a future artifact contract
defines a wider scope. Artifact existence, content, redaction, and digest
verification are deferred. Each gate's evaluator revision must appear in report
provenance. `validate_run_report_with_events_v0` additionally verifies run
binding, boundary/count, event-ID/sequence pairs, and non-empty ranges against a
complete supplied stream. V0 does not implement storage or artifact resolution.

## Immutability and canonical representation

JSON text loaders reject duplicate object keys, malformed JSON, and non-finite
numbers. Inputs are defensively copied before validation. Validated wrapper
objects retain only RFC 8785 (JCS) canonical bytes, and `to_dict()` returns a
fresh copy, so callers cannot mutate the loaded contract accidentally.

The event `payload_digest` is a deterministic checksum: `sha256:` plus lowercase
SHA-256 over the typed payload's RFC 8785 bytes. It covers only the payload, not
event/run identity, sequence, timestamps, producer, correlation, causation, or
trace context. It is neither a signature nor standalone tamper evidence because
an actor able to change the payload may also recompute the checksum. Issue #4
does not introduce an event or report identity digest; later persistence/export
checksums must define their own versioned semantics.

JCS sorts object keys and canonicalizes supported JSON number/string spellings.
It does not reorder arrays globally. Event streams are ordered. The following
contract-owned collections are semantically set-like: related event IDs,
critical gates keyed by gate ID, diagnostic metrics keyed by metric ID,
evaluator revisions, fault IDs, and evidence-reference collections. Set-like
`related_event_ids` inside an event payload are sorted before payload checksum
and canonical event serialization. Report set-like arrays preserve supplied
representation order because Report v0 has no content digest; consumers must not
infer meaning from that order. Duplicate set members or semantic keys are
rejected where expressible.

## Validation responsibilities

Normative Event/Report V0 validity consists of both Draft 2020-12 structural
validation and the ChaosAgent V0 semantic validation profile implemented by the
version-specific loader. JSON Schema rejects malformed shapes, unknown
properties, unsupported discriminators, and local enum/range/conditional
violations. The semantic profile enables timestamp format checking, recomputes
payload checksums, normalizes declared payload sets, and enforces relationships
that JSON Schema cannot express clearly: stream ordering/identity and tool
attempt coherence, report boundary arithmetic, referenced sequence bounds,
duplicate semantic IDs, and optional report/stream evidence binding. JSON Schema
alone does not verify checksum equality or all format assertions.

Neither layer resolves revision or artifact references, compares events with a
Scenario, proves causation, validates lifecycle transitions, or evaluates gate
truth.

## Frozen-version policy

The two schema identifiers and their behavior are frozen once released. Generic
loaders dispatch only on the exact `schema_version` and fail closed for unknown
versions. A future v1 receives new schema resources and dedicated validation; it
must not change how stored v0 documents are interpreted. Additive changes to v0
are breaking because objects are strict.

## Deferred work

Persistence, append transactions, artifact storage/redaction, execution state
machines, retry semantics, model/tool calls, fault matching/execution, evaluator
and report-building algorithms, Campaign aggregation, SSE, OpenTelemetry,
exports, UI, and external revision catalogs are intentionally outside Issue #4.
