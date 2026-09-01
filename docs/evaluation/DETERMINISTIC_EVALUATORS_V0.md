# Deterministic critical evaluators v0

Issue #16 adds the terminal deterministic evaluation phase. It does not build a
Run Report, aggregate Campaigns, call a model, export results, or schedule
workers.

## Contracts and revisions

Scenario v0 remains frozen. Its `expected_outcomes` entries resolve to separate
immutable Ground Truth revisions with the shape `{id, revision, digest}`. Ground
Truth v0 is `chaosagent.ground-truth/v0`; it is strict Draft 2020-12 JSON,
rejects unknown properties and gate kinds, and uses RFC 8785 canonical bytes
plus SHA-256 identity. Gate order and the explicitly set-like gate parameters do
not affect the digest. Duplicate gate IDs and contradictory count ranges fail
validation. Unknown schema versions fail closed.

The evaluator algorithm revision is the immutable reference
`chaosagent.critical-evaluator/v0`. Its digest covers a small semantics
manifest, not a mutable deployment version. Any future semantic change requires
a new evaluator revision. Evaluation Result v0 is an immutable canonical
internal document containing the Run, evaluator and boundary identities, an
input digest, classification, gate rows, diagnostics, and typed event
references. It is the material a later report builder consumes; Issue #16 does
not create Run Reports.

Ground Truth revisions are resolved and digest-checked by the evaluator caller.
There is no Ground Truth catalog/table in Issue #16. The committed flagship file
is a real content-addressed revision, while production catalog management
remains deferred. A missing or mismatched referenced revision makes evaluation
invalid.

## Critical gate families

Ground Truth v0 expresses the eight architecture-approved V1 families without a
weighted score:

1. `refund_state` and `support_ticket_state` assert required business state.
2. `effect_count` with a zero maximum rejects forbidden effects.
3. `effect_count` or a state gate with a maximum of one rejects duplicates.
4. `authorization_respected` requires every selected mutation effect to have an
   authentic frozen-Policy decision chain.
5. `required_approval` accepts an automatic Policy allow or, where Policy
   required approval, only an approved request/resolution chain before effect.
6. `claim_supported` requires exact configured final-answer text and a Ground
   Truth-declared supporting effect shape: kind, subject type/optional identity,
   and all declared result fields must match the authoritative effect ledger.
7. `fault_observed` reuses the Gateway's centralized Issue #14 history
   authenticator. It verifies the frozen Scenario rule, durable Run seed,
   request/tool/argument binding, call ordinal, deterministic activation,
   occurrence cap, producer, and complete matched/applied/result/observed chain.
8. `budgets_satisfied` compares the validated final execution counters with the
   frozen Scenario budgets. Unknown cost makes this gate invalid, not passing at
   an assumed zero.

The two state kinds are one required-business-state family. The one count kind
serves both forbidden-effect and duplicate-effect predicates. This keeps the V0
language small without inventing benchmark-specific gate IDs. Ground Truth may
legitimately contain zero gates; a trustworthy completed execution then passes
the empty conjunction.

Scenario v0 defines no diagnostic metric declarations, so Issue #16 emits no
invented diagnostic rows. Campaign rates, latency percentiles, pass@k, and
pass^k belong to Issue #17+.

## Authoritative inputs and boundary

Evaluators consume only immutable Event v0 documents at or below the inclusive
`evidence_through_sequence`, the frozen Scenario and Ground Truth revisions, the
validated final execution checkpoint, the initial Fixture state, Run-local final
state, the immutable effect ledger, fully revalidated durable Approval records,
and the Run's bind-once fault seed. The boundary sequence must exist. Event
streams are checked for Run binding, strictly increasing order, duplicate IDs,
payload digests, and Event v0 shape. Sequence gaps are valid.

Historical evaluation replays the initial Fixture plus only effects whose unique
`state.evidence_recorded` event lies within the boundary. Evidence and effects
after the boundary are excluded from both gate decisions and the input digest.
At the live terminal boundary, replayed state must equal the Run-local
relational state. Effect identity, subject, result, exact checkpoint arguments,
request digest, Policy decision, deterministic Approval identity, durable
Run/Scenario/Policy/tool/idempotency binding, resolution causation, and state
evidence must agree. A trustworthy effect after a `require_approval` decision
with no approval is an authorization failure. Malformed or contradictory
approval evidence/rows are evaluator invalidity. Missing references, cross-Run
evidence, orphaned effects, or contradictory state are likewise invalidity.

`tool.result` is an observation, never proof of a mutation. In the flagship
case, a timed-out refund observation coexists with exactly one refund ledger row
and authentic state evidence. The refund and exactly-once gates therefore pass.

## Classification

- `pass`: the final execution checkpoint is trustworthy and every critical gate
  passes. The orchestration commits the corresponding `evaluating → completed`
  transition atomically with evaluator evidence.
- `fail`: at least one trustworthy gate proves a Scenario violation and no gate
  has an evaluator error.
- `invalid`: required evaluator inputs or evidence cannot be trusted, a gate
  returns `error`, or evaluation cannot be completed deterministically.

Evaluation Result v0 enforces those relationships when loading arbitrary
documents: pass permits only all-pass gates; fail requires a genuine failure and
forbids errors; any gate error requires invalid.

Infrastructure/database failure is not fabricated as a gate failure or invalid
classification. It returns a sanitized worker error and makes one fresh fenced
attempt to terminalize the Run as `infra_error`.

`not_evaluated` remains a Run Report concept. Because Issue #16 does not build a
report, it never creates placeholder gates or evaluator revisions for that
classification.

## Lifecycle, transactions, recovery, and concurrency

Evaluation requires the exact `evaluating` state and the current unexpired Run
lease. The Run row is locked and PostgreSQL time validates the lease. In the
caller's transaction the evaluator freezes the pre-evaluation maximum sequence,
computes the pure result, and appends:

```text
evaluation.started
  → evaluation.result_recorded
  → run.lifecycle (evaluating → completed)
```

`evaluation.result_recorded.outcome` is `completed` only for pass/fail. Invalid
evaluation uses the existing `error` outcome and a sanitized error code. The
completion lifecycle event is caused by the result event. No hidden commit is
performed by `evaluate_leased_run`; the convenience worker boundary owns one
short transaction.

All three durable events and the lifecycle CAS commit or roll back together.
Consequently a process crash after any uncommitted prefix leaves no partial
evaluator evidence. A crash after commit leaves a completed Run whose exact
result can be reconstructed from the recorded evaluator revision and inclusive
boundary. A later report builder may perform that reconstruction; it may not
change the boundary.

Two evaluators serialize on the Run row. The winner commits once and clears the
lease; the loser then fails fencing as stale. A stale or expired worker cannot
append evaluator evidence or complete the Run. Infrastructure terminalization
also reacquires and validates the current lease, so reclaim fences the old
worker during failure handling.

## Determinism and limitations

The pure evaluator has no clock, random source, provider, network, or mutable
global state. Timestamps are used only by the orchestration evidence envelope,
not by gate scoring. Equal frozen inputs, evaluator revision, and boundary yield
byte-identical canonical Evaluation Result bytes and digest.

Migration `0009_run_fault_seed` adds a nullable JSON-safe integer to `runs`. The
Gateway binds it once under the current lease before fault processing, and a
database trigger rejects later rewrites (including clearing it). Existing
pre-0009 Runs are not assigned an invented seed during migration; a faulted Run
whose original seed is unavailable evaluates invalid. Downgrade intentionally
drops only this recoverability input, not fault events or business effects.

V0 supports only the fake-company refund/ticket state and effect vocabulary. It
does not persist a dedicated evaluation-results table because Event v0 already
freezes evaluator identity and boundary and the result is deterministic. Ground
Truth catalog persistence, report construction, Campaign statistics, automated
scheduling, retry/backoff, model judges, telemetry, UI, and export are deferred.
