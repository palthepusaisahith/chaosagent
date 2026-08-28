# Fault Matcher v0

Issue #13 implements the provider-neutral, side-effect-free compiler and matcher
for declarations already frozen by Scenario v0. The implementation is in
`packages/faults`. It deliberately has no SQLAlchemy, Gateway, runtime,
provider, clock, or persistence dependency.

This is the boundary approved by the architecture backlog. Transport and data
fault application belongs to Issue #14. Ambiguous post-commit execution belongs
to Issue #15. Consequently Issue #13 does **not** invoke tools, change tool
results, sleep, mutate synthetic state, reserve idempotency effects, or emit
`fault.*` evidence. The shipment/refund and shipping-503 files are structural
Scenario examples, not execution traces.

## Compilation

`compile_fault_plan_v0` accepts an already validated immutable `Scenario`. It
revalidates the Scenario canonical bytes and digest, optionally restricts the
plan to an explicit set of selected fault IDs, rejects unknown or duplicate
selections, and returns immutable rules sorted by fault ID. Sorting is valid
because Scenario v0 defines `faults` as a set keyed by ID.

Compiled plans and rules are output-only values: their public constructors are
disabled, compiler-created instances are tracked by identity in a weak registry,
and each value carries an integrity digest. Matching rechecks the registered
identity and integrity of both the plan and every rule. The single-rule matcher
is internal so every public match is bound through the originating Scenario
digest. Copied or reflectively altered compiled values fail closed before
predicates are evaluated.

Compilation gives runtime meaning to the frozen kind/phase combinations:

| Phase          | Allowed kinds                                                                                              | Boundary meaning for later application                                                                                                |
| -------------- | ---------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `before_tool`  | `delay`, `timeout`, `http_error`, `auth_error`                                                             | After Gateway authorization, input validation, policy, and approval; before a handler or business effect.                             |
| `after_tool`   | `delay`, `timeout`, `malformed_response`, `stale_field`, `indirect_prompt_injection`, `duplicate_response` | After a handler produced a result; later application must not corrupt authoritative business state.                                   |
| `after_commit` | `ambiguous_post_commit_timeout`                                                                            | After a mutation/effect is durably established but before its acknowledgement is delivered. Issue #15 owns this transaction boundary. |

Scenario v0 intentionally accepts structurally valid combinations without
assigning runtime meaning. The Issue #13 compiler now rejects incompatible
combinations early. It does not implement the behavior of any kind.

Parameters remain immutable copies of the schema-validated Scenario values.
Scenario v0 remains authoritative for their closed shapes, integer/range
constraints, and unknown-property rejection. Revalidation prevents Boolean
values from masquerading as integers and rejects values outside the RFC 8785
JSON domain before compilation.

## Match context and predicates

The caller supplies one `FaultMatchContext` for a physical attempt. It contains
the frozen Scenario digest, Run identity and seed, tool and phase, stable
logical call ID, physical attempt ID and number, the per-tool logical call
ordinal, canonical arguments and digest, and counts of prior **applied**
occurrences. The matcher has no authority to load or infer any of those values.

At match entry, the engine reads each caller-owned mapping exactly once,
recursively materializes ordinary JSON, validates its RFC 8785 domain, and
freezes that snapshot. Argument digest verification, predicates, activation-cap
lookup, and probability selection all use only this same snapshot. Stateful or
concurrently changing mappings cannot supply different validation and matching
views. Python tuples are rejected rather than silently normalized into JSON
arrays.

Predicates are evaluated in this deterministic order:

1. exact tool ID;
2. exact phase;
3. optional logical call ordinal;
4. optional top-level `argument_equals` subset using RFC 8785 JSON equality;
5. `max_occurrences` against prior applied occurrences; and
6. deterministic probability selection.

An omitted call ordinal matches every logical occurrence. Retries retain the
same logical ordinal but use their physical `attempt_number`. Argument matching
is exact: no coercion, case folding, Unicode normalization, or Boolean/integer
aliasing occurs. The supplied arguments digest is recomputed and must agree.

The plan returns one explicit decision per selected rule. It does not choose a
winner when more than one rule matches; Campaign selection and later application
conflict policy are not part of Issue #13. A non-match includes a stable reason,
but Issue #14 decides whether and how conservative `fault.not_matched` evidence
is emitted.

## Deterministic probability and activation identity

The algorithm identifier is `chaosagent.fault-matcher/sha256-v0`.

The matcher JCS-serializes an object containing the algorithm ID, Run seed and
ID, Scenario digest, fault ID, tool, phase, logical call ID, physical attempt
number, logical call ordinal, and arguments digest. SHA-256 of those bytes is
scaled into the integer interval `0..999999`; the rule is selected when the
bucket is less than `probability_ppm`. There is no global PRNG state and no
wall-clock input.

The selection input is semantic and recorded by its constituent frozen values.
The physical attempt ID is intentionally excluded from probability selection:
renaming the evidence identity cannot change whether otherwise identical work
matches. It is included when deriving the deterministic `activation_id`, so the
event identity remains bound to the exact physical attempt. Changing the attempt
number, seed, logical call, arguments, rule, Run, or Scenario may change the
probability decision.

`max_occurrences` counts prior applied activations, not merely matches. This
keeps `matched`, `applied`, and `observed` separate. A later injector must
supply the count from authoritative, fenced state or immutable evidence; Issue
#13 does not persist or trust itself with that state.

## Safety and deferred execution

The matcher is pure and cannot bypass Scenario authorization, Policy, approval,
lease fencing, idempotency, or business invariants. Those checks remain in the
Gateway. Later integration must run matching only after authorization and must
recheck current lease ownership before effects or evidence.

No `fault.matched`, `fault.applied`, `fault.observed`, or `fault.not_matched`
events are produced yet. No claim is made about rollback, crash recovery,
post-commit ambiguity, replay, or observation until Issues #14 and #15 add the
transactional application boundary and evidence lifecycle. There is no database
migration in Issue #13 because a pure decision needs no durable model.

Also deferred are Campaign seed derivation and selected-rule manifests,
retry/backoff, evaluators, reports, workers, provider/network chaos, SSE,
OpenTelemetry, and UI behavior.
