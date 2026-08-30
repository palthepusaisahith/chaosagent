# Tool Gateway fault application v0

Issue #13 owns immutable Scenario compilation and pure deterministic matching.
Issue #14 owns the narrow `FaultEngine` application service and its Tool Gateway
integration. A caller supplies an explicitly compiled plan, including its
selected fault IDs, plus a nonnegative JSON-safe run seed. The Gateway does not
activate all Scenario declarations implicitly and does not invent Campaign
selection or seeds.

## Gateway order and scope

The v0 order is lease/fencing, Scenario tool authorization, input validation,
Policy evaluation, approval resolution where required, `before_tool` matching
and application, handler execution, authoritative handler-output validation,
`after_tool` matching and application for reads, then result and
fault-observation evidence. Faults cannot grant tool access, turn a Policy
denial into an allow, satisfy an approval, weaken input/output validation, or
bypass mutation idempotency.

Issue #14 applies these committed combinations:

| Phase              | Kinds                                 | Observation                                                                                                                                                      |
| ------------------ | ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `before_tool`      | `delay`                               | Synchronously blocks the current Gateway path using the injected sleeper, then invokes the handler.                                                              |
| `before_tool`      | `timeout`, `http_error`, `auth_error` | Returns a sanitized provider-neutral failure without invoking the handler.                                                                                       |
| `after_tool` reads | `delay`                               | Delays delivery of a validated read result.                                                                                                                      |
| `after_tool` reads | `timeout`                             | The read ran, but its result is withheld and a timeout failure is observed.                                                                                      |
| `after_tool` reads | `malformed_response`                  | Returns a valid ChaosAgent failure wrapper describing the invalid external representation; invalid data is never persisted as a successful tool contract output. |
| `after_tool` reads | `stale_field`                         | Replaces an existing JSON-pointer field in an observation copy. The company state is unchanged.                                                                  |
| `after_tool` reads | `indirect_prompt_injection`           | Wraps the validated response with explicitly untrusted content visible only as tool output.                                                                      |
| `after_tool` reads | `duplicate_response`                  | Wraps repeated observation copies and a delivery count without invoking the handler more than once.                                                              |

Only the first matched non-delay directive in stable compiled fault-ID order is
applied within a phase; all matched delays are applied. Other selected
declarations remain truthfully `fault.matched` but are not recorded as applied.
This prevents contradictory replacement failures while preserving deterministic
behavior.

After-tool mutation faults are intentionally not evaluated in Issue #14.
Altering the acknowledgement of an already committed mutation overlaps the Issue
#15 post-commit ambiguity boundary. `ambiguous_post_commit_timeout` is neither
matched nor applied here.

## Evidence and history

The Gateway uses the existing Event v0 types:

- `fault.matched` means the Issue #13 matcher selected a declaration.
- `fault.applied` means timing, execution, or the returned observation was
  actually changed.
- `fault.observed` follows `tool.result` and links the delivered consequence
  back to the application.
- `fault.not_matched` is emitted only for a declaration targeting the current
  tool and phase. Rules for other tools or phases produce no event spam.

The causal order is
`tool.requested -> fault.matched -> fault.applied -> tool.result -> fault.observed`.
A normal policy or approval result retains its existing causation when no fault
applies.

`max_occurrences` is reconstructed on every match from complete committed
application chains, not by trusting the `fault.applied` discriminator alone. The
Gateway validates the complete Event v0 stream and requires a unique
Scenario-plan fault and activation, a preceding coherent
`tool.requested -> fault.matched -> fault.applied` chain, matching producer,
Run, tool, logical and physical attempt bindings, and one later
`tool.result -> fault.observed` chain. The activation ID is recomputed with
Issue #13's exact Scenario digest, run seed, fault, phase, logical/physical
attempt, ordinal, and arguments-digest material; a merely well-formed or
cross-copied activation is not trusted. Unknown faults, duplicate
activations/applications, broken causation, or missing results fail closed as an
infrastructure-integrity error before another match or handler execution. The
current in-progress request is excluded because its transactional
result/observation has not yet been emitted.

Process-local counters, caller history, and runtime checkpoints are not
authoritative. A caller rollback removes the request, fault evidence, result,
and mutation together; a restart or reclaim reads only committed, authenticated
application evidence. The Run-row lock serializes this reconstruction and the
new application, so concurrent sessions cannot both consume one remaining
occurrence.

## Transactions, fencing, and recovery

The Gateway preserves caller-owned transactions and locks the Run row for the
attempt. Fault and tool events use the same run-row-serialized sequence
allocator as lifecycle, policy, approval, and state evidence. Lease ownership
and database expiry are checked before application and again after every delay
or handler execution, before further authoritative work. In particular, an
after-tool read delay is followed by a fresh database-time lease check before
`fault.applied`, `tool.result`, or `fault.observed`. A reclaimed or expired
worker cannot apply a fault, call a mutation handler, or persist fault evidence.

Matched/application/result/observed evidence is one transaction. Persistence or
application failure rolls it back rather than leaving a partial causal chain. A
read handler has no business effect. A pre-tool mutation failure creates neither
a business effect, effect-ledger row, nor `state.evidence_recorded`. Crash
recovery therefore derives caps from the committed evidence prefix: a
rolled-back application is not counted, while a committed result remains
reconstructible even if the runtime checkpoint was not yet advanced.

Production delay is synchronous and uses `time.sleep`; tests inject a recording
or blocking sleeper and do not wait for Scenario-scale durations. This issue
does not implement cancellation, asynchronous scheduling, retries,
provider/network chaos, or worker daemons.

## Deferred boundary

Issue #15 owns committed-effect acknowledgement ambiguity, including
`ambiguous_post_commit_timeout`. Campaign selection/seeds, automatic retry and
backoff, evaluators, reports, telemetry, streaming, and UI remain deferred.

Issue #15's committed-effect boundary, recovery marker, and exact evidence
ordering are specified in
[POST_COMMIT_AMBIGUITY_V0.md](POST_COMMIT_AMBIGUITY_V0.md).
