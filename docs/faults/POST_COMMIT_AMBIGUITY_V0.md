# Ambiguous post-commit mutation semantics v0

Issue #15 implements only Scenario v0's `ambiguous_post_commit_timeout` at
`after_commit`. It applies to the committed synthetic mutation tools; read tools
never enter this protocol.

## Commit and observation boundary

An eligible call passes the existing lease, Scenario allowlist, input, Policy,
and exact Approval checks before its handler runs. The Gateway then uses two
explicit, Gateway-owned PostgreSQL transactions:

1. The first transaction rechecks the database-time lease, establishes or reuses
   the idempotent business effect, validates the ledger projection, records
   `state.evidence_recorded` for a new effect, and inserts an immutable recovery
   marker. Its commit is the authoritative effect boundary.
2. A second transaction rechecks the lease and atomically records
   `fault.matched -> fault.applied -> tool.result -> fault.observed`. The result
   has `outcome=timed_out`, `error_code=fault_timeout`, no response digest, and
   never claims success, rollback, or absence of an effect.

The marker is required because an Event v0 prefix alone cannot preserve the
exact pending activation and evidence identities across a crash. It binds the
Run, logical/physical attempt, logical-call ordinal, exact tool and request
digests, idempotency identity, effect, request/state evidence, fault and
activation, lease generation, and planned acknowledgement event IDs. Foreign
keys protect the effect, approval, and same-Run committed event references;
repository validation recomputes the activation and protects the full semantic
binding.

Both recovery and acknowledgement completion require the locked authoritative
Run to be exactly `running`; the broader Issue #6 active-status lease primitive
does not by itself authorize this Tool Gateway work. Marker validation resolves
and digest-checks the persisted Policy revision referenced by the frozen
Scenario. It also requires the marker's logical call, original physical attempt,
and original lease generation to equal the immutable effect-ledger provenance; a
later worker may recover that older effect only while holding its own newer
current lease. The four planned acknowledgement Event IDs are mutually distinct.

Normal calls retain caller-owned transaction behavior. A call whose frozen plan
can apply an `after_commit` mutation fault deliberately uses this documented
Gateway-owned boundary. Its caller must provide an Engine-bound Session with no
active transaction; the Gateway rejects an active caller transaction rather than
risk self-deadlock or inconsistently observe uncommitted caller state.

## Idempotency, recovery, and concurrency

A same-key, semantically identical replay returns the established result with
`application=already_applied` and creates neither a second business effect nor
another state-evidence event. A different key re-runs current Policy and
business invariants.

If failure occurs before the first commit, effect, marker, and state evidence
roll back together. After it, the effect remains. Re-executing the same physical
attempt validates the marker, recomputes deterministic matching, and completes
or returns the already completed ambiguous observation. The Agent Runtime also
reconciles a completed acknowledgement that precedes checkpoint persistence.

If the lease expires after the effect commit, the stale worker cannot write the
acknowledgement or checkpoint. A current reclaimed worker can finish the marker
without duplicating the effect. A PostgreSQL session advisory lock keyed by Run
serializes the short interval across both transactions, so a competing attempt
waits and then reconstructs committed authenticated `max_occurrences` history.
The Run-wide scope is intentional: `max_occurrences` and cumulative business
invariants are Run-scoped, so distinct keys cannot race through the commit gap.
Different Runs use different 64-bit hash keys; a theoretical hash collision can
only cause extra serialization, never cross-Run authorization. Closing or losing
the dedicated connection releases the session lock. Lifecycle operations never
take this advisory lock, and the fresh second-transaction lease check remains
authoritative.

Mutation calls that enter the durable path but stop at policy denial, approval
waiting, or an ordinary non-matching result commit complete request/result
evidence without an ambiguity marker. Runtime restart authenticates that
markerless mutation attempt and advances the checkpoint; read-only calls never
enter this recovery path, and recovery never infers an effect from a result
digest alone.

## Crash boundaries and guarantee

- Mutation, ledger, state-evidence, or marker failure before commit leaves no
  new effect.
- Failure after effect commit but before fault evidence leaves a recoverable
  marker and authoritative effect.
- Failure at matched, applied, result, or observed persistence rolls back the
  complete acknowledgement transaction, never a partial causal chain.
- Failure after ambiguous result but before checkpoint is reconciled from the
  validated marker and evidence without redispatching the effect.

The guarantee is exactly one local synthetic business effect per idempotency
identity, with at-least-once attempt semantics and a possibly lost or ambiguous
acknowledgement. It is not exactly-once transport, an external payment protocol,
automatic retries/backoff, evaluation, Campaign logic, or Issue #16+ behavior.
