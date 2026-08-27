# Policy and approval boundary v0

Policy v0 (`chaosagent.policy/v0`) is frozen trusted configuration, validated by
Draft 2020-12 JSON Schema and canonicalized with RFC 8785/JCS before its SHA-256
digest is calculated. Object key order and insignificant JSON whitespace do not
affect identity. Array order, strings, numbers, and all other semantic values
do. Duplicate object keys and non-finite numbers fail loading. Unknown versions
fail closed; v0 will not be silently reinterpreted when a later version is
added.

V0 deliberately is not a general policy language. It contains explicit rules for
the four Scenario v0 tools. Reads and `support.update_ticket` are independently
allow/deny. `payments.refund` has a currency, an inclusive automatic limit, and
an inclusive approval limit; values above the approval limit are denied. The
golden USD policy allows through 5,000 minor units, requires approval from 5,001
through 12,000, and denies larger amounts. There is no executable expression,
embedded Python, regex script, SQL, IAM, account, or role model.

Authorization order is input contract, current Run lease/fence, Scenario
`allowed_tools`, policy decision, approval gate, handler, then effect/evidence.
The Scenario capability boundary cannot be widened by Policy. Every accepted
Scenario tool request records `tool.requested`, followed by `policy.decision`. A
denial records a failed `tool.result`; it never invokes a handler or writes an
effect. Read tools also receive an allow decision but never require approval in
the golden policy.

## Approval binding and persistence

An approval request authorizes one exact frozen logical mutation. Its
deterministic ID and durable uniqueness key bind the Run ID; Scenario ID,
revision, and digest; Policy ID, revision, and digest; exact tool and contract
version; the canonical full-request digest; and the idempotency-key digest. The
immutable row also retains the complete arguments, logical call, requesting
attempt, lease attempt, and evidence references. A separate immutable one-to-one
resolution row represents either `approved` or `denied`; pending is the absence
of that row. Actor identity is trusted input until a later authentication
boundary exists.

PostgreSQL guarantees that an approval's Scenario tuple is the tuple frozen on
its Run with a composite foreign key. It also guarantees that Scenario and
Policy references independently resolve and that the v0 tool/version shape is
valid. The repository supplies the cross-document rule that the approval Policy
must be the Policy frozen inside that Scenario. This rule is deliberately not
duplicated into another relational projection.

Every authoritative approval read runs one centralized integrity validator. It
recomputes the request and idempotency digests and deterministic approval ID,
reloads the current Run and validated immutable Scenario and Policy revisions,
and verifies the stored tool, arguments, logical identity, and evidence chain.
Gateway authorization additionally compares that validated record with the
current requested tool/version/arguments. Missing, fabricated, mismatched, or
corrupt state fails closed before handler execution; an approval status is never
trusted merely because a row exists at an expected key.

Creation first locks and reloads the authoritative Run, derives its Scenario and
that Scenario's Policy from PostgreSQL, and rejects caller-supplied snapshot or
digest disagreement. The referenced preceding evidence must be a same-Run
`policy.decision` with `require_approval`, the matching decision ID, Policy,
logical call, and causative `tool.requested` request fingerprint. Creation and
`approval.requested` evidence share one nested transaction. Resolution and
`approval.resolved` share another. Repository methods flush but never commit;
the caller owns the outer transaction. Resolution may occur without a worker
lease and does not alter Run lifecycle. A later agent attempt must again prove
the current lease before it can use an approval.

Approval is permission to attempt, not a promise that the operation will still
succeed. Current Issue #9 business invariants are rechecked under the mutation
transaction, so intervening refunds can make an approved refund fail. Exact
idempotent replays of an already-established approval-required effect revalidate
the same approval and use the effect ledger; they create neither a second
approval nor a second effect. A changed request cannot reuse the old approval.

`tool.result.payload.request_event_id` always names the original
`tool.requested` event for that attempt. Its envelope `causation_event_id` names
the authorization event that caused the result: `policy.decision` for automatic
allow or deny, `approval.requested` while waiting, and `approval.resolved` for a
denied or approved request (including an approved exact replay). This keeps the
request relationship stable while making authorization provenance explicit.

Run creation resolves and digest-checks the Policy referenced by its validated
Scenario, just as it resolves the Fixture. Missing, mismatched, or corrupt
Policy content therefore prevents creation rather than failing at the first tool
call.

Policy revisions, requests, and resolutions are append-only through repository
APIs and protected against UPDATE/DELETE by PostgreSQL triggers. Event ordering
uses the existing Run-row-serialized allocator, so policy, approval, lifecycle,
and tool evidence share one monotonic sequence. These constraints and validators
detect accidental corruption and reject unauthorized application paths; they do
not protect against a malicious database owner able to disable constraints,
rewrite rows, and rewrite all matching evidence. V0 does not provide
authentication, RBAC, notifications, expiry workflows, UI, execution scheduling,
faults, evaluators, or external payment authorization.
