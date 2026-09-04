# PostgreSQL persistence v0

## Scope and ownership

This package is the PostgreSQL adapter introduced by Issue #5 and extended by
Issues #6–#7 and #9–#11. It persists validated Scenario, Fixture, and Policy
revisions, unresolved Agent Configuration revision references, Runs, isolated
Run-local synthetic company state, Run Event evidence, and one final Run Report
per Run. It does not implement workers, evaluation, agent-facing tools, fault
behavior, or streaming.

The tables have deliberately different responsibilities:

- `scenario_revisions` stores the validated canonical Scenario JSON value, its
  schema version and JCS SHA-256 digest, plus creation metadata.
- `fixture_revisions` stores immutable validated Fixture v0 documents and their
  canonical digests. Mutable Run-local company rows are described in the Fixture
  documentation.
- `policy_revisions` stores immutable validated Policy v0 documents and their
  JCS digests. `approval_requests` freezes one exact mutation authorization
  request; `approval_resolutions` provides its immutable zero-or-one human
  resolution. A composite foreign key binds the approval's full Scenario tuple
  to its Run; repository validation additionally proves its Policy is the Policy
  frozen by that Scenario and revalidates the complete evidence chain.
- `agent_configuration_revisions` stores either a legacy immutable
  `{id, revision, digest}` placeholder for scripted Issue #11 adapters or a
  validated hosted Agent Configuration v0 document and verified digest. The
  hosted document freezes its exact model snapshot, compatibility profile, and
  deterministic token-accounting schedule; it contains no credentials.
- `runs` assigns a stable Run ID and freezes the Scenario and Agent
  Configuration keys _and digests_. New Issue #7 Runs also freeze the Fixture
  reference resolved from their Scenario. Issue #6 makes `status` authoritative
  through the lifecycle/lease protocol.
- `run_events` stores each validated Run Event document and the small set of
  envelope columns needed for identity, ordering, and later replay queries.
- `run_reports` stores one validated, final Run Report document per Run. V0 does
  not support replacing or rebuilding a report. A future versioned-report policy
  requires an explicit migration and contract decision.
- `execution_checkpoints` stores the mutable, versioned Issue #11 safe
  trajectory needed for crash recovery and approval pause/resume. Writes are
  lease-fenced and use an independent checkpoint CAS version; unlike immutable
  evidence, this table intentionally has no UPDATE/DELETE rejection trigger. Its
  raw-document writer is private to the runtime: PostgreSQL owns projection/FK
  constraints, persistence owns JSONB representability, digest, lease and CAS
  checks, and the runtime owns JSON Schema plus evidence-semantic
  reconstruction.
- `company_effects` stores the immutable Issue #9 idempotency/effect ledger for
  the two synthetic mutation tools. Its full identity, locking, evidence, and
  exactly-once scope are documented in the mutation-tool contract.
- `campaign_plans` and `campaign_trial_memberships` store only Issue #17's
  immutable pre-execution Campaign arm, planned index space, selected compiled
  fault plan, and Run assignments. Run rows are locked while still queued;
  primary/unique constraints prevent cross-process Run or index substitution.

Campaign orchestration, scheduling, APIs, and mutable Campaign state remain
outside this package. The narrow membership authority does not absorb those
later control-plane responsibilities.

## Relational columns and JSONB documents

Migration 0007 extends `agent_configuration_revisions` with nullable
`schema_version` and `canonical_document` columns. New hosted configurations use
the strict `chaosagent.agent-configuration/v0` document and verified digest;
pre-Issue-12 scripted placeholder rows retain both columns as NULL. Projection
checks fail closed when a hosted document omits or contradicts its ID, revision,
or schema version. Semantic loading additionally requires the embedded
accounting model to equal the configured exact model snapshot. Because the
entire document participates in the digest, accounting rates cannot change
without a new configuration revision/digest. The existing immutability trigger
continues to reject UPDATE and DELETE for both forms. Downgrading 0007 to 0006
intentionally removes the hosted document and schema columns while retaining the
original identity/digest placeholder; re-upgrade cannot reconstruct removed
configuration content.

Opaque contract IDs remain bounded text rather than UUIDs. JSONB stores the
validated semantic JSON value; PostgreSQL does not preserve the input's key
order, whitespace, or numeric spelling. On reads, the contract loader
revalidates and re-canonicalizes that value. Scenario digests therefore retain
their Scenario/JCS meaning even though the database does not store a second copy
of canonical bytes.

V0 applies one explicit persistence profile before insertion: U+0000 is rejected
in JSON string values and object keys because PostgreSQL JSONB cannot represent
it. This is a typed `PersistenceProfileError`, not an unpredictable driver
failure and not a change to Scenario v0 validation. Other contract semantics
remain owned by the versioned Python/JSON Schema loaders.

Relational columns duplicate only values needed for keys, foreign keys,
constraints, or expected queries. Check constraints bind those columns to the
corresponding JSON envelope/reference fields. Runs include revision digests so a
report's Scenario and Agent Configuration references can be tied to the same
frozen Run by a composite foreign key. Projection checks use fail-closed
null-safe comparisons, so missing keys, JSON null, malformed nested paths, and
mismatched projected values cannot become immutable rows. These checks do not
replace full contract validation.

PostgreSQL cannot recompute the Scenario semantic-normalization/JCS digest with
built-in functions. The validated loader computes it; database immutability,
primary keys, and repository conflict checks prevent an existing
`(scenario_id, revision)` from being remapped. Reads recompute and verify the
stored digest, detecting corruption or privileged out-of-band writes.

## Immutability and append-only guarantees

The repository returns frozen records containing immutable validated contract
wrappers and exposes no update/delete API for Scenario revisions, Fixture
revisions, Agent Configuration references, events, or reports. Migrations also
install row-level `BEFORE UPDATE OR DELETE` triggers for those immutable tables.

This is a database-level guard for normal DML, not an absolute tamper-proof
ledger. A database owner or superuser can disable/drop triggers, change schema,
use privileged maintenance paths, or restore different data. Production role
provisioning is deployment-specific and deferred; the application role must not
own these tables and should receive only `SELECT`/`INSERT` on immutable tables.
`TRUNCATE` and DDL must not be granted. The migration intentionally does not
create cluster-global roles.

For approvals, database constraints enforce relational references and the v0
tool/version shape. The centralized repository loader separately recomputes the
approval identity and request fingerprints and validates Run, Scenario, Policy,
and evidence coherence. Neither layer is presented as protection from a
malicious database owner.

Runs are deliberately mutable only through the Issue #6 lifecycle CAS methods.
See [`RUN_LIFECYCLE_LEASES.md`](RUN_LIFECYCLE_LEASES.md) for their state,
ownership, recovery, and evidence guarantees.

Execution checkpoints are mutable application coordination state, not evidence.
Their repository writes retain Issue #5 caller-owned transactions, lock the Run
row, validate the current running lease, use a database CAS predicate, and bind
the write to the latest event sequence. The runtime contract and additional
semantic read validation are documented in
[`../runtime/AGENT_RUNTIME_V0.md`](../runtime/AGENT_RUNTIME_V0.md).

## Transactions, conflicts, and event ordering

Repository methods call `flush()` but never `commit()`. Callers own the
SQLAlchemy `Session` and transaction, so a Scenario/Run/event/report operation
can be composed atomically and rolls back with the caller's transaction.
Expected uniqueness conflicts use savepoints so they do not poison that outer
transaction.

At PostgreSQL's default `READ COMMITTED` isolation:

- identical Scenario or Agent Configuration revision inserts are idempotent;
  different content/digests raise `RevisionConflictError`;
- duplicate Run IDs raise `PersistenceConflictError`;
- duplicate event IDs raise `DuplicateEventIDError`;
- duplicate `(run_id, sequence)` values raise `EventSequenceConflictError`;
- a write conflicting on both dimensions raises
  `EventIdentityAndSequenceConflictError`, independent of which unique index
  PostgreSQL reports first;
- an identical final-report retry is idempotent, while any different report for
  that Run raises `FinalReportConflictError`.

Ordinary event producers assign the positive sequence. Issue #6 lifecycle
mutations are the bounded exception: while holding the Run row lock they derive
the next lifecycle evidence sequence, and ordinary appends take that same lock
before insertion. Concurrent writers may insert different caller-assigned
sequence values in either commit order; sequence remains the authoritative
logical order, not insertion time, `occurred_at`, or `recorded_at`. PostgreSQL's
unique constraint serializes a collision so exactly one same-sequence insert
wins. Fetches always order by sequence, and Run Event v0 intentionally permits
gaps.

## Migrations and local PostgreSQL

Alembic reads `CHAOSAGENT_DATABASE_URL`, fails closed for non-PostgreSQL URLs,
and explicitly owns its tables, indexes, trigger function, triggers, and version
table in the `public` schema. From the repository root:

```shell
docker compose -f deploy/compose/postgres.yml up -d
$env:CHAOSAGENT_DATABASE_URL = "postgresql+psycopg://chaosagent:chaosagent@127.0.0.1:55432/chaosagent_test"
uv run alembic -c packages/persistence/alembic.ini upgrade head
uv run alembic -c packages/persistence/alembic.ini downgrade base
```

The Compose file contains only an ephemeral PostgreSQL service, binds it to
`127.0.0.1`, and uses fixed development-only credentials that are unsuitable for
production. CI and Compose use the same immutable PostgreSQL 17.11 Alpine image
digest. CI sets both test safeguards. For a local integration run:

```shell
$env:CHAOSAGENT_TEST_DATABASE_URL = "postgresql+psycopg://chaosagent:chaosagent@127.0.0.1:55432/chaosagent_test"
$env:CHAOSAGENT_ALLOW_DESTRUCTIVE_DATABASE_TESTS = "1"
uv run pytest tests/python/test_postgres_persistence.py
```

Tests skip when the database URL is absent. Before any migration teardown, the
fixture also requires the explicit destructive-test opt-in and a database name
ending in `_test`; otherwise it fails without touching the database. When all
guards pass, connection or migration failures fail the suite.

SQLAlchemy supplies typed PostgreSQL mappings and transaction behavior, Alembic
supplies auditable migrations, and psycopg is the PostgreSQL driver. The
`binary` psycopg extra gives local development and CI a self-contained,
cross-platform libpq runtime instead of requiring a machine-level client
installation. The standard library and prior JSON-contract dependencies provide
none of those capabilities, so all three are necessary runtime dependencies.

## Deferred beyond Issue #17

- worker processes/daemons, heartbeats, and automatic recovery scheduling;
- external side-effect fencing or reconciliation beyond the committed local
  synthetic effect ledger;
- Campaign orchestration, scheduling, and API-managed Campaign creation;
- authentication/RBAC, approval UI/notifications, expiry workflows, fault
  activations, evaluators, SSE, telemetry, exports, and deployment role
  creation;
- any report rebuild/version history policy.

Migration `0008_post_commit_ack` adds the immutable
`post_commit_acknowledgements` recovery table and same-Run event-reference
constraints. The marker commits with the existing effect and state evidence;
completion is derived from four mutually distinct planned immutable Event IDs,
not mutable status. Repository authentication additionally binds the marker to
the effect ledger's original logical call, physical attempt, and lease
generation and resolves the persisted frozen Policy revision. Downgrade to
`0007` intentionally drops markers while preserving effects and events. See
[POST_COMMIT_AMBIGUITY_V0.md](../faults/POST_COMMIT_AMBIGUITY_V0.md).

Migration `0009_run_fault_seed` adds the nullable, bind-once `runs.fault_seed`
used to authenticate deterministic Issue #13 activation identities during later
evaluation. Repository binding requires the current DB-time-valid lease; the
database permits only `NULL → value` and rejects every subsequent change. No
seed is fabricated for older Runs. Downgrade removes the column and trigger, so
an old faulted Run without another trustworthy source cannot later be evaluated
as though its activation were authenticated.

Migration `0010_campaign_memberships` adds the two immutable Campaign authority
tables and a bind-once `runs.fault_plan_digest`. Planning locks Run rows in
stable order and inserts the complete plan and assignments in the caller-owned
transaction. Rollback leaves no authority. Runtime and Gateway entry points bind
the compiled plan used for execution; evaluator reconstruction requires it to
match the committed Campaign assignment. Downgrade removes only this narrow
Issue #17 state and does not fabricate assignments for existing Runs.
