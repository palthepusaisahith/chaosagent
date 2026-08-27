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
- `agent_configuration_revisions` is only an immutable `{id, revision, digest}`
  reference registry. There is no Agent Configuration contract yet, so this
  table does not claim that the referenced content was loaded or that its digest
  was independently verified. In particular, the all-zero digest in structural
  examples remains an explicit unresolved placeholder.
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

Campaigns remain outside this package. Scenario v0's trial intent does not
become campaign persistence here, preserving the boundary established in Issue
#3.

## Relational columns and JSONB documents

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

## Deferred beyond Issue #11

- worker processes/daemons, heartbeats, and automatic recovery scheduling;
- external side-effect fencing or reconciliation beyond the committed local
  synthetic effect ledger;
- Campaign persistence and campaign statistics;
- authentication/RBAC, approval UI/notifications, expiry workflows, fault
  activations, evaluators, SSE, telemetry, exports, and deployment role
  creation;
- an actual versioned Agent Configuration document contract;
- any report rebuild/version history policy.
