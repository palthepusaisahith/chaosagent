# ChaosAgent

ChaosAgent is an open-source reliability laboratory for action-taking AI agents.
It runs agents against a bounded synthetic company, injects controlled faults at
tool boundaries, records immutable evidence, and evaluates whether the resulting
business state is correct.

## Why ChaosAgent exists

Action-taking agents can fail through ambiguous tool outcomes, retries, stale
dependencies, unsafe content, or incorrect claims about external state.
Happy-path tests and general tracing do not create controlled failure campaigns
or prove what happened to stateful effects.

The dangerous case is not simply when a tool fails. It is when the tool succeeds
but the agent cannot tell that it succeeded. ChaosAgent makes that failure
reproducible and judges the result from authoritative state and evidence, rather
than from the agent's narration.

## Flagship experiment

The shipment/refund benchmark follows one causal chain:

```text
CAUSE       Customer requests a refund for a failed shipment
  |
  v
ACTION      Agent calls payments.refund
  |
  v
FAULT       Refund commits, but its acknowledgement is hidden or times out
  |
  v
RECOVERY    Agent retries safely or reconciles the ambiguous outcome
  |
  v
PROOF       Effect evidence shows exactly one refund
  |
  v
VERDICT     Deterministic evaluator returns PASS, FAIL, or INVALID
```

For the same local synthetic mutation identity, idempotency and the durable
effect ledger prevent a retry from creating a second refund. The evaluator
verifies the external business state, the fault's authenticated evidence chain,
and the evidence cutoff before producing a verdict. Structural benchmark
artifacts live in [`benchmarks/shipment-refund`](benchmarks/shipment-refund).

## What ChaosAgent provides

- A PostgreSQL-backed Run state machine with worker leases, fencing, recovery,
  and immutable revision/digest references.
- A provider-neutral agent runtime plus a bounded OpenAI Responses adapter.
- Exact Tool Gateway capability dispatch over an isolated synthetic company,
  including idempotent mutations and a durable effect ledger.
- Durable Policy decisions and human Approval bindings that fail closed.
- Deterministic fault matching and application, including ambiguous post-commit
  acknowledgement recovery.
- Evidence-bound critical evaluators, Campaign statistics/comparison, final Run
  reports, and deterministic tamper-detecting export bundles.
- A versioned REST control plane, replay-safe SSE, failure-isolated
  OpenTelemetry instrumentation, and a React evidence dashboard.
- A capability-restricted execution boundary with adversarial abuse tests.

ChaosAgent does **not** support arbitrary real-world credentials or real payment
systems. Do not use real secrets, production accounts, customer data, or live
financial systems with this project.

## Architecture

```mermaid
flowchart TD
    C[Scenario + Agent Configuration] --> R[Agent Runtime]
    P[Provider Adapter] <--> R
    R --> G[Tool Gateway]
    G --> Y[Policy evaluation + Approval checks]
    G --> F[Fault Engine]
    G --> S[Synthetic Company]
    G --> D[Persistence boundary]
    S --> D
    R --> D
    D --> DB[(PostgreSQL evidence + state)]
    DB --> E[Evaluators + Reports + Exports]
    E --> API[REST + replay-safe SSE]
    DB --> API
    API --> UI[React Dashboard]
    R -. operational .-> O[OpenTelemetry]
    G -. operational .-> O
```

PostgreSQL and the versioned domain contracts are authoritative. The provider,
control plane, telemetry, and dashboard are bounded adapters or projections;
none replaces the Run lifecycle, evidence, or evaluator truth. See the
[`approved architecture dossier`](docs/architecture/CHAOSAGENT_PRODUCT_ARCHITECTURE.md)
for the full boundaries and threat model.

## Engineering guarantees

- Scenario, Fixture, Policy, Agent Configuration, evidence, and evaluation
  contracts are versioned, strictly validated, canonically digested, and loaded
  defensively.
- Run mutations use PostgreSQL row locking, compare-and-swap lifecycle versions,
  database-time leases, and fencing so stale workers cannot write after reclaim.
- Tool authorization precedes execution. Mutation state, effects, and
  authoritative evidence are atomic where the protocol requires them.
- Within one authoritative PostgreSQL database, a local synthetic mutation is
  deduplicated for its
  `(run_id, tool_id, contract_version, idempotency-key digest)` identity. A
  different key is a different potential effect and must pass the current
  business rules. This is not distributed exactly-once delivery.
- The post-commit protocol preserves a committed local synthetic effect even
  when its acknowledgement transaction fails.
- Run events are append-only product evidence. OpenTelemetry is operational
  telemetry and never becomes evaluation authority.
- Evaluators use authenticated state/evidence through a frozen sequence cutoff;
  malformed or incomplete authoritative inputs produce `INVALID`, not a
  misleading ordinary failure.

## Demo / dashboard

The React dashboard presents the flagship Run as an evidence-led causal story:
request, committed effect, injected fault, retry/reconciliation, one-refund
proof, and evaluator verdict. It can create queued Runs, open a known Run or
Campaign, hydrate persisted evidence, and continue from the last replay-safe SSE
cursor. It does not invent global Run or Campaign listings.

**Current pre-1.0 demo limitation:** the commands below start PostgreSQL, apply
migrations, and run the control plane and dashboard. `POST /api/v1/runs` creates
queued work; the control plane intentionally does not claim or execute it in the
HTTP request. The repository does not yet package a worker daemon or one-command
flagship bootstrap/runner, so this setup alone cannot reproduce the complete
ambiguous-refund experiment from a fresh empty database. The dashboard can
inspect known persisted Runs and Campaigns.

With PostgreSQL migrated through `0010` and the control plane running on
`127.0.0.1:8000`, start it with:

```shell
pnpm --filter @chaosagent/web dev
```

Vite serves `http://127.0.0.1:5173` and proxies `/api` to the local control
plane. For a separately hosted API, set `VITE_CONTROL_PLANE_URL` to an
explicitly allowed origin. See
[`docs/dashboard/DASHBOARD_V1.md`](docs/dashboard/DASHBOARD_V1.md).

## Quick start / local development

### Prerequisites

- Python 3.12.x
- [uv 0.12.1](https://docs.astral.sh/uv/)
- Node.js 22.x
- pnpm 10.15.1
- GNU Make
- Docker with Docker Compose support (used by the documented PostgreSQL setup)

The repository Compose service currently supplies PostgreSQL 17.11 through its
digest-pinned `postgres:17.11-alpine3.24` development/test image. This is the
reproducible local configuration, not a claim that every ChaosAgent deployment
must use exactly PostgreSQL 17.11.

Install the pinned pnpm release through the Corepack bundled with Node.js:

```shell
corepack enable
corepack prepare pnpm@10.15.1 --activate
```

Install uv 0.12.1 using the official versioned installer for your platform:

```shell
# Linux and macOS
curl --proto '=https' --tlsv1.2 -LsSf \
  https://releases.astral.sh/github/uv/releases/download/0.12.1/uv-installer.sh | sh
```

```powershell
# Windows PowerShell
powershell -ExecutionPolicy Bypass -c `
  "irm https://releases.astral.sh/github/uv/releases/download/0.12.1/uv-installer.ps1 | iex"
```

The repository requires these versions and rejects other uv releases. From a
clean clone, install the locked workspace and run the default checks:

```shell
make install
make check
```

For local PostgreSQL development, use the disposable loopback-only service and
apply all migrations:

```powershell
docker compose -f deploy/compose/postgres.yml up -d
$env:CHAOSAGENT_DATABASE_URL = "postgresql+psycopg://chaosagent:chaosagent@127.0.0.1:55432/chaosagent_test"
uv run alembic -c packages/persistence/alembic.ini upgrade head
```

The fixed credentials are development-only and unsuitable for production. Full
setup, destructive-test safeguards, and Unix environment syntax are documented
in [`docs/persistence/POSTGRESQL_V0.md`](docs/persistence/POSTGRESQL_V0.md).

Configure the same migrated development database and benchmark Ground Truth,
then start the control plane from the repository root:

```powershell
$env:CHAOSAGENT_DATABASE_URL = "postgresql+psycopg://chaosagent:chaosagent@127.0.0.1:55432/chaosagent_test"
$env:CHAOSAGENT_GROUND_TRUTH_PATHS = "benchmarks/shipment-refund/ground-truth/refund-once-and-close-ticket.v0.json"
uv run uvicorn chaosagent_control_plane.app:create_app_from_environment --factory
```

Windows uses `;` between multiple Ground Truth paths; Unix systems use `:`.
Generated OpenAPI is at `/openapi.json`, and API documentation is at `/docs`.
See
[`docs/control-plane/CONTROL_PLANE_V1.md`](docs/control-plane/CONTROL_PLANE_V1.md).

## Testing

The verification suite combines focused unit/contract tests with guarded real
PostgreSQL integration and independent-session concurrency tests. Coverage
includes leases and fencing, same-key synthetic-effect deduplication and
ambiguous recovery, evaluator evidence boundaries, REST/SSE replay, export
tamper detection, provider abuse attempts, and frontend rendering behavior.

Without the PostgreSQL test URL and explicit destructive-test opt-in, pytest
skips the guarded real-PostgreSQL suites. To run the full configured checks
against the disposable Compose database, use:

```powershell
$env:CHAOSAGENT_TEST_DATABASE_URL = "postgresql+psycopg://chaosagent:chaosagent@127.0.0.1:55432/chaosagent_test"
$env:CHAOSAGENT_ALLOW_DESTRUCTIVE_DATABASE_TESTS = "1"
make check
```

The database-name guard requires `_test`; the opt-in permits migration teardown
only in that disposable test database.

At canonical V1 completion, the real-PostgreSQL suite contained 882 passing
tests. That number is a completion snapshot, not a permanent repository
guarantee; `make check` is the current source of truth.

## Security model

ChaosAgent uses a capability-restricted execution boundary. Agents can invoke
only registered synthetic tools and receive no direct shell, filesystem,
arbitrary network, database, environment, Docker, or control-plane authority.

This is not OS/container isolation. Runtime, provider adapters, and tool
implementations are trusted in-process code. The implemented threat model,
abuse-test map, deployment assumptions, and residual risks are documented in
[`docs/security/EXECUTION_TRUST_BOUNDARY_V1.md`](docs/security/EXECUTION_TRUST_BOUNDARY_V1.md).
Report suspected vulnerabilities privately according to
[`SECURITY.md`](SECURITY.md).

## Repository layout

```text
apps/web/                       React/Vite reliability dashboard
services/control-plane/         Versioned REST and replay-safe SSE service
packages/agent-configurations/  Immutable hosted-agent configuration
packages/agent-runtime/         Provider-neutral loop and checkpoint contract
packages/evidence/              Run Event/Report schemas and validated loaders
packages/evaluators/            Critical gates and Campaign statistics/comparison
packages/exports/               Deterministic manifests and export bundles
packages/faults/                Scenario fault compiler, matcher, and application
packages/fixtures/              Fixture schema, loader, and synthetic company state
packages/persistence/           PostgreSQL models, Alembic migrations, repositories
packages/policies/              Policy contract, decisions, and approval binding
packages/providers-openai/      Bounded OpenAI Responses provider adapter
packages/scenarios/             Scenario schema, validation, and canonicalization
packages/telemetry/             Failure-isolated OpenTelemetry integration
packages/tool-gateway/          Authorized tool dispatch and effect protocol
packages/shared/                Shared TypeScript package and smoke test
benchmarks/                     Shipment/refund structural benchmark artifacts
tests/python/                   Python unit and PostgreSQL integration tests
docs/                           Architecture and subsystem documentation
.github/workflows/              Linux CI
```

Key subsystem references include:

- [PostgreSQL persistence](docs/persistence/POSTGRESQL_V0.md) and the
  [Run lifecycle/lease protocol](docs/persistence/RUN_LIFECYCLE_LEASES.md)
- [Fixture and synthetic-state semantics](docs/fixtures/FIXTURE_V0.md)
- [Agent runtime and recovery](docs/runtime/AGENT_RUNTIME_V0.md)
- [OpenAI Responses adapter](docs/providers/OPENAI_RESPONSES_V0.md)
- [OpenTelemetry boundary](docs/observability/OPENTELEMETRY_V0.md)
- [Control-plane REST/SSE contract](docs/control-plane/CONTROL_PLANE_V1.md)
- [Dashboard evidence model](docs/dashboard/DASHBOARD_V1.md)

## Developer commands

All commands run from the repository root:

| Command             | Purpose                                                     |
| ------------------- | ----------------------------------------------------------- |
| `make install`      | Install exact Python and Node dependencies from lockfiles.  |
| `make lint`         | Run Ruff and ESLint.                                        |
| `make format`       | Apply Ruff and Prettier formatting.                         |
| `make format-check` | Verify formatting without changing files.                   |
| `make typecheck`    | Run mypy and TypeScript's compiler in strict checking mode. |
| `make test`         | Run pytest and Vitest; guarded PostgreSQL tests may skip.   |
| `make check`        | Run lint, formatting, type, and configured tests.           |

Dependency changes use `uv add --dev <package>` or `pnpm add -Dw <package>` as
appropriate, with the resulting `uv.lock` or `pnpm-lock.yaml` committed. The
control-plane package uses Hatchling as its PEP 517 backend; the root uv
configuration pins that isolated build dependency for reproducible clean builds.

## Contributing and security

Read [`CONTRIBUTING.md`](CONTRIBUTING.md) before opening a pull request and
follow the [Code of Conduct](CODE_OF_CONDUCT.md). Never post real secrets or
exploit details in a public issue; use the private process in
[`SECURITY.md`](SECURITY.md).

## Project status / future work

The canonical V1 roadmap is complete, and CI is green. The project remains
pre-1.0 while interfaces and deployment assumptions continue to mature; this
status is not a claim of production readiness. Future work is post-V1
enhancement rather than unfinished canonical V1 scope.

## License

ChaosAgent is licensed under the [Apache License 2.0](LICENSE).
