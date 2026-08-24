# ChaosAgent

ChaosAgent is an open-source platform for chaos engineering, reliability
evaluation, observability, and security testing of autonomous AI agents.

## Project status

ChaosAgent is pre-1.0 and under active design.

- **Implemented:** the Python/TypeScript monorepo bootstrap, reproducible
  developer tooling, Linux CI smoke checks, repository governance documents, and
  the versioned Scenario v0 contract with validation and stable digests.
- **Planned:** the V1 capabilities described below and in the architecture
  dossier. They are not available yet.
- **Experimental:** architecture, interfaces, and roadmap decisions may change
  before the first release.

No experiment execution behavior is implemented today.

## Why ChaosAgent exists

Action-taking agents can fail through ambiguous tool outcomes, retries, stale
dependencies, unsafe content, or incorrect claims about external state.
Happy-path tests and general tracing do not by themselves create controlled
failure campaigns or verify stateful effects. ChaosAgent is intended to combine
controlled perturbation, evidence capture, and outcome evaluation without
claiming novelty over existing chaos-engineering or agent-evaluation work.

## V1 goals

The planned V1 focuses on:

- reproducible baseline and fault campaigns against a bounded synthetic
  environment;
- deterministic fault selection and evidence-linked evaluation of externally
  observable behavior;
- traceable run, tool, policy, and state evidence;
- security tests for agent/tool trust boundaries; and
- an inspectable local demonstration that does not require live provider
  credentials in CI.

ChaosAgent currently does **not** support arbitrary real-world credentials or
real payment systems. Do not use real secrets, production accounts, customer
data, or live financial systems with this project.

See the
[approved architecture dossier](docs/architecture/CHAOSAGENT_PRODUCT_ARCHITECTURE.md)
for the product boundaries, threat model, and staged roadmap.

## Local development

### Prerequisites

- Python 3.12.x
- [uv 0.12.1](https://docs.astral.sh/uv/)
- Node.js 22.x
- pnpm 10.15.1
- GNU Make

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

Then verify the toolchain:

```shell
python --version
uv --version
node --version
pnpm --version
```

The reported versions must satisfy the versions above. The repository's uv
configuration rejects other uv versions.

From a clean clone, install the locked dependencies and run every verification
step:

```shell
make install
make check
```

## Repository layout

```text
apps/web/                  TypeScript web-package placeholder
services/control-plane/    Installable Python package placeholder
packages/shared/           Shared TypeScript package and smoke test
packages/scenarios/        Scenario v0 schema, validation, and canonicalization
tests/python/              Python smoke tests
docs/                      Project documentation
.github/workflows/         Linux CI
```

No application behavior is included in these placeholders.

## Developer commands

All commands run from the repository root:

| Command             | Purpose                                                       |
| ------------------- | ------------------------------------------------------------- |
| `make install`      | Install exact Python and Node dependencies from lockfiles.    |
| `make lint`         | Run Ruff and ESLint.                                          |
| `make format`       | Apply Ruff and Prettier formatting.                           |
| `make format-check` | Verify formatting without changing files.                     |
| `make typecheck`    | Run mypy and TypeScript's compiler in strict checking mode.   |
| `make test`         | Run pytest and Vitest.                                        |
| `make check`        | Run lint, formatting checks, type checks, and all unit tests. |

Dependency changes should be made with `uv add --dev <package>` or
`pnpm add -Dw <package>` as appropriate, and the resulting `uv.lock` or
`pnpm-lock.yaml` should be committed.

The control-plane package uses Hatchling as its PEP 517 backend. Because
isolated build dependencies are separate from the project lockfile, the root uv
configuration constrains Hatchling to an exact version for reproducible clean
builds.

## Contributing and security

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request and follow
the [Code of Conduct](CODE_OF_CONDUCT.md). Report suspected vulnerabilities
privately according to [SECURITY.md](SECURITY.md); never post real secrets or
exploit details in a public issue.

## License

ChaosAgent is licensed under the [Apache License 2.0](LICENSE).
