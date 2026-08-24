# ChaosAgent

ChaosAgent is an open-source platform for chaos engineering, reliability
evaluation, observability, and security testing of autonomous AI agents.

This repository currently contains only the Python/TypeScript monorepo
bootstrap. Product behavior and runtime infrastructure are intentionally
deferred to later issues.

## Prerequisites

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
