# Contributing to ChaosAgent

Thanks for helping improve ChaosAgent. The project is pre-1.0, so focused
changes with clear evidence are more valuable than broad rewrites.

By participating, you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md).
Contributions are submitted under the [Apache License 2.0](LICENSE).

## Development setup

You need Python 3.12.x, uv 0.12.1, Node.js 22.x, pnpm 10.15.1, and GNU Make.
Follow the [README setup instructions](README.md#local-development), then run:

```shell
make install
make check
```

Both lockfiles must remain committed and consistent. Use `uv add` or `pnpm add`
rather than editing resolved dependency entries manually.

## Branches and issue scope

- Start from an issue with agreed scope. Ask before undertaking a large or
  architectural change.
- Use a short-lived branch named `codex/<issue>-<slug>` or
  `<username>/<issue>-<slug>`.
- Keep each pull request focused on one issue. Do not implement unrelated
  backlog items.
- Avoid drive-by formatting, dependency upgrades, file moves, or refactors
  unrelated to the issue.
- Prefer small commits that explain why the change is needed.

## Pull requests

Link the issue and explain the problem, solution, scope boundaries, risks, and
rollback approach. Update documentation when behavior, configuration, developer
commands, or architecture changes. Architectural changes must explain
alternatives, consequences, and their effect on trust boundaries,
reproducibility, or evaluation semantics.

Before opening a pull request:

```shell
make check
```

New or changed behavior requires deterministic tests. Run the relevant focused
tests as well as the root lint, formatting, type-check, and test commands. If a
check cannot run, state exactly why in the pull request; do not claim it passed.

## Security-sensitive changes

Treat credentials, agent/tool content, authorization, isolation, fault controls,
evidence, and CI permissions as security-sensitive. Never commit real secrets or
use real-world credentials or payment systems in tests. Explain threat-boundary
changes and add negative tests where applicable. Report suspected
vulnerabilities privately according to [SECURITY.md](SECURITY.md), not in an
issue or draft pull request.

## AI-assisted development

AI tools such as Codex may be used, but the contributor remains responsible for
correctness, licensing, security, and maintainability. Review generated code and
documentation line by line, add appropriate tests, and disclose material AI
assistance in the pull request. Do not submit opaque, large generated diffs.
Important architectural decisions require a human-readable explanation of the
reasoning and alternatives, regardless of how the change was produced.
