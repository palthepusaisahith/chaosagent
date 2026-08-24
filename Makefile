.PHONY: install lint format format-check typecheck test check

install:
	uv sync --locked --all-packages
	pnpm install --frozen-lockfile

lint:
	uv run --locked ruff check .
	pnpm lint

format:
	uv run --locked ruff format .
	pnpm exec prettier --write .

format-check:
	uv run --locked ruff format --check .
	pnpm format-check

typecheck:
	uv run --locked mypy
	pnpm typecheck

test:
	uv run --locked pytest
	pnpm test

check: lint format-check typecheck test
