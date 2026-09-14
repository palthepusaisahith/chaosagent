"""Console entry point for the deterministic ChaosAgent flagship demo."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from .runner import DEFAULT_DASHBOARD_URL, FlagshipDemoError, FlagshipDemoResult, run_flagship_demo


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chaosagent-demo",
        description="Run the deterministic shipment/refund flagship experiment locally.",
    )
    parser.add_argument("--run-id", help="use a specific new Run ID instead of generating one")
    parser.add_argument(
        "--dashboard-base-url",
        default=DEFAULT_DASHBOARD_URL,
        help="base URL used only to print the dashboard route",
    )
    return parser


def _repository_root(start: Path) -> Path:
    current = start.resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").is_file() and (
            candidate / "benchmarks" / "shipment-refund"
        ).is_dir():
            return candidate
    raise FlagshipDemoError("run chaosagent-demo from the ChaosAgent repository")


def render_success(result: FlagshipDemoResult) -> str:
    proof = result.proof
    return "\n".join(
        (
            "ChaosAgent Flagship Demo",
            "",
            f"Run: {result.run_id}",
            "",
            "CAUSE      refund request persisted",
            "FAULT      refund committed; acknowledgement timed out",
            (f"RECOVERY   same idempotency key replayed; disposition {proof.recovery_disposition}"),
            (
                f"PROOF      authoritative refund effects = {proof.refund_effect_count}; "
                "exactly-once gates PASS"
            ),
            f"VERDICT    {proof.classification.upper()}",
            "",
            "Dashboard:",
            result.dashboard_url,
        )
    )


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    database_url = os.environ.get("CHAOSAGENT_DATABASE_URL", "")
    try:
        result = run_flagship_demo(
            database_url,
            _repository_root(Path.cwd()),
            run_id=arguments.run_id,
            dashboard_base_url=arguments.dashboard_base_url,
        )
    except FlagshipDemoError as error:
        print(f"ChaosAgent flagship demo failed: {error}", file=sys.stderr)
        return 1
    except Exception:
        print("ChaosAgent flagship demo failed safely: internal error", file=sys.stderr)
        return 1
    print(render_success(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
