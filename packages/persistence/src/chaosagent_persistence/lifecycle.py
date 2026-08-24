"""Centralized Run v0 lifecycle transition rules."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Literal, cast

type RunStatus = Literal[
    "queued",
    "provisioning",
    "running",
    "evaluating",
    "completed",
    "failed",
    "timed_out",
    "cancelled",
    "infra_error",
]

RUN_STATUSES: tuple[RunStatus, ...] = (
    "queued",
    "provisioning",
    "running",
    "evaluating",
    "completed",
    "failed",
    "timed_out",
    "cancelled",
    "infra_error",
)
ACTIVE_STATUSES: frozenset[RunStatus] = frozenset({"provisioning", "running", "evaluating"})
TERMINAL_STATUSES: frozenset[RunStatus] = frozenset(
    {"completed", "failed", "timed_out", "cancelled", "infra_error"}
)

_NORMAL_TRANSITIONS: Mapping[RunStatus, frozenset[RunStatus]] = MappingProxyType(
    {
        "queued": frozenset({"provisioning", "cancelled"}),
        "provisioning": frozenset({"running", "failed", "timed_out", "cancelled", "infra_error"}),
        "running": frozenset({"evaluating", "failed", "timed_out", "cancelled", "infra_error"}),
        "evaluating": frozenset({"completed", "failed", "timed_out", "cancelled", "infra_error"}),
        "completed": frozenset(),
        "failed": frozenset(),
        "timed_out": frozenset(),
        "cancelled": frozenset(),
        "infra_error": frozenset(),
    }
)


class IllegalRunTransitionError(ValueError):
    """Raised when a requested Run lifecycle edge is not legal."""


def parse_run_status(value: str) -> RunStatus:
    """Return a typed status or fail closed for an unknown persisted value."""
    if value not in RUN_STATUSES:
        raise ValueError(f"unknown run status {value!r}")
    return cast(RunStatus, value)


def require_claim_transition(source: RunStatus, target: RunStatus) -> None:
    """Validate the queue claim edge, which may only establish provisioning."""
    if source != "queued" or target != "provisioning":
        raise IllegalRunTransitionError(f"claim cannot transition {source!r} to {target!r}")


def require_owned_transition(source: RunStatus, target: RunStatus) -> None:
    """Validate a transition performed by the current lease holder."""
    if source not in ACTIVE_STATUSES or target not in _NORMAL_TRANSITIONS[source]:
        raise IllegalRunTransitionError(
            f"lease holder cannot transition run from {source!r} to {target!r}"
        )


def require_unleased_transition(source: RunStatus, target: RunStatus) -> None:
    """Validate the sole control-plane transition that needs no worker lease."""
    if source != "queued" or target != "cancelled":
        raise IllegalRunTransitionError(
            f"unleased transition from {source!r} to {target!r} is not legal"
        )


def require_recovery_transition(source: RunStatus, target: RunStatus) -> None:
    """Validate recovery of an expired active lease back to the queue."""
    if source not in ACTIVE_STATUSES or target != "queued":
        raise IllegalRunTransitionError(
            f"lease recovery cannot transition {source!r} to {target!r}"
        )


def legal_targets(source: RunStatus) -> frozenset[RunStatus]:
    """Expose immutable normal transition targets for documentation/tests."""
    return _NORMAL_TRANSITIONS[source]
