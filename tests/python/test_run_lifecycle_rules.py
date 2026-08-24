from __future__ import annotations

import pytest
from chaosagent_persistence import (
    IllegalRunTransitionError,
    legal_targets,
)
from chaosagent_persistence.lifecycle import (
    require_claim_transition,
    require_owned_transition,
    require_recovery_transition,
    require_unleased_transition,
)


def test_normal_lifecycle_graph_is_small_and_terminal_states_have_no_exits() -> None:
    assert legal_targets("queued") == frozenset({"provisioning", "cancelled"})
    assert legal_targets("provisioning") == frozenset(
        {"running", "failed", "timed_out", "cancelled", "infra_error"}
    )
    assert legal_targets("running") == frozenset(
        {"evaluating", "failed", "timed_out", "cancelled", "infra_error"}
    )
    assert legal_targets("evaluating") == frozenset(
        {"completed", "failed", "timed_out", "cancelled", "infra_error"}
    )
    for terminal in ("completed", "failed", "timed_out", "cancelled", "infra_error"):
        assert legal_targets(terminal) == frozenset()


def test_operation_specific_transition_guards() -> None:
    require_claim_transition("queued", "provisioning")
    require_unleased_transition("queued", "cancelled")
    require_owned_transition("provisioning", "running")
    require_owned_transition("running", "evaluating")
    require_owned_transition("evaluating", "completed")
    require_recovery_transition("running", "queued")

    with pytest.raises(IllegalRunTransitionError):
        require_owned_transition("provisioning", "evaluating")
    with pytest.raises(IllegalRunTransitionError):
        require_owned_transition("completed", "running")
    with pytest.raises(IllegalRunTransitionError):
        require_unleased_transition("running", "cancelled")
    with pytest.raises(IllegalRunTransitionError):
        require_recovery_transition("completed", "queued")
