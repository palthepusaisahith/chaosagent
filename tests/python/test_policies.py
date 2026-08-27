from __future__ import annotations

import json
from pathlib import Path

import pytest
from chaosagent_policies import (
    POLICY_V0_SCHEMA_VERSION,
    PolicyValidationError,
    evaluate_policy_v0,
    load_policy,
    loads_policy,
)

ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = ROOT / "benchmarks/shipment-refund/policies/refund-policy.v0.json"


def test_golden_policy_is_valid_stable_and_immutable() -> None:
    policy = load_policy(POLICY_PATH)
    document = policy.to_dict()
    reordered = json.dumps(dict(reversed(document.items())), separators=(",", ":"))

    assert document["schema_version"] == POLICY_V0_SCHEMA_VERSION
    assert loads_policy(reordered).digest == policy.digest
    assert (
        policy.digest == "sha256:5a0c2127ac8f2cd2f29cbf50d2475a6387005db0fbc240e2bf2417d536e4f354"
    )
    document["policy_id"] = "changed"
    assert policy.to_dict()["policy_id"] == "fake-company.refund-policy"


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.update({"schema_version": "chaosagent.policy/v1"}),
        lambda value: value.update({"unknown": True}),
        lambda value: value["tools"]["payments.refund"].update(
            {"automatic_max_minor": 12000, "approval_max_minor": 5000}
        ),
    ],
)
def test_invalid_policy_fails_closed(mutation: object) -> None:
    document = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    mutation(document)  # type: ignore[operator]
    with pytest.raises(PolicyValidationError):
        loads_policy(json.dumps(document))


def test_policy_decisions_and_boundaries_are_deterministic() -> None:
    policy = load_policy(POLICY_PATH)
    base = {"amount_minor": 1}
    for tool, version in (
        ("orders.get", "chaosagent.tool/orders.get/v0"),
        ("shipping.get_status", "chaosagent.tool/shipping.get_status/v0"),
        ("support.update_ticket", "chaosagent.tool/support.update_ticket/v0"),
    ):
        assert (
            evaluate_policy_v0(
                policy, tool_id=tool, contract_version=version, arguments={}
            ).decision
            == "allow"
        )
    expected = {5000: "allow", 5001: "require_approval", 12000: "require_approval", 12001: "deny"}
    for amount, outcome in expected.items():
        arguments = dict(base, amount_minor=amount)
        first = evaluate_policy_v0(
            policy,
            tool_id="payments.refund",
            contract_version="chaosagent.tool/payments.refund/v0",
            arguments=arguments,
            payment_currency="USD",
        )
        second = evaluate_policy_v0(
            policy,
            tool_id="payments.refund",
            contract_version="chaosagent.tool/payments.refund/v0",
            arguments=arguments,
            payment_currency="USD",
        )
        assert first == second
        assert first.decision == outcome


def test_policy_evaluator_rejects_non_integer_refund_amount() -> None:
    with pytest.raises(PolicyValidationError):
        evaluate_policy_v0(
            load_policy(POLICY_PATH),
            tool_id="payments.refund",
            contract_version="chaosagent.tool/payments.refund/v0",
            arguments={"amount_minor": 5000.0},
            payment_currency="USD",
        )
