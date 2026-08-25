from __future__ import annotations

from dataclasses import replace
from importlib.resources import files
from pathlib import Path
from typing import cast

import pytest
from chaosagent_persistence import LeaseIdentity
from chaosagent_tool_gateway import (
    ORDERS_GET_V0,
    PAYMENTS_REFUND_V0,
    SCENARIO_V0_SCHEMA_VERSION,
    SCENARIO_V0_TOOL_VERSIONS,
    SHIPPING_GET_STATUS_V0,
    SUPPORT_UPDATE_TICKET_V0,
    ToolGateway,
    ToolRegistry,
    default_tool_registry,
)
from jsonschema import Draft202012Validator  # type: ignore[import-untyped]
from sqlalchemy.orm import Session


def test_default_registry_is_explicit_deterministic_and_valid() -> None:
    first = default_tool_registry()
    second = default_tool_registry()

    assert [(item.tool_id, item.contract_version) for item in first.definitions] == [
        ("orders.get", ORDERS_GET_V0),
        ("payments.refund", PAYMENTS_REFUND_V0),
        ("shipping.get_status", SHIPPING_GET_STATUS_V0),
        ("support.update_ticket", SUPPORT_UPDATE_TICKET_V0),
    ]
    assert first.definitions == second.definitions
    for definition in first.definitions:
        assert definition.read_only is (definition.capability == "read")
        # Registry construction already checks each immutable bundled schema.
        assert definition.input_schema["type"] == "object"
        assert definition.output_schema["type"] == "object"


def test_registry_rejects_duplicate_exact_identity() -> None:
    definition = default_tool_registry().definitions[0]
    try:
        ToolRegistry((definition, definition))
    except ValueError as error:
        assert "duplicate tool definition" in str(error)
    else:
        raise AssertionError("duplicate registry identity was accepted")


def test_registry_schema_metadata_is_recursively_immutable() -> None:
    definition = default_tool_registry().definitions[0]
    properties = cast(dict[str, object], definition.input_schema["properties"])
    with pytest.raises(TypeError):
        properties["order_id"] = {}


def test_scenario_v0_has_a_frozen_tool_v0_compatibility_mapping() -> None:
    assert SCENARIO_V0_SCHEMA_VERSION == "chaosagent.scenario/v0"
    assert dict(SCENARIO_V0_TOOL_VERSIONS) == {
        "orders.get": ORDERS_GET_V0,
        "payments.refund": PAYMENTS_REFUND_V0,
        "shipping.get_status": SHIPPING_GET_STATUS_V0,
        "support.update_ticket": SUPPORT_UPDATE_TICKET_V0,
    }
    with pytest.raises(TypeError):
        cast(dict[str, str], SCENARIO_V0_TOOL_VERSIONS)["orders.get"] = (
            "chaosagent.tool/orders.get/v1"
        )


def test_invalid_unknown_and_wrong_version_calls_fail_before_database_access() -> None:
    gateway = ToolGateway(Session())
    lease = _unused_lease()

    unknown = gateway.execute(
        lease,
        tool_id="unknown.read",
        contract_version="chaosagent.tool/unknown.read/v0",
        arguments={},
        logical_call_id="logical-1",
        attempt_id="attempt-1",
    )
    wrong_version = gateway.execute(
        lease,
        tool_id="orders.get",
        contract_version="chaosagent.tool/orders.get/v1",
        arguments={"order_id": "ORD-1007"},
        logical_call_id="logical-1",
        attempt_id="attempt-1",
    )
    missing = gateway.execute(
        lease,
        tool_id="orders.get",
        contract_version=ORDERS_GET_V0,
        arguments={},
        logical_call_id="logical-1",
        attempt_id="attempt-1",
    )
    extra = gateway.execute(
        lease,
        tool_id="orders.get",
        contract_version=ORDERS_GET_V0,
        arguments={"order_id": "ORD-1007", "secret": "no"},
        logical_call_id="logical-1",
        attempt_id="attempt-1",
    )
    assert unknown.error is not None and unknown.error.code == "unsupported_tool"
    assert wrong_version.error is not None and wrong_version.error.code == "unsupported_tool"
    for result in (missing, extra):
        assert result.error is not None and result.error.code == "invalid_request"
        assert result.request_event_id is None


def test_positive_physical_attempt_number_is_structurally_accepted() -> None:
    result = ToolGateway(Session()).execute(
        _unused_lease(),
        tool_id="orders.get",
        contract_version=ORDERS_GET_V0,
        arguments={"order_id": "ORD-1007"},
        logical_call_id="logical-1",
        attempt_id="attempt-2",
        attempt_number=2,
    )
    assert result.error is not None
    assert result.error.code == "infrastructure_error"


@pytest.mark.parametrize(
    ("logical_call_id", "attempt_id", "attempt_number"),
    [
        (None, "attempt-1", 1),
        ("logical-1", None, 1),
        ("", "attempt-1", 1),
        ("logical-1", "", 1),
        ("   ", "attempt-1", 1),
        ("logical-1", "   ", 1),
        ("logical/1", "attempt-1", 1),
        ("logical-1", "attempt/1", 1),
        ("logical-1", "attempt-1", 0),
        ("logical-1", "attempt-1", True),
        ("logical-1", "attempt-1", "1"),
        ("logical-1", "attempt-1", 9_007_199_254_740_992),
    ],
)
def test_malformed_runtime_invocation_identity_is_structured_invalid_request(
    logical_call_id: object, attempt_id: object, attempt_number: object
) -> None:
    result = ToolGateway(Session()).execute(
        _unused_lease(),
        tool_id="orders.get",
        contract_version=ORDERS_GET_V0,
        arguments={"order_id": "ORD-1007"},
        logical_call_id=logical_call_id,
        attempt_id=attempt_id,
        attempt_number=attempt_number,
    )
    assert result.error is not None and result.error.code == "invalid_request"
    assert result.request_event_id is None


def test_other_dynamic_gateway_fields_are_runtime_validated() -> None:
    gateway = ToolGateway(Session())
    common = {
        "arguments": {"order_id": "ORD-1007"},
        "logical_call_id": "logical-1",
        "attempt_id": "attempt-1",
    }
    results = (
        gateway.execute(object(), tool_id="orders.get", contract_version=ORDERS_GET_V0, **common),
        gateway.execute(_unused_lease(), tool_id=[], contract_version=ORDERS_GET_V0, **common),
        gateway.execute(_unused_lease(), tool_id="orders.get", contract_version=object(), **common),
        gateway.execute(
            _unused_lease(),
            tool_id="orders.get",
            contract_version=ORDERS_GET_V0,
            step_id="bad/id",
            **common,
        ),
        gateway.execute(
            replace(_unused_lease(), attempt=0),
            tool_id="orders.get",
            contract_version=ORDERS_GET_V0,
            **common,
        ),
        gateway.execute(
            replace(_unused_lease(), worker_id="bad/worker"),
            tool_id="orders.get",
            contract_version=ORDERS_GET_V0,
            **common,
        ),
    )
    for result in results:
        assert result.error is not None and result.error.code == "invalid_request"


@pytest.mark.parametrize(
    ("component", "instance"),
    [
        ("", None),
        ("Tool-Gateway", None),
        ("tool gateway", None),
        ("tool-gateway", ""),
        ("tool-gateway", "bad/id"),
    ],
)
def test_invalid_producer_configuration_fails_at_construction(
    component: str, instance: str | None
) -> None:
    with pytest.raises(ValueError, match="Event v0"):
        ToolGateway(Session(), producer_component=component, producer_instance_id=instance)


def test_golden_read_only_calls_use_registered_contracts() -> None:
    root = Path(__file__).resolve().parents[2]
    golden = root / "benchmarks/shipment-refund/tools/v0/read-only-calls.json"
    assert golden.is_file()
    mutation_golden = root / "benchmarks/shipment-refund/tools/v0/mutation-calls.json"
    assert mutation_golden.is_file()


def test_all_tool_schema_resources_are_packaged() -> None:
    resources = files("chaosagent_tool_gateway.schema")
    for name in (
        "orders-get-v0.input.schema.json",
        "orders-get-v0.output.schema.json",
        "shipping-get-status-v0.input.schema.json",
        "shipping-get-status-v0.output.schema.json",
        "payments-refund-v0.input.schema.json",
        "payments-refund-v0.output.schema.json",
        "support-update-ticket-v0.input.schema.json",
        "support-update-ticket-v0.output.schema.json",
    ):
        assert resources.joinpath(name).is_file()


def test_mutation_contracts_are_strict_and_use_integer_money() -> None:
    definitions = {item.tool_id: item for item in default_tool_registry().definitions}
    refund = definitions["payments.refund"]
    ticket = definitions["support.update_ticket"]
    valid_refund = {
        "order_id": "ORD-1007",
        "payment_id": "PAY-1007",
        "amount_minor": 12999,
        "reason": "Shipment failed",
        "idempotency_key": "refund-ord-1007",
    }
    assert not list(Draft202012Validator(dict(refund.input_schema)).iter_errors(valid_refund))
    assert list(
        Draft202012Validator(dict(refund.input_schema)).iter_errors(
            {**valid_refund, "amount_minor": 12.99}
        )
    )
    assert list(
        Draft202012Validator(dict(refund.input_schema)).iter_errors(
            {**valid_refund, "unexpected": True}
        )
    )
    assert not list(
        Draft202012Validator(dict(ticket.input_schema)).iter_errors(
            {
                "ticket_id": "TKT-204",
                "status": "closed",
                "note": "Refund completed.",
                "idempotency_key": "ticket-ord-1007",
            }
        )
    )
    wrong_version = ToolGateway(Session()).execute(
        _unused_lease(),
        tool_id="payments.refund",
        contract_version="chaosagent.tool/payments.refund/v1",
        arguments=valid_refund,
        logical_call_id="logical-refund",
        attempt_id="attempt-refund",
    )
    assert wrong_version.error is not None
    assert wrong_version.error.code == "unsupported_tool"


def _unused_lease() -> LeaseIdentity:
    return LeaseIdentity("run-unused", "worker-unused", "lease-unused", 1)


def invalid_output_registry() -> ToolRegistry:
    definition = default_tool_registry().definitions[0]
    return ToolRegistry(
        (
            replace(
                definition,
                handler=lambda _company, _arguments: {"bad": True},
            ),
        )
    )
