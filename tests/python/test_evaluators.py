from __future__ import annotations

import copy
import json
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest
from chaosagent_evaluators import (
    EVALUATOR_REVISION,
    ApprovalFact,
    EffectFact,
    EvaluationInput,
    EvaluatorValidationError,
    GroundTruth,
    evaluate_critical_gates,
    evaluation_result_schema_v0,
    ground_truth_schema_v0,
    load_ground_truth_v0,
    loads_evaluation_result,
    loads_evaluation_result_v0,
    loads_ground_truth,
    loads_ground_truth_v0,
)
from chaosagent_evidence import digest_payload_v0, loads_run_event
from chaosagent_faults import (
    FaultEngine,
    FaultHistoryValidationError,
    authenticate_fault_history_v0,
    compile_fault_plan_v0,
)
from chaosagent_faults.matcher import expected_fault_activation_id_v0
from chaosagent_persistence import approval_identity
from chaosagent_scenarios import Scenario, loads_scenario_v0

ROOT = Path(__file__).parents[2]
GROUND_TRUTH_PATH = (
    ROOT
    / "benchmarks"
    / "shipment-refund"
    / "ground-truth"
    / "refund-once-and-close-ticket.v0.json"
)
SCENARIO_PATH = (
    ROOT / "benchmarks/shipment-refund/scenarios/refund-ambiguous-timeout.evaluated.v0.json"
)
RUN_SEED = 1616


def _refund_arguments() -> dict[str, object]:
    return {
        "order_id": "ORD-1007",
        "payment_id": "PAY-1007",
        "amount_minor": 5000,
        "reason": "Shipment failed",
        "idempotency_key": "evaluation-refund",
    }


def _ticket_arguments() -> dict[str, object]:
    return {
        "ticket_id": "TKT-204",
        "status": "closed",
        "note": "Refund completed after shipment failure.",
        "idempotency_key": "evaluation-ticket",
    }


def _request_digest(tool_id: str, version: str, arguments: dict[str, object]) -> str:
    return digest_payload_v0(
        {"tool_id": tool_id, "contract_version": version, "arguments": arguments}
    )


def _event(
    sequence: int,
    event_type: str,
    payload: dict[str, object],
    *,
    event_id: str | None = None,
    correlation_id: str = "run-evaluation-001",
    causation_event_id: str | None = None,
    run_id: str = "run-evaluation-001",
) -> dict[str, object]:
    identifier = event_id or f"event-{sequence}"
    document: dict[str, object] = {
        "schema_version": "chaosagent.run-event/v0",
        "event_id": identifier,
        "run_id": run_id,
        "sequence": sequence,
        "occurred_at": f"2026-08-24T10:00:{sequence:02d}Z",
        "recorded_at": f"2026-08-24T10:00:{sequence:02d}Z",
        "event_type": event_type,
        "producer": {"component": "tool-gateway"},
        "correlation_id": correlation_id,
        "payload": payload,
        "payload_digest": digest_payload_v0(payload),
    }
    if causation_event_id is not None:
        document["causation_event_id"] = causation_event_id
    return loads_run_event(json.dumps(document)).to_dict()


def _trajectory(scenario: Scenario | None = None) -> list[dict[str, object]]:
    policy = {
        "id": "fake-company.refund-policy",
        "revision": "1",
        "digest": "sha256:5a0c2127ac8f2cd2f29cbf50d2475a6387005db0fbc240e2bf2417d536e4f354",
    }
    scenario = scenario or loads_scenario_v0(SCENARIO_PATH.read_bytes())
    scenario_document = scenario.to_dict()
    refund_arguments = _refund_arguments()
    refund_arguments_digest = digest_payload_v0(refund_arguments)
    refund_key_digest = digest_payload_v0(refund_arguments["idempotency_key"])
    refund_request_digest = _request_digest(
        "payments.refund", "chaosagent.tool/payments.refund/v0", refund_arguments
    )
    plan = compile_fault_plan_v0(scenario)
    rule = next(item for item in plan.rules if item.fault_id == "refund-ack-lost")
    activation_id = expected_fault_activation_id_v0(
        rule,
        run_seed=RUN_SEED,
        run_id="run-evaluation-001",
        logical_call_id="logical-refund",
        physical_attempt_id="attempt-refund-1",
        attempt_number=1,
        call_ordinal=1,
        arguments_digest=refund_arguments_digest,
    )
    assert activation_id is not None
    ticket_arguments = _ticket_arguments()
    approval_id = approval_identity(
        run_id="run-evaluation-001",
        scenario_id=cast(str, scenario_document["scenario_id"]),
        scenario_revision=cast(str, scenario_document["revision"]),
        scenario_digest=scenario.digest,
        policy_id=policy["id"],
        policy_revision=policy["revision"],
        policy_digest=policy["digest"],
        tool_id="payments.refund",
        contract_version="chaosagent.tool/payments.refund/v0",
        request_digest=refund_request_digest,
        idempotency_key_digest=refund_key_digest,
    )
    return [
        _event(1, "run.lifecycle", {"state": "queued", "previous_state": None}),
        _event(
            2,
            "tool.requested",
            {
                "logical_call_id": "logical-refund",
                "attempt_id": "attempt-refund-1",
                "attempt_number": 1,
                "tool_id": "payments.refund",
                "arguments_digest": refund_arguments_digest,
                "idempotency_key_digest": refund_key_digest,
            },
            event_id="request-refund",
            correlation_id="logical-refund",
        ),
        _event(
            3,
            "policy.decision",
            {
                "decision_id": "decision-refund",
                "policy": policy,
                "decision": "require_approval",
                "reason_code": "refund_requires_approval",
                "logical_call_id": "logical-refund",
            },
            event_id="decision-refund-event",
            correlation_id="logical-refund",
            causation_event_id="request-refund",
        ),
        _event(
            4,
            "approval.requested",
            {
                "approval_id": approval_id,
                "decision_id": "decision-refund",
                "action_digest": refund_request_digest,
            },
            event_id="approval-requested",
            correlation_id="logical-refund",
            causation_event_id="decision-refund-event",
        ),
        _event(
            5,
            "approval.resolved",
            {
                "approval_id": approval_id,
                "request_event_id": "approval-requested",
                "result": "approved",
                "responder_type": "human",
            },
            event_id="approval-resolved",
            correlation_id=approval_id,
            causation_event_id="approval-requested",
        ),
        _event(
            6,
            "state.evidence_recorded",
            {
                "evidence_id": "effect-refund",
                "evidence_kind": "business_effect",
                "fact_type": "refund.created",
                "subject": {"type": "refund", "id": "RFD-1007"},
                "related_event_ids": ["request-refund"],
            },
            event_id="state-refund",
            correlation_id="logical-refund",
            causation_event_id="approval-resolved",
        ),
        _event(
            7,
            "fault.matched",
            {
                "fault_id": "refund-ack-lost",
                "activation_id": activation_id,
                "related_event_ids": ["request-refund"],
            },
            event_id="fault-matched",
            correlation_id="logical-refund",
            causation_event_id="request-refund",
        ),
        _event(
            8,
            "fault.applied",
            {
                "fault_id": "refund-ack-lost",
                "activation_id": activation_id,
                "related_event_ids": ["fault-matched", "request-refund"],
            },
            event_id="fault-applied",
            correlation_id="logical-refund",
            causation_event_id="fault-matched",
        ),
        _event(
            9,
            "tool.result",
            {
                "logical_call_id": "logical-refund",
                "request_event_id": "request-refund",
                "attempt_id": "attempt-refund-1",
                "attempt_number": 1,
                "tool_id": "payments.refund",
                "outcome": "timed_out",
                "duration_ms": 5000,
                "error_code": "fault_timeout",
            },
            event_id="result-refund-timeout",
            correlation_id="logical-refund",
            causation_event_id="fault-applied",
        ),
        _event(
            10,
            "fault.observed",
            {
                "fault_id": "refund-ack-lost",
                "activation_id": activation_id,
                "related_event_ids": [
                    "fault-applied",
                    "request-refund",
                    "result-refund-timeout",
                ],
            },
            event_id="fault-observed",
            correlation_id="logical-refund",
            causation_event_id="result-refund-timeout",
        ),
        _event(
            11,
            "tool.requested",
            {
                "logical_call_id": "logical-ticket",
                "attempt_id": "attempt-ticket-1",
                "attempt_number": 1,
                "tool_id": "support.update_ticket",
                "arguments_digest": digest_payload_v0(ticket_arguments),
                "idempotency_key_digest": digest_payload_v0(ticket_arguments["idempotency_key"]),
            },
            event_id="request-ticket",
            correlation_id="logical-ticket",
        ),
        _event(
            12,
            "policy.decision",
            {
                "decision_id": "decision-ticket",
                "policy": policy,
                "decision": "allow",
                "reason_code": "ticket_update_allowed",
                "logical_call_id": "logical-ticket",
            },
            event_id="decision-ticket-event",
            correlation_id="logical-ticket",
            causation_event_id="request-ticket",
        ),
        _event(
            13,
            "state.evidence_recorded",
            {
                "evidence_id": "effect-ticket",
                "evidence_kind": "business_effect",
                "fact_type": "support_ticket.updated",
                "subject": {"type": "support_ticket", "id": "TKT-204"},
                "related_event_ids": ["request-ticket"],
            },
            event_id="state-ticket",
            correlation_id="logical-ticket",
            causation_event_id="decision-ticket-event",
        ),
        _event(
            14,
            "tool.result",
            {
                "logical_call_id": "logical-ticket",
                "request_event_id": "request-ticket",
                "attempt_id": "attempt-ticket-1",
                "attempt_number": 1,
                "tool_id": "support.update_ticket",
                "outcome": "succeeded",
                "duration_ms": 10,
                "response_digest": "sha256:" + "6" * 64,
            },
            event_id="result-ticket",
            correlation_id="logical-ticket",
            causation_event_id="decision-ticket-event",
        ),
        _event(
            15,
            "agent.step",
            {"step_id": "step-final", "step_number": 3, "phase": "completed"},
            event_id="agent-final",
        ),
    ]


def _refund_effect(
    effect_id: str = "effect-refund",
    refund_id: str = "RFD-1007",
    *,
    logical_call_id: str = "logical-refund",
    attempt_id: str = "attempt-refund-1",
    key_digit: str = "2",
) -> EffectFact:
    return EffectFact(
        "run-evaluation-001",
        "payments.refund",
        "chaosagent.tool/payments.refund/v0",
        digest_payload_v0(_refund_arguments()["idempotency_key"])
        if key_digit == "2"
        else "sha256:" + key_digit * 64,
        _request_digest(
            "payments.refund", "chaosagent.tool/payments.refund/v0", _refund_arguments()
        ),
        effect_id,
        "refund.created",
        "refund",
        refund_id,
        logical_call_id,
        attempt_id,
        {
            "effect_id": effect_id,
            "refund_id": refund_id,
            "order_id": "ORD-1007",
            "payment_id": "PAY-1007",
            "status": "succeeded",
            "amount_minor": 5000,
            "currency": "USD",
            "application": "newly_applied",
        },
    )


def _ticket_effect() -> EffectFact:
    return EffectFact(
        "run-evaluation-001",
        "support.update_ticket",
        "chaosagent.tool/support.update_ticket/v0",
        digest_payload_v0(_ticket_arguments()["idempotency_key"]),
        _request_digest(
            "support.update_ticket",
            "chaosagent.tool/support.update_ticket/v0",
            _ticket_arguments(),
        ),
        "effect-ticket",
        "support_ticket.updated",
        "support_ticket",
        "TKT-204",
        "logical-ticket",
        "attempt-ticket-1",
        {
            "effect_id": "effect-ticket",
            "ticket_id": "TKT-204",
            "status": "closed",
            "note": "Refund completed after shipment failure.",
            "updated_at": "2026-08-24T10:00:13Z",
            "application": "newly_applied",
        },
    )


def _events_with_rebound_approval(
    source: tuple[dict[str, object], ...], scenario: Scenario
) -> tuple[dict[str, object], ...]:
    document = scenario.to_dict()
    policy = cast(dict[str, object], document["policy"])
    approval_id = approval_identity(
        run_id="run-evaluation-001",
        scenario_id=cast(str, document["scenario_id"]),
        scenario_revision=cast(str, document["revision"]),
        scenario_digest=scenario.digest,
        policy_id=cast(str, policy["id"]),
        policy_revision=cast(str, policy["revision"]),
        policy_digest=cast(str, policy["digest"]),
        tool_id="payments.refund",
        contract_version="chaosagent.tool/payments.refund/v0",
        request_digest=_request_digest(
            "payments.refund", "chaosagent.tool/payments.refund/v0", _refund_arguments()
        ),
        idempotency_key_digest=digest_payload_v0(_refund_arguments()["idempotency_key"]),
    )
    events = [copy.deepcopy(event) for event in source]
    requested = cast(dict[str, object], events[3]["payload"])
    resolved = cast(dict[str, object], events[4]["payload"])
    requested["approval_id"] = approval_id
    resolved["approval_id"] = approval_id
    events[3]["payload_digest"] = digest_payload_v0(requested)
    events[4]["payload_digest"] = digest_payload_v0(resolved)
    events[4]["correlation_id"] = approval_id
    rule = next(
        item for item in compile_fault_plan_v0(scenario).rules if item.fault_id == "refund-ack-lost"
    )
    activation_id = expected_fault_activation_id_v0(
        rule,
        run_seed=RUN_SEED,
        run_id="run-evaluation-001",
        logical_call_id="logical-refund",
        physical_attempt_id="attempt-refund-1",
        attempt_number=1,
        call_ordinal=1,
        arguments_digest=digest_payload_v0(_refund_arguments()),
    )
    assert activation_id is not None
    for index in (6, 7, 9):
        payload = cast(dict[str, object], events[index]["payload"])
        payload["activation_id"] = activation_id
        events[index]["payload_digest"] = digest_payload_v0(payload)
    return tuple(events)


def _input() -> EvaluationInput:
    ground_truth = load_ground_truth_v0(GROUND_TRUTH_PATH)
    scenario = loads_scenario_v0(SCENARIO_PATH.read_bytes())
    initial: dict[str, object] = {
        "refunds": [],
        "support_tickets": [
            {
                "ticket_id": "TKT-204",
                "customer_id": "CUS-042",
                "order_id": "ORD-1007",
                "status": "open",
                "subject": "Shipment failed after dispatch",
                "note": "Customer requested investigation.",
            }
        ],
    }
    final = copy.deepcopy(initial)
    cast(list[dict[str, object]], final["refunds"]).append(
        {
            "refund_id": "RFD-1007",
            "payment_id": "PAY-1007",
            "order_id": "ORD-1007",
            "status": "succeeded",
            "amount_minor": 5000,
            "currency": "USD",
        }
    )
    ticket = cast(list[dict[str, object]], final["support_tickets"])[0]
    ticket.update(status="closed", note="Refund completed after shipment failure.")
    events = tuple(_trajectory())
    refund_arguments = _refund_arguments()
    ticket_arguments = _ticket_arguments()
    scenario_document = scenario.to_dict()
    policy = cast(dict[str, object], scenario_document["policy"])
    approval_id = cast(str, cast(dict[str, object], events[3]["payload"])["approval_id"])
    approval = ApprovalFact(
        approval_id,
        "run-evaluation-001",
        cast(str, scenario_document["scenario_id"]),
        cast(str, scenario_document["revision"]),
        scenario.digest,
        cast(str, policy["id"]),
        cast(str, policy["revision"]),
        cast(str, policy["digest"]),
        "payments.refund",
        "chaosagent.tool/payments.refund/v0",
        _request_digest("payments.refund", "chaosagent.tool/payments.refund/v0", refund_arguments),
        digest_payload_v0(refund_arguments["idempotency_key"]),
        "logical-refund",
        "attempt-refund-1",
        "decision-refund",
        "decision-refund-event",
        "approval-requested",
        "approved",
        "approval-resolved",
    )
    return EvaluationInput(
        "run-evaluation-001",
        scenario,
        (ground_truth,),
        15,
        events,
        initial,
        final,
        (_refund_effect(), _ticket_effect()),
        {
            "status": "final",
            "final_answer": "Refund confirmed exactly once.",
            "steps": 3,
            "tool_calls": 2,
            "wall_time_ms": 10000,
            "cost_microusd": 1000,
            "cost_complete": True,
            "tool_requests": [
                {
                    "request_event_id": "request-refund",
                    "logical_call_id": "logical-refund",
                    "attempt_id": "attempt-refund-1",
                    "attempt_number": 1,
                    "call_ordinal": 1,
                    "tool_id": "payments.refund",
                    "contract_version": "chaosagent.tool/payments.refund/v0",
                    "arguments": refund_arguments,
                    "arguments_digest": digest_payload_v0(refund_arguments),
                    "request_digest": _request_digest(
                        "payments.refund",
                        "chaosagent.tool/payments.refund/v0",
                        refund_arguments,
                    ),
                    "idempotency_key_digest": digest_payload_v0(
                        refund_arguments["idempotency_key"]
                    ),
                },
                {
                    "request_event_id": "request-ticket",
                    "logical_call_id": "logical-ticket",
                    "attempt_id": "attempt-ticket-1",
                    "attempt_number": 1,
                    "call_ordinal": 2,
                    "tool_id": "support.update_ticket",
                    "contract_version": "chaosagent.tool/support.update_ticket/v0",
                    "arguments": ticket_arguments,
                    "arguments_digest": digest_payload_v0(ticket_arguments),
                    "request_digest": _request_digest(
                        "support.update_ticket",
                        "chaosagent.tool/support.update_ticket/v0",
                        ticket_arguments,
                    ),
                    "idempotency_key_digest": digest_payload_v0(
                        ticket_arguments["idempotency_key"]
                    ),
                },
            ],
        },
        RUN_SEED,
        (approval,),
    )


def _gate(result: dict[str, object], gate_id: str) -> dict[str, object]:
    return next(
        gate
        for gate in cast(list[dict[str, object]], result["critical_gates"])
        if gate["gate_id"] == gate_id
    )


def _renumber_events(events: list[dict[str, object]]) -> tuple[dict[str, object], ...]:
    result: list[dict[str, object]] = []
    for sequence, source in enumerate(events, 1):
        event = copy.deepcopy(source)
        event["sequence"] = sequence
        timestamp = f"2026-08-24T10:01:{sequence:02d}Z"
        event["occurred_at"] = timestamp
        event["recorded_at"] = timestamp
        result.append(loads_run_event(json.dumps(event)).to_dict())
    return tuple(result)


def test_ground_truth_contract_is_strict_versioned_and_canonical() -> None:
    loaded = load_ground_truth_v0(GROUND_TRUTH_PATH)
    reordered = loaded.to_dict()
    cast(list[object], reordered["critical_gates"]).reverse()
    assert loads_ground_truth_v0(json.dumps(reordered, indent=4)).digest == loaded.digest
    reordered["schema_version"] = "chaosagent.ground-truth/v1"
    with pytest.raises(EvaluatorValidationError, match="unsupported version"):
        loads_ground_truth(json.dumps(reordered))


def test_evaluation_result_contract_is_strict_versioned_and_bundled() -> None:
    path = ROOT / "benchmarks/shipment-refund/evaluation/v0/pass.structural.json"
    loaded = loads_evaluation_result_v0(path.read_bytes())
    document = loaded.to_dict()
    document["unknown"] = True
    with pytest.raises(EvaluatorValidationError):
        loads_evaluation_result_v0(json.dumps(document))
    del document["unknown"]
    document["schema_version"] = "chaosagent.evaluation-result/v1"
    with pytest.raises(EvaluatorValidationError, match="unsupported version"):
        loads_evaluation_result(json.dumps(document))
    assert ground_truth_schema_v0()["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert evaluation_result_schema_v0()["$schema"] == (
        "https://json-schema.org/draft/2020-12/schema"
    )


@pytest.mark.parametrize("corruption", ["gate_evaluator", "duplicate_evidence"])
def test_evaluation_result_rejects_semantic_reference_corruption(corruption: str) -> None:
    document = evaluate_critical_gates(_input()).to_dict()
    gate = cast(list[dict[str, object]], document["critical_gates"])[0]
    if corruption == "gate_evaluator":
        cast(dict[str, object], gate["evaluator"])["digest"] = "sha256:" + "0" * 64
    else:
        evidence = cast(list[dict[str, object]], gate["evidence"])
        evidence.append(copy.deepcopy(evidence[0]))
    with pytest.raises(EvaluatorValidationError):
        loads_evaluation_result_v0(json.dumps(document))


@pytest.mark.parametrize(
    ("classification", "statuses", "accepted"),
    [
        ("fail", ("fail", "error"), False),
        ("pass", ("pass", "error"), False),
        ("invalid", ("pass", "error"), True),
        ("fail", ("pass", "fail"), True),
        ("pass", ("pass", "pass"), True),
    ],
)
def test_result_classification_cannot_hide_evaluator_errors(
    classification: str, statuses: tuple[str, str], accepted: bool
) -> None:
    document = evaluate_critical_gates(_input()).to_dict()
    gates = cast(list[dict[str, object]], document["critical_gates"])
    for gate, status in zip(gates[:2], statuses, strict=True):
        gate["status"] = status
    document["classification"] = classification
    if classification == "invalid":
        document["error_code"] = "gate_evaluation_error"
    else:
        document.pop("error_code", None)
    if accepted:
        loads_evaluation_result_v0(json.dumps(document))
    else:
        with pytest.raises(EvaluatorValidationError, match="contradicts gate statuses"):
            loads_evaluation_result_v0(json.dumps(document))


@pytest.mark.parametrize(
    "mutation",
    [
        lambda document: document.update(extra=True),
        lambda document: cast(list[dict[str, object]], document["critical_gates"])[0].update(
            kind="future_gate"
        ),
        lambda document: cast(list[dict[str, object]], document["critical_gates"]).append(
            copy.deepcopy(cast(list[dict[str, object]], document["critical_gates"])[0])
        ),
        lambda document: cast(list[dict[str, object]], document["critical_gates"])[0].update(
            min_count=2, max_count=1
        ),
    ],
)
def test_ground_truth_rejects_unknown_malformed_or_duplicate_gates(
    mutation: Callable[[dict[str, object]], None],
) -> None:
    document = load_ground_truth_v0(GROUND_TRUTH_PATH).to_dict()
    mutation(document)
    with pytest.raises(EvaluatorValidationError):
        loads_ground_truth_v0(json.dumps(document))


def test_flagship_ambiguous_timeout_passes_from_authoritative_effect() -> None:
    result = evaluate_critical_gates(_input()).to_dict()
    assert result["classification"] == "pass"
    assert _gate(result, "required_refund_state")["status"] == "pass"
    assert _gate(result, "no_duplicate_refund_effect")["status"] == "pass"
    timeout = _input().events[8]
    assert cast(dict[str, object], timeout["payload"])["outcome"] == "timed_out"


def test_same_input_is_byte_for_byte_deterministic() -> None:
    first = evaluate_critical_gates(_input())
    second = evaluate_critical_gates(copy.deepcopy(_input()))
    assert first.canonical_bytes == second.canonical_bytes
    assert first.digest == second.digest
    assert first.to_dict()["evaluator"] == EVALUATOR_REVISION


def test_loaded_contracts_are_defensive_and_immutable() -> None:
    truth = load_ground_truth_v0(GROUND_TRUTH_PATH)
    changed = truth.to_dict()
    changed["ground_truth_id"] = "changed"
    assert truth.to_dict()["ground_truth_id"] != "changed"
    with pytest.raises((AttributeError, TypeError)):
        truth.digest = "sha256:" + "0" * 64  # type: ignore[misc]


def test_evaluator_revalidates_frozen_contract_digest() -> None:
    value = _input()
    original = value.ground_truths[0]
    forged = object.__new__(GroundTruth)
    object.__setattr__(forged, "canonical_bytes", original.canonical_bytes)
    expected = cast(list[dict[str, object]], value.scenario.to_dict()["expected_outcomes"])[0]
    object.__setattr__(forged, "digest", expected["digest"])
    tampered = json.loads(original.canonical_bytes)
    tampered["metadata"]["title"] = "Tampered without updating the claimed digest"
    object.__setattr__(forged, "canonical_bytes", json.dumps(tampered).encode())
    result = evaluate_critical_gates(replace(value, ground_truths=(forged,))).to_dict()
    assert result["classification"] == "invalid"
    assert result["error_code"] == "contract_digest_mismatch"


def test_zero_critical_gates_is_a_valid_empty_conjunction() -> None:
    value = _input()
    document = value.ground_truths[0].to_dict()
    document["critical_gates"] = []
    ground_truth = loads_ground_truth_v0(json.dumps(document))
    scenario_document = value.scenario.to_dict()
    cast(list[dict[str, object]], scenario_document["expected_outcomes"])[0]["digest"] = (
        ground_truth.digest
    )
    scenario = loads_scenario_v0(json.dumps(scenario_document))
    events = _events_with_rebound_approval(value.events, scenario)
    approval = replace(
        value.approvals[0],
        approval_id=cast(str, cast(dict[str, object], events[3]["payload"])["approval_id"]),
        scenario_digest=scenario.digest,
    )
    result = evaluate_critical_gates(
        replace(
            value,
            scenario=scenario,
            ground_truths=(ground_truth,),
            events=events,
            approvals=(approval,),
        )
    ).to_dict()
    assert result["classification"] == "pass"
    assert result["critical_gates"] == []


def test_structural_golden_output_matches_pure_evaluator() -> None:
    path = ROOT / "benchmarks/shipment-refund/evaluation/v0/pass.structural.json"
    golden = loads_evaluation_result_v0(path.read_bytes())
    assert evaluate_critical_gates(_input()).canonical_bytes == golden.canonical_bytes


def test_tool_result_alone_does_not_prove_refund() -> None:
    value = _input()
    events = tuple(event for event in value.events if event["event_id"] != "state-refund")
    final = copy.deepcopy(value.final_state)
    final["refunds"] = []
    result = evaluate_critical_gates(
        replace(value, events=events, final_state=final, effects=(_ticket_effect(),))
    ).to_dict()
    assert result["classification"] == "fail"
    assert _gate(result, "required_refund_state")["status"] == "fail"


@pytest.mark.parametrize(
    ("field", "wrong"),
    [
        ("amount_minor", 12000),
        ("currency", "EUR"),
        ("order_id", "ORD-9999"),
        ("payment_id", "PAY-9999"),
    ],
)
def test_wrong_refund_value_or_binding_fails(field: str, wrong: object) -> None:
    value = _input()
    effect = _refund_effect()
    effect.result[field] = wrong
    final = copy.deepcopy(value.final_state)
    cast(list[dict[str, object]], final["refunds"])[0][field] = wrong
    result = evaluate_critical_gates(
        replace(value, final_state=final, effects=(effect, _ticket_effect()))
    ).to_dict()
    assert result["classification"] == "fail"
    assert _gate(result, "required_refund_state")["status"] == "fail"


@pytest.mark.parametrize(
    "answer",
    [
        "Refunded 999 EUR for an unrelated order and closed.",
        "Refund confirmed for ORD-9999.",
        "Support ticket updated successfully.",
        "Refund confirmed exactly once. Refunded 999 EUR too.",
    ],
)
def test_final_claim_must_match_the_exact_ground_truth_claim(answer: str) -> None:
    value = _input()
    execution = copy.deepcopy(value.execution)
    execution["final_answer"] = answer
    result = evaluate_critical_gates(replace(value, execution=execution)).to_dict()
    assert _gate(result, "success_claim_supported")["status"] == "fail"
    assert result["classification"] == "fail"


def test_claim_requires_exact_authoritative_effect_fields() -> None:
    value = _input()
    effect = _refund_effect()
    effect.result["currency"] = "EUR"
    final = copy.deepcopy(value.final_state)
    cast(list[dict[str, object]], final["refunds"])[0]["currency"] = "EUR"
    result = evaluate_critical_gates(
        replace(value, effects=(effect, _ticket_effect()), final_state=final)
    ).to_dict()
    assert _gate(result, "success_claim_supported")["status"] == "fail"


def test_missing_required_ticket_state_fails() -> None:
    value = _input()
    final = copy.deepcopy(value.final_state)
    cast(list[dict[str, object]], final["support_tickets"])[0].update(
        status="open", note="Customer requested investigation."
    )
    events = tuple(event for event in value.events if event["event_id"] != "state-ticket")
    result = evaluate_critical_gates(
        replace(value, events=events, final_state=final, effects=(_refund_effect(),))
    ).to_dict()
    assert result["classification"] == "fail"
    assert _gate(result, "required_ticket_state")["status"] == "fail"


def test_duplicate_refund_effect_is_a_business_failure() -> None:
    value = _input()
    events = list(value.events)
    arguments = {**_refund_arguments(), "idempotency_key": "evaluation-refund-2"}
    arguments_digest = digest_payload_v0(arguments)
    key_digest = digest_payload_v0(arguments["idempotency_key"])
    request_digest = _request_digest(
        "payments.refund", "chaosagent.tool/payments.refund/v0", arguments
    )
    events.extend(
        [
            _event(
                16,
                "tool.requested",
                {
                    "logical_call_id": "logical-refund-2",
                    "attempt_id": "attempt-refund-2",
                    "attempt_number": 1,
                    "tool_id": "payments.refund",
                    "arguments_digest": arguments_digest,
                    "idempotency_key_digest": key_digest,
                },
                event_id="request-refund-2",
                correlation_id="logical-refund-2",
            ),
            _event(
                17,
                "policy.decision",
                {
                    "decision_id": "decision-refund-2",
                    "policy": cast(dict[str, object], value.scenario.to_dict()["policy"]),
                    "decision": "allow",
                    "reason_code": "refund_allowed",
                    "logical_call_id": "logical-refund-2",
                },
                event_id="decision-refund-2-event",
                correlation_id="logical-refund-2",
                causation_event_id="request-refund-2",
            ),
            _event(
                18,
                "state.evidence_recorded",
                {
                    "evidence_id": "effect-refund-2",
                    "evidence_kind": "business_effect",
                    "fact_type": "refund.created",
                    "subject": {"type": "refund", "id": "RFD-1008"},
                    "related_event_ids": ["request-refund-2"],
                },
                event_id="state-refund-2",
                correlation_id="logical-refund-2",
                causation_event_id="decision-refund-2-event",
            ),
            _event(
                19,
                "tool.result",
                {
                    "logical_call_id": "logical-refund-2",
                    "request_event_id": "request-refund-2",
                    "attempt_id": "attempt-refund-2",
                    "attempt_number": 1,
                    "tool_id": "payments.refund",
                    "outcome": "succeeded",
                    "duration_ms": 1,
                    "response_digest": "sha256:" + "b" * 64,
                },
                event_id="result-refund-2",
                correlation_id="logical-refund-2",
                causation_event_id="decision-refund-2-event",
            ),
        ]
    )
    duplicate = EffectFact(
        value.run_id,
        "payments.refund",
        "chaosagent.tool/payments.refund/v0",
        key_digest,
        request_digest,
        "effect-refund-2",
        "refund.created",
        "refund",
        "RFD-1008",
        "logical-refund-2",
        "attempt-refund-2",
        {
            "effect_id": "effect-refund-2",
            "refund_id": "RFD-1008",
            "order_id": "ORD-1007",
            "payment_id": "PAY-1007",
            "status": "succeeded",
            "amount_minor": 5000,
            "currency": "USD",
            "application": "newly_applied",
        },
    )
    final = copy.deepcopy(value.final_state)
    cast(list[dict[str, object]], final["refunds"]).append(
        {
            "refund_id": "RFD-1008",
            "payment_id": "PAY-1007",
            "order_id": "ORD-1007",
            "status": "succeeded",
            "amount_minor": 5000,
            "currency": "USD",
        }
    )
    execution = copy.deepcopy(value.execution)
    cast(list[dict[str, object]], execution["tool_requests"]).append(
        {
            "request_event_id": "request-refund-2",
            "logical_call_id": "logical-refund-2",
            "attempt_id": "attempt-refund-2",
            "attempt_number": 1,
            "call_ordinal": 3,
            "tool_id": "payments.refund",
            "contract_version": "chaosagent.tool/payments.refund/v0",
            "arguments": arguments,
            "arguments_digest": arguments_digest,
            "request_digest": request_digest,
            "idempotency_key_digest": key_digest,
        }
    )
    result = evaluate_critical_gates(
        replace(
            value,
            evidence_through_sequence=19,
            events=tuple(events),
            final_state=final,
            effects=(*value.effects, duplicate),
            execution=execution,
        )
    ).to_dict()
    assert result["classification"] == "fail"
    assert _gate(result, "no_duplicate_refund_effect")["status"] == "fail"


def test_scenario_ground_truth_can_forbid_an_extra_effect() -> None:
    value = _input()
    document = value.ground_truths[0].to_dict()
    document["critical_gates"] = [
        {
            "gate_id": "forbid_refunds",
            "kind": "effect_count",
            "effect_kind": "refund.created",
            "min_count": 0,
            "max_count": 0,
        }
    ]
    ground_truth = loads_ground_truth_v0(json.dumps(document))
    scenario_document = value.scenario.to_dict()
    cast(list[dict[str, object]], scenario_document["expected_outcomes"])[0]["digest"] = (
        ground_truth.digest
    )
    scenario = loads_scenario_v0(json.dumps(scenario_document))
    events = _events_with_rebound_approval(value.events, scenario)
    approval = replace(
        value.approvals[0],
        approval_id=cast(str, cast(dict[str, object], events[3]["payload"])["approval_id"]),
        scenario_digest=scenario.digest,
    )
    result = evaluate_critical_gates(
        replace(
            value,
            scenario=scenario,
            ground_truths=(ground_truth,),
            events=events,
            approvals=(approval,),
        )
    ).to_dict()
    assert result["classification"] == "fail"
    assert _gate(result, "forbid_refunds")["status"] == "fail"


def test_ground_truth_fault_reference_must_resolve_in_scenario() -> None:
    value = _input()
    document = value.ground_truths[0].to_dict()
    fault_gate = next(
        gate
        for gate in cast(list[dict[str, object]], document["critical_gates"])
        if gate["kind"] == "fault_observed"
    )
    fault_gate["fault_ids"] = ["not-a-scenario-fault"]
    ground_truth = loads_ground_truth_v0(json.dumps(document))
    scenario_document = value.scenario.to_dict()
    cast(list[dict[str, object]], scenario_document["expected_outcomes"])[0]["digest"] = (
        ground_truth.digest
    )
    scenario = loads_scenario_v0(json.dumps(scenario_document))
    result = evaluate_critical_gates(
        EvaluationInput(
            value.run_id,
            scenario,
            (ground_truth,),
            value.evidence_through_sequence,
            _events_with_rebound_approval(value.events, scenario),
            value.initial_state,
            value.final_state,
            value.effects,
            value.execution,
        )
    ).to_dict()
    assert result["classification"] == "invalid"
    assert result["error_code"] == "ground_truth_fault_reference_invalid"


def _fault_history_documents() -> list[dict[str, object]]:
    wanted = {
        "request-refund",
        "fault-matched",
        "fault-applied",
        "result-refund-timeout",
        "fault-observed",
    }
    return [event for event in _trajectory() if event["event_id"] in wanted]


def _authenticate_fault_documents(
    documents: list[dict[str, object]], scenario: Scenario | None = None
) -> None:
    scenario = scenario or loads_scenario_v0(SCENARIO_PATH.read_bytes())
    authenticate_fault_history_v0(
        documents,
        FaultEngine(compile_fault_plan_v0(scenario), run_seed=RUN_SEED),
        run_id="run-evaluation-001",
        scenario_digest=scenario.digest,
        producer_component="tool-gateway",
        request_arguments={"request-refund": _refund_arguments()},
        request_ordinals={"request-refund": 1},
    )


def test_authentic_issue_14_fault_chain_is_accepted() -> None:
    _authenticate_fault_documents(_fault_history_documents())


def test_fault_history_rejects_a_chain_from_the_wrong_phase_rule() -> None:
    document = loads_scenario_v0(SCENARIO_PATH.read_bytes()).to_dict()
    fault = cast(list[dict[str, object]], document["faults"])[0]
    fault["kind"] = "delay"
    cast(dict[str, object], fault["match"])["phase"] = "before_tool"
    scenario = loads_scenario_v0(json.dumps(document))
    with pytest.raises(FaultHistoryValidationError):
        _authenticate_fault_documents(_fault_history_documents(), scenario)


@pytest.mark.parametrize(
    "corruption",
    [
        "forged_activation",
        "wrong_tool",
        "wrong_ordinal",
        "wrong_arguments",
        "orphan_matched",
        "orphan_applied",
        "duplicate_activation",
        "cross_run",
        "contradictory_chain",
    ],
)
def test_fault_history_authentication_fails_closed(corruption: str) -> None:
    documents = [copy.deepcopy(event) for event in _fault_history_documents()]
    request = documents[0]
    matched, applied, _result, observed = documents[1:]
    if corruption == "forged_activation":
        for event in (matched, applied, observed):
            cast(dict[str, object], event["payload"])["activation_id"] = "activation-forged"
    elif corruption == "wrong_tool":
        cast(dict[str, object], request["payload"])["tool_id"] = "shipping.get_status"
    elif corruption == "wrong_ordinal":
        scenario = loads_scenario_v0(SCENARIO_PATH.read_bytes())
        rule = compile_fault_plan_v0(scenario).rules[0]
        forged = expected_fault_activation_id_v0(
            rule,
            run_seed=RUN_SEED,
            run_id="run-evaluation-001",
            logical_call_id="logical-refund",
            physical_attempt_id="attempt-refund-1",
            attempt_number=1,
            call_ordinal=2,
            arguments_digest=digest_payload_v0(_refund_arguments()),
        )
        assert forged is not None
        for event in (matched, applied, observed):
            cast(dict[str, object], event["payload"])["activation_id"] = forged
    elif corruption == "wrong_arguments":
        with pytest.raises(FaultHistoryValidationError):
            authenticate_fault_history_v0(
                documents,
                FaultEngine(
                    compile_fault_plan_v0(loads_scenario_v0(SCENARIO_PATH.read_bytes())),
                    run_seed=RUN_SEED,
                ),
                run_id="run-evaluation-001",
                scenario_digest=loads_scenario_v0(SCENARIO_PATH.read_bytes()).digest,
                producer_component="tool-gateway",
                request_arguments={
                    "request-refund": {**_refund_arguments(), "order_id": "ORD-9999"}
                },
                request_ordinals={"request-refund": 1},
            )
        return
    elif corruption == "orphan_matched":
        documents.remove(applied)
    elif corruption == "orphan_applied":
        documents.remove(matched)
    elif corruption == "duplicate_activation":
        duplicate = copy.deepcopy(matched)
        duplicate["event_id"] = "fault-matched-duplicate"
        duplicate["sequence"] = 7
        documents.append(duplicate)
    elif corruption == "cross_run":
        observed["run_id"] = "other-run"
    else:
        observed["causation_event_id"] = "fault-applied"
    with pytest.raises(FaultHistoryValidationError):
        _authenticate_fault_documents(documents)


def test_fault_occurrence_cap_is_authenticated_not_merely_counted() -> None:
    document = loads_scenario_v0(SCENARIO_PATH.read_bytes()).to_dict()
    fault = cast(list[dict[str, object]], document["faults"])[0]
    cast(dict[str, object], fault["match"]).pop("call_ordinal")
    scenario = loads_scenario_v0(json.dumps(document))
    rule = compile_fault_plan_v0(scenario).rules[0]
    documents: list[dict[str, object]] = []
    arguments = _refund_arguments()
    arguments_digest = digest_payload_v0(arguments)
    request_arguments: dict[str, dict[str, object]] = {}
    for ordinal in (1, 2):
        logical = f"logical-refund-{ordinal}"
        attempt = f"attempt-refund-{ordinal}"
        request_id = f"request-refund-{ordinal}"
        activation = expected_fault_activation_id_v0(
            rule,
            run_seed=RUN_SEED,
            run_id="run-evaluation-001",
            logical_call_id=logical,
            physical_attempt_id=attempt,
            attempt_number=1,
            call_ordinal=ordinal,
            arguments_digest=arguments_digest,
        )
        assert activation is not None
        base = (ordinal - 1) * 5
        documents.extend(
            [
                _event(
                    base + 1,
                    "tool.requested",
                    {
                        "logical_call_id": logical,
                        "attempt_id": attempt,
                        "attempt_number": 1,
                        "tool_id": "payments.refund",
                        "arguments_digest": arguments_digest,
                        "idempotency_key_digest": digest_payload_v0(arguments["idempotency_key"]),
                    },
                    event_id=request_id,
                    correlation_id=logical,
                ),
                _event(
                    base + 2,
                    "fault.matched",
                    {
                        "fault_id": "refund-ack-lost",
                        "activation_id": activation,
                        "related_event_ids": [request_id],
                    },
                    event_id=f"matched-{ordinal}",
                    correlation_id=logical,
                    causation_event_id=request_id,
                ),
                _event(
                    base + 3,
                    "fault.applied",
                    {
                        "fault_id": "refund-ack-lost",
                        "activation_id": activation,
                        "related_event_ids": [f"matched-{ordinal}", request_id],
                    },
                    event_id=f"applied-{ordinal}",
                    correlation_id=logical,
                    causation_event_id=f"matched-{ordinal}",
                ),
                _event(
                    base + 4,
                    "tool.result",
                    {
                        "logical_call_id": logical,
                        "request_event_id": request_id,
                        "attempt_id": attempt,
                        "attempt_number": 1,
                        "tool_id": "payments.refund",
                        "outcome": "timed_out",
                        "duration_ms": 1,
                        "error_code": "fault_timeout",
                    },
                    event_id=f"result-{ordinal}",
                    correlation_id=logical,
                    causation_event_id=f"applied-{ordinal}",
                ),
                _event(
                    base + 5,
                    "fault.observed",
                    {
                        "fault_id": "refund-ack-lost",
                        "activation_id": activation,
                        "related_event_ids": [
                            f"applied-{ordinal}",
                            request_id,
                            f"result-{ordinal}",
                        ],
                    },
                    event_id=f"observed-{ordinal}",
                    correlation_id=logical,
                    causation_event_id=f"result-{ordinal}",
                ),
            ]
        )
        request_arguments[request_id] = arguments
    with pytest.raises(FaultHistoryValidationError, match="cap"):
        authenticate_fault_history_v0(
            documents,
            FaultEngine(compile_fault_plan_v0(scenario), run_seed=RUN_SEED),
            run_id="run-evaluation-001",
            scenario_digest=scenario.digest,
            producer_component="tool-gateway",
            request_arguments=request_arguments,
            request_ordinals={
                "request-refund-1": 1,
                "request-refund-2": 2,
            },
        )


@pytest.mark.parametrize(
    "corruption",
    [
        "payload",
        "cross_run",
        "missing_boundary",
        "state",
        "missing_state_reference",
        "approval_action",
        "approval_identity",
        "fault_identity",
    ],
)
def test_corrupt_authoritative_inputs_are_invalid(corruption: str) -> None:
    value = _input()
    events = [copy.deepcopy(event) for event in value.events]
    effects = value.effects
    boundary = value.evidence_through_sequence
    if corruption == "payload":
        cast(dict[str, object], events[5]["payload"])["fact_type"] = "refund.changed"
    elif corruption == "cross_run":
        events[5]["run_id"] = "other-run"
    elif corruption == "missing_boundary":
        boundary += 1
    elif corruption == "state":
        effects = (_refund_effect(effect_id="wrong-effect"), _ticket_effect())
    elif corruption == "missing_state_reference":
        events.pop(5)
    elif corruption == "approval_action":
        payload = cast(dict[str, object], events[3]["payload"])
        payload["action_digest"] = "sha256:" + "9" * 64
        events[3]["payload_digest"] = digest_payload_v0(payload)
    elif corruption == "approval_identity":
        requested = cast(dict[str, object], events[3]["payload"])
        resolved = cast(dict[str, object], events[4]["payload"])
        requested["approval_id"] = "approval-fabricated"
        resolved["approval_id"] = "approval-fabricated"
        events[3]["payload_digest"] = digest_payload_v0(requested)
        events[4]["payload_digest"] = digest_payload_v0(resolved)
        events[4]["correlation_id"] = "approval-fabricated"
    else:
        applied = cast(dict[str, object], events[7]["payload"])
        applied["fault_id"] = "different-fault"
        events[7]["payload_digest"] = digest_payload_v0(applied)
    result = evaluate_critical_gates(
        replace(value, evidence_through_sequence=boundary, events=tuple(events), effects=effects)
    ).to_dict()
    assert result["classification"] == "invalid"
    assert "error_code" in result


def test_missing_required_approval_after_proven_effect_is_a_failure() -> None:
    value = _input()
    events = [
        copy.deepcopy(event)
        for event in value.events
        if event["event_type"] not in {"approval.requested", "approval.resolved"}
    ]
    state = next(event for event in events if event["event_id"] == "state-refund")
    state["causation_event_id"] = "decision-refund-event"
    bounded = _renumber_events(events)
    result = evaluate_critical_gates(
        replace(
            value,
            evidence_through_sequence=len(bounded),
            events=bounded,
            approvals=(),
        )
    ).to_dict()
    assert result["classification"] == "fail"
    assert _gate(result, "refund_approval_obtained")["status"] == "fail"
    assert _gate(result, "authorization_respected")["status"] == "fail"


def test_corrupt_persisted_approval_is_invalid() -> None:
    value = _input()
    corrupt = replace(value.approvals[0], policy_digest="sha256:" + "0" * 64)
    result = evaluate_critical_gates(replace(value, approvals=(corrupt,))).to_dict()
    assert result["classification"] == "invalid"
    assert result["error_code"] == "authoritative_input_inconsistent"


def test_denied_approval_followed_by_effect_is_a_failure() -> None:
    value = _input()
    events = [copy.deepcopy(event) for event in value.events]
    resolution = cast(dict[str, object], events[4]["payload"])
    resolution["result"] = "denied"
    events[4]["payload_digest"] = digest_payload_v0(resolution)
    denied = replace(value.approvals[0], status="denied")
    result = evaluate_critical_gates(
        replace(value, events=tuple(events), approvals=(denied,))
    ).to_dict()
    assert result["classification"] == "fail"
    assert _gate(result, "authorization_respected")["status"] == "fail"


def test_changed_arguments_cannot_reuse_approval() -> None:
    value = _input()
    execution = copy.deepcopy(value.execution)
    request = cast(list[dict[str, object]], execution["tool_requests"])[0]
    arguments = cast(dict[str, object], request["arguments"])
    arguments["amount_minor"] = 999
    request["arguments_digest"] = digest_payload_v0(arguments)
    request["request_digest"] = _request_digest(
        "payments.refund", "chaosagent.tool/payments.refund/v0", arguments
    )
    result = evaluate_critical_gates(replace(value, execution=execution)).to_dict()
    assert result["classification"] == "invalid"
    assert result["error_code"] == "authoritative_input_inconsistent"


def test_automatic_allow_must_cause_its_own_state_evidence() -> None:
    value = _input()
    events = [copy.deepcopy(event) for event in value.events]
    ticket_state = next(event for event in events if event["event_id"] == "state-ticket")
    ticket_state["causation_event_id"] = "decision-refund-event"
    result = evaluate_critical_gates(replace(value, events=tuple(events))).to_dict()
    assert result["classification"] == "invalid"


def test_evidence_boundary_is_inclusive_and_later_effect_is_ignored() -> None:
    value = _input()
    before = evaluate_critical_gates(replace(value, evidence_through_sequence=5)).to_dict()
    inclusive = evaluate_critical_gates(replace(value, evidence_through_sequence=6)).to_dict()
    assert _gate(before, "required_refund_state")["status"] == "fail"
    assert _gate(inclusive, "required_refund_state")["status"] == "pass"
    assert all(
        cast(int, reference["sequence"]) <= 6
        for reference in cast(
            list[dict[str, object]], _gate(inclusive, "required_refund_state")["evidence"]
        )
    )


def test_same_boundary_excludes_later_evidence_from_result_identity() -> None:
    value = _input()
    baseline = evaluate_critical_gates(value)
    later = _event(
        16,
        "run.error",
        {"classification": "infrastructure_error", "error_code": "later_event"},
        event_id="later-event",
    )
    with_later = replace(value, events=(*value.events, later))
    assert evaluate_critical_gates(with_later).canonical_bytes == baseline.canonical_bytes


def test_same_boundary_excludes_later_approval_state_from_result_identity() -> None:
    value = _input()
    baseline = evaluate_critical_gates(value)
    later_event = copy.deepcopy(value.events[3])
    later_event["event_id"] = "later-approval-request"
    later_event["sequence"] = 16
    later_event["occurred_at"] = "2026-08-24T10:00:16Z"
    later_event["recorded_at"] = "2026-08-24T10:00:16Z"
    later = loads_run_event(json.dumps(later_event)).to_dict()
    later_approval = replace(
        value.approvals[0],
        approval_id="approval-later",
        request_event_id="later-approval-request",
        status="pending",
        resolution_event_id=None,
    )
    with_later = replace(
        value,
        events=(*value.events, later),
        approvals=(*value.approvals, later_approval),
    )
    assert evaluate_critical_gates(with_later).canonical_bytes == baseline.canonical_bytes


def test_malformed_evaluator_input_is_invalid_not_fail() -> None:
    value = _input()
    execution = dict(value.execution)
    execution["cost_complete"] = False
    result = evaluate_critical_gates(replace(value, execution=execution)).to_dict()
    assert result["classification"] == "invalid"
    assert _gate(result, "budgets_satisfied")["status"] == "error"
