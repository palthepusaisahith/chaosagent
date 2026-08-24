from __future__ import annotations

import json
from copy import deepcopy
from importlib.resources import files
from pathlib import Path
from typing import cast

import pytest
from chaosagent_evidence import (
    RUN_EVENT_V0_SCHEMA_VERSION,
    EvidenceValidationError,
    RunEvent,
    RunReport,
    canonicalize_run_event,
    digest_payload_v0,
    load_run_event,
    load_run_report,
    loads_run_event,
    loads_run_report,
    run_event_schema,
    run_report_schema,
    validate_run_event_stream_v0,
    validate_run_event_v0,
    validate_run_report_v0,
    validate_run_report_with_events_v0,
)
from chaosagent_scenarios import load_scenario

ROOT = Path(__file__).parents[2]
EXAMPLES = ROOT / "benchmarks" / "shipment-refund" / "evidence" / "v0"
SCENARIO_EXAMPLE = (
    ROOT / "benchmarks" / "shipment-refund" / "scenarios" / "refund-ambiguous-timeout.v0.json"
)
ZERO_DIGEST = "sha256:" + "0" * 64


def _revision(identifier: str = "example.ref") -> dict[str, object]:
    return {"id": identifier, "revision": "1", "digest": ZERO_DIGEST}


PAYLOADS: dict[str, dict[str, object]] = {
    "run.lifecycle": {"state": "running", "previous_state": "provisioning"},
    "agent.step": {
        "step_id": "step-01",
        "step_number": 1,
        "phase": "completed",
        "model_call_id": "model-call-01",
        "model": {"provider": "provider-neutral", "requested_model": "example-model"},
    },
    "tool.requested": {
        "logical_call_id": "logical-01",
        "attempt_id": "attempt-01",
        "attempt_number": 1,
        "step_id": "step-01",
        "tool_id": "payments.refund",
        "arguments_digest": ZERO_DIGEST,
    },
    "tool.result": {
        "logical_call_id": "logical-01",
        "request_event_id": "evt-01",
        "attempt_id": "attempt-01",
        "attempt_number": 1,
        "tool_id": "payments.refund",
        "outcome": "succeeded",
        "duration_ms": 10,
    },
    "fault.not_matched": {"fault_id": "fault-01", "reason_code": "predicate_false"},
    "fault.matched": {
        "fault_id": "fault-01",
        "activation_id": "activation-01",
        "related_event_ids": ["evt-01"],
    },
    "fault.applied": {
        "fault_id": "fault-01",
        "activation_id": "activation-01",
        "related_event_ids": ["evt-01"],
    },
    "fault.observed": {
        "fault_id": "fault-01",
        "activation_id": "activation-01",
        "related_event_ids": ["evt-01"],
    },
    "state.evidence_recorded": {
        "evidence_id": "effect-01",
        "evidence_kind": "business_effect",
        "fact_type": "refund.created",
        "subject": {"type": "order", "id": "ORD-1007"},
        "related_event_ids": ["evt-01"],
    },
    "policy.decision": {
        "decision_id": "decision-01",
        "policy": _revision("policy.ref"),
        "decision": "allow",
        "reason_code": "within_limit",
    },
    "approval.requested": {
        "approval_id": "approval-01",
        "decision_id": "decision-01",
        "action_digest": ZERO_DIGEST,
    },
    "approval.resolved": {
        "approval_id": "approval-01",
        "request_event_id": "evt-01",
        "result": "approved",
        "responder_type": "human",
    },
    "evaluation.started": {
        "evaluation_id": "evaluation-01",
        "evaluator": _revision("evaluator.ref"),
        "evidence_through_sequence": 1,
    },
    "evaluation.result_recorded": {
        "evaluation_id": "evaluation-01",
        "evaluator": _revision("evaluator.ref"),
        "outcome": "completed",
        "evidence_through_sequence": 1,
    },
    "run.error": {"classification": "infrastructure_error", "error_code": "worker_lost"},
}


def _event(event_type: str = "run.lifecycle", sequence: int = 1) -> dict[str, object]:
    payload = deepcopy(PAYLOADS[event_type])
    return {
        "schema_version": RUN_EVENT_V0_SCHEMA_VERSION,
        "event_id": f"evt-{sequence:02d}",
        "run_id": "run-01",
        "sequence": sequence,
        "occurred_at": "2026-08-24T10:00:00Z",
        "recorded_at": "2026-08-24T10:00:01Z",
        "event_type": event_type,
        "producer": {"component": "test-producer"},
        "correlation_id": "run-01",
        "payload": payload,
        "payload_digest": digest_payload_v0(payload),
    }


def _report() -> dict[str, object]:
    return load_run_report(EXAMPLES / "run-report.json").to_dict()


@pytest.mark.parametrize("event_type", sorted(PAYLOADS))
def test_accepts_each_event_variant(event_type: str) -> None:
    validate_run_event_v0(_event(event_type))


def test_golden_stream_and_report_are_valid() -> None:
    events = [load_run_event(path).to_dict() for path in sorted(EXAMPLES.glob("[0-9]*.json"))]
    validate_run_event_stream_v0(events, complete=True)
    validate_run_report_with_events_v0(_report(), events)


def test_golden_report_references_the_real_scenario_example_digest() -> None:
    report = _report()
    scenario = cast(dict[str, object], report["scenario"])
    assert scenario["digest"] == load_scenario(SCENARIO_EXAMPLE).digest


def test_golden_refund_gate_uses_authoritative_state_evidence() -> None:
    report = _report()
    gate = cast(list[dict[str, object]], report["critical_gates"])[0]
    reference = cast(list[dict[str, object]], gate["evidence"])[0]
    event = load_run_event(EXAMPLES / "005-state-evidence.json").to_dict()
    assert reference["event_id"] == event["event_id"]
    assert event["event_type"] == "state.evidence_recorded"


def test_discriminator_cannot_be_paired_with_another_payload() -> None:
    event = _event("tool.result")
    event["event_type"] = "tool.requested"
    with pytest.raises(EvidenceValidationError):
        validate_run_event_v0(event)


@pytest.mark.parametrize("field", ["event_id", "run_id", "sequence", "payload_digest"])
def test_event_rejects_missing_required_fields(field: str) -> None:
    event = _event()
    del event[field]
    with pytest.raises(EvidenceValidationError):
        validate_run_event_v0(event)


def test_event_rejects_unknown_properties_at_every_level() -> None:
    event = _event()
    cast(dict[str, object], event["payload"])["surprise"] = True
    with pytest.raises(EvidenceValidationError):
        validate_run_event_v0(event)


@pytest.mark.parametrize(
    ("field", "value"),
    [("sequence", 0), ("occurred_at", "2026-08-24 10:00:00"), ("event_id", "bad id")],
)
def test_event_rejects_invalid_sequence_timestamp_and_reference(field: str, value: object) -> None:
    event = _event()
    event[field] = value
    with pytest.raises(EvidenceValidationError):
        validate_run_event_v0(event)


def test_event_rejects_payload_digest_mismatch() -> None:
    event = _event()
    event["payload_digest"] = ZERO_DIGEST
    with pytest.raises(EvidenceValidationError, match="does not match payload"):
        validate_run_event_v0(event)


def test_agent_step_does_not_require_a_model_invocation() -> None:
    event = _event("agent.step")
    payload = cast(dict[str, object], event["payload"])
    del payload["model_call_id"]
    del payload["model"]
    event["payload_digest"] = digest_payload_v0(payload)
    validate_run_event_v0(event)

    payload["model_call_id"] = "orphan-model-call"
    event["payload_digest"] = digest_payload_v0(payload)
    with pytest.raises(EvidenceValidationError, match="model"):
        validate_run_event_v0(event)


def test_evaluation_result_enforces_outcome_specific_fields() -> None:
    event = _event("evaluation.result_recorded")
    payload = cast(dict[str, object], event["payload"])
    payload["outcome"] = "error"
    event["payload_digest"] = digest_payload_v0(payload)
    with pytest.raises(EvidenceValidationError, match="error_code"):
        validate_run_event_v0(event)


def test_stream_rejects_duplicate_ids_wrong_run_and_nonmonotonic_order() -> None:
    first = _event(sequence=2)
    second = _event(sequence=1)
    second["event_id"] = first["event_id"]
    second["run_id"] = "run-02"
    with pytest.raises(EvidenceValidationError) as raised:
        validate_run_event_stream_v0([first, second])
    message = str(raised.value)
    assert "same run_id" in message
    assert "duplicate event_id" in message
    assert "greater than" in message


def test_stream_allows_sequence_gaps() -> None:
    validate_run_event_stream_v0([_event(sequence=1), _event(sequence=3)])


def test_complete_stream_starts_at_one_and_duplicate_sequences_fail() -> None:
    with pytest.raises(EvidenceValidationError, match="must begin at sequence 1"):
        validate_run_event_stream_v0([_event(sequence=2)], complete=True)

    first = _event(sequence=1)
    duplicate = _event(sequence=1)
    duplicate["event_id"] = "evt-duplicate-sequence"
    with pytest.raises(EvidenceValidationError, match="greater than"):
        validate_run_event_stream_v0([first, duplicate])


def _tool_attempt_events() -> tuple[dict[str, object], dict[str, object]]:
    request = _event("tool.requested", sequence=1)
    result = _event("tool.result", sequence=2)
    payload = cast(dict[str, object], result["payload"])
    payload["request_event_id"] = request["event_id"]
    result["causation_event_id"] = request["event_id"]
    result["payload_digest"] = digest_payload_v0(payload)
    return request, result


def test_stream_binds_tool_result_to_its_request() -> None:
    request, result = _tool_attempt_events()
    validate_run_event_stream_v0([request, result], complete=True)

    for field in ("logical_call_id", "attempt_id", "attempt_number", "tool_id"):
        changed = deepcopy(result)
        payload = cast(dict[str, object], changed["payload"])
        payload[field] = 2 if field == "attempt_number" else f"different-{field}"
        changed["payload_digest"] = digest_payload_v0(payload)
        with pytest.raises(EvidenceValidationError, match=field):
            validate_run_event_stream_v0([request, changed], complete=True)


def test_stream_rejects_orphan_results_and_duplicate_physical_attempts() -> None:
    request, result = _tool_attempt_events()
    orphan = deepcopy(result)
    orphan_payload = cast(dict[str, object], orphan["payload"])
    orphan_payload["request_event_id"] = "evt-missing"
    orphan["payload_digest"] = digest_payload_v0(orphan_payload)
    with pytest.raises(EvidenceValidationError, match="no preceding tool request"):
        validate_run_event_stream_v0([request, orphan])

    duplicate_request = _event("tool.requested", sequence=2)
    with pytest.raises(EvidenceValidationError, match="duplicate tool request attempt_id"):
        validate_run_event_stream_v0([request, duplicate_request])

    duplicate_result = deepcopy(result)
    duplicate_result["event_id"] = "evt-03"
    duplicate_result["sequence"] = 3
    with pytest.raises(EvidenceValidationError, match="duplicate tool result attempt_id"):
        validate_run_event_stream_v0([request, result, duplicate_result])


def test_utc_is_required_but_timestamp_order_does_not_order_events() -> None:
    offset = _event()
    offset["occurred_at"] = "2026-08-24T15:30:00+05:30"
    with pytest.raises(EvidenceValidationError):
        validate_run_event_v0(offset)

    reversed_timestamps = _event()
    reversed_timestamps["occurred_at"] = "2026-08-24T10:00:02Z"
    reversed_timestamps["recorded_at"] = "2026-08-24T10:00:01Z"
    validate_run_event_v0(reversed_timestamps)


def test_report_rejects_status_and_gate_contradictions() -> None:
    passing = _report()
    cast(list[dict[str, object]], passing["critical_gates"])[0]["status"] = "fail"
    with pytest.raises(EvidenceValidationError):
        validate_run_report_v0(passing)

    for status in ("failed", "timed_out", "cancelled", "infra_error"):
        not_completed = _report()
        not_completed["run_status"] = status
        with pytest.raises(EvidenceValidationError):
            validate_run_report_v0(not_completed)

    invalid = _report()
    invalid["classification"] = "invalid"
    validate_run_report_v0(invalid)


def test_infrastructure_report_can_end_before_evaluation() -> None:
    report = _report()
    report["run_status"] = "infra_error"
    report["classification"] = "not_evaluated"
    report["critical_gates"] = []
    fault = cast(dict[str, object], report["fault_observation"])
    fault["status"] = "not_applicable"
    fault["fault_ids"] = []
    fault["evidence"] = []
    provenance = cast(dict[str, object], report["provenance"])
    provenance["evaluator_revisions"] = []
    del provenance["evaluated_through_sequence"]
    report["totals"] = {}
    validate_run_report_v0(report)

    report["classification"] = "fail"
    with pytest.raises(EvidenceValidationError):
        validate_run_report_v0(report)


def test_not_evaluated_report_cannot_contain_gate_or_evaluator_results() -> None:
    report = _report()
    report["classification"] = "not_evaluated"
    with pytest.raises(EvidenceValidationError):
        validate_run_report_v0(report)


def test_pass_does_not_require_a_minimum_gate_suite() -> None:
    report = _report()
    report["critical_gates"] = []
    cast(dict[str, object], report["provenance"])["evaluator_revisions"] = []
    validate_run_report_v0(report)


def test_failing_report_requires_failed_gate() -> None:
    report = _report()
    report["classification"] = "fail"
    with pytest.raises(EvidenceValidationError):
        validate_run_report_v0(report)

    report["run_status"] = "failed"
    cast(list[dict[str, object]], report["critical_gates"])[0]["status"] = "fail"
    validate_run_report_v0(report)


@pytest.mark.parametrize("field", ["report_id", "scenario", "critical_gates", "provenance"])
def test_report_rejects_missing_required_fields(field: str) -> None:
    report = _report()
    del report[field]
    with pytest.raises(EvidenceValidationError):
        validate_run_report_v0(report)


def test_report_rejects_unknown_properties_and_unevidenced_gate_results() -> None:
    report = _report()
    cast(dict[str, object], report["totals"])["surprise"] = True
    with pytest.raises(EvidenceValidationError):
        validate_run_report_v0(report)


def test_unknown_totals_are_omitted_and_zero_remains_a_known_value() -> None:
    unavailable = _report()
    unavailable["totals"] = {}
    validate_run_report_v0(unavailable)

    partial = _report()
    partial["totals"] = {"usage": {"tool_calls": 0}}
    validate_run_report_v0(partial)

    invalid_empty_category = _report()
    invalid_empty_category["totals"] = {"usage": {}}
    with pytest.raises(EvidenceValidationError, match="non-empty"):
        validate_run_report_v0(invalid_empty_category)

    report = _report()
    cast(list[dict[str, object]], report["critical_gates"])[0]["evidence"] = []
    with pytest.raises(EvidenceValidationError):
        validate_run_report_v0(report)


def test_report_rejects_out_of_boundary_and_reversed_evidence() -> None:
    report = _report()
    fault = cast(dict[str, object], report["fault_observation"])
    fault["evidence"] = [{"kind": "event_range", "start_sequence": 8, "end_sequence": 2}]
    with pytest.raises(EvidenceValidationError, match="reversed"):
        validate_run_report_v0(report)

    outside = _report()
    outside_fault = cast(dict[str, object], outside["fault_observation"])
    outside_fault["evidence"] = [{"kind": "event_range", "start_sequence": 12, "end_sequence": 13}]
    with pytest.raises(EvidenceValidationError, match="outside the boundary"):
        validate_run_report_v0(outside)


def test_report_rejects_boundary_arithmetic_and_provenance_mismatch() -> None:
    report = _report()
    boundary = cast(dict[str, object], report["evidence_boundary"])
    boundary["event_count"] = 12
    provenance = cast(dict[str, object], report["provenance"])
    provenance["evaluated_through_sequence"] = 12
    with pytest.raises(EvidenceValidationError) as raised:
        validate_run_report_v0(report)
    assert "event_count exceeds" in str(raised.value)
    assert "outside the evidence boundary" in str(raised.value)


def test_combined_validator_binds_event_ids_sequences_and_run() -> None:
    events = [load_run_event(path).to_dict() for path in sorted(EXAMPLES.glob("[0-9]*.json"))]

    wrong_id = _report()
    gate = cast(list[dict[str, object]], wrong_id["critical_gates"])[0]
    cast(list[dict[str, object]], gate["evidence"])[0]["event_id"] = "evt-missing"
    with pytest.raises(EvidenceValidationError, match="unknown event_id"):
        validate_run_report_with_events_v0(wrong_id, events)

    wrong_sequence = _report()
    gate = cast(list[dict[str, object]], wrong_sequence["critical_gates"])[0]
    cast(list[dict[str, object]], gate["evidence"])[0]["sequence"] = 6
    with pytest.raises(EvidenceValidationError, match="does not match event_id"):
        validate_run_report_with_events_v0(wrong_sequence, events)

    wrong_run = _report()
    wrong_run["run_id"] = "another-run"
    with pytest.raises(EvidenceValidationError, match="does not match every supplied event"):
        validate_run_report_with_events_v0(wrong_run, events)


def test_combined_validator_rejects_a_range_containing_only_a_sequence_gap() -> None:
    events = [_event(sequence=1), _event(sequence=3)]
    report = _report()
    report["run_id"] = "run-01"
    report["evidence_boundary"] = {
        "first_sequence": 1,
        "last_sequence": 3,
        "event_count": 2,
    }
    report["fault_observation"] = {
        "status": "not_applicable",
        "fault_ids": [],
        "evidence": [],
    }
    report["critical_gates"] = []
    report["diagnostic_metrics"] = [
        {
            "metric_id": "gap_only",
            "value": 0,
            "unit": "count",
            "interpretation": "informational",
            "evidence": [{"kind": "event_range", "start_sequence": 2, "end_sequence": 2}],
        }
    ]
    report["totals"] = {}
    provenance = cast(dict[str, object], report["provenance"])
    provenance["evaluator_revisions"] = []
    provenance["evaluated_through_sequence"] = 3
    with pytest.raises(EvidenceValidationError, match="contains no supplied event"):
        validate_run_report_with_events_v0(report, events)


def test_report_rejects_duplicate_semantic_ids() -> None:
    report = _report()
    gates = cast(list[dict[str, object]], report["critical_gates"])
    gates.append(deepcopy(gates[0]))
    with pytest.raises(EvidenceValidationError, match="duplicate gate_id"):
        validate_run_report_v0(report)


def test_gate_evaluator_must_be_declared_in_report_provenance() -> None:
    report = _report()
    gates = cast(list[dict[str, object]], report["critical_gates"])
    gates[0]["evaluator"] = _revision("undeclared.evaluator")
    with pytest.raises(EvidenceValidationError, match="not listed"):
        validate_run_report_v0(report)


def test_revision_references_reject_malformed_digests() -> None:
    report = _report()
    cast(dict[str, object], report["scenario"])["digest"] = "sha256:not-a-digest"
    with pytest.raises(EvidenceValidationError):
        validate_run_report_v0(report)


def test_generic_loaders_fail_closed_for_unknown_versions() -> None:
    event = _event()
    event["schema_version"] = "chaosagent.run-event/v1"
    with pytest.raises(EvidenceValidationError, match="unsupported version"):
        loads_run_event(json.dumps(event))
    report = _report()
    report["schema_version"] = "chaosagent.run-report/v1"
    with pytest.raises(EvidenceValidationError, match="unsupported version"):
        loads_run_report(json.dumps(report))
    with pytest.raises(EvidenceValidationError):
        run_event_schema("chaosagent.run-event/v1")
    with pytest.raises(EvidenceValidationError):
        run_report_schema("chaosagent.run-report/v1")


def test_schema_resources_are_available_through_the_package() -> None:
    resources = files("chaosagent_evidence.schema")
    assert resources.joinpath("run-event-v0.schema.json").is_file()
    assert resources.joinpath("run-report-v0.schema.json").is_file()


def test_loaded_wrappers_are_immutable_and_return_defensive_documents() -> None:
    event = load_run_event(EXAMPLES / "001-run-started.json")
    report = load_run_report(EXAMPLES / "run-report.json")
    with pytest.raises(Exception):
        event.canonical_bytes = b"changed"  # type: ignore[misc]
    changed = event.to_dict()
    cast(dict[str, object], changed["payload"])["state"] = "failed"
    assert cast(dict[str, object], event.to_dict()["payload"])["state"] == "running"
    changed_report = report.to_dict()
    cast(dict[str, object], changed_report["totals"])["usage"] = {}
    assert cast(dict[str, object], report.to_dict()["totals"])["usage"] != {}


def test_wrapper_construction_is_restricted_to_validated_loaders() -> None:
    with pytest.raises(TypeError):
        RunEvent()
    with pytest.raises(TypeError):
        RunReport()


def test_json_loader_rejects_duplicate_keys() -> None:
    with pytest.raises(EvidenceValidationError, match="duplicate JSON object key"):
        loads_run_event('{"schema_version":"chaosagent.run-event/v0","schema_version":"x"}')


def test_canonical_event_bytes_normalize_only_declared_payload_sets() -> None:
    event = _event("fault.matched")
    payload = cast(dict[str, object], event["payload"])
    cast(list[str], payload["related_event_ids"]).append("evt-02")
    event["payload_digest"] = digest_payload_v0(payload)
    reordered = dict(reversed(list(event.items())))
    assert canonicalize_run_event(event) == canonicalize_run_event(reordered)

    reversed_set = deepcopy(event)
    reversed_payload = cast(dict[str, object], reversed_set["payload"])
    cast(list[str], reversed_payload["related_event_ids"]).reverse()
    reversed_set["payload_digest"] = digest_payload_v0(reversed_payload)
    assert canonicalize_run_event(event) == canonicalize_run_event(reversed_set)

    changed = deepcopy(event)
    related = cast(list[str], cast(dict[str, object], changed["payload"])["related_event_ids"])
    related.append("evt-03")
    changed["payload_digest"] = digest_payload_v0(changed["payload"])
    assert canonicalize_run_event(event) != canonicalize_run_event(changed)


def test_payload_digest_uses_jcs_number_unicode_and_array_semantics() -> None:
    assert digest_payload_v0({"value": -0.0}) == digest_payload_v0({"value": 0})
    assert digest_payload_v0({"value": "Café"}) != digest_payload_v0({"value": "Café"})
    assert digest_payload_v0({"value": [1, 2]}) != digest_payload_v0({"value": [2, 1]})
