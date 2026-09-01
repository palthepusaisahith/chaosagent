"""Lease-fenced, atomic evaluator lifecycle orchestration for PostgreSQL Runs."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, cast

from chaosagent_agent_runtime import AgentOutputValidationError, validate_final_execution_snapshot
from chaosagent_evidence import (
    EvidenceValidationError,
    RunEvent,
    digest_payload_v0,
    loads_run_event,
)
from chaosagent_persistence import (
    LeaseExpiredError,
    LeaseIdentity,
    LifecycleConflictError,
    LifecycleEvidence,
    PersistenceError,
    PersistenceIntegrityError,
    PersistenceRepository,
    StaleLeaseError,
)
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from .contracts import EvaluationResult, EvaluatorValidationError, GroundTruth
from .engine import (
    EVALUATOR_REVISION,
    ApprovalFact,
    EffectFact,
    EvaluationInput,
    evaluate_critical_gates,
    invalid_evaluation_result_v0,
)

type EvaluationExecutionStatus = Literal[
    "completed", "invalid", "stale_lease", "run_not_ready", "internal_error"
]


@dataclass(frozen=True, slots=True)
class EvaluationExecutionResult:
    status: EvaluationExecutionStatus
    run_id: str
    result: EvaluationResult | None = None
    error_code: str | None = None


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _identity(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\0".join((prefix, *parts)).encode()).hexdigest()
    return f"{prefix}-{digest[:32]}"


def _state_document(state: object) -> dict[str, object]:
    payments = {item.payment_id: item for item in state.payments}  # type: ignore[attr-defined]
    return {
        "orders": [
            {
                "order_id": item.order_id,
                "customer_id": item.customer_id,
                "status": item.status,
                "currency": item.currency,
                "total_minor": item.total_minor,
            }
            for item in state.orders  # type: ignore[attr-defined]
        ],
        "payments": [
            {
                "payment_id": item.payment_id,
                "order_id": item.order_id,
                "status": item.status,
                "currency": item.currency,
                "amount_minor": item.amount_minor,
            }
            for item in state.payments  # type: ignore[attr-defined]
        ],
        "refunds": [
            {
                "refund_id": item.refund_id,
                "payment_id": item.payment_id,
                "order_id": item.order_id,
                "status": item.status,
                "amount_minor": item.amount_minor,
                "currency": payments[item.payment_id].currency,
            }
            for item in state.refunds  # type: ignore[attr-defined]
        ],
        "support_tickets": [
            {
                "ticket_id": item.ticket_id,
                "customer_id": item.customer_id,
                "order_id": item.order_id,
                "status": item.status,
                "subject": item.subject,
                "note": item.note,
            }
            for item in state.support_tickets  # type: ignore[attr-defined]
        ],
    }


def _initial_state_document(fixture: dict[str, object]) -> dict[str, object]:
    payments = {
        cast(str, row["payment_id"]): row
        for row in cast(list[dict[str, object]], fixture["payments"])
    }
    refunds = []
    for original in cast(list[dict[str, object]], fixture["refunds"]):
        row = dict(original)
        row["currency"] = payments[cast(str, row["payment_id"])]["currency"]
        refunds.append(row)
    return {
        "orders": fixture["orders"],
        "payments": fixture["payments"],
        "refunds": refunds,
        "support_tickets": fixture["support_tickets"],
    }


def _execution_document(checkpoint: dict[str, object]) -> dict[str, object]:
    trajectory = cast(list[dict[str, object]], checkpoint["trajectory"])
    calls: dict[str, tuple[dict[str, object], int]] = {}
    ordinal = 0
    for turn in trajectory:
        if turn["kind"] != "assistant":
            continue
        for call in cast(list[dict[str, object]], turn["tool_calls"]):
            ordinal += 1
            calls[cast(str, call["logical_call_id"])] = (call, ordinal)
    requests: list[dict[str, object]] = []
    for turn in trajectory:
        if turn["kind"] != "tool":
            continue
        call, call_ordinal = calls[cast(str, turn["logical_call_id"])]
        arguments = cast(dict[str, object], call["arguments"])
        key = arguments.get("idempotency_key")
        requests.append(
            {
                "request_event_id": turn["request_event_id"],
                "logical_call_id": turn["logical_call_id"],
                "attempt_id": turn["attempt_id"],
                "attempt_number": turn["attempt_number"],
                "call_ordinal": call_ordinal,
                "tool_id": turn["tool_id"],
                "contract_version": turn["contract_version"],
                "arguments": arguments,
                "arguments_digest": digest_payload_v0(arguments),
                "request_digest": digest_payload_v0(
                    {
                        "tool_id": turn["tool_id"],
                        "contract_version": turn["contract_version"],
                        "arguments": arguments,
                    }
                ),
                "idempotency_key_digest": (
                    None if not isinstance(key, str) else digest_payload_v0(key)
                ),
            }
        )
    return {
        "status": checkpoint["status"],
        "final_answer": checkpoint["final_answer"],
        "steps": sum(item["kind"] == "assistant" for item in trajectory),
        "tool_calls": checkpoint["tool_attempts"],
        "wall_time_ms": checkpoint["active_wall_time_ms"],
        "cost_microusd": checkpoint["known_cost_microusd"],
        "cost_complete": checkpoint["cost_complete"],
        "tool_requests": requests,
    }


def _event(
    repository: PersistenceRepository,
    run_id: str,
    event_id: str,
    event_type: Literal["evaluation.started", "evaluation.result_recorded"],
    payload: dict[str, object],
    *,
    correlation_id: str,
    causation_event_id: str,
    worker_id: str,
) -> None:
    observed = repository.database_time()

    def factory(sequence: int) -> RunEvent:
        document: dict[str, object] = {
            "schema_version": "chaosagent.run-event/v0",
            "event_id": event_id,
            "run_id": run_id,
            "sequence": sequence,
            "occurred_at": _timestamp(observed),
            "recorded_at": _timestamp(observed),
            "event_type": event_type,
            "producer": {"component": "critical-evaluator", "instance_id": worker_id},
            "correlation_id": correlation_id,
            "causation_event_id": causation_event_id,
            "payload": payload,
            "payload_digest": digest_payload_v0(payload),
        }
        return loads_run_event(json.dumps(document))

    repository.append_event_allocated(run_id, factory)


def evaluate_leased_run(
    repository: PersistenceRepository,
    lease: LeaseIdentity,
    ground_truths: tuple[GroundTruth, ...],
) -> EvaluationExecutionResult:
    """Evaluate and terminalize atomically inside the caller-owned transaction."""
    run = repository.lock_current_lease(lease)
    if run.status != "evaluating":
        return EvaluationExecutionResult(
            "run_not_ready", run.run_id, error_code="run_not_evaluating"
        )
    latest = repository.latest_event_projection(run.run_id)
    if latest is None:
        raise PersistenceIntegrityError("evaluation Run has no evidence boundary")
    prior_event_id, boundary = latest
    try:
        scenario_record = repository.get_scenario_revision(run.scenario.id, run.scenario.revision)
        if scenario_record is None or scenario_record.scenario.digest != run.scenario.digest:
            raise PersistenceIntegrityError("Run Scenario binding does not resolve")
        scenario_document = scenario_record.scenario.to_dict()
        policy_reference = cast(dict[str, object], scenario_document["policy"])
        policy_record = repository.get_policy_revision(
            cast(str, policy_reference["id"]), cast(str, policy_reference["revision"])
        )
        if policy_record is None or policy_record.policy.digest != policy_reference["digest"]:
            raise PersistenceIntegrityError("Run Policy binding does not resolve")
        if run.fixture is None:
            raise PersistenceIntegrityError("Run Fixture binding is absent")
        fixture_record = repository.get_fixture_revision(run.fixture.id, run.fixture.revision)
        if fixture_record is None or fixture_record.fixture.digest != run.fixture.digest:
            raise PersistenceIntegrityError("Run Fixture binding does not resolve")
        state = repository.get_run_company_state(run.run_id)
        checkpoint_record = repository.get_execution_checkpoint(run.run_id)
        records = repository.fetch_events(run.run_id)
        if state is None or checkpoint_record is None or not records:
            raise PersistenceIntegrityError("evaluation inputs are incomplete")
        checkpoint = validate_final_execution_snapshot(
            checkpoint_record,
            run,
            scenario_record.scenario.to_dict(),
            records,
        )
        effects = tuple(
            EffectFact(
                item.run_id,
                item.tool_id,
                item.contract_version,
                item.idempotency_key_digest,
                item.request_digest,
                item.effect_id,
                item.effect_kind,
                item.subject_type,
                item.subject_id,
                item.logical_call_id,
                item.first_attempt_id,
                cast(dict[str, object], dict(item.result)),
            )
            for item in repository.list_company_effects(run.run_id)
        )
        approvals = tuple(
            ApprovalFact(
                item.approval_id,
                item.run_id,
                item.scenario.id,
                item.scenario.revision,
                item.scenario.digest,
                item.policy.id,
                item.policy.revision,
                item.policy.digest,
                item.tool_id,
                item.contract_version,
                item.request_digest,
                item.idempotency_key_digest,
                item.logical_call_id,
                item.requested_attempt_id,
                item.decision_id,
                item.decision_event_id,
                item.request_event_id,
                item.status,
                item.resolution_event_id,
            )
            for item in repository.list_approval_requests(run.run_id)
        )
        evaluation_input = EvaluationInput(
            run_id=run.run_id,
            scenario=scenario_record.scenario,
            ground_truths=ground_truths,
            evidence_through_sequence=boundary,
            events=tuple(record.event.to_dict() for record in records),
            initial_state=_initial_state_document(fixture_record.fixture.to_dict()),
            final_state=_state_document(state),
            effects=effects,
            execution=_execution_document(checkpoint),
            run_seed=run.fault_seed,
            approvals=approvals,
        )
        result = evaluate_critical_gates(evaluation_input)
    except (
        AgentOutputValidationError,
        EvidenceValidationError,
        EvaluatorValidationError,
        PersistenceIntegrityError,
        AttributeError,
        IndexError,
        KeyError,
        TypeError,
        ValueError,
    ):
        result = invalid_evaluation_result_v0(
            run.run_id,
            boundary,
            "authoritative_input_inconsistent",
            identity=(
                f"{run.run_id}\0{run.scenario.digest}\0{boundary}\0{EVALUATOR_REVISION['digest']}"
            ),
        )
    result_document = result.to_dict()
    evaluation_id = cast(str, result_document["evaluation_id"])
    started_id = _identity("event-evaluation-started", evaluation_id)
    result_id = _identity("event-evaluation-result", evaluation_id)
    evaluator = cast(dict[str, object], result_document["evaluator"])
    _event(
        repository,
        run.run_id,
        started_id,
        "evaluation.started",
        {
            "evaluation_id": evaluation_id,
            "evaluator": evaluator,
            "evidence_through_sequence": boundary,
        },
        correlation_id=evaluation_id,
        causation_event_id=prior_event_id,
        worker_id=lease.worker_id,
    )
    classification = cast(str, result_document["classification"])
    payload: dict[str, object] = {
        "evaluation_id": evaluation_id,
        "evaluator": evaluator,
        "outcome": "error" if classification == "invalid" else "completed",
        "evidence_through_sequence": boundary,
    }
    if classification == "invalid":
        payload["error_code"] = cast(str, result_document["error_code"])
    _event(
        repository,
        run.run_id,
        result_id,
        "evaluation.result_recorded",
        payload,
        correlation_id=evaluation_id,
        causation_event_id=started_id,
        worker_id=lease.worker_id,
    )
    repository.transition_owned_run(
        lease,
        "completed",
        expected_version=run.lifecycle_version,
        evidence=LifecycleEvidence(
            _identity("event-run-completed", evaluation_id),
            "critical-evaluator",
            lease.worker_id,
            correlation_id=evaluation_id,
            causation_event_id=result_id,
            reason_code="evaluation_finished",
        ),
    )
    return EvaluationExecutionResult(
        "invalid" if classification == "invalid" else "completed", run.run_id, result
    )


def execute_evaluation(
    engine: Engine,
    lease: LeaseIdentity,
    ground_truths: tuple[GroundTruth, ...],
) -> EvaluationExecutionResult:
    """Own a short transaction and contain persistence details at the worker boundary."""
    try:
        with Session(engine) as session, session.begin():
            return evaluate_leased_run(PersistenceRepository(session), lease, ground_truths)
    except (StaleLeaseError, LeaseExpiredError):
        return EvaluationExecutionResult("stale_lease", lease.run_id, error_code="stale_lease")
    except LifecycleConflictError:
        return EvaluationExecutionResult("run_not_ready", lease.run_id, error_code="run_not_ready")
    except (PersistenceError, SQLAlchemyError):
        _terminalize_evaluator_failure(engine, lease)
        return EvaluationExecutionResult(
            "internal_error", lease.run_id, error_code="internal_error"
        )


def _terminalize_evaluator_failure(engine: Engine, lease: LeaseIdentity) -> None:
    """Best-effort fresh fenced infra terminalization; never recurse or leak details."""
    try:
        with Session(engine) as session, session.begin():
            repository = PersistenceRepository(session)
            run = repository.lock_current_lease(lease)
            if run.status != "evaluating":
                return
            repository.transition_owned_run(
                lease,
                "infra_error",
                expected_version=run.lifecycle_version,
                evidence=LifecycleEvidence(
                    _identity("event-evaluator-infra-error", run.run_id, str(lease.attempt)),
                    "critical-evaluator",
                    lease.worker_id,
                    reason_code="evaluator_internal_error",
                ),
            )
    except (PersistenceError, SQLAlchemyError):
        return
