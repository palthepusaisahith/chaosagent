"""Thin orchestration over the production ChaosAgent flagship path."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import cast
from urllib.parse import quote
from uuid import uuid4

from alembic.config import Config
from alembic.script import ScriptDirectory
from chaosagent_agent_runtime import (
    AgentOutput,
    AgentOutputValidationError,
    AgentToolCall,
    AgentUsage,
    ScriptedAgentAdapter,
    execute_run,
    validate_final_execution_snapshot,
)
from chaosagent_evaluators import (
    AuthoritativeEvaluationSnapshot,
    GroundTruth,
    execute_evaluation,
    load_authoritative_evaluation_snapshot,
    load_ground_truth_v0,
)
from chaosagent_evidence import EvidenceValidationError, digest_payload_v0
from chaosagent_faults import FaultEngine, compile_fault_plan_v0
from chaosagent_fixtures import Fixture, load_fixture
from chaosagent_persistence import (
    ClaimedRun,
    ExecutionCheckpointRecord,
    LifecycleEvidence,
    PersistenceError,
    PersistenceRepository,
    RevisionReference,
    RunEventRecord,
    create_postgres_engine,
)
from chaosagent_policies import Policy, load_policy
from chaosagent_scenarios import Scenario, load_scenario
from chaosagent_tool_gateway import (
    ORDERS_GET_V0,
    PAYMENTS_REFUND_V0,
    SHIPPING_GET_STATUS_V0,
    SUPPORT_UPDATE_TICKET_V0,
)
from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

SCRIPTED_AGENT_ID = "shipment-refund.flagship-scripted-agent"
SCRIPTED_AGENT_REVISION = "v1"
FLAGSHIP_FAULT_ID = "refund-ack-lost"
FLAGSHIP_SEED = 1616
WORKER_ID = "flagship-demo-worker"
LEASE_DURATION_SECONDS = 180
DEFAULT_DASHBOARD_URL = "http://127.0.0.1:5173"


class FlagshipDemoError(RuntimeError):
    """Sanitized failure at the trusted local orchestration boundary."""


@dataclass(frozen=True, slots=True)
class _ScriptStep:
    text: str
    call_id: str | None
    tool_id: str | None
    contract_version: str | None
    arguments: tuple[tuple[str, str | int], ...]
    final: bool
    input_tokens: int
    output_tokens: int


_SCRIPT_STEPS = (
    _ScriptStep(
        "Verify the order before taking action.",
        "verify-order",
        "orders.get",
        ORDERS_GET_V0,
        (("order_id", "ORD-1007"),),
        False,
        24,
        8,
    ),
    _ScriptStep(
        "Verify the shipment status.",
        "verify-shipment",
        "shipping.get_status",
        SHIPPING_GET_STATUS_V0,
        (("order_id", "ORD-1007"),),
        False,
        32,
        8,
    ),
    _ScriptStep(
        "Refund the failed shipment.",
        "refund-first",
        "payments.refund",
        PAYMENTS_REFUND_V0,
        (
            ("order_id", "ORD-1007"),
            ("payment_id", "PAY-1007"),
            ("amount_minor", 5000),
            ("reason", "Failed shipment"),
            ("idempotency_key", "flagship-refund-v1"),
        ),
        False,
        40,
        10,
    ),
    _ScriptStep(
        "Reconcile the ambiguous refund with the same idempotency key.",
        "refund-recovery",
        "payments.refund",
        PAYMENTS_REFUND_V0,
        (
            ("order_id", "ORD-1007"),
            ("payment_id", "PAY-1007"),
            ("amount_minor", 5000),
            ("reason", "Failed shipment"),
            ("idempotency_key", "flagship-refund-v1"),
        ),
        False,
        48,
        10,
    ),
    _ScriptStep(
        "Close the support ticket with the verified outcome.",
        "close-ticket",
        "support.update_ticket",
        SUPPORT_UPDATE_TICKET_V0,
        (
            ("ticket_id", "TKT-204"),
            ("status", "closed"),
            ("note", "Refund completed after the failed shipment."),
            ("idempotency_key", "flagship-ticket-v1"),
        ),
        False,
        56,
        10,
    ),
    _ScriptStep(
        "Refund confirmed exactly once.",
        None,
        None,
        None,
        (),
        True,
        64,
        8,
    ),
)


@dataclass(frozen=True, slots=True)
class FlagshipAssets:
    fixture: Fixture
    policy: Policy
    scenario: Scenario
    ground_truth: GroundTruth


@dataclass(frozen=True, slots=True)
class FlagshipProof:
    run_id: str
    cause_event_id: str
    effect_event_id: str
    fault_event_id: str
    ambiguous_result_event_id: str
    recovery_request_event_id: str
    recovery_result_event_id: str
    refund_effect_count: int
    recovery_disposition: str
    classification: str


@dataclass(frozen=True, slots=True)
class FlagshipDemoResult:
    run_id: str
    dashboard_url: str
    proof: FlagshipProof


def script_manifest() -> dict[str, object]:
    """Return a fresh canonicalizable description of the deterministic adapter."""
    steps: list[dict[str, object]] = []
    for ordinal, step in enumerate(_SCRIPT_STEPS, start=1):
        document: dict[str, object] = {
            "step": ordinal,
            "text": step.text,
            "final": step.final,
            "usage": {
                "input_tokens": step.input_tokens,
                "output_tokens": step.output_tokens,
                "cost_microusd": 0,
            },
        }
        if step.tool_id is not None:
            document["tool_call"] = {
                "call_id": step.call_id,
                "tool_id": step.tool_id,
                "contract_version": step.contract_version,
                "arguments": dict(step.arguments),
            }
        steps.append(document)
    return {
        "schema_version": "chaosagent.demo-script-manifest/v0",
        "agent_id": SCRIPTED_AGENT_ID,
        "revision": SCRIPTED_AGENT_REVISION,
        "adapter_type": "ScriptedAgentAdapter",
        "steps": steps,
    }


def script_manifest_digest() -> str:
    """Derive the unresolved RevisionReference digest from actual script semantics."""
    return digest_payload_v0(script_manifest())


def scripted_agent_reference() -> RevisionReference:
    document = script_manifest()
    digest = digest_payload_v0(document)
    if digest != script_manifest_digest():
        raise FlagshipDemoError("scripted Agent manifest could not be reproduced")
    return RevisionReference(SCRIPTED_AGENT_ID, SCRIPTED_AGENT_REVISION, digest)


def build_scripted_adapter() -> ScriptedAgentAdapter:
    outputs: list[AgentOutput] = []
    for step in _SCRIPT_STEPS:
        calls: tuple[AgentToolCall, ...] = ()
        if step.tool_id is not None:
            assert step.call_id is not None and step.contract_version is not None
            calls = (
                AgentToolCall(
                    step.call_id,
                    step.tool_id,
                    step.contract_version,
                    dict(step.arguments),
                ),
            )
        outputs.append(
            AgentOutput(
                step.text,
                calls,
                final=step.final,
                usage=AgentUsage(step.input_tokens, step.output_tokens, 0),
            )
        )
    return ScriptedAgentAdapter(SCRIPTED_AGENT_ID, SCRIPTED_AGENT_REVISION, tuple(outputs))


def load_flagship_assets(repository_root: Path) -> FlagshipAssets:
    benchmark = repository_root / "benchmarks" / "shipment-refund"
    try:
        return FlagshipAssets(
            load_fixture(benchmark / "fixtures" / "failed-shipment.v0.json"),
            load_policy(benchmark / "policies" / "refund-policy.v0.json"),
            load_scenario(benchmark / "scenarios" / "refund-ambiguous-timeout.demo.v0.json"),
            load_ground_truth_v0(
                benchmark / "ground-truth" / "refund-once-and-close-ticket.v0.json"
            ),
        )
    except (OSError, ValueError) as error:
        raise FlagshipDemoError("flagship benchmark assets are missing or invalid") from error


def preflight_database(engine: Engine, repository_root: Path) -> None:
    """Require reachable PostgreSQL at the current repository migration head."""
    try:
        configuration = Config(str(repository_root / "packages" / "persistence" / "alembic.ini"))
        expected_head = ScriptDirectory.from_config(configuration).get_current_head()
        if expected_head is None:
            raise FlagshipDemoError("persistence migration head is unavailable")
        with engine.connect() as connection:
            versions = tuple(
                connection.scalars(text("SELECT version_num FROM public.alembic_version"))
            )
    except FlagshipDemoError:
        raise
    except (OSError, SQLAlchemyError) as error:
        raise FlagshipDemoError(
            "PostgreSQL is unavailable or not migrated; apply the repository migrations"
        ) from error
    if versions != (expected_head,):
        raise FlagshipDemoError(
            "PostgreSQL migration state is out of date; apply the repository migrations"
        )


def _event_document(record: RunEventRecord) -> dict[str, object]:
    return record.event.to_dict()


def _payload(event: dict[str, object]) -> dict[str, object]:
    value = event.get("payload")
    if not isinstance(value, dict):
        raise FlagshipDemoError("authoritative evidence contains a malformed payload")
    return value


def _related(event: dict[str, object], event_id: str) -> bool:
    related = _payload(event).get("related_event_ids")
    return isinstance(related, list) and event_id in related


def _find_event(
    events: tuple[dict[str, object], ...],
    description: str,
    predicate: Callable[[dict[str, object]], bool],
) -> dict[str, object]:
    matches = tuple(event for event in events if predicate(event))
    if len(matches) != 1:
        raise FlagshipDemoError(f"authoritative {description} proof is unavailable")
    return matches[0]


def _event_id(event: dict[str, object]) -> str:
    value = event.get("event_id")
    if not isinstance(value, str):
        raise FlagshipDemoError("authoritative evidence has no event identity")
    return value


def _sequence(event: dict[str, object]) -> int:
    value = event.get("sequence")
    if not isinstance(value, int) or isinstance(value, bool):
        raise FlagshipDemoError("authoritative evidence has no sequence")
    return value


def _checkpoint_recovery_disposition(
    checkpoint: Mapping[str, object], recovery_request_id: str
) -> str:
    trajectory = checkpoint.get("trajectory")
    if not isinstance(trajectory, Sequence) or isinstance(trajectory, str | bytes):
        raise FlagshipDemoError("authenticated execution checkpoint is malformed")
    matches: list[str] = []
    for item in trajectory:
        if not isinstance(item, Mapping) or item.get("kind") != "tool":
            continue
        if item.get("request_event_id") != recovery_request_id:
            continue
        output = item.get("output")
        if isinstance(output, Mapping) and isinstance(output.get("application"), str):
            matches.append(cast(str, output["application"]))
    if matches != ["already_applied"]:
        raise FlagshipDemoError("authoritative idempotent recovery proof is unavailable")
    return matches[0]


def _plain_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _plain_json(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return [_plain_json(item) for item in value]
    return value


def _authenticate_proof_inputs(
    snapshot: AuthoritativeEvaluationSnapshot,
    event_records: tuple[RunEventRecord, ...],
    checkpoint: ExecutionCheckpointRecord,
    boundary: int,
) -> tuple[tuple[dict[str, object], ...], dict[str, object]]:
    """Bind supplementary persistence records to one authenticated evaluation."""
    try:
        result = snapshot.result.to_dict()
        run_id = snapshot.run.run_id
        if (
            snapshot.evaluation_input.run_id != run_id
            or result.get("run_id") != run_id
            or snapshot.evaluation_input.evidence_through_sequence != boundary
        ):
            raise FlagshipDemoError("authenticated evaluation Run binding is inconsistent")

        record_documents = tuple((record, _event_document(record)) for record in event_records)
        if any(document.get("run_id") != run_id for _, document in record_documents):
            raise FlagshipDemoError("supplementary evidence belongs to another Run")
        prefix = tuple(
            (record, document)
            for record, document in record_documents
            if _sequence(document) <= boundary
        )
        events = tuple(document for _, document in prefix)
        if events != snapshot.evaluation_input.events:
            raise FlagshipDemoError(
                "supplementary evidence does not match the authenticated evaluation boundary"
            )

        document_value = _plain_json(checkpoint.document)
        if not isinstance(document_value, dict):
            raise FlagshipDemoError("authenticated execution checkpoint is malformed")
        document = cast(dict[str, object], document_value)
        if (
            checkpoint.run_id != run_id
            or checkpoint.lease_attempt != snapshot.run.attempt
            or document.get("run_id") != run_id
            or document.get("checkpoint_version") != checkpoint.checkpoint_version
            or document.get("lease_attempt") != checkpoint.lease_attempt
            or document.get("last_event_sequence") != checkpoint.last_event_sequence
            or digest_payload_v0(document) != checkpoint.document_digest
        ):
            raise FlagshipDemoError("execution checkpoint binding is inconsistent")

        validated = validate_final_execution_snapshot(
            checkpoint,
            replace(snapshot.run, status="evaluating"),
            snapshot.evaluation_input.scenario.to_dict(),
            tuple(record for record, _ in prefix),
        )
        if validated != document:
            raise FlagshipDemoError("execution checkpoint authentication is inconsistent")
        return events, validated
    except FlagshipDemoError:
        raise
    except (
        AgentOutputValidationError,
        EvidenceValidationError,
        PersistenceError,
        AttributeError,
        KeyError,
        TypeError,
        ValueError,
    ) as error:
        raise FlagshipDemoError("authoritative proof material is inconsistent") from error


def extract_flagship_proof(
    snapshot: AuthoritativeEvaluationSnapshot,
    event_records: tuple[RunEventRecord, ...],
    checkpoint: ExecutionCheckpointRecord,
) -> FlagshipProof:
    """Fail closed unless committed evidence proves the complete flagship story."""
    result = snapshot.result.to_dict()
    classification = result.get("classification")
    if classification != "pass":
        raise FlagshipDemoError("authenticated evaluator verdict is not PASS")
    boundary = result.get("evidence_through_sequence")
    if not isinstance(boundary, int) or isinstance(boundary, bool):
        raise FlagshipDemoError("authenticated evaluator evidence boundary is malformed")
    events, authenticated_checkpoint = _authenticate_proof_inputs(
        snapshot, event_records, checkpoint, boundary
    )
    requests = tuple(
        event
        for event in events
        if event.get("event_type") == "tool.requested"
        and _payload(event).get("tool_id") == "payments.refund"
    )
    if len(requests) != 2:
        raise FlagshipDemoError("authoritative refund request/recovery proof is unavailable")
    cause, recovery = requests
    cause_id = _event_id(cause)
    recovery_id = _event_id(recovery)
    cause_payload = _payload(cause)
    recovery_payload = _payload(recovery)
    idempotency_digest = cause_payload.get("idempotency_key_digest")
    if (
        not isinstance(idempotency_digest, str)
        or recovery_payload.get("idempotency_key_digest") != idempotency_digest
        or _sequence(recovery) <= _sequence(cause)
    ):
        raise FlagshipDemoError("refund recovery did not preserve its idempotency identity")
    logical_call = cause_payload.get("logical_call_id")
    if not isinstance(logical_call, str):
        raise FlagshipDemoError("refund request logical identity is malformed")

    effect_event = _find_event(
        events,
        "refund commit",
        lambda event: event.get("event_type") == "state.evidence_recorded"
        and _payload(event).get("evidence_kind") == "business_effect"
        and _payload(event).get("fact_type") == "refund.created"
        and _related(event, cause_id)
        and event.get("correlation_id") == logical_call,
    )
    fault_event = _find_event(
        events,
        "fault application",
        lambda event: event.get("event_type") == "fault.applied"
        and _payload(event).get("fault_id") == FLAGSHIP_FAULT_ID
        and _related(event, cause_id)
        and event.get("correlation_id") == logical_call,
    )
    fault_id = _event_id(fault_event)
    ambiguity = _find_event(
        events,
        "ambiguous acknowledgement",
        lambda event: event.get("event_type") == "tool.result"
        and _payload(event).get("request_event_id") == cause_id
        and _payload(event).get("logical_call_id") == logical_call
        and _payload(event).get("outcome") in {"timed_out", "unknown"}
        and event.get("causation_event_id") == fault_id,
    )
    recovery_result = _find_event(
        events,
        "recovery result",
        lambda event: event.get("event_type") == "tool.result"
        and _payload(event).get("request_event_id") == recovery_id
        and _payload(event).get("outcome") == "succeeded"
        and _sequence(event) > _sequence(recovery),
    )
    observed = _find_event(
        events,
        "fault observation",
        lambda event: event.get("event_type") == "fault.observed"
        and _payload(event).get("fault_id") == FLAGSHIP_FAULT_ID
        and _related(event, cause_id)
        and _related(event, fault_id)
        and _related(event, _event_id(ambiguity)),
    )
    if _sequence(observed) <= _sequence(ambiguity):
        raise FlagshipDemoError("authoritative fault observation ordering is inconsistent")

    refund_effects = tuple(
        effect
        for effect in snapshot.evaluation_input.effects
        if effect.effect_kind == "refund.created"
    )
    if (
        len(refund_effects) != 1
        or refund_effects[0].run_id != snapshot.run.run_id
        or refund_effects[0].tool_id != "payments.refund"
    ):
        raise FlagshipDemoError("authoritative refund effect count is not exactly one")
    refunds = snapshot.evaluation_input.final_state.get("refunds")
    if not isinstance(refunds, list) or len(refunds) != 1:
        raise FlagshipDemoError("authoritative refund state count is not exactly one")

    gates = result.get("critical_gates")
    if not isinstance(gates, list):
        raise FlagshipDemoError("authenticated evaluator gates are malformed")
    effect_event_id = _event_id(effect_event)
    effect_sequence = _sequence(effect_event)
    for gate_id in ("required_refund_state", "no_duplicate_refund_effect"):
        matching = tuple(
            gate for gate in gates if isinstance(gate, dict) and gate.get("gate_id") == gate_id
        )
        if len(matching) != 1 or matching[0].get("status") != "pass":
            raise FlagshipDemoError(f"authenticated evaluator gate {gate_id!r} did not PASS")
        evidence = matching[0].get("evidence")
        if not isinstance(evidence, list) or not any(
            isinstance(reference, dict)
            and reference.get("kind") == "event"
            and reference.get("event_id") == effect_event_id
            and reference.get("sequence") == effect_sequence
            for reference in evidence
        ):
            raise FlagshipDemoError(f"authenticated evaluator gate {gate_id!r} lacks proof")

    disposition = _checkpoint_recovery_disposition(authenticated_checkpoint, recovery_id)
    return FlagshipProof(
        snapshot.run.run_id,
        cause_id,
        effect_event_id,
        fault_id,
        _event_id(ambiguity),
        recovery_id,
        _event_id(recovery_result),
        len(refund_effects),
        disposition,
        cast(str, classification),
    )


def _bootstrap_run(
    engine: Engine, assets: FlagshipAssets, run_id: str, agent: RevisionReference
) -> None:
    with Session(engine) as session, session.begin():
        repository = PersistenceRepository(session)
        repository.insert_fixture_revision(assets.fixture, created_by="flagship-demo")
        repository.insert_policy_revision(assets.policy, created_by="flagship-demo")
        repository.insert_scenario_revision(assets.scenario, created_by="flagship-demo")
        repository.insert_agent_configuration_reference(agent, created_by="flagship-demo")
        repository.create_run(
            run_id,
            scenario_id=cast(str, assets.scenario.to_dict()["scenario_id"]),
            scenario_revision=cast(str, assets.scenario.to_dict()["revision"]),
            agent_configuration_id=agent.id,
            agent_configuration_revision=agent.revision,
            created_by="flagship-demo",
        )
        repository.initialize_run_company_state(run_id)


def _claim_run(engine: Engine, run_id: str) -> ClaimedRun:
    with Session(engine) as session, session.begin():
        claimed = PersistenceRepository(session).claim_next_run(
            WORKER_ID,
            lease_duration_seconds=LEASE_DURATION_SECONDS,
            evidence=LifecycleEvidence(
                f"event-demo-claim-{uuid4().hex}",
                "flagship-demo-runner",
                WORKER_ID,
                correlation_id=run_id,
                reason_code="flagship_demo_requested",
            ),
            run_id=run_id,
        )
        if claimed is None:
            raise FlagshipDemoError("new flagship Run could not be claimed")
        return claimed


def run_flagship_demo(
    database_url: str,
    repository_root: Path,
    *,
    run_id: str | None = None,
    dashboard_base_url: str = DEFAULT_DASHBOARD_URL,
) -> FlagshipDemoResult:
    """Execute and authenticate one real deterministic flagship Run."""
    if not database_url.strip():
        raise FlagshipDemoError("CHAOSAGENT_DATABASE_URL is required")
    selected_run_id = run_id or f"run-flagship-{uuid4().hex}"
    assets = load_flagship_assets(repository_root)
    agent = scripted_agent_reference()
    adapter = build_scripted_adapter()
    plan = compile_fault_plan_v0(assets.scenario)
    fault_engine = FaultEngine(plan, run_seed=FLAGSHIP_SEED)
    try:
        engine = create_postgres_engine(database_url)
    except (ValueError, SQLAlchemyError) as error:
        raise FlagshipDemoError("CHAOSAGENT_DATABASE_URL must identify PostgreSQL") from error
    try:
        preflight_database(engine, repository_root)
        _bootstrap_run(engine, assets, selected_run_id, agent)
        claimed = _claim_run(engine, selected_run_id)
        lease = claimed.lease
        execution = execute_run(engine, lease, adapter, fault_engine=fault_engine)
        if execution.status != "evaluation_ready":
            raise FlagshipDemoError(
                f"flagship runtime did not reach evaluation readiness ({execution.status})"
            )
        evaluation = execute_evaluation(engine, lease, (assets.ground_truth,))
        if evaluation.status != "completed":
            raise FlagshipDemoError(
                f"flagship evaluator did not complete successfully ({evaluation.status})"
            )
        with Session(engine) as session:
            repository = PersistenceRepository(session)
            snapshot = load_authoritative_evaluation_snapshot(
                repository, selected_run_id, (assets.ground_truth,)
            )
            events = repository.fetch_events(selected_run_id)
            checkpoint_record = repository.get_execution_checkpoint(selected_run_id)
            if checkpoint_record is None:
                raise FlagshipDemoError("authenticated execution checkpoint is unavailable")
            proof = extract_flagship_proof(snapshot, events, checkpoint_record)
            if repository.get_final_report(selected_run_id) is not None:
                raise FlagshipDemoError("flagship demo unexpectedly produced a Run Report")
        dashboard_url = f"{dashboard_base_url.rstrip('/')}/runs/{quote(selected_run_id, safe='')}"
        return FlagshipDemoResult(selected_run_id, dashboard_url, proof)
    except FlagshipDemoError:
        raise
    except (PersistenceError, SQLAlchemyError, ValueError) as error:
        raise FlagshipDemoError("PostgreSQL rejected the flagship demo operation") from error
    finally:
        engine.dispose()
