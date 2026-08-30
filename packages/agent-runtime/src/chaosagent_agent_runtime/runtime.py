"""Provider-neutral, lease-fenced deterministic ChaosAgent execution loop."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC
from functools import lru_cache
from importlib.resources import files
from time import monotonic_ns
from types import MappingProxyType
from typing import Literal, Protocol, cast

from chaosagent_evidence import (
    EvidenceValidationError,
    RunEvent,
    digest_payload_v0,
    loads_run_event,
)
from chaosagent_faults import FaultEngine
from chaosagent_persistence import (
    CheckpointConflictError,
    ExecutionCheckpointRecord,
    LeaseExpiredError,
    LeaseIdentity,
    LifecycleConflictError,
    LifecycleEvidence,
    PersistenceError,
    PersistenceIntegrityError,
    PersistenceRepository,
    RunEventRecord,
    RunRecord,
    StaleLeaseError,
    validate_jsonb_persistence_profile,
)
from chaosagent_tool_gateway import (
    SCENARIO_V0_TOOL_VERSIONS,
    ToolDefinition,
    ToolExecutionResult,
    ToolGateway,
    ToolRegistry,
    default_tool_registry,
)
from jsonschema import Draft202012Validator  # type: ignore[import-untyped]
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

CHECKPOINT_SCHEMA_VERSION = "chaosagent.execution-checkpoint/v0"
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
_NAME_RE = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")
_SAFE_INTEGER = 9_007_199_254_740_991
_MAX_TOOL_CALLS_PER_STEP = 128
_MAX_ASSISTANT_TEXT = 100_000
_MAX_SINGLE_CALL_COST_MICROUSD = 1_000_000_000_000
_MAX_MEASURED_DURATION_MS = 86_400_001
_INFRASTRUCTURE_EXCEPTIONS = (
    PersistenceError,
    SQLAlchemyError,
    EvidenceValidationError,
)

type RuntimeStatus = Literal[
    "evaluation_ready",
    "waiting_for_approval",
    "failed",
    "infra_error",
    "timed_out",
    "stale_lease",
    "run_not_ready",
]


class AgentProviderError(RuntimeError):
    """Sanitized adapter failure; raw provider details must remain inside the adapter."""


class AgentProviderTimeout(AgentProviderError):
    """The provider invocation exceeded its adapter-owned timeout."""


class AgentOutputValidationError(ValueError):
    """The adapter returned a structurally unsafe or unsupported response."""


@dataclass(frozen=True, slots=True)
class AgentUsage:
    input_tokens: int | None = None
    output_tokens: int | None = None
    cost_microusd: int | None = None


@dataclass(frozen=True, slots=True)
class AgentProviderMetadata:
    """Safe provider invocation identifiers for evidence, never provider objects."""

    provider: str
    requested_model: str
    resolved_model: str | None = None
    provider_request_id: str | None = None


@dataclass(frozen=True, slots=True)
class AgentToolCall:
    call_id: str
    tool_id: str
    contract_version: str
    arguments: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class AgentOutput:
    text: str
    tool_calls: tuple[AgentToolCall, ...] = ()
    final: bool = False
    usage: AgentUsage = AgentUsage()
    provider_metadata: AgentProviderMetadata | None = None


@dataclass(frozen=True, slots=True)
class AgentToolSpec:
    tool_id: str
    contract_version: str
    description: str
    input_schema: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class AgentContext:
    run_id: str
    task: str
    instructions: tuple[str, ...]
    step_number: int
    trajectory: tuple[Mapping[str, object], ...]
    tools: tuple[AgentToolSpec, ...]
    remaining_steps: int
    remaining_tool_calls: int
    remaining_wall_time_ms: int
    remaining_known_cost_microusd: int
    cost_complete: bool


class AgentAdapter(Protocol):
    """The minimal provider-neutral turn interface used by Scenario v0."""

    @property
    def adapter_id(self) -> str: ...

    @property
    def adapter_version(self) -> str: ...

    def invoke(self, context: AgentContext) -> AgentOutput: ...


class ScriptedAgentAdapter:
    """Deterministic in-process adapter keyed only by durable step number."""

    def __init__(
        self,
        adapter_id: str,
        adapter_version: str,
        outputs: Sequence[object],
    ) -> None:
        _require_name(adapter_id, "adapter_id")
        _require_id(adapter_version, "adapter_version")
        self._adapter_id = adapter_id
        self._adapter_version = adapter_version
        self._outputs = tuple(outputs)

    @property
    def adapter_id(self) -> str:
        return self._adapter_id

    @property
    def adapter_version(self) -> str:
        return self._adapter_version

    @property
    def configuration_digest(self) -> None:
        """Scripted adapters retain the legacy unresolved reference contract."""
        return None

    def invoke(self, context: AgentContext) -> AgentOutput:
        index = context.step_number - 1
        if index >= len(self._outputs):
            raise AgentProviderError("script has no response for the requested step")
        value = self._outputs[index]
        if isinstance(value, Exception):
            raise value
        return cast(AgentOutput, value)


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    status: RuntimeStatus
    run_id: str
    checkpoint_version: int | None
    final_answer: str | None = None
    approval_id: str | None = None
    error_code: str | None = None


@dataclass(slots=True)
class _State:
    run: RunRecord
    scenario: dict[str, object]
    checkpoint: dict[str, object] | None


def execution_checkpoint_schema_v0() -> dict[str, object]:
    return cast(dict[str, object], json.loads(json.dumps(_checkpoint_schema_cached())))


@lru_cache(maxsize=1)
def _checkpoint_schema_cached() -> dict[str, object]:
    value = json.loads(
        files("chaosagent_agent_runtime.schema")
        .joinpath("execution-checkpoint-v0.schema.json")
        .read_text("utf-8")
    )
    if not isinstance(value, dict):
        raise RuntimeError("bundled execution checkpoint schema is not an object")
    schema = cast(dict[str, object], value)
    Draft202012Validator.check_schema(schema)
    return schema


def validate_execution_checkpoint(document: object) -> None:
    if not isinstance(document, dict):
        raise AgentOutputValidationError("execution checkpoint must be an object")
    version = document.get("schema_version")
    if version != CHECKPOINT_SCHEMA_VERSION:
        raise AgentOutputValidationError(f"unsupported execution checkpoint version {version!r}")
    errors = sorted(
        Draft202012Validator(_checkpoint_schema_cached()).iter_errors(document),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if errors:
        error = errors[0]
        path = "$" + "".join(f"[{part!r}]" for part in error.absolute_path)
        raise AgentOutputValidationError(f"invalid execution checkpoint at {path}: {error.message}")


def execute_run(
    engine: Engine,
    lease: LeaseIdentity,
    adapter: AgentAdapter,
    *,
    registry: ToolRegistry | None = None,
    producer_component: str = "agent-runtime",
    fault_engine: FaultEngine | None = None,
) -> ExecutionResult:
    """Execute one leased Run until evaluation-ready, approval wait, or failure."""
    tool_registry = registry or default_tool_registry()
    try:
        state = _load_state(engine, lease, adapter, tool_registry, producer_component, fault_engine)
    except (StaleLeaseError, LeaseExpiredError, CheckpointConflictError):
        return ExecutionResult("stale_lease", lease.run_id, None, error_code="stale_lease")
    except _INFRASTRUCTURE_EXCEPTIONS:
        return ExecutionResult("run_not_ready", lease.run_id, None, error_code="internal_error")
    except AgentOutputValidationError:
        return ExecutionResult("run_not_ready", lease.run_id, None, error_code="run_not_ready")

    if state.run.status == "evaluating":
        answer = (
            None if state.checkpoint is None else cast(str | None, state.checkpoint["final_answer"])
        )
        version = (
            None if state.checkpoint is None else cast(int, state.checkpoint["checkpoint_version"])
        )
        return ExecutionResult("evaluation_ready", lease.run_id, version, answer)

    while True:
        checkpoint = state.checkpoint
        if checkpoint is not None and cast(list[object], checkpoint["pending_tool_calls"]):
            result = _dispatch_pending(
                engine,
                lease,
                adapter,
                tool_registry,
                state,
                producer_component,
                fault_engine,
            )
            if result is not None:
                return result
            try:
                state = _load_state(
                    engine, lease, adapter, tool_registry, producer_component, fault_engine
                )
            except (
                StaleLeaseError,
                LeaseExpiredError,
                CheckpointConflictError,
                LifecycleConflictError,
            ):
                return ExecutionResult(
                    "stale_lease",
                    lease.run_id,
                    _checkpoint_version(checkpoint),
                    error_code="stale_lease",
                )
            except _INFRASTRUCTURE_EXCEPTIONS:
                return ExecutionResult(
                    "run_not_ready",
                    lease.run_id,
                    _checkpoint_version(checkpoint),
                    error_code="internal_error",
                )
            except AgentOutputValidationError:
                return ExecutionResult(
                    "run_not_ready",
                    lease.run_id,
                    _checkpoint_version(checkpoint),
                    error_code="run_not_ready",
                )
            continue

        budgets = cast(dict[str, object], state.scenario["budgets"])
        next_step = 1 if checkpoint is None else cast(int, checkpoint["next_step_number"])
        tool_attempts = 0 if checkpoint is None else cast(int, checkpoint["tool_attempts"])
        wall_ms = 0 if checkpoint is None else cast(int, checkpoint["active_wall_time_ms"])
        known_cost = 0 if checkpoint is None else cast(int, checkpoint["known_cost_microusd"])
        cost_complete = True if checkpoint is None else cast(bool, checkpoint["cost_complete"])
        budget_error = _pre_step_budget_error(
            budgets, next_step, tool_attempts, wall_ms, known_cost, cost_complete
        )
        if budget_error is not None:
            return _terminate(
                engine, lease, state.run, "timed_out", budget_error, producer_component
            )

        context = _agent_context(state, tool_registry)
        started = monotonic_ns()
        try:
            output = adapter.invoke(context)
            elapsed_ms = _elapsed_ms(started)
            validated = _validated_output(output, state.scenario, tool_registry, adapter)
        except AgentProviderTimeout:
            return _terminate(
                engine,
                lease,
                state.run,
                "timed_out",
                "provider_timeout",
                producer_component,
                failed_adapter=adapter,
                failed_context=context,
            )
        except AgentProviderError:
            return _terminate(
                engine,
                lease,
                state.run,
                "infra_error",
                "provider_error",
                producer_component,
                failed_adapter=adapter,
                failed_context=context,
            )
        except Exception as error:
            if isinstance(error, AgentOutputValidationError):
                code = "invalid_agent_output"
                target: Literal["failed", "infra_error"] = "failed"
            else:
                code = "provider_error"
                target = "infra_error"
            return _terminate(
                engine,
                lease,
                state.run,
                target,
                code,
                producer_component,
                failed_adapter=adapter,
                failed_context=context,
            )

        try:
            state = _persist_agent_output(
                engine,
                lease,
                adapter,
                state,
                validated,
                context,
                elapsed_ms,
                producer_component,
            )
        except (
            StaleLeaseError,
            LeaseExpiredError,
            CheckpointConflictError,
            LifecycleConflictError,
        ):
            return ExecutionResult(
                "stale_lease",
                lease.run_id,
                _checkpoint_version(checkpoint),
                error_code="stale_lease",
            )
        except _INFRASTRUCTURE_EXCEPTIONS:
            return _terminate(
                engine,
                lease,
                state.run,
                "infra_error",
                "internal_error",
                producer_component,
                failed_adapter=adapter,
                failed_context=context,
            )
        except AgentOutputValidationError:
            return _terminate(
                engine,
                lease,
                state.run,
                "failed",
                "invalid_agent_output",
                producer_component,
                failed_adapter=adapter,
                failed_context=context,
            )

        checkpoint = state.checkpoint
        assert checkpoint is not None
        budget_error = _post_step_budget_error(budgets, checkpoint)
        if budget_error is not None:
            return _terminate(
                engine, lease, state.run, "timed_out", budget_error, producer_component
            )
        if cast(str, checkpoint["status"]) == "final":
            return ExecutionResult(
                "evaluation_ready",
                lease.run_id,
                cast(int, checkpoint["checkpoint_version"]),
                cast(str, checkpoint["final_answer"]),
            )


def _load_state(
    engine: Engine,
    lease: LeaseIdentity,
    adapter: AgentAdapter,
    registry: ToolRegistry,
    producer_component: str,
    fault_engine: FaultEngine | None,
) -> _State:
    with Session(engine) as session, session.begin():
        repository = PersistenceRepository(session)
        run = repository.lock_current_lease(lease)
        if run.status == "provisioning":
            run = repository.transition_owned_run(
                lease,
                "running",
                expected_version=run.lifecycle_version,
                evidence=LifecycleEvidence(
                    _identity("event-running", lease.run_id, str(lease.attempt)),
                    producer_component,
                    lease.worker_id,
                    reason_code="execution_started",
                ),
            )
        elif run.status not in {"running", "evaluating"}:
            raise LifecycleConflictError("Run is not ready for agent execution")
        scenario_record = repository.get_scenario_revision(run.scenario.id, run.scenario.revision)
        if scenario_record is None or scenario_record.scenario.digest != run.scenario.digest:
            raise PersistenceIntegrityError("Run Scenario binding does not resolve")
        if not repository.has_run_company_state(run.run_id):
            raise PersistenceIntegrityError("Run-local synthetic state is not initialized")
        if adapter.adapter_id != run.agent_configuration.id:
            raise PersistenceIntegrityError("adapter ID does not match the frozen Run reference")
        if adapter.adapter_version != run.agent_configuration.revision:
            raise PersistenceIntegrityError(
                "adapter version does not match the frozen Run reference"
            )
        configuration_digest = getattr(adapter, "configuration_digest", None)
        if configuration_digest is not None:
            configuration = repository.get_agent_configuration_reference(
                run.agent_configuration.id, run.agent_configuration.revision
            )
            if (
                configuration is None
                or configuration.configuration is None
                or configuration_digest != run.agent_configuration.digest
            ):
                raise PersistenceIntegrityError(
                    "hosted adapter configuration does not match the frozen Run reference"
                )
        _require_name(adapter.adapter_id, "adapter_id")
        _require_name(producer_component, "producer_component")
        record = repository.get_execution_checkpoint(run.run_id)
        checkpoint = None if record is None else _checkpoint_document(record, run, adapter)
        scenario = scenario_record.scenario.to_dict()
        if checkpoint is not None:
            marker_allowed_ids: set[str] = set()
            pending = cast(list[dict[str, object]], checkpoint["pending_tool_calls"])
            if pending and fault_engine is not None:
                call = pending[0]
                tool_id = cast(str, call["tool_id"])
                contract_version = cast(str, call["contract_version"])
                definition = registry.resolve(tool_id, contract_version)
                mutation_recovery = definition is not None and definition.capability == "mutation"
                attempt_number = cast(int, call["attempt_number"])
                attempt_id = _identity(
                    "attempt", cast(str, call["logical_call_id"]), str(attempt_number)
                )
                marker = repository.get_post_commit_acknowledgement(run.run_id, attempt_id)
                if marker is not None and not mutation_recovery:
                    raise PersistenceIntegrityError(
                        "post-commit marker is bound to a non-mutation pending call"
                    )
                if marker is not None:
                    recovered_result = ToolGateway(
                        session,
                        registry=registry,
                        producer_component="tool-gateway",
                        producer_instance_id=lease.worker_id,
                        fault_engine=fault_engine,
                    ).recover_post_commit_attempt(
                        lease,
                        marker=marker,
                        tool_id=cast(str, call["tool_id"]),
                        contract_version=cast(str, call["contract_version"]),
                        arguments=cast(dict[str, object], call["arguments"]),
                        logical_call_id=cast(str, call["logical_call_id"]),
                        attempt_id=attempt_id,
                        attempt_number=attempt_number,
                        call_ordinal=_logical_call_ordinal(
                            checkpoint, cast(str, call["logical_call_id"])
                        ),
                    )
                    marker_allowed_ids = {
                        marker.request_event_id,
                        marker.policy_decision_event_id,
                        marker.state_evidence_event_id,
                    }
                    if recovered_result is not None:
                        result_event = next(
                            item.event.to_dict()
                            for item in repository.fetch_events(run.run_id)
                            if item.event.to_dict()["event_id"] == marker.result_event_id
                        )
                        result_payload = cast(dict[str, object], result_event["payload"])
                        duration_ms = result_payload.get("duration_ms")
                        if type(duration_ms) is not int:
                            raise PersistenceIntegrityError(
                                "post-commit acknowledgement duration is corrupt"
                            )
                        updated = _checkpoint_after_tool(
                            checkpoint, call, attempt_id, recovered_result, duration_ms
                        )
                        all_events = repository.fetch_events(run.run_id)
                        updated["last_event_sequence"] = cast(
                            int, all_events[-1].event.to_dict()["sequence"]
                        )
                        updated["checkpoint_version"] = (
                            cast(int, checkpoint["checkpoint_version"]) + 1
                        )
                        updated["lease_attempt"] = lease.attempt
                        validate_execution_checkpoint(updated)
                        stored = repository._store_execution_checkpoint(
                            lease,
                            updated,
                            expected_version=cast(int, checkpoint["checkpoint_version"]),
                        )
                        checkpoint = _checkpoint_document(stored, run, adapter)
                        marker_allowed_ids = set()
                else:
                    recovered_result = None
                    if mutation_recovery:
                        recovered_result = ToolGateway(
                            session,
                            registry=registry,
                            producer_component="tool-gateway",
                            producer_instance_id=lease.worker_id,
                            fault_engine=fault_engine,
                        ).recover_committed_tool_attempt(
                            lease,
                            tool_id=tool_id,
                            contract_version=contract_version,
                            arguments=cast(dict[str, object], call["arguments"]),
                            logical_call_id=cast(str, call["logical_call_id"]),
                            attempt_id=attempt_id,
                            attempt_number=attempt_number,
                        )
                    if recovered_result is not None:
                        result_event = next(
                            item.event.to_dict()
                            for item in repository.fetch_events(run.run_id)
                            if item.event.to_dict()["event_id"] == recovered_result.result_event_id
                        )
                        duration_ms = cast(dict[str, object], result_event["payload"]).get(
                            "duration_ms"
                        )
                        if type(duration_ms) is not int:
                            raise PersistenceIntegrityError(
                                "durable tool result duration is corrupt"
                            )
                        updated = _checkpoint_after_tool(
                            checkpoint, call, attempt_id, recovered_result, duration_ms
                        )
                        all_events = repository.fetch_events(run.run_id)
                        updated["last_event_sequence"] = cast(
                            int, all_events[-1].event.to_dict()["sequence"]
                        )
                        updated["checkpoint_version"] = (
                            cast(int, checkpoint["checkpoint_version"]) + 1
                        )
                        updated["lease_attempt"] = lease.attempt
                        validate_execution_checkpoint(updated)
                        stored = repository._store_execution_checkpoint(
                            lease,
                            updated,
                            expected_version=cast(int, checkpoint["checkpoint_version"]),
                        )
                        checkpoint = _checkpoint_document(stored, run, adapter)
            _validate_checkpoint_semantics(checkpoint, scenario, registry, run)
            sequence = cast(int, checkpoint["last_event_sequence"])
            all_evidence = repository.fetch_events(run.run_id)
            evidence = [
                record
                for record in all_evidence
                if cast(int, record.event.to_dict()["sequence"]) <= sequence
            ]
            if not evidence or evidence[-1].event.to_dict()["sequence"] != sequence:
                raise PersistenceIntegrityError(
                    "checkpoint last_event_sequence does not identify committed Run evidence"
                )
            _validate_checkpoint_evidence(checkpoint, evidence, scenario, registry, run, adapter)
            _validate_evidence_after_checkpoint(
                checkpoint,
                all_evidence[len(evidence) :],
                allowed_event_ids=marker_allowed_ids,
            )
        if run.status == "evaluating" and (
            checkpoint is None or checkpoint.get("status") != "final"
        ):
            raise PersistenceIntegrityError("evaluating Run has no final execution checkpoint")
        if (
            checkpoint is not None
            and checkpoint.get("status") == "final"
            and run.status != "evaluating"
        ):
            raise PersistenceIntegrityError(
                "final execution checkpoint belongs to a non-evaluating Run"
            )
        return _State(run, scenario, checkpoint)


def _checkpoint_document(
    record: ExecutionCheckpointRecord, run: RunRecord, adapter: AgentAdapter
) -> dict[str, object]:
    document = cast(dict[str, object], _thaw(record.document))
    validate_execution_checkpoint(document)
    if (
        document["run_id"] != run.run_id
        or document["checkpoint_version"] != record.checkpoint_version
        or document["last_event_sequence"] != record.last_event_sequence
    ):
        raise PersistenceIntegrityError("execution checkpoint projections are inconsistent")
    adapter_ref = cast(dict[str, object], document["adapter"])
    if adapter_ref != {"id": adapter.adapter_id, "version": adapter.adapter_version}:
        raise PersistenceIntegrityError("checkpoint was created by a different adapter")
    return document


def _validate_checkpoint_semantics(
    checkpoint: dict[str, object],
    scenario: dict[str, object],
    registry: ToolRegistry,
    run: RunRecord,
) -> None:
    trajectory = cast(list[dict[str, object]], checkpoint["trajectory"])
    assistant_turns = [turn for turn in trajectory if turn["kind"] == "assistant"]
    tool_turns = [turn for turn in trajectory if turn["kind"] == "tool"]
    if checkpoint["next_step_number"] != len(assistant_turns) + 1:
        raise PersistenceIntegrityError("checkpoint next step is inconsistent")
    if checkpoint["tool_attempts"] != len(tool_turns):
        raise PersistenceIntegrityError("checkpoint tool attempt counter is inconsistent")

    budgets = cast(dict[str, object], scenario["budgets"])
    if len(assistant_turns) > cast(int, budgets["max_steps"]):
        raise PersistenceIntegrityError("checkpoint exceeds the frozen step budget")
    if len(tool_turns) > cast(int, budgets["max_tool_calls"]):
        raise PersistenceIntegrityError("checkpoint exceeds the frozen tool-call budget")

    expected_pending: list[dict[str, object]] = []
    position = 0
    expected_step = 1
    seen_logical: set[str] = set()
    while position < len(trajectory):
        assistant = trajectory[position]
        if assistant["kind"] != "assistant" or assistant["step_number"] != expected_step:
            raise PersistenceIntegrityError("checkpoint trajectory order is invalid")
        step_id = _identity("step", run.run_id, str(expected_step))
        if assistant["step_id"] != step_id:
            raise PersistenceIntegrityError("checkpoint assistant step identity is invalid")
        calls = cast(list[dict[str, object]], assistant["tool_calls"])
        if assistant["final"] and calls:
            raise PersistenceIntegrityError("final assistant turn cannot request tools")
        position += 1
        segment: list[dict[str, object]] = []
        while position < len(trajectory) and trajectory[position]["kind"] == "tool":
            segment.append(trajectory[position])
            position += 1

        segment_position = 0
        unresolved_index: int | None = None
        unresolved_approval: str | None = None
        next_attempt = 1
        for call_index, call in enumerate(calls, start=1):
            logical_id = _identity(
                "logical", run.run_id, step_id, str(call_index), cast(str, call["call_id"])
            )
            if (
                call["call_index"] != call_index
                or call["logical_call_id"] != logical_id
                or logical_id in seen_logical
            ):
                raise PersistenceIntegrityError("checkpoint logical tool identity is invalid")
            seen_logical.add(logical_id)
            _validate_checkpoint_call(call, scenario, registry)
            attempt = 1
            completed = False
            last_approval: str | None = None
            while (
                segment_position < len(segment)
                and segment[segment_position]["logical_call_id"] == logical_id
            ):
                tool_turn = segment[segment_position]
                if (
                    tool_turn["call_index"] != call_index
                    or tool_turn["step_id"] != step_id
                    or tool_turn["attempt_number"] != attempt
                    or tool_turn["attempt_id"] != _identity("attempt", logical_id, str(attempt))
                    or tool_turn["tool_id"] != call["tool_id"]
                    or tool_turn["contract_version"] != call["contract_version"]
                ):
                    raise PersistenceIntegrityError("checkpoint tool attempt identity is invalid")
                approval_wait = _is_approval_wait(tool_turn)
                last_approval = cast(str, tool_turn["approval_id"]) if approval_wait else None
                segment_position += 1
                attempt += 1
                if not approval_wait:
                    completed = True
                    break
            if completed:
                continue
            unresolved_index = call_index - 1
            unresolved_approval = last_approval
            next_attempt = attempt
            break

        if unresolved_index is not None:
            if position != len(trajectory) or segment_position != len(segment):
                raise PersistenceIntegrityError(
                    "checkpoint continued after an unresolved tool call"
                )
            for index, call in enumerate(calls[unresolved_index:], start=unresolved_index + 1):
                pending = {
                    **call,
                    "step_id": step_id,
                    "attempt_number": next_attempt if index == unresolved_index + 1 else 1,
                }
                if index == unresolved_index + 1 and unresolved_approval is not None:
                    pending["approval_id"] = unresolved_approval
                expected_pending.append(pending)
            break
        if segment_position != len(segment):
            raise PersistenceIntegrityError("checkpoint tool attempts are out of call order")
        if assistant["final"]:
            if position != len(trajectory):
                raise PersistenceIntegrityError(
                    "checkpoint trajectory continues after final intent"
                )
            break
        expected_step += 1

    if checkpoint["pending_tool_calls"] != expected_pending:
        raise PersistenceIntegrityError(
            "checkpoint pending calls are not the exact unprocessed suffix"
        )

    known_cost = 0
    cost_complete = True
    active_wall = 0
    for turn in trajectory:
        active_wall = _checked_counter_add(
            active_wall, cast(int, turn["duration_ms"]), "active wall time"
        )
        if turn["kind"] == "assistant":
            cost = cast(dict[str, object], turn["usage"])["cost_microusd"]
            if cost is None:
                cost_complete = False
            else:
                known_cost = _checked_counter_add(known_cost, cast(int, cost), "known cost")
    if (
        checkpoint["known_cost_microusd"] != known_cost
        or checkpoint["cost_complete"] is not cost_complete
        or checkpoint["active_wall_time_ms"] != active_wall
    ):
        raise PersistenceIntegrityError("checkpoint budget accumulators are inconsistent")

    last_assistant = assistant_turns[-1] if assistant_turns else None
    final_intent = last_assistant is not None and last_assistant["final"] is True
    expected_answer = None if last_assistant is None or not final_intent else last_assistant["text"]
    if checkpoint["final_answer"] != expected_answer:
        raise PersistenceIntegrityError("checkpoint final answer is inconsistent")
    if checkpoint["status"] == "final":
        if not final_intent or expected_pending or run.status != "evaluating":
            raise PersistenceIntegrityError("checkpoint final state is contradictory")
    elif run.status == "evaluating":
        raise PersistenceIntegrityError("evaluating Run has no final checkpoint")
    if final_intent and checkpoint["status"] != "final":
        if expected_pending or _post_step_budget_error(budgets, checkpoint) is None:
            raise PersistenceIntegrityError("non-final checkpoint contains valid final intent")
    waiting = checkpoint["status"] == "waiting_for_approval"
    if waiting != bool(expected_pending and "approval_id" in expected_pending[0]):
        raise PersistenceIntegrityError("checkpoint approval-wait state is inconsistent")


def _validate_checkpoint_call(
    call: dict[str, object], scenario: dict[str, object], registry: ToolRegistry
) -> None:
    allowed = set(cast(list[str], cast(dict[str, object], scenario["agent"])["allowed_tools"]))
    tool_id = cast(str, call["tool_id"])
    version = cast(str, call["contract_version"])
    definition = registry.resolve(tool_id, version)
    if (
        tool_id not in allowed
        or SCENARIO_V0_TOOL_VERSIONS.get(tool_id) != version
        or definition is None
    ):
        raise PersistenceIntegrityError("checkpoint tool is not authorized")
    if list(Draft202012Validator(dict(definition.input_schema)).iter_errors(call["arguments"])):
        raise PersistenceIntegrityError("checkpoint tool arguments are invalid")


def _is_approval_wait(turn: dict[str, object]) -> bool:
    error = turn["error"]
    return isinstance(error, dict) and error.get("code") in {
        "approval_required",
        "approval_pending",
    }


def _validate_checkpoint_evidence(
    checkpoint: dict[str, object],
    evidence: Sequence[RunEventRecord],
    scenario: dict[str, object],
    registry: ToolRegistry,
    run: RunRecord,
    adapter: AgentAdapter,
) -> None:
    documents = [record.event.to_dict() for record in evidence]
    by_id = {cast(str, document["event_id"]): document for document in documents}
    if len(by_id) != len(documents):
        raise PersistenceIntegrityError("Run evidence repeats an event identity")
    trajectory = cast(list[dict[str, object]], checkpoint["trajectory"])
    used_tool_events: set[str] = set()
    used_agent_events: set[str] = set()
    prior_sequence = 0
    prefix: list[dict[str, object]] = []
    for turn in trajectory:
        if turn["kind"] == "assistant":
            step_number = cast(int, turn["step_number"])
            event = by_id.get(_identity("event-agent", run.run_id, str(step_number)))
            payload = None if event is None else cast(dict[str, object], event["payload"])
            context = _agent_context_from_trajectory(run, scenario, registry, prefix, step_number)
            expected_output = _assistant_output_payload(turn)
            if (
                event is None
                or event["run_id"] != run.run_id
                or event["event_type"] != "agent.step"
                or cast(int, event["sequence"]) <= prior_sequence
                or payload is None
                or payload.get("phase") != "completed"
                or payload.get("step_id") != turn["step_id"]
                or payload.get("step_number") != step_number
                or payload.get("model_call_id")
                != _identity("model-call", run.run_id, str(step_number))
                or not _model_matches_adapter(payload.get("model"), adapter)
                or payload.get("input_digest") != digest_payload_v0(_context_payload(context))
                or payload.get("output_digest") != digest_payload_v0(expected_output)
            ):
                raise PersistenceIntegrityError("assistant trajectory has no matching evidence")
            prior_sequence = cast(int, event["sequence"])
            used_agent_events.add(cast(str, event["event_id"]))
            prefix.append(turn)
            continue
        request = by_id.get(cast(str, turn["request_event_id"]))
        result = by_id.get(cast(str, turn["result_event_id"]))
        request_payload = None if request is None else cast(dict[str, object], request["payload"])
        result_payload = None if result is None else cast(dict[str, object], result["payload"])
        if (
            request is None
            or result is None
            or request["run_id"] != run.run_id
            or result["run_id"] != run.run_id
            or request["event_type"] != "tool.requested"
            or result["event_type"] != "tool.result"
            or request["event_id"] in used_tool_events
            or result["event_id"] in used_tool_events
            or cast(int, request["sequence"]) <= prior_sequence
            or cast(int, result["sequence"]) <= cast(int, request["sequence"])
            or request_payload is None
            or result_payload is None
            or request_payload.get("logical_call_id") != turn["logical_call_id"]
            or request_payload.get("attempt_id") != turn["attempt_id"]
            or request_payload.get("attempt_number") != turn["attempt_number"]
            or request_payload.get("step_id") != turn["step_id"]
            or request_payload.get("tool_id") != turn["tool_id"]
            or request_payload.get("arguments_digest")
            != digest_payload_v0(_arguments_for_tool_turn(trajectory, turn))
            or result_payload.get("logical_call_id") != turn["logical_call_id"]
            or result_payload.get("attempt_id") != turn["attempt_id"]
            or result_payload.get("attempt_number") != turn["attempt_number"]
            or result_payload.get("tool_id") != turn["tool_id"]
            or result_payload.get("request_event_id") != turn["request_event_id"]
            or result_payload.get("duration_ms") != turn["duration_ms"]
            or not _tool_result_matches_checkpoint(turn, result_payload)
            or request.get("causation_event_id")
            != _identity(
                "event-agent", run.run_id, str(_step_number_for_id(trajectory, turn["step_id"]))
            )
            or not _tool_causation_is_valid(request, result, documents, turn)
        ):
            raise PersistenceIntegrityError("tool trajectory has no matching evidence pair")
        used_tool_events.update((cast(str, request["event_id"]), cast(str, result["event_id"])))
        prior_sequence = cast(int, result["sequence"])
        prefix.append(turn)

    authoritative_agent_events = {
        cast(str, event["event_id"])
        for event in documents
        if event["event_type"] == "agent.step"
        and cast(dict[str, object], event["payload"]).get("phase") == "completed"
    }
    represented_step_ids = {turn["step_id"] for turn in trajectory if turn["kind"] == "assistant"}
    authoritative_requests = {
        cast(str, event["event_id"])
        for event in documents
        if event["event_type"] == "tool.requested"
        and cast(dict[str, object], event["payload"]).get("step_id") in represented_step_ids
    }
    authoritative_results = {
        cast(str, event["event_id"])
        for event in documents
        if event["event_type"] == "tool.result"
        and cast(dict[str, object], event["payload"]).get("request_event_id")
        in authoritative_requests
    }
    if used_agent_events != authoritative_agent_events or used_tool_events != (
        authoritative_requests | authoritative_results
    ):
        raise PersistenceIntegrityError(
            "checkpoint omits or fabricates authoritative execution evidence"
        )


def _validate_evidence_after_checkpoint(
    checkpoint: dict[str, object],
    later_evidence: Sequence[RunEventRecord],
    *,
    allowed_event_ids: set[str] | None = None,
) -> None:
    pending = cast(list[dict[str, object]], checkpoint["pending_tool_calls"])
    pending_approval = (
        pending[0].get("approval_id") if pending and "approval_id" in pending[0] else None
    )
    for record in later_evidence:
        event = record.event.to_dict()
        if event["event_id"] in (allowed_event_ids or set()):
            continue
        if event["event_type"] == "run.lifecycle":
            continue
        if (
            event["event_type"] == "approval.resolved"
            and cast(dict[str, object], event["payload"]).get("approval_id") == pending_approval
        ):
            continue
        raise PersistenceIntegrityError(
            "authoritative execution evidence exists after the checkpoint boundary"
        )


def _assistant_output_payload(turn: dict[str, object]) -> dict[str, object]:
    return {
        "text": turn["text"],
        "final": turn["final"],
        "tool_calls": [
            {
                "call_id": call["call_id"],
                "tool_id": call["tool_id"],
                "contract_version": call["contract_version"],
                "arguments": call["arguments"],
            }
            for call in cast(list[dict[str, object]], turn["tool_calls"])
        ],
        "usage": turn["usage"],
        "duration_ms": turn["duration_ms"],
    }


def _arguments_for_tool_turn(
    trajectory: Sequence[dict[str, object]], tool_turn: dict[str, object]
) -> object:
    for turn in trajectory:
        if turn["kind"] != "assistant":
            continue
        for call in cast(list[dict[str, object]], turn["tool_calls"]):
            if call["logical_call_id"] == tool_turn["logical_call_id"]:
                return call["arguments"]
    raise PersistenceIntegrityError("tool trajectory has no originating assistant call")


def _step_number_for_id(trajectory: Sequence[dict[str, object]], step_id: object) -> int:
    for turn in trajectory:
        if turn["kind"] == "assistant" and turn["step_id"] == step_id:
            return cast(int, turn["step_number"])
    raise PersistenceIntegrityError("tool trajectory has no originating assistant step")


def _tool_result_matches_checkpoint(
    turn: dict[str, object], result_payload: dict[str, object]
) -> bool:
    error = turn["error"]
    if result_payload["outcome"] == "succeeded":
        return (
            turn["outcome"] == "succeeded"
            and error is None
            and isinstance(turn["output"], dict)
            and result_payload.get("response_digest") == digest_payload_v0(turn["output"])
            and "error_code" not in result_payload
        )
    if not isinstance(error, dict):
        return False
    code = error.get("code")
    denied_codes = {
        "policy_denied",
        "approval_required",
        "approval_not_found",
        "approval_pending",
        "approval_denied",
        "approval_mismatch",
    }
    expected_checkpoint_outcome = "denied" if code in denied_codes else "failed"
    expected_event_outcome = "timed_out" if code == "fault_timeout" else "failed"
    output = turn["output"]
    response_matches = (
        "response_digest" not in result_payload
        if output is None
        else isinstance(output, dict)
        and result_payload.get("response_digest") == digest_payload_v0(output)
    )
    return (
        result_payload["outcome"] == expected_event_outcome
        and turn["outcome"] == expected_checkpoint_outcome
        and result_payload.get("error_code") == code
        and response_matches
    )


def _tool_causation_is_valid(
    request: dict[str, object],
    result: dict[str, object],
    documents: Sequence[dict[str, object]],
    turn: dict[str, object],
) -> bool:
    request_id = request["event_id"]
    logical_id = turn["logical_call_id"]
    policy_events = [
        event
        for event in documents
        if event["event_type"] == "policy.decision"
        and event.get("causation_event_id") == request_id
        and cast(dict[str, object], event["payload"]).get("logical_call_id") == logical_id
        and cast(int, request["sequence"])
        < cast(int, event["sequence"])
        < cast(int, result["sequence"])
    ]
    if len(policy_events) != 1:
        return False
    cause_id = result.get("causation_event_id")
    if "approval_id" not in turn:
        expected_base_cause = policy_events[0]["event_id"]
    else:
        approval_id = turn["approval_id"]
        expected_type = "approval.requested" if _is_approval_wait(turn) else "approval.resolved"
        approval_causes = [
            event
            for event in documents
            if event["event_type"] == expected_type
            and cast(dict[str, object], event["payload"]).get("approval_id") == approval_id
            and cast(int, event["sequence"]) < cast(int, result["sequence"])
        ]
        if len(approval_causes) != 1:
            return False
        expected_base_cause = approval_causes[0]["event_id"]
    if cause_id == expected_base_cause:
        return True
    return _fault_causation_is_valid(request, result, documents, turn)


def _fault_causation_is_valid(
    request: dict[str, object],
    result: dict[str, object],
    documents: Sequence[dict[str, object]],
    turn: dict[str, object],
) -> bool:
    request_id = cast(str, request["event_id"])
    result_id = cast(str, result["event_id"])
    logical_id = turn["logical_call_id"]
    request_sequence = cast(int, request["sequence"])
    result_sequence = cast(int, result["sequence"])
    relevant = [
        event
        for event in documents
        if event.get("correlation_id") == logical_id
        and event["event_type"]
        in {"fault.not_matched", "fault.matched", "fault.applied", "fault.observed"}
        and (
            event.get("causation_event_id") in {request_id, result_id}
            or request_id
            in cast(
                list[object], cast(dict[str, object], event["payload"]).get("related_event_ids", [])
            )
        )
    ]
    by_id = {cast(str, event["event_id"]): event for event in relevant}
    matched = [event for event in relevant if event["event_type"] == "fault.matched"]
    applied = [event for event in relevant if event["event_type"] == "fault.applied"]
    observed = [event for event in relevant if event["event_type"] == "fault.observed"]
    not_matched = [event for event in relevant if event["event_type"] == "fault.not_matched"]
    if (
        not applied
        or result.get("causation_event_id")
        != max(applied, key=lambda event: cast(int, event["sequence"]))["event_id"]
    ):
        return False
    if any(
        event.get("causation_event_id") != request_id
        or not request_sequence < cast(int, event["sequence"]) < result_sequence
        for event in matched + not_matched
    ):
        return False
    for application in applied:
        payload = cast(dict[str, object], application["payload"])
        matched_event = by_id.get(cast(str, application.get("causation_event_id")))
        if matched_event is None or matched_event["event_type"] != "fault.matched":
            return False
        matched_payload = cast(dict[str, object], matched_event["payload"])
        related = payload.get("related_event_ids")
        if (
            not isinstance(related, list)
            or not request_sequence < cast(int, application["sequence"]) < result_sequence
            or payload.get("fault_id") != matched_payload.get("fault_id")
            or payload.get("activation_id") != matched_payload.get("activation_id")
            or set(cast(list[str], related)) != {request_id, matched_event["event_id"]}
        ):
            return False
        matching_observed = [
            event
            for event in observed
            if event.get("causation_event_id") == result_id
            and cast(dict[str, object], event["payload"]).get("fault_id") == payload.get("fault_id")
            and cast(dict[str, object], event["payload"]).get("activation_id")
            == payload.get("activation_id")
        ]
        if len(matching_observed) != 1:
            return False
        observed_payload = cast(dict[str, object], matching_observed[0]["payload"])
        observed_related = observed_payload.get("related_event_ids")
        if (
            not isinstance(observed_related, list)
            or cast(int, matching_observed[0]["sequence"]) <= result_sequence
            or set(cast(list[str], observed_related))
            != {request_id, application["event_id"], result_id}
        ):
            return False
    return len(observed) == len(applied)


def _agent_context_from_trajectory(
    run: RunRecord,
    scenario: dict[str, object],
    registry: ToolRegistry,
    trajectory: Sequence[dict[str, object]],
    step_number: int,
) -> AgentContext:
    attempts = sum(turn["kind"] == "tool" for turn in trajectory)
    wall = sum(cast(int, turn["duration_ms"]) for turn in trajectory)
    costs = [
        cast(dict[str, object], turn["usage"])["cost_microusd"]
        for turn in trajectory
        if turn["kind"] == "assistant"
    ]
    known_cost = sum(cast(int, cost) for cost in costs if cost is not None)
    return _build_agent_context(
        run,
        scenario,
        registry,
        trajectory,
        step_number,
        attempts,
        wall,
        known_cost,
        all(cost is not None for cost in costs),
    )


def _context_payload(context: AgentContext) -> dict[str, object]:
    return {
        "run_id": context.run_id,
        "step_number": context.step_number,
        "task": context.task,
        "instructions": list(context.instructions),
        "trajectory": [_thaw(turn) for turn in context.trajectory],
        "tools": [
            {
                "tool_id": tool.tool_id,
                "contract_version": tool.contract_version,
                "description": tool.description,
                "input_schema": _thaw(tool.input_schema),
            }
            for tool in context.tools
        ],
        "remaining_budgets": {
            "steps": context.remaining_steps,
            "tool_calls": context.remaining_tool_calls,
            "active_wall_time_ms": context.remaining_wall_time_ms,
            "known_cost_microusd": context.remaining_known_cost_microusd,
            "cost_complete": context.cost_complete,
        },
    }


def _agent_context(state: _State, registry: ToolRegistry) -> AgentContext:
    checkpoint = state.checkpoint
    next_step = 1 if checkpoint is None else cast(int, checkpoint["next_step_number"])
    attempts = 0 if checkpoint is None else cast(int, checkpoint["tool_attempts"])
    wall = 0 if checkpoint is None else cast(int, checkpoint["active_wall_time_ms"])
    cost = 0 if checkpoint is None else cast(int, checkpoint["known_cost_microusd"])
    complete = True if checkpoint is None else cast(bool, checkpoint["cost_complete"])
    trajectory = (
        [] if checkpoint is None else cast(list[dict[str, object]], checkpoint["trajectory"])
    )
    return _build_agent_context(
        state.run,
        state.scenario,
        registry,
        trajectory,
        next_step,
        attempts,
        wall,
        cost,
        complete,
    )


def _build_agent_context(
    run: RunRecord,
    scenario: dict[str, object],
    registry: ToolRegistry,
    trajectory: Sequence[dict[str, object]],
    next_step: int,
    attempts: int,
    wall: int,
    cost: int,
    complete: bool,
) -> AgentContext:
    agent = cast(dict[str, object], scenario["agent"])
    budgets = cast(dict[str, object], scenario["budgets"])
    allowed = set(cast(list[str], agent["allowed_tools"]))
    definitions = tuple(
        _tool_spec(definition)
        for definition in registry.definitions
        if definition.tool_id in allowed
        and SCENARIO_V0_TOOL_VERSIONS.get(definition.tool_id) == definition.contract_version
    )
    return AgentContext(
        run.run_id,
        cast(str, agent["task"]),
        tuple(cast(list[str], agent["instructions"])),
        next_step,
        tuple(cast(Mapping[str, object], _freeze(_thaw(item))) for item in trajectory),
        definitions,
        cast(int, budgets["max_steps"]) - next_step + 1,
        cast(int, budgets["max_tool_calls"]) - attempts,
        cast(int, budgets["max_wall_time_ms"]) - wall,
        cast(int, budgets["max_cost_microusd"]) - cost,
        complete,
    )


def _tool_spec(definition: ToolDefinition) -> AgentToolSpec:
    return AgentToolSpec(
        definition.tool_id,
        definition.contract_version,
        definition.description,
        cast(Mapping[str, object], _freeze(_thaw(definition.input_schema))),
    )


def _validated_output(
    output: object,
    scenario: dict[str, object],
    registry: ToolRegistry,
    adapter: AgentAdapter,
) -> AgentOutput:
    if type(output) is not AgentOutput:
        raise AgentOutputValidationError("adapter output must be AgentOutput")
    assert isinstance(output, AgentOutput)
    if type(output.text) is not str or len(output.text) > _MAX_ASSISTANT_TEXT:
        raise AgentOutputValidationError("assistant text is invalid")
    if type(output.final) is not bool or type(output.tool_calls) is not tuple:
        raise AgentOutputValidationError("adapter output fields have invalid types")
    if len(output.tool_calls) > _MAX_TOOL_CALLS_PER_STEP:
        raise AgentOutputValidationError("adapter requested too many tools in one step")
    if output.final and output.tool_calls:
        raise AgentOutputValidationError("final answer cannot also request tools")
    _validate_usage(output.usage)
    _validate_provider_metadata(output.provider_metadata, adapter)
    allowed = set(cast(list[str], cast(dict[str, object], scenario["agent"])["allowed_tools"]))
    seen: set[str] = set()
    validated_calls: list[AgentToolCall] = []
    for call in output.tool_calls:
        if type(call) is not AgentToolCall:
            raise AgentOutputValidationError("tool call must be AgentToolCall")
        _require_id(call.call_id, "call_id")
        if call.call_id in seen:
            raise AgentOutputValidationError("duplicate adapter tool call ID")
        seen.add(call.call_id)
        if type(call.tool_id) is not str or type(call.contract_version) is not str:
            raise AgentOutputValidationError("tool ID and contract version must be strings")
        if call.tool_id not in allowed:
            raise AgentOutputValidationError("adapter requested a tool outside the Scenario")
        expected = SCENARIO_V0_TOOL_VERSIONS.get(call.tool_id)
        definition = registry.resolve(call.tool_id, call.contract_version)
        if expected != call.contract_version or definition is None:
            raise AgentOutputValidationError("adapter requested an unknown tool version")
        if not isinstance(call.arguments, Mapping):
            raise AgentOutputValidationError("tool arguments must be an object")
        arguments = _thaw(call.arguments)
        if not isinstance(arguments, dict):
            raise AgentOutputValidationError("tool arguments must snapshot to an object")
        errors = list(Draft202012Validator(dict(definition.input_schema)).iter_errors(arguments))
        if errors:
            raise AgentOutputValidationError("adapter supplied invalid tool arguments")
        validated_calls.append(
            AgentToolCall(
                call.call_id,
                call.tool_id,
                call.contract_version,
                cast(Mapping[str, object], _freeze(arguments)),
            )
        )
    validated = AgentOutput(
        output.text,
        tuple(validated_calls),
        output.final,
        output.usage,
        output.provider_metadata,
    )
    try:
        validate_jsonb_persistence_profile(_output_payload(validated, 0), "agent output")
        digest_payload_v0(_output_payload(validated, 0))
    except PersistenceError as error:
        raise AgentOutputValidationError("agent output is not persistable JSON") from error
    return validated


def _validate_usage(usage: object) -> None:
    if type(usage) is not AgentUsage:
        raise AgentOutputValidationError("usage must be AgentUsage")
    assert isinstance(usage, AgentUsage)
    for value in (usage.input_tokens, usage.output_tokens, usage.cost_microusd):
        if value is not None and (type(value) is not int or not 0 <= value <= _SAFE_INTEGER):
            raise AgentOutputValidationError("usage values must be non-negative safe integers")
    if usage.cost_microusd is not None and usage.cost_microusd > _MAX_SINGLE_CALL_COST_MICROUSD:
        raise AgentOutputValidationError("cost exceeds the Scenario v0 single-call bound")


def _persist_agent_output(
    engine: Engine,
    lease: LeaseIdentity,
    adapter: AgentAdapter,
    prior: _State,
    output: AgentOutput,
    context: AgentContext,
    elapsed_ms: int,
    producer_component: str,
) -> _State:
    with Session(engine) as session, session.begin():
        repository = PersistenceRepository(session)
        run = repository.lock_current_lease(lease)
        if run.status != "running":
            raise LifecycleConflictError("Run left running before adapter output was committed")
        current_record = repository.get_execution_checkpoint(run.run_id)
        expected_version = _checkpoint_version(prior.checkpoint) or 0
        if (0 if current_record is None else current_record.checkpoint_version) != expected_version:
            raise CheckpointConflictError("adapter output is based on a stale checkpoint")
        step_number = (
            1 if prior.checkpoint is None else cast(int, prior.checkpoint["next_step_number"])
        )
        step_id = _identity("step", run.run_id, str(step_number))
        input_payload = _context_payload(context)
        output_payload = _output_payload(output, elapsed_ms)
        event = _append_agent_step(
            repository,
            run,
            adapter,
            step_id,
            step_number,
            input_payload,
            output_payload,
            output.provider_metadata,
            producer_component,
        )
        previous = prior.checkpoint
        trajectory = [] if previous is None else cast(list[object], _thaw(previous["trajectory"]))
        calls: list[dict[str, object]] = []
        pending: list[dict[str, object]] = []
        for ordinal, call in enumerate(output.tool_calls, start=1):
            logical_id = _identity("logical", run.run_id, step_id, str(ordinal), call.call_id)
            call_document = {
                "call_id": call.call_id,
                "call_index": ordinal,
                "logical_call_id": logical_id,
                "tool_id": call.tool_id,
                "contract_version": call.contract_version,
                "arguments": _thaw(call.arguments),
            }
            calls.append(call_document)
            pending.append({**call_document, "step_id": step_id, "attempt_number": 1})
        usage = {
            "input_tokens": output.usage.input_tokens,
            "output_tokens": output.usage.output_tokens,
            "cost_microusd": output.usage.cost_microusd,
        }
        trajectory.append(
            {
                "kind": "assistant",
                "step_id": step_id,
                "step_number": step_number,
                "text": output.text,
                "final": output.final,
                "duration_ms": elapsed_ms,
                "tool_calls": calls,
                "usage": usage,
            }
        )
        prior_cost = 0 if previous is None else cast(int, previous["known_cost_microusd"])
        prior_complete = True if previous is None else cast(bool, previous["cost_complete"])
        next_wall = _checked_counter_add(
            0 if previous is None else cast(int, previous["active_wall_time_ms"]),
            elapsed_ms,
            "active wall time",
        )
        next_cost = _checked_counter_add(prior_cost, output.usage.cost_microusd or 0, "known cost")
        next_cost_complete = prior_complete and output.usage.cost_microusd is not None
        budgets = cast(dict[str, object], prior.scenario["budgets"])
        within_post_call_budgets = (
            next_cost_complete
            and next_cost <= cast(int, budgets["max_cost_microusd"])
            and next_wall < cast(int, budgets["max_wall_time_ms"])
        )
        document: dict[str, object] = {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "run_id": run.run_id,
            "checkpoint_version": expected_version + 1,
            "lease_attempt": lease.attempt,
            "last_event_sequence": cast(int, event.event.to_dict()["sequence"]),
            "adapter": {"id": adapter.adapter_id, "version": adapter.adapter_version},
            "next_step_number": step_number + 1,
            "tool_attempts": 0 if previous is None else previous["tool_attempts"],
            "active_wall_time_ms": next_wall,
            "known_cost_microusd": next_cost,
            "cost_complete": next_cost_complete,
            "status": "final" if output.final and within_post_call_budgets else "active",
            "trajectory": trajectory,
            "pending_tool_calls": pending,
            "final_answer": output.text if output.final else None,
        }
        validate_execution_checkpoint(document)
        stored = repository._store_execution_checkpoint(
            lease, document, expected_version=expected_version
        )
        if output.final and within_post_call_budgets:
            run = repository.transition_owned_run(
                lease,
                "evaluating",
                expected_version=run.lifecycle_version,
                evidence=LifecycleEvidence(
                    _identity("event-evaluating", run.run_id, str(step_number)),
                    producer_component,
                    lease.worker_id,
                    causation_event_id=cast(str, event.event.to_dict()["event_id"]),
                    reason_code="agent_final_answer",
                ),
            )
        return _State(run, prior.scenario, _checkpoint_document(stored, run, adapter))


def _append_agent_step(
    repository: PersistenceRepository,
    run: RunRecord,
    adapter: AgentAdapter,
    step_id: str,
    step_number: int,
    input_payload: object,
    output_payload: object,
    provider_metadata: AgentProviderMetadata | None,
    producer_component: str,
) -> RunEventRecord:
    observed = repository.database_time()
    timestamp = observed.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    payload: dict[str, object] = {
        "step_id": step_id,
        "step_number": step_number,
        "phase": "completed",
        "model_call_id": _identity("model-call", run.run_id, str(step_number)),
        "model": _provider_model(adapter, provider_metadata),
        "input_digest": digest_payload_v0(input_payload),
        "output_digest": digest_payload_v0(output_payload),
    }

    def factory(sequence: int) -> RunEvent:
        document = {
            "schema_version": "chaosagent.run-event/v0",
            "event_id": _identity("event-agent", run.run_id, str(step_number)),
            "run_id": run.run_id,
            "sequence": sequence,
            "occurred_at": timestamp,
            "recorded_at": timestamp,
            "event_type": "agent.step",
            "producer": {"component": producer_component, "instance_id": run.lease_owner},
            "correlation_id": step_id,
            "payload": payload,
            "payload_digest": digest_payload_v0(payload),
        }
        return loads_run_event(json.dumps(document))

    return repository.append_event_allocated(run.run_id, factory)


def _dispatch_pending(
    engine: Engine,
    lease: LeaseIdentity,
    adapter: AgentAdapter,
    registry: ToolRegistry,
    state: _State,
    producer_component: str,
    fault_engine: FaultEngine | None,
) -> ExecutionResult | None:
    checkpoint = state.checkpoint
    assert checkpoint is not None
    budgets = cast(dict[str, object], state.scenario["budgets"])
    if cast(int, checkpoint["tool_attempts"]) >= cast(int, budgets["max_tool_calls"]):
        return _terminate(
            engine, lease, state.run, "timed_out", "max_tool_calls_exceeded", producer_component
        )
    if cast(int, checkpoint["active_wall_time_ms"]) >= cast(int, budgets["max_wall_time_ms"]):
        return _terminate(
            engine, lease, state.run, "timed_out", "max_wall_time_exceeded", producer_component
        )
    call = cast(dict[str, object], cast(list[object], checkpoint["pending_tool_calls"])[0])
    if (
        fault_engine is not None
        and isinstance(call.get("tool_id"), str)
        and fault_engine.has_after_commit_rule(cast(str, call["tool_id"]))
    ):
        return _dispatch_pending_post_commit(
            engine,
            lease,
            adapter,
            registry,
            state,
            producer_component,
            fault_engine,
            call,
        )
    try:
        with Session(engine) as session, session.begin():
            repository = PersistenceRepository(session)
            run = repository.lock_current_lease(lease)
            if run.status != "running":
                raise LifecycleConflictError("Run left running before tool dispatch")
            record = repository.get_execution_checkpoint(run.run_id)
            if record is None or record.checkpoint_version != checkpoint["checkpoint_version"]:
                raise CheckpointConflictError("tool dispatch is based on a stale checkpoint")
            attempt_number = cast(int, call["attempt_number"])
            attempt_id = _identity(
                "attempt", cast(str, call["logical_call_id"]), str(attempt_number)
            )
            gateway = ToolGateway(
                session,
                registry=registry,
                producer_component="tool-gateway",
                producer_instance_id=lease.worker_id,
                fault_engine=fault_engine,
            )
            result = gateway.execute(
                lease,
                tool_id=call["tool_id"],
                contract_version=call["contract_version"],
                arguments=call["arguments"],
                logical_call_id=call["logical_call_id"],
                attempt_id=attempt_id,
                attempt_number=attempt_number,
                call_ordinal=_logical_call_ordinal(checkpoint, cast(str, call["logical_call_id"])),
                step_id=call["step_id"],
                causation_event_id=_identity(
                    "event-agent",
                    run.run_id,
                    str(cast(int, checkpoint["next_step_number"]) - 1),
                ),
                approval_id=call.get("approval_id"),
            )
            if result.error is not None and result.error.code == "stale_lease":
                raise StaleLeaseError("gateway rejected stale execution lease")
            if result.error is not None and result.error.code == "infrastructure_error":
                raise PersistenceIntegrityError("gateway infrastructure failure")
            if result.request_event_id is None or result.result_event_id is None:
                raise PersistenceIntegrityError("gateway returned no authoritative tool evidence")
            latest_events = repository.fetch_events(run.run_id)
            if not latest_events:
                raise PersistenceIntegrityError("tool execution committed no evidence")
            result_documents = [
                record.event.to_dict()
                for record in latest_events
                if record.event.to_dict()["event_id"] == result.result_event_id
            ]
            if len(result_documents) != 1:
                raise PersistenceIntegrityError("gateway result evidence does not resolve")
            result_payload = cast(dict[str, object], result_documents[0]["payload"])
            duration_ms = result_payload.get("duration_ms")
            if type(duration_ms) is not int:
                raise PersistenceIntegrityError("gateway result duration is invalid")
            updated = _checkpoint_after_tool(checkpoint, call, attempt_id, result, duration_ms)
            updated["last_event_sequence"] = cast(
                int, latest_events[-1].event.to_dict()["sequence"]
            )
            updated["checkpoint_version"] = cast(int, checkpoint["checkpoint_version"]) + 1
            updated["lease_attempt"] = lease.attempt
            validate_execution_checkpoint(updated)
            stored = repository._store_execution_checkpoint(
                lease,
                updated,
                expected_version=cast(int, checkpoint["checkpoint_version"]),
            )
            new_checkpoint = _checkpoint_document(stored, run, adapter)
    except (
        StaleLeaseError,
        LeaseExpiredError,
        CheckpointConflictError,
        LifecycleConflictError,
    ):
        return ExecutionResult(
            "stale_lease",
            lease.run_id,
            cast(int, checkpoint["checkpoint_version"]),
            error_code="stale_lease",
        )
    except _INFRASTRUCTURE_EXCEPTIONS:
        return _terminate(
            engine, lease, state.run, "infra_error", "internal_error", producer_component
        )
    if new_checkpoint["status"] == "waiting_for_approval":
        first = cast(dict[str, object], cast(list[object], new_checkpoint["pending_tool_calls"])[0])
        return ExecutionResult(
            "waiting_for_approval",
            lease.run_id,
            cast(int, new_checkpoint["checkpoint_version"]),
            approval_id=cast(str, first["approval_id"]),
            error_code="approval_pending",
        )
    return None


def _dispatch_pending_post_commit(
    engine: Engine,
    lease: LeaseIdentity,
    adapter: AgentAdapter,
    registry: ToolRegistry,
    state: _State,
    producer_component: str,
    fault_engine: FaultEngine,
    call: dict[str, object],
) -> ExecutionResult | None:
    """Keep the effect/acknowledgement commits outside checkpoint persistence."""
    checkpoint = state.checkpoint
    assert checkpoint is not None
    attempt_number = cast(int, call["attempt_number"])
    attempt_id = _identity("attempt", cast(str, call["logical_call_id"]), str(attempt_number))
    try:
        with Session(engine) as validation_session, validation_session.begin():
            repository = PersistenceRepository(validation_session)
            run = repository.lock_current_lease(lease)
            if run.status != "running":
                raise LifecycleConflictError("Run left running before tool dispatch")
            record = repository.get_execution_checkpoint(run.run_id)
            if record is None or record.checkpoint_version != checkpoint["checkpoint_version"]:
                raise CheckpointConflictError("tool dispatch is based on a stale checkpoint")

        with Session(engine) as gateway_session:
            result = ToolGateway(
                gateway_session,
                registry=registry,
                producer_component="tool-gateway",
                producer_instance_id=lease.worker_id,
                fault_engine=fault_engine,
            ).execute(
                lease,
                tool_id=call["tool_id"],
                contract_version=call["contract_version"],
                arguments=call["arguments"],
                logical_call_id=call["logical_call_id"],
                attempt_id=attempt_id,
                attempt_number=attempt_number,
                call_ordinal=_logical_call_ordinal(checkpoint, cast(str, call["logical_call_id"])),
                step_id=call["step_id"],
                causation_event_id=_identity(
                    "event-agent",
                    lease.run_id,
                    str(cast(int, checkpoint["next_step_number"]) - 1),
                ),
                approval_id=call.get("approval_id"),
            )
        if result.error is not None and result.error.code == "stale_lease":
            raise StaleLeaseError("gateway rejected stale execution lease")
        if result.error is not None and result.error.code == "infrastructure_error":
            raise PersistenceIntegrityError("gateway infrastructure failure")
        if result.request_event_id is None or result.result_event_id is None:
            raise PersistenceIntegrityError("gateway returned no authoritative tool evidence")

        with Session(engine) as checkpoint_session, checkpoint_session.begin():
            repository = PersistenceRepository(checkpoint_session)
            run = repository.lock_current_lease(lease)
            record = repository.get_execution_checkpoint(run.run_id)
            if record is None or record.checkpoint_version != checkpoint["checkpoint_version"]:
                raise CheckpointConflictError("tool dispatch is based on a stale checkpoint")
            latest_events = repository.fetch_events(run.run_id)
            result_documents = [
                event.event.to_dict()
                for event in latest_events
                if event.event.to_dict()["event_id"] == result.result_event_id
            ]
            if len(result_documents) != 1:
                raise PersistenceIntegrityError("gateway result evidence does not resolve")
            duration_ms = cast(dict[str, object], result_documents[0]["payload"]).get("duration_ms")
            if type(duration_ms) is not int:
                raise PersistenceIntegrityError("gateway result duration is invalid")
            updated = _checkpoint_after_tool(checkpoint, call, attempt_id, result, duration_ms)
            updated["last_event_sequence"] = cast(
                int, latest_events[-1].event.to_dict()["sequence"]
            )
            updated["checkpoint_version"] = cast(int, checkpoint["checkpoint_version"]) + 1
            updated["lease_attempt"] = lease.attempt
            validate_execution_checkpoint(updated)
            stored = repository._store_execution_checkpoint(
                lease,
                updated,
                expected_version=cast(int, checkpoint["checkpoint_version"]),
            )
            new_checkpoint = _checkpoint_document(stored, run, adapter)
    except (StaleLeaseError, LeaseExpiredError, CheckpointConflictError, LifecycleConflictError):
        return ExecutionResult(
            "stale_lease",
            lease.run_id,
            cast(int, checkpoint["checkpoint_version"]),
            error_code="stale_lease",
        )
    except _INFRASTRUCTURE_EXCEPTIONS:
        return _terminate(
            engine, lease, state.run, "infra_error", "internal_error", producer_component
        )
    if new_checkpoint["status"] == "waiting_for_approval":
        first = cast(dict[str, object], cast(list[object], new_checkpoint["pending_tool_calls"])[0])
        return ExecutionResult(
            "waiting_for_approval",
            lease.run_id,
            cast(int, new_checkpoint["checkpoint_version"]),
            approval_id=cast(str, first["approval_id"]),
            error_code="approval_pending",
        )
    return None


def _checkpoint_after_tool(
    checkpoint: dict[str, object],
    call: dict[str, object],
    attempt_id: str,
    result: ToolExecutionResult,
    elapsed_ms: int,
) -> dict[str, object]:
    updated = cast(dict[str, object], _thaw(checkpoint))
    trajectory = cast(list[object], updated["trajectory"])
    error = None
    if result.error is not None:
        error = {"code": result.error.code}
    turn: dict[str, object] = {
        "kind": "tool",
        "call_index": call["call_index"],
        "step_id": call["step_id"],
        "logical_call_id": call["logical_call_id"],
        "attempt_id": attempt_id,
        "attempt_number": call["attempt_number"],
        "duration_ms": elapsed_ms,
        "request_event_id": result.request_event_id,
        "result_event_id": result.result_event_id,
        "tool_id": result.tool_id,
        "contract_version": result.contract_version,
        "outcome": result.outcome,
        "output": None if result.output is None else _thaw(result.output),
        "error": error,
    }
    if result.approval_id is not None:
        turn["approval_id"] = result.approval_id
    trajectory.append(turn)
    pending = cast(list[dict[str, object]], updated["pending_tool_calls"])
    approval_wait = result.error is not None and result.error.code in {
        "approval_required",
        "approval_pending",
    }
    if approval_wait:
        pending[0]["approval_id"] = result.approval_id
        pending[0]["attempt_number"] = cast(int, pending[0]["attempt_number"]) + 1
        updated["status"] = "waiting_for_approval"
    else:
        pending.pop(0)
        updated["status"] = "active"
    updated["tool_attempts"] = cast(int, updated["tool_attempts"]) + 1
    updated["active_wall_time_ms"] = _checked_counter_add(
        cast(int, updated["active_wall_time_ms"]), elapsed_ms, "active wall time"
    )
    return updated


def _logical_call_ordinal(checkpoint: dict[str, object], logical_call_id: str) -> int:
    """Return the stable one-based ordinal of a logical call across physical attempts."""
    seen: list[str] = []
    for item in cast(list[object], checkpoint["trajectory"]):
        turn = cast(dict[str, object], item)
        if turn.get("kind") != "tool":
            continue
        existing = cast(str, turn["logical_call_id"])
        if existing not in seen:
            seen.append(existing)
    if logical_call_id in seen:
        return seen.index(logical_call_id) + 1
    return len(seen) + 1


def _terminate(
    engine: Engine,
    lease: LeaseIdentity,
    prior_run: RunRecord,
    target: Literal["failed", "timed_out", "infra_error"],
    code: str,
    producer_component: str,
    *,
    failed_adapter: AgentAdapter | None = None,
    failed_context: AgentContext | None = None,
) -> ExecutionResult:
    try:
        with Session(engine) as session, session.begin():
            repository = PersistenceRepository(session)
            run = repository.lock_current_lease(lease)
            if run.status != "running":
                raise LifecycleConflictError("Run is no longer executing")
            failed_step_event_id: str | None = None
            if failed_adapter is not None:
                checkpoint = repository.get_execution_checkpoint(run.run_id)
                step_number = (
                    1 if checkpoint is None else cast(int, checkpoint.document["next_step_number"])
                )
                failed_step = _append_failed_agent_step(
                    repository,
                    run,
                    failed_adapter,
                    step_number,
                    code,
                    producer_component,
                    failed_context,
                )
                failed_step_event_id = cast(str, failed_step.event.to_dict()["event_id"])
            observed = repository.database_time()
            timestamp = (
                observed.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            )
            payload = {
                "classification": (
                    "infrastructure_error" if target == "infra_error" else "agent_error"
                ),
                "error_code": code,
            }

            def factory(sequence: int) -> RunEvent:
                document = {
                    "schema_version": "chaosagent.run-event/v0",
                    "event_id": _identity(
                        "event-error", run.run_id, str(run.lifecycle_version), code
                    ),
                    "run_id": run.run_id,
                    "sequence": sequence,
                    "occurred_at": timestamp,
                    "recorded_at": timestamp,
                    "event_type": "run.error",
                    "producer": {
                        "component": producer_component,
                        "instance_id": lease.worker_id,
                    },
                    "correlation_id": run.run_id,
                    **(
                        {"causation_event_id": failed_step_event_id}
                        if failed_step_event_id is not None
                        else {}
                    ),
                    "payload": payload,
                    "payload_digest": digest_payload_v0(payload),
                }
                return loads_run_event(json.dumps(document))

            error_event = repository.append_event_allocated(run.run_id, factory)
            repository.transition_owned_run(
                lease,
                target,
                expected_version=run.lifecycle_version,
                evidence=LifecycleEvidence(
                    _identity("event-terminal", run.run_id, str(run.lifecycle_version), code),
                    producer_component,
                    lease.worker_id,
                    causation_event_id=cast(str, error_event.event.to_dict()["event_id"]),
                    reason_code=code,
                ),
            )
            checkpoint = repository.get_execution_checkpoint(run.run_id)
            version = None if checkpoint is None else checkpoint.checkpoint_version
        status: RuntimeStatus = target
        return ExecutionResult(status, lease.run_id, version, error_code=code)
    except (StaleLeaseError, LeaseExpiredError, LifecycleConflictError):
        checkpoint_version = None
        return ExecutionResult(
            "stale_lease", prior_run.run_id, checkpoint_version, error_code="stale_lease"
        )
    except _INFRASTRUCTURE_EXCEPTIONS:
        return ExecutionResult("run_not_ready", prior_run.run_id, None, error_code="internal_error")


def _append_failed_agent_step(
    repository: PersistenceRepository,
    run: RunRecord,
    adapter: AgentAdapter,
    step_number: int,
    error_code: str,
    producer_component: str,
    context: AgentContext | None,
) -> RunEventRecord:
    observed = repository.database_time()
    timestamp = observed.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    step_id = _identity("step", run.run_id, str(step_number))
    payload: dict[str, object] = {
        "step_id": step_id,
        "step_number": step_number,
        "phase": "failed",
        "model_call_id": _identity("model-call", run.run_id, str(step_number)),
        "model": _provider_model(adapter, None),
        "input_digest": digest_payload_v0(
            _context_payload(context)
            if context is not None
            else {"run_id": run.run_id, "step_number": step_number, "context_unavailable": True}
        ),
    }

    def factory(sequence: int) -> RunEvent:
        document = {
            "schema_version": "chaosagent.run-event/v0",
            "event_id": _identity("event-agent", run.run_id, str(step_number)),
            "run_id": run.run_id,
            "sequence": sequence,
            "occurred_at": timestamp,
            "recorded_at": timestamp,
            "event_type": "agent.step",
            "producer": {"component": producer_component, "instance_id": lease_owner(run)},
            "correlation_id": step_id,
            "payload": payload,
            "payload_digest": digest_payload_v0(payload),
        }
        return loads_run_event(json.dumps(document))

    return repository.append_event_allocated(run.run_id, factory)


def lease_owner(run: RunRecord) -> str:
    if run.lease_owner is None:
        raise PersistenceIntegrityError("active Run has no lease owner")
    return run.lease_owner


def _validate_provider_metadata(metadata: object, adapter: AgentAdapter) -> None:
    if metadata is None:
        return
    if type(metadata) is not AgentProviderMetadata:
        raise AgentOutputValidationError("provider metadata must be AgentProviderMetadata")
    assert isinstance(metadata, AgentProviderMetadata)
    _require_name(metadata.provider, "provider")
    for value, field, maximum in (
        (metadata.requested_model, "requested_model", 256),
        (metadata.resolved_model, "resolved_model", 256),
        (metadata.provider_request_id, "provider_request_id", 128),
    ):
        if value is None:
            continue
        if type(value) is not str or not value or len(value) > maximum:
            raise AgentOutputValidationError(f"{field} is invalid")
        if field == "provider_request_id":
            _require_id(value, field)
    expected_provider = getattr(adapter, "provider_name", adapter.adapter_id)
    expected_model = getattr(adapter, "requested_model", adapter.adapter_version)
    if metadata.provider != expected_provider or metadata.requested_model != expected_model:
        raise AgentOutputValidationError("provider metadata does not match the active adapter")


def _provider_model(
    adapter: AgentAdapter, metadata: AgentProviderMetadata | None
) -> dict[str, object]:
    if metadata is None:
        return {
            "provider": getattr(adapter, "provider_name", adapter.adapter_id),
            "requested_model": getattr(adapter, "requested_model", adapter.adapter_version),
        }
    model: dict[str, object] = {
        "provider": metadata.provider,
        "requested_model": metadata.requested_model,
    }
    if metadata.resolved_model is not None:
        model["resolved_model"] = metadata.resolved_model
    if metadata.provider_request_id is not None:
        model["provider_request_id"] = metadata.provider_request_id
    return model


def _model_matches_adapter(value: object, adapter: AgentAdapter) -> bool:
    if not isinstance(value, dict):
        return False
    expected_provider = getattr(adapter, "provider_name", adapter.adapter_id)
    expected_model = getattr(adapter, "requested_model", adapter.adapter_version)
    return (
        value.get("provider") == expected_provider
        and value.get("requested_model") == expected_model
    )


def _pre_step_budget_error(
    budgets: dict[str, object],
    next_step: int,
    tool_attempts: int,
    wall_ms: int,
    known_cost: int,
    cost_complete: bool,
) -> str | None:
    if next_step > cast(int, budgets["max_steps"]):
        return "max_steps_exceeded"
    if tool_attempts > cast(int, budgets["max_tool_calls"]):
        return "max_tool_calls_exceeded"
    if wall_ms >= cast(int, budgets["max_wall_time_ms"]):
        return "max_wall_time_exceeded"
    if not cost_complete:
        return "cost_unavailable"
    if known_cost > cast(int, budgets["max_cost_microusd"]):
        return "max_cost_exceeded"
    return None


def _post_step_budget_error(
    budgets: dict[str, object], checkpoint: dict[str, object]
) -> str | None:
    if not cast(bool, checkpoint["cost_complete"]):
        return "cost_unavailable"
    if cast(int, checkpoint["known_cost_microusd"]) > cast(int, budgets["max_cost_microusd"]):
        return "max_cost_exceeded"
    if cast(int, checkpoint["active_wall_time_ms"]) >= cast(int, budgets["max_wall_time_ms"]):
        return "max_wall_time_exceeded"
    return None


def _output_payload(output: AgentOutput, duration_ms: int) -> dict[str, object]:
    return {
        "text": output.text,
        "final": output.final,
        "tool_calls": [
            {
                "call_id": call.call_id,
                "tool_id": call.tool_id,
                "contract_version": call.contract_version,
                "arguments": _thaw(call.arguments),
            }
            for call in output.tool_calls
        ],
        "usage": {
            "input_tokens": output.usage.input_tokens,
            "output_tokens": output.usage.output_tokens,
            "cost_microusd": output.usage.cost_microusd,
        },
        "duration_ms": duration_ms,
    }


def _checkpoint_version(checkpoint: dict[str, object] | None) -> int | None:
    return None if checkpoint is None else cast(int, checkpoint["checkpoint_version"])


def _identity(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode()).hexdigest()[:32]
    return f"{prefix}-{digest}"


def _require_id(value: object, field: str) -> None:
    if not isinstance(value, str) or len(value) > 128 or _ID_RE.fullmatch(value) is None:
        raise AgentOutputValidationError(f"{field} is not a valid identifier")


def _require_name(value: object, field: str) -> None:
    if not isinstance(value, str) or len(value) > 128 or _NAME_RE.fullmatch(value) is None:
        raise AgentOutputValidationError(f"{field} is not a valid component name")


def _elapsed_ms(started: int) -> int:
    elapsed = monotonic_ns() - started
    return max(0, min(_MAX_MEASURED_DURATION_MS, (elapsed + 999_999) // 1_000_000))


def _checked_counter_add(current: int, increment: int, label: str) -> int:
    value = current + increment
    if value > _SAFE_INTEGER:
        raise PersistenceIntegrityError(f"checkpoint {label} exceeds the safe-integer range")
    return value


def _freeze(value: object) -> object:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _thaw(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_thaw(item) for item in value]
    return value
