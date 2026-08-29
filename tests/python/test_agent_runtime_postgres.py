from __future__ import annotations

import json
import os
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Event, get_ident
from typing import cast
from uuid import uuid4

import chaosagent_agent_runtime.runtime as runtime_module
import pytest
from alembic import command
from alembic.config import Config
from chaosagent_agent_configurations import AgentConfiguration, loads_agent_configuration
from chaosagent_agent_runtime import (
    AgentContext,
    AgentOutput,
    AgentProviderError,
    AgentProviderMetadata,
    AgentProviderTimeout,
    AgentToolCall,
    AgentUsage,
    ScriptedAgentAdapter,
    execute_run,
)
from chaosagent_evidence import EvidenceValidationError, digest_payload_v0
from chaosagent_faults import FaultEngine, compile_fault_plan_v0
from chaosagent_fixtures import load_fixture
from chaosagent_persistence import (
    CheckpointConflictError,
    ClaimedRun,
    ExecutionCheckpointRecord,
    LeaseIdentity,
    LifecycleEvidence,
    PersistenceIntegrityError,
    PersistenceRepository,
    RevisionReference,
    StaleLeaseError,
    create_postgres_engine,
)
from chaosagent_policies import load_policy
from chaosagent_provider_openai import OpenAIResponsesAdapter
from chaosagent_scenarios import loads_scenario
from chaosagent_tool_gateway import (
    ORDERS_GET_V0,
    PAYMENTS_REFUND_V0,
    SHIPPING_GET_STATUS_V0,
    SUPPORT_UPDATE_TICKET_V0,
    ToolGateway,
)
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

pytestmark = pytest.mark.postgres

ROOT = Path(__file__).resolve().parents[2]
SCENARIO_PATH = ROOT / "benchmarks/shipment-refund/scenarios/refund-ambiguous-timeout.v0.json"
FIXTURE_PATH = ROOT / "benchmarks/shipment-refund/fixtures/failed-shipment.v0.json"
POLICY_PATH = ROOT / "benchmarks/shipment-refund/policies/refund-policy.v0.json"
ALEMBIC_INI = ROOT / "packages/persistence/alembic.ini"
AGENT = RevisionReference("scripted-agent", "1", "sha256:" + "d" * 64)


@pytest.fixture(scope="session")
def runtime_engine() -> Iterator[Engine]:
    database_url = os.environ.get("CHAOSAGENT_TEST_DATABASE_URL")
    if database_url is None:
        pytest.skip("CHAOSAGENT_TEST_DATABASE_URL is not configured")
    if os.environ.get("CHAOSAGENT_ALLOW_DESTRUCTIVE_DATABASE_TESTS") != "1":
        raise RuntimeError("destructive PostgreSQL tests require explicit opt-in")
    if not (make_url(database_url).database or "").endswith("_test"):
        raise RuntimeError("PostgreSQL integration database name must end with '_test'")
    os.environ["CHAOSAGENT_DATABASE_URL"] = database_url
    configuration = Config(str(ALEMBIC_INI))
    command.downgrade(configuration, "base")
    command.upgrade(configuration, "head")
    engine = create_postgres_engine(database_url)
    yield engine
    engine.dispose()
    command.downgrade(configuration, "base")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex}"


def _plain(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_plain(item) for item in value]
    return value


def _evidence(label: str) -> LifecycleEvidence:
    return LifecycleEvidence(_unique(f"event-{label}"), "runtime-test", "worker-runtime")


def _create_run(
    engine: Engine,
    *,
    budgets: dict[str, int] | None = None,
    worker: str = "worker-runtime",
    agent_configuration: AgentConfiguration | None = None,
    faults: list[dict[str, object]] | None = None,
) -> ClaimedRun:
    run_id = _unique("run")
    scenario_document = cast(
        dict[str, object], json.loads(SCENARIO_PATH.read_text(encoding="utf-8"))
    )
    scenario_document["scenario_id"] = _unique("scenario")
    scenario_document["revision"] = "1"
    scenario_document["faults"] = [] if faults is None else faults
    if budgets is not None:
        scenario_document["budgets"] = budgets
    scenario = loads_scenario(json.dumps(scenario_document))
    with Session(engine) as session, session.begin():
        repository = PersistenceRepository(session)
        repository.insert_fixture_revision(load_fixture(FIXTURE_PATH), created_by="runtime-test")
        repository.insert_policy_revision(load_policy(POLICY_PATH), created_by="runtime-test")
        repository.insert_scenario_revision(scenario, created_by="runtime-test")
        if agent_configuration is None:
            agent_reference = AGENT
            repository.insert_agent_configuration_reference(AGENT, created_by="runtime-test")
        else:
            record = repository.insert_agent_configuration(
                agent_configuration, created_by="runtime-test"
            )
            agent_reference = record.reference
        repository.create_run(
            run_id,
            scenario_id=cast(str, scenario_document["scenario_id"]),
            scenario_revision="1",
            agent_configuration_id=agent_reference.id,
            agent_configuration_revision=agent_reference.revision,
            created_by="runtime-test",
        )
        repository.initialize_run_company_state(run_id)
        claimed = repository.claim_next_run(
            worker, lease_duration_seconds=600, evidence=_evidence("claim"), run_id=run_id
        )
        assert claimed is not None
        return claimed


def _adapter(*outputs: object) -> ScriptedAgentAdapter:
    return ScriptedAgentAdapter(AGENT.id, AGENT.revision, outputs)


def _rewrite_checkpoint(
    engine: Engine, run_id: str, mutate: Callable[[dict[str, object]], None]
) -> None:
    with Session(engine) as session:
        checkpoint = PersistenceRepository(session).get_execution_checkpoint(run_id)
        assert checkpoint is not None
        document = cast(dict[str, object], _plain(checkpoint.document))
    mutate(document)
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE public.execution_checkpoints "
                "SET document = CAST(:document AS jsonb), document_digest = :digest "
                "WHERE run_id = :run_id"
            ),
            {
                "document": json.dumps(document),
                "digest": digest_payload_v0(document),
                "run_id": run_id,
            },
        )


def _checkpoint_after_one_tool(engine: Engine, *, cost: int = 0) -> ClaimedRun:
    claimed = _create_run(engine)

    class Stop(BaseException):
        pass

    class StopAtSecondStep(ScriptedAgentAdapter):
        def invoke(self, context):  # type: ignore[no-untyped-def]
            if context.step_number == 2:
                raise Stop
            return super().invoke(context)

    output = AgentOutput(
        "Read order",
        (AgentToolCall("order", "orders.get", ORDERS_GET_V0, {"order_id": "ORD-1007"}),),
        usage=_usage(cost),
    )
    with pytest.raises(Stop):
        execute_run(
            engine,
            claimed.lease,
            StopAtSecondStep(AGENT.id, AGENT.revision, (output,)),
        )
    return claimed


def _assert_corruption_stops_before_progress(engine: Engine, claimed: ClaimedRun) -> None:
    with Session(engine) as session:
        repository = PersistenceRepository(session)
        before_events = len(repository.fetch_events(claimed.run.run_id))
        before_state = repository.get_run_company_state(claimed.run.run_id)
        assert before_state is not None
        before_refunds = len(before_state.refunds)

    class CountingAdapter(ScriptedAgentAdapter):
        calls = 0

        def invoke(self, context):  # type: ignore[no-untyped-def]
            self.calls += 1
            return super().invoke(context)

    adapter = CountingAdapter(
        AGENT.id, AGENT.revision, (AgentOutput("unused", final=True, usage=_usage()),)
    )
    result = execute_run(engine, claimed.lease, adapter)
    assert result.status == "run_not_ready" and adapter.calls == 0
    with Session(engine) as session:
        repository = PersistenceRepository(session)
        assert len(repository.fetch_events(claimed.run.run_id)) == before_events
        state = repository.get_run_company_state(claimed.run.run_id)
        assert state is not None and len(state.refunds) == before_refunds


def _usage(cost: int | None = 0) -> AgentUsage:
    return AgentUsage(input_tokens=10, output_tokens=5, cost_microusd=cost)


def _hosted_configuration(prefix: str) -> AgentConfiguration:
    return loads_agent_configuration(
        json.dumps(
            {
                "schema_version": "chaosagent.agent-configuration/v0",
                "agent_configuration_id": _unique(prefix),
                "revision": "r1",
                "provider": "openai",
                "adapter": {"id": "openai-responses", "version": "v0"},
                "model": "gpt-4.1-2025-04-14",
                "compatibility_profile": "openai-responses-stateless-non-reasoning/v0",
                "token_accounting": {
                    "schema_version": "chaosagent.token-accounting/v0",
                    "schedule_id": "test-openai-rates",
                    "revision": "2026-08-28",
                    "model": "gpt-4.1-2025-04-14",
                    "unit": "microusd",
                    "tokens_per_rate_unit": 1000000,
                    "rounding": "ceiling_per_response",
                    "input_rate_microusd": 1000000,
                    "cached_input_rate_microusd": 500000,
                    "output_rate_microusd": 2000000,
                },
                "timeout_ms": 5000,
                "max_output_tokens": 256,
                "temperature": None,
                "parallel_tool_calls": True,
                "store": False,
                "max_retries": 0,
            }
        )
    )


def _provider_response(identifier: str, *output: object, usage: object = ...) -> dict[str, object]:
    reported_usage = (
        {
            "input_tokens": 10,
            "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
            "output_tokens": 5,
            "output_tokens_details": {"reasoning_tokens": 0},
            "total_tokens": 15,
        }
        if usage is ...
        else usage
    )
    return {
        "id": identifier,
        "model": "gpt-4.1-2025-04-14",
        "status": "completed",
        "error": None,
        "output": list(output),
        "usage": reported_usage,
    }


class _ProviderResponses:
    def __init__(self, outputs: list[object]) -> None:
        self.outputs = outputs
        self.requests: list[dict[str, object]] = []

    def create(self, **kwargs: object) -> object:
        self.requests.append(dict(kwargs))
        return self.outputs.pop(0)


class _ProviderClient:
    def __init__(self, *outputs: object) -> None:
        self.responses = _ProviderResponses(list(outputs))

    def with_options(self, **kwargs: object) -> _ProviderClient:
        return self


def _expire_requeue_and_reclaim(engine: Engine, claimed: ClaimedRun, *, worker: str) -> ClaimedRun:
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE public.runs SET heartbeat_at = "
                "clock_timestamp() - interval '2 seconds', lease_expires_at = "
                "clock_timestamp() - interval '1 second' WHERE run_id = :run_id"
            ),
            {"run_id": claimed.run.run_id},
        )
    with Session(engine) as session, session.begin():
        repository = PersistenceRepository(session)
        current = repository.get_run(claimed.run.run_id)
        assert current is not None
        repository.requeue_expired_run(
            current.run_id,
            expected_version=current.lifecycle_version,
            evidence=_evidence("requeue-for-runtime-recovery"),
        )
        replacement = repository.claim_next_run(
            worker,
            lease_duration_seconds=600,
            evidence=_evidence("reclaim-for-runtime-recovery"),
            run_id=current.run_id,
        )
        assert replacement is not None
        return replacement


def _call(call_id: str, tool: str, version: str, arguments: dict[str, object]) -> AgentOutput:
    return AgentOutput(
        f"Calling {tool}",
        (AgentToolCall(call_id, tool, version, arguments),),
        usage=_usage(),
    )


def _multi_call(*calls: AgentToolCall) -> AgentOutput:
    return AgentOutput("Calling tools in order", calls, usage=_usage())


def _approval_multi_output() -> AgentOutput:
    return _multi_call(
        AgentToolCall("order", "orders.get", ORDERS_GET_V0, {"order_id": "ORD-1007"}),
        AgentToolCall(
            "refund",
            "payments.refund",
            PAYMENTS_REFUND_V0,
            {
                "order_id": "ORD-1007",
                "payment_id": "PAY-1007",
                "amount_minor": 6000,
                "reason": "Approved multi-call refund",
                "idempotency_key": "multi-call-approval",
            },
        ),
        AgentToolCall(
            "shipping",
            "shipping.get_status",
            SHIPPING_GET_STATUS_V0,
            {"order_id": "ORD-1007"},
        ),
    )


def test_final_answer_transitions_only_to_evaluating(runtime_engine: Engine) -> None:
    claimed = _create_run(runtime_engine)
    result = execute_run(
        runtime_engine, claimed.lease, _adapter(AgentOutput("Done", final=True, usage=_usage()))
    )

    assert result.status == "evaluation_ready"
    assert result.final_answer == "Done"
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        run = repository.get_run(claimed.run.run_id)
        assert run is not None and run.status == "evaluating"
        events = repository.fetch_events(run.run_id)
        assert [event.event.to_dict()["event_type"] for event in events] == [
            "run.lifecycle",
            "run.lifecycle",
            "agent.step",
            "run.lifecycle",
        ]
        checkpoint = repository.get_execution_checkpoint(run.run_id)
        assert checkpoint is not None
        assert checkpoint.document["final_answer"] == "Done"
        assert "reasoning" not in cast(dict[str, object], checkpoint.document)


def test_agent_step_digests_cover_exact_context_and_output(runtime_engine: Engine) -> None:
    claimed = _create_run(runtime_engine)
    output = AgentOutput("Digest-bound final", final=True, usage=_usage(7))

    class RecordingAdapter(ScriptedAgentAdapter):
        context: AgentContext | None = None

        def invoke(self, context):  # type: ignore[no-untyped-def]
            self.context = context
            return super().invoke(context)

    adapter = RecordingAdapter(AGENT.id, AGENT.revision, (output,))
    assert execute_run(runtime_engine, claimed.lease, adapter).status == "evaluation_ready"
    assert adapter.context is not None
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        event = next(
            row.event.to_dict()
            for row in repository.fetch_events(claimed.run.run_id)
            if row.event.to_dict()["event_type"] == "agent.step"
        )
        checkpoint = repository.get_execution_checkpoint(claimed.run.run_id)
        assert checkpoint is not None
        assistant = cast(list[dict[str, object]], _plain(checkpoint.document["trajectory"]))[0]
    payload = cast(dict[str, object], event["payload"])
    assert payload["input_digest"] == digest_payload_v0(
        runtime_module._context_payload(adapter.context)
    )
    assert payload["output_digest"] == digest_payload_v0(
        runtime_module._output_payload(output, cast(int, assistant["duration_ms"]))
    )


def test_failed_provider_step_uses_the_exact_supplied_context_digest(
    runtime_engine: Engine,
) -> None:
    claimed = _create_run(runtime_engine)

    class FailingAdapter(ScriptedAgentAdapter):
        context: AgentContext | None = None

        def invoke(self, context):  # type: ignore[no-untyped-def]
            self.context = context
            raise AgentProviderError("private provider detail")

    adapter = FailingAdapter(AGENT.id, AGENT.revision, ())
    result = execute_run(runtime_engine, claimed.lease, adapter)
    assert result.status == "infra_error" and adapter.context is not None
    with Session(runtime_engine) as session:
        event = next(
            row.event.to_dict()
            for row in PersistenceRepository(session).fetch_events(claimed.run.run_id)
            if row.event.to_dict()["event_type"] == "agent.step"
        )
    assert cast(dict[str, object], event["payload"])["input_digest"] == digest_payload_v0(
        runtime_module._context_payload(adapter.context)
    )


def test_deterministic_full_fixture_tool_trajectory(runtime_engine: Engine) -> None:
    claimed = _create_run(runtime_engine)
    adapter = _adapter(
        _call("order", "orders.get", ORDERS_GET_V0, {"order_id": "ORD-1007"}),
        _call("ship", "shipping.get_status", SHIPPING_GET_STATUS_V0, {"order_id": "ORD-1007"}),
        _call(
            "refund",
            "payments.refund",
            PAYMENTS_REFUND_V0,
            {
                "order_id": "ORD-1007",
                "payment_id": "PAY-1007",
                "amount_minor": 5000,
                "reason": "Failed shipment",
                "idempotency_key": "runtime-refund-1007",
            },
        ),
        _call(
            "ticket",
            "support.update_ticket",
            SUPPORT_UPDATE_TICKET_V0,
            {
                "ticket_id": "TKT-204",
                "status": "closed",
                "note": "Refunded failed shipment.",
                "idempotency_key": "runtime-ticket-204",
            },
        ),
        AgentOutput("Refunded once and closed the ticket.", final=True, usage=_usage()),
    )
    result = execute_run(runtime_engine, claimed.lease, adapter)

    assert result.status == "evaluation_ready"
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        state = repository.get_run_company_state(claimed.run.run_id)
        assert state is not None
        assert len(state.refunds) == 1
        assert (
            next(ticket for ticket in state.support_tickets if ticket.ticket_id == "TKT-204").status
            == "closed"
        )
        event_types = [
            row.event.to_dict()["event_type"] for row in repository.fetch_events(claimed.run.run_id)
        ]
        assert event_types.count("agent.step") == 5
        assert event_types.count("tool.requested") == 4


def test_scripted_runtime_receives_faulted_gateway_observation(runtime_engine: Engine) -> None:
    claimed = _create_run(
        runtime_engine,
        faults=[
            {
                "id": "shipping-runtime-503",
                "kind": "http_error",
                "match": {
                    "tool_id": "shipping.get_status",
                    "phase": "before_tool",
                    "call_ordinal": 1,
                },
                "activation": {"probability_ppm": 1_000_000, "max_occurrences": 1},
                "parameters": {"status": 503},
            }
        ],
    )
    with Session(runtime_engine) as session:
        scenario_record = PersistenceRepository(session).get_scenario_revision(
            claimed.run.scenario.id, claimed.run.scenario.revision
        )
        assert scenario_record is not None
        fault_engine = FaultEngine(compile_fault_plan_v0(scenario_record.scenario), run_seed=2026)

    class RecordingAdapter(ScriptedAgentAdapter):
        contexts: list[AgentContext]

        def __init__(self) -> None:
            super().__init__(
                AGENT.id,
                AGENT.revision,
                (
                    _call(
                        "ship",
                        "shipping.get_status",
                        SHIPPING_GET_STATUS_V0,
                        {"order_id": "ORD-1007"},
                    ),
                    AgentOutput(
                        "Observed a transient shipping failure.", final=True, usage=_usage()
                    ),
                ),
            )
            self.contexts = []

        def invoke(self, context: AgentContext) -> AgentOutput:
            self.contexts.append(context)
            return super().invoke(context)

    adapter = RecordingAdapter()
    result = execute_run(runtime_engine, claimed.lease, adapter, fault_engine=fault_engine)
    assert result.status == "evaluation_ready"
    assert len(adapter.contexts) == 2
    tool_turn = cast(dict[str, object], dict(adapter.contexts[1].trajectory[-1]))
    assert tool_turn["outcome"] == "failed"
    assert cast(dict[str, object], tool_turn["error"])["code"] == "fault_http_503"
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        event_types = [
            item.event.to_dict()["event_type"]
            for item in repository.fetch_events(claimed.run.run_id)
        ]
        assert event_types.count("fault.matched") == 1
        assert event_types.count("fault.applied") == 1
        assert event_types.count("fault.observed") == 1
        checkpoint = repository.get_execution_checkpoint(claimed.run.run_id)
        assert checkpoint is not None


def test_scripted_runtime_receives_after_tool_faulted_response(runtime_engine: Engine) -> None:
    claimed = _create_run(
        runtime_engine,
        faults=[
            {
                "id": "shipping-runtime-stale",
                "kind": "stale_field",
                "match": {
                    "tool_id": "shipping.get_status",
                    "phase": "after_tool",
                    "call_ordinal": 1,
                },
                "activation": {"probability_ppm": 1_000_000, "max_occurrences": 1},
                "parameters": {"json_pointer": "/status", "value": "in_transit"},
            }
        ],
    )
    with Session(runtime_engine) as session:
        scenario_record = PersistenceRepository(session).get_scenario_revision(
            claimed.run.scenario.id, claimed.run.scenario.revision
        )
        assert scenario_record is not None
        fault_engine = FaultEngine(compile_fault_plan_v0(scenario_record.scenario), run_seed=2027)

    class RecordingAdapter(ScriptedAgentAdapter):
        contexts: list[AgentContext]

        def __init__(self) -> None:
            super().__init__(
                AGENT.id,
                AGENT.revision,
                (
                    _call(
                        "ship",
                        "shipping.get_status",
                        SHIPPING_GET_STATUS_V0,
                        {"order_id": "ORD-1007"},
                    ),
                    AgentOutput("Observed a stale shipping status.", final=True, usage=_usage()),
                ),
            )
            self.contexts = []

        def invoke(self, context: AgentContext) -> AgentOutput:
            self.contexts.append(context)
            return super().invoke(context)

    adapter = RecordingAdapter()
    result = execute_run(runtime_engine, claimed.lease, adapter, fault_engine=fault_engine)
    assert result.status == "evaluation_ready"
    tool_turn = cast(dict[str, object], dict(adapter.contexts[1].trajectory[-1]))
    output = cast(dict[str, object], tool_turn["output"])
    assert output["status"] == "in_transit"
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        shipment = repository.get_company_shipment_for_order(claimed.run.run_id, "ORD-1007")
        assert shipment is not None and shipment.status == "failed"


def test_malformed_response_checkpoint_resumes_without_replaying_fault(
    runtime_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    claimed = _create_run(
        runtime_engine,
        faults=[
            {
                "id": "shipping-runtime-malformed",
                "kind": "malformed_response",
                "match": {
                    "tool_id": "shipping.get_status",
                    "phase": "after_tool",
                    "call_ordinal": 1,
                },
                "activation": {"probability_ppm": 1_000_000, "max_occurrences": 1},
                "parameters": {"mode": "invalid_json"},
            }
        ],
    )
    with Session(runtime_engine) as session:
        scenario_record = PersistenceRepository(session).get_scenario_revision(
            claimed.run.scenario.id, claimed.run.scenario.revision
        )
        assert scenario_record is not None
        fault_engine = FaultEngine(compile_fault_plan_v0(scenario_record.scenario), run_seed=2028)

    output = _multi_call(
        AgentToolCall(
            "shipping-malformed",
            "shipping.get_status",
            SHIPPING_GET_STATUS_V0,
            {"order_id": "ORD-1007"},
        ),
        AgentToolCall("order", "orders.get", ORDERS_GET_V0, {"order_id": "ORD-1007"}),
    )
    adapter = _adapter(
        output, AgentOutput("Recovered malformed observation.", final=True, usage=_usage())
    )
    original = ToolGateway.execute
    calls = 0

    class Crash(BaseException):
        pass

    def crash_second(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        if calls == 2:
            raise Crash
        return original(self, *args, **kwargs)

    monkeypatch.setattr(ToolGateway, "execute", crash_second)
    with pytest.raises(Crash):
        execute_run(runtime_engine, claimed.lease, adapter, fault_engine=fault_engine)

    with Session(runtime_engine) as session:
        checkpoint = PersistenceRepository(session).get_execution_checkpoint(claimed.run.run_id)
        assert checkpoint is not None
        trajectory = cast(list[dict[str, object]], _plain(checkpoint.document["trajectory"]))
        tool_turn = next(turn for turn in trajectory if turn["kind"] == "tool")
        assert tool_turn["outcome"] == "failed"
        assert cast(dict[str, object], tool_turn["error"])["code"] == ("fault_malformed_response")

    resumed = execute_run(runtime_engine, claimed.lease, adapter, fault_engine=fault_engine)
    assert resumed.status == "evaluation_ready"
    with Session(runtime_engine) as session:
        events = [
            item.event.to_dict()
            for item in PersistenceRepository(session).fetch_events(claimed.run.run_id)
        ]
        assert sum(item["event_type"] == "fault.applied" for item in events) == 1
        assert sum(item["event_type"] == "tool.requested" for item in events) == 2


def test_two_read_calls_from_one_step_are_ordered_and_checkpointed(
    runtime_engine: Engine,
) -> None:
    claimed = _create_run(runtime_engine)
    output = _multi_call(
        AgentToolCall("order", "orders.get", ORDERS_GET_V0, {"order_id": "ORD-1007"}),
        AgentToolCall(
            "shipping",
            "shipping.get_status",
            SHIPPING_GET_STATUS_V0,
            {"order_id": "ORD-1007"},
        ),
    )
    result = execute_run(
        runtime_engine,
        claimed.lease,
        _adapter(output, AgentOutput("Done", final=True, usage=_usage())),
    )
    assert result.status == "evaluation_ready"
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        checkpoint = repository.get_execution_checkpoint(claimed.run.run_id)
        assert checkpoint is not None
        trajectory = cast(list[dict[str, object]], _plain(checkpoint.document["trajectory"]))
        assert [turn["kind"] for turn in trajectory] == ["assistant", "tool", "tool", "assistant"]
        assert [turn.get("call_index") for turn in trajectory[1:3]] == [1, 2]
        requests = [
            event.event.to_dict()
            for event in repository.fetch_events(claimed.run.run_id)
            if event.event.to_dict()["event_type"] == "tool.requested"
        ]
        assert [cast(dict[str, object], event["payload"])["tool_id"] for event in requests] == [
            "orders.get",
            "shipping.get_status",
        ]
        assert [event["sequence"] for event in requests] == sorted(
            cast(list[int], [event["sequence"] for event in requests])
        )


def test_crash_between_multiple_calls_resumes_without_replaying_completed_prefix(
    runtime_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    claimed = _create_run(runtime_engine)
    output = _multi_call(
        AgentToolCall("order", "orders.get", ORDERS_GET_V0, {"order_id": "ORD-1007"}),
        AgentToolCall(
            "shipping",
            "shipping.get_status",
            SHIPPING_GET_STATUS_V0,
            {"order_id": "ORD-1007"},
        ),
    )
    original = ToolGateway.execute
    calls = 0

    class Crash(BaseException):
        pass

    def crash_second(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        if calls == 2:
            raise Crash
        return original(self, *args, **kwargs)

    monkeypatch.setattr(ToolGateway, "execute", crash_second)
    adapter = _adapter(output, AgentOutput("Done", final=True, usage=_usage()))
    with pytest.raises(Crash):
        execute_run(runtime_engine, claimed.lease, adapter)
    assert execute_run(runtime_engine, claimed.lease, adapter).status == "evaluation_ready"
    with Session(runtime_engine) as session:
        events = [
            row.event.to_dict()
            for row in PersistenceRepository(session).fetch_events(claimed.run.run_id)
        ]
    requested_tools = [
        cast(dict[str, object], event["payload"])["tool_id"]
        for event in events
        if event["event_type"] == "tool.requested"
    ]
    assert requested_tools == ["orders.get", "shipping.get_status"]


def test_mutation_prefix_crash_reclaim_resumes_without_replaying_effect(
    runtime_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    claimed = _create_run(runtime_engine)
    output = _multi_call(
        AgentToolCall(
            "ticket",
            "support.update_ticket",
            SUPPORT_UPDATE_TICKET_V0,
            {
                "ticket_id": "TKT-204",
                "status": "closed",
                "note": "Mutation prefix committed before recovery.",
                "idempotency_key": "runtime-mutation-prefix-recovery",
            },
        ),
        AgentToolCall(
            "shipping",
            "shipping.get_status",
            SHIPPING_GET_STATUS_V0,
            {"order_id": "ORD-1007"},
        ),
    )
    original = ToolGateway.execute
    calls = 0

    class Crash(BaseException):
        pass

    def crash_second(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        if calls == 2:
            raise Crash
        return original(self, *args, **kwargs)

    monkeypatch.setattr(ToolGateway, "execute", crash_second)
    adapter = _adapter(output, AgentOutput("Recovered", final=True, usage=_usage()))
    with pytest.raises(Crash):
        execute_run(runtime_engine, claimed.lease, adapter)

    replacement = _expire_requeue_and_reclaim(
        runtime_engine, claimed, worker="worker-mutation-prefix-replacement"
    )
    assert execute_run(runtime_engine, claimed.lease, adapter).status == "stale_lease"
    assert execute_run(runtime_engine, replacement.lease, adapter).status == "evaluation_ready"

    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        state = repository.get_run_company_state(claimed.run.run_id)
        events = [row.event.to_dict() for row in repository.fetch_events(claimed.run.run_id)]
        assert state is not None
        ticket = next(item for item in state.support_tickets if item.ticket_id == "TKT-204")
        assert ticket.status == "closed"
        requests = [
            cast(dict[str, object], event["payload"])
            for event in events
            if event["event_type"] == "tool.requested"
        ]
        assert [payload["tool_id"] for payload in requests] == [
            "support.update_ticket",
            "shipping.get_status",
        ]
        assert [payload["attempt_number"] for payload in requests] == [1, 1]
        assert len({cast(str, payload["logical_call_id"]) for payload in requests}) == 2
        assert sum(event["event_type"] == "state.evidence_recorded" for event in events) == 1


def test_approval_on_second_call_preserves_completed_prefix_and_pending_suffix(
    runtime_engine: Engine,
) -> None:
    claimed = _create_run(runtime_engine)
    output = _multi_call(
        AgentToolCall("order", "orders.get", ORDERS_GET_V0, {"order_id": "ORD-1007"}),
        AgentToolCall(
            "refund",
            "payments.refund",
            PAYMENTS_REFUND_V0,
            {
                "order_id": "ORD-1007",
                "payment_id": "PAY-1007",
                "amount_minor": 6000,
                "reason": "Approved multi-call refund",
                "idempotency_key": "multi-call-approval",
            },
        ),
        AgentToolCall(
            "shipping",
            "shipping.get_status",
            SHIPPING_GET_STATUS_V0,
            {"order_id": "ORD-1007"},
        ),
    )
    adapter = _adapter(output, AgentOutput("Done", final=True, usage=_usage()))
    paused = execute_run(runtime_engine, claimed.lease, adapter)
    assert paused.status == "waiting_for_approval" and paused.approval_id is not None
    with Session(runtime_engine) as session:
        checkpoint = PersistenceRepository(session).get_execution_checkpoint(claimed.run.run_id)
        assert checkpoint is not None
        pending = cast(list[dict[str, object]], _plain(checkpoint.document["pending_tool_calls"]))
        assert [(call["call_index"], call["attempt_number"]) for call in pending] == [
            (2, 2),
            (3, 1),
        ]
        assert "approval_id" in pending[0] and "approval_id" not in pending[1]
    with Session(runtime_engine) as session, session.begin():
        PersistenceRepository(session).resolve_approval_request(
            paused.approval_id,
            result="approved",
            actor_id="reviewer",
            resolution_event_id=_unique("event-resolution"),
        )
    assert execute_run(runtime_engine, claimed.lease, adapter).status == "evaluation_ready"
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        state = repository.get_run_company_state(claimed.run.run_id)
        assert state is not None and len(state.refunds) == 1
        events = [row.event.to_dict() for row in repository.fetch_events(claimed.run.run_id)]
    tools = [
        cast(dict[str, object], event["payload"])["tool_id"]
        for event in events
        if event["event_type"] == "tool.requested"
    ]
    assert tools == ["orders.get", "payments.refund", "payments.refund", "shipping.get_status"]


def test_mutation_then_business_failure_in_one_step_preserves_first_effect(
    runtime_engine: Engine,
) -> None:
    claimed = _create_run(runtime_engine)
    first = {
        "order_id": "ORD-1007",
        "payment_id": "PAY-1007",
        "amount_minor": 5000,
        "reason": "First refund",
        "idempotency_key": "multi-first-refund",
    }
    output = _multi_call(
        AgentToolCall("first", "payments.refund", PAYMENTS_REFUND_V0, first),
        AgentToolCall(
            "second",
            "support.update_ticket",
            SUPPORT_UPDATE_TICKET_V0,
            {
                "ticket_id": "TKT-NOT-FOUND",
                "status": "closed",
                "note": "This mutation must fail without rolling back call one.",
                "idempotency_key": "multi-missing-ticket",
            },
        ),
    )
    result = execute_run(
        runtime_engine,
        claimed.lease,
        _adapter(output, AgentOutput("Observed second failure", final=True, usage=_usage())),
    )
    assert result.status == "evaluation_ready"
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        state = repository.get_run_company_state(claimed.run.run_id)
        checkpoint = repository.get_execution_checkpoint(claimed.run.run_id)
        assert state is not None and len(state.refunds) == 1
        assert checkpoint is not None
        errors = [
            cast(dict[str, object], turn["error"])["code"]
            for turn in cast(list[dict[str, object]], _plain(checkpoint.document["trajectory"]))
            if turn["kind"] == "tool" and turn["error"] is not None
        ]
        assert errors == ["entity_not_found"]


def test_approval_pause_and_exact_resume_applies_once(runtime_engine: Engine) -> None:
    claimed = _create_run(runtime_engine)
    adapter = _adapter(
        _call(
            "refund",
            "payments.refund",
            PAYMENTS_REFUND_V0,
            {
                "order_id": "ORD-1007",
                "payment_id": "PAY-1007",
                "amount_minor": 6000,
                "reason": "Failed shipment",
                "idempotency_key": "approved-runtime-refund",
            },
        ),
        AgentOutput("Approved refund completed.", final=True, usage=_usage()),
    )
    paused = execute_run(runtime_engine, claimed.lease, adapter)
    assert paused.status == "waiting_for_approval" and paused.approval_id is not None

    with Session(runtime_engine) as session, session.begin():
        PersistenceRepository(session).resolve_approval_request(
            paused.approval_id,
            result="approved",
            actor_id="reviewer",
            resolution_event_id=_unique("event-resolution"),
        )
    resumed = execute_run(runtime_engine, claimed.lease, adapter)
    assert resumed.status == "evaluation_ready"
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        state = repository.get_run_company_state(claimed.run.run_id)
        assert state is not None and len(state.refunds) == 1
        events = [row.event.to_dict() for row in repository.fetch_events(claimed.run.run_id)]
        assert sum(event["event_type"] == "approval.requested" for event in events) == 1
        assert sum(event["event_type"] == "state.evidence_recorded" for event in events) == 1


def test_approval_pause_expiry_reclaim_and_resume_applies_exactly_once(
    runtime_engine: Engine,
) -> None:
    claimed = _create_run(runtime_engine)
    adapter = _adapter(
        _call(
            "refund",
            "payments.refund",
            PAYMENTS_REFUND_V0,
            {
                "order_id": "ORD-1007",
                "payment_id": "PAY-1007",
                "amount_minor": 6000,
                "reason": "Approval after worker reclaim",
                "idempotency_key": "approval-after-reclaim",
            },
        ),
        AgentOutput("Approved after recovery", final=True, usage=_usage()),
    )
    paused = execute_run(runtime_engine, claimed.lease, adapter)
    assert paused.status == "waiting_for_approval" and paused.approval_id is not None
    replacement = _expire_requeue_and_reclaim(
        runtime_engine, claimed, worker="worker-approval-replacement"
    )
    assert execute_run(runtime_engine, claimed.lease, adapter).status == "stale_lease"
    with Session(runtime_engine) as session, session.begin():
        PersistenceRepository(session).resolve_approval_request(
            paused.approval_id,
            result="approved",
            actor_id="reviewer-after-reclaim",
            resolution_event_id=_unique("event-resolution-after-reclaim"),
        )
    resumed = execute_run(runtime_engine, replacement.lease, adapter)
    assert resumed.status == "evaluation_ready"

    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        state = repository.get_run_company_state(claimed.run.run_id)
        approval = repository.get_approval_request(paused.approval_id)
        events = [row.event.to_dict() for row in repository.fetch_events(claimed.run.run_id)]
        assert state is not None and len(state.refunds) == 1
        assert approval is not None and approval.status == "approved"
        assert sum(event["event_type"] == "approval.requested" for event in events) == 1
        assert sum(event["event_type"] == "approval.resolved" for event in events) == 1
        assert sum(event["event_type"] == "state.evidence_recorded" for event in events) == 1
        refund_requests = [
            cast(dict[str, object], event["payload"])
            for event in events
            if event["event_type"] == "tool.requested"
            and cast(dict[str, object], event["payload"])["tool_id"] == "payments.refund"
        ]
        assert [payload["attempt_number"] for payload in refund_requests] == [1, 2]
        assert len({cast(str, payload["logical_call_id"]) for payload in refund_requests}) == 1


def test_denied_approval_is_observed_and_agent_can_finish(runtime_engine: Engine) -> None:
    claimed = _create_run(runtime_engine)
    adapter = _adapter(
        _call(
            "refund",
            "payments.refund",
            PAYMENTS_REFUND_V0,
            {
                "order_id": "ORD-1007",
                "payment_id": "PAY-1007",
                "amount_minor": 6000,
                "reason": "Failed shipment",
                "idempotency_key": "denied-runtime-refund",
            },
        ),
        AgentOutput("Approval was denied; no refund was made.", final=True, usage=_usage()),
    )
    paused = execute_run(runtime_engine, claimed.lease, adapter)
    assert paused.approval_id is not None
    with Session(runtime_engine) as session, session.begin():
        PersistenceRepository(session).resolve_approval_request(
            paused.approval_id,
            result="denied",
            actor_id="reviewer",
            resolution_event_id=_unique("event-resolution"),
        )
    assert execute_run(runtime_engine, claimed.lease, adapter).status == "evaluation_ready"
    with Session(runtime_engine) as session:
        state = PersistenceRepository(session).get_run_company_state(claimed.run.run_id)
        assert state is not None and state.refunds == ()


def test_approval_resolution_transaction_serializes_with_runtime_resume(
    runtime_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    claimed = _create_run(runtime_engine)
    adapter = _adapter(
        _call(
            "refund",
            "payments.refund",
            PAYMENTS_REFUND_V0,
            {
                "order_id": "ORD-1007",
                "payment_id": "PAY-1007",
                "amount_minor": 6000,
                "reason": "Concurrent approval",
                "idempotency_key": "concurrent-approval",
            },
        ),
        AgentOutput("Done", final=True, usage=_usage()),
    )
    paused = execute_run(runtime_engine, claimed.lease, adapter)
    assert paused.approval_id is not None
    locked = Event()
    runtime_reached_lock = Event()
    release = Event()
    resolver_thread: int | None = None
    original_lock = PersistenceRepository.lock_current_lease

    def observed_lock(self: PersistenceRepository, lease: LeaseIdentity):  # type: ignore[no-untyped-def]
        if get_ident() != resolver_thread:
            runtime_reached_lock.set()
        return original_lock(self, lease)

    monkeypatch.setattr(PersistenceRepository, "lock_current_lease", observed_lock)

    def resolve_while_holding_transaction() -> None:
        nonlocal resolver_thread
        resolver_thread = get_ident()
        with Session(runtime_engine) as session, session.begin():
            PersistenceRepository(session).resolve_approval_request(
                cast(str, paused.approval_id),
                result="approved",
                actor_id="reviewer",
                resolution_event_id=_unique("event-resolution-race"),
            )
            locked.set()
            assert release.wait(timeout=10)

    with ThreadPoolExecutor(max_workers=2) as pool:
        resolution = pool.submit(resolve_while_holding_transaction)
        assert locked.wait(timeout=10)
        resumed = pool.submit(execute_run, runtime_engine, claimed.lease, adapter)
        assert runtime_reached_lock.wait(timeout=10)
        release.set()
        resolution.result(timeout=10)
        result = resumed.result(timeout=20)
    assert result.status == "evaluation_ready"
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        state = repository.get_run_company_state(claimed.run.run_id)
        events = [row.event.to_dict() for row in repository.fetch_events(claimed.run.run_id)]
        assert state is not None and len(state.refunds) == 1
        assert sum(event["event_type"] == "approval.requested" for event in events) == 1
        sequences = [cast(int, event["sequence"]) for event in events]
        assert sequences == list(range(1, len(sequences) + 1))


@pytest.mark.parametrize(
    ("budgets", "outputs", "error_code"),
    [
        (
            {
                "max_steps": 1,
                "max_tool_calls": 5,
                "max_wall_time_ms": 120000,
                "max_cost_microusd": 100,
            },
            (
                AgentOutput("continue", usage=_usage()),
                AgentOutput("never", final=True, usage=_usage()),
            ),
            "max_steps_exceeded",
        ),
        (
            {
                "max_steps": 5,
                "max_tool_calls": 0,
                "max_wall_time_ms": 120000,
                "max_cost_microusd": 100,
            },
            (_call("order", "orders.get", ORDERS_GET_V0, {"order_id": "ORD-1007"}),),
            "max_tool_calls_exceeded",
        ),
        (
            {
                "max_steps": 5,
                "max_tool_calls": 5,
                "max_wall_time_ms": 120000,
                "max_cost_microusd": 5,
            },
            (AgentOutput("too expensive", final=True, usage=_usage(6)),),
            "max_cost_exceeded",
        ),
        (
            {
                "max_steps": 5,
                "max_tool_calls": 5,
                "max_wall_time_ms": 120000,
                "max_cost_microusd": 5,
            },
            (AgentOutput("unknown cost", final=True, usage=_usage(None)),),
            "cost_unavailable",
        ),
    ],
)
def test_hard_budgets_fail_closed(
    runtime_engine: Engine,
    budgets: dict[str, int],
    outputs: tuple[AgentOutput, ...],
    error_code: str,
) -> None:
    claimed = _create_run(runtime_engine, budgets=budgets)
    result = execute_run(runtime_engine, claimed.lease, _adapter(*outputs))
    assert result.status == "timed_out" and result.error_code == error_code
    with Session(runtime_engine) as session:
        run = PersistenceRepository(session).get_run(claimed.run.run_id)
        assert run is not None and run.status == "timed_out"


def test_multi_call_budget_counts_each_actual_gateway_dispatch(runtime_engine: Engine) -> None:
    claimed = _create_run(
        runtime_engine,
        budgets={
            "max_steps": 5,
            "max_tool_calls": 1,
            "max_wall_time_ms": 120000,
            "max_cost_microusd": 100,
        },
    )
    result = execute_run(
        runtime_engine,
        claimed.lease,
        _adapter(
            _multi_call(
                AgentToolCall("order", "orders.get", ORDERS_GET_V0, {"order_id": "ORD-1007"}),
                AgentToolCall(
                    "shipping",
                    "shipping.get_status",
                    SHIPPING_GET_STATUS_V0,
                    {"order_id": "ORD-1007"},
                ),
            )
        ),
    )
    assert result.status == "timed_out" and result.error_code == "max_tool_calls_exceeded"
    with Session(runtime_engine) as session:
        events = [
            row.event.to_dict()
            for row in PersistenceRepository(session).fetch_events(claimed.run.run_id)
        ]
    assert sum(event["event_type"] == "tool.requested" for event in events) == 1


@pytest.mark.parametrize(
    ("output", "error_code"),
    [
        (object(), "invalid_agent_output"),
        (
            AgentOutput(
                "bad",
                (
                    AgentToolCall("same", "orders.get", ORDERS_GET_V0, {"order_id": "ORD-1007"}),
                    AgentToolCall("same", "orders.get", ORDERS_GET_V0, {"order_id": "ORD-1007"}),
                ),
                usage=AgentUsage(cost_microusd=0),
            ),
            "invalid_agent_output",
        ),
        (
            AgentOutput(
                "bad",
                (AgentToolCall("call", "orders.get", ORDERS_GET_V0, {}),),
                usage=AgentUsage(cost_microusd=0),
            ),
            "invalid_agent_output",
        ),
        (
            AgentOutput(
                "bad",
                (AgentToolCall("call", "unknown.tool", "chaosagent.tool/unknown/v0", {}),),
                usage=AgentUsage(cost_microusd=0),
            ),
            "invalid_agent_output",
        ),
        (
            AgentOutput(
                "bad",
                (
                    AgentToolCall(
                        "call",
                        "orders.get",
                        "chaosagent.tool/orders.get/v1",
                        {"order_id": "ORD-1007"},
                    ),
                ),
                usage=AgentUsage(cost_microusd=0),
            ),
            "invalid_agent_output",
        ),
        (
            _call(
                "nul-value",
                "support.update_ticket",
                SUPPORT_UPDATE_TICKET_V0,
                {
                    "ticket_id": "TKT-204",
                    "status": "closed",
                    "note": "contains\x00nul",
                    "idempotency_key": "nul-value",
                },
            ),
            "invalid_agent_output",
        ),
        (
            _call(
                "nul-key",
                "orders.get",
                ORDERS_GET_V0,
                {"order_id": "ORD-1007", "bad\x00key": "value"},
            ),
            "invalid_agent_output",
        ),
        (
            _call("malformed call id", "orders.get", ORDERS_GET_V0, {"order_id": "ORD-1007"}),
            "invalid_agent_output",
        ),
        (AgentOutput("negative usage", usage=AgentUsage(cost_microusd=-1)), "invalid_agent_output"),
        (AgentOutput("x" * 100_001, usage=_usage()), "invalid_agent_output"),
        (
            _call(
                "nested",
                "orders.get",
                ORDERS_GET_V0,
                {"order_id": {"unexpected": "object"}},
            ),
            "invalid_agent_output",
        ),
        (AgentProviderError("secret provider detail"), "provider_error"),
        (AgentProviderTimeout("timeout detail"), "provider_timeout"),
        (AgentOutput("contains\x00nul", usage=_usage()), "invalid_agent_output"),
        (
            AgentOutput(
                "too many",
                tuple(
                    AgentToolCall(
                        f"call-{index}",
                        "orders.get",
                        ORDERS_GET_V0,
                        {"order_id": "ORD-1007"},
                    )
                    for index in range(129)
                ),
                usage=_usage(),
            ),
            "invalid_agent_output",
        ),
        (
            AgentOutput(
                "bad tool type",
                (AgentToolCall("call", cast(str, ["orders.get"]), ORDERS_GET_V0, {}),),
                usage=_usage(),
            ),
            "invalid_agent_output",
        ),
        (
            AgentOutput(
                "bad version type",
                (AgentToolCall("call", "orders.get", cast(str, 7), {}),),
                usage=_usage(),
            ),
            "invalid_agent_output",
        ),
        (AgentOutput("bool usage", usage=AgentUsage(cost_microusd=True)), "invalid_agent_output"),
        (
            AgentOutput("NaN usage", usage=AgentUsage(cost_microusd=cast(int, float("nan")))),
            "invalid_agent_output",
        ),
        (
            AgentOutput("infinite usage", usage=AgentUsage(cost_microusd=cast(int, float("inf")))),
            "invalid_agent_output",
        ),
        (
            AgentOutput(
                "oversized cost",
                usage=AgentUsage(cost_microusd=1_000_000_000_001),
            ),
            "invalid_agent_output",
        ),
        (
            AgentOutput(
                "unsafe integer",
                usage=AgentUsage(input_tokens=9_007_199_254_740_992, cost_microusd=0),
            ),
            "invalid_agent_output",
        ),
    ],
)
def test_invalid_and_provider_failures_are_sanitized(
    runtime_engine: Engine, output: object, error_code: str
) -> None:
    claimed = _create_run(runtime_engine)
    result = execute_run(runtime_engine, claimed.lease, _adapter(output))
    assert result.error_code == error_code
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        documents = [row.event.to_dict() for row in repository.fetch_events(claimed.run.run_id)]
        serialized = json.dumps(documents)
        assert "secret provider detail" not in serialized and "timeout detail" not in serialized
        failed_steps = [
            event
            for event in documents
            if event["event_type"] == "agent.step"
            and cast(dict[str, object], event["payload"])["phase"] == "failed"
        ]
        assert len(failed_steps) == 1
        run = repository.get_run(claimed.run.run_id)
        assert run is not None and run.status in {"failed", "timed_out", "infra_error"}
        assert any(event["event_type"] == "run.error" for event in documents)


def test_stateful_mapping_is_snapshotted_once_before_validation_and_dispatch(
    runtime_engine: Engine,
) -> None:
    class StatefulArguments(Mapping[str, object]):
        reads = 0

        def __getitem__(self, key: str) -> object:
            assert key == "order_id"
            self.reads += 1
            return "ORD-1007" if self.reads == 1 else object()

        def __iter__(self):  # type: ignore[no-untyped-def]
            return iter(("order_id",))

        def __len__(self) -> int:
            return 1

    claimed = _create_run(runtime_engine)
    arguments = StatefulArguments()
    result = execute_run(
        runtime_engine,
        claimed.lease,
        _adapter(
            AgentOutput(
                "read",
                (AgentToolCall("order", "orders.get", ORDERS_GET_V0, arguments),),
                usage=_usage(),
            ),
            AgentOutput("Done", final=True, usage=_usage()),
        ),
    )
    assert result.status == "evaluation_ready"
    assert arguments.reads == 1


def test_competing_executors_commit_only_one_provider_output(runtime_engine: Engine) -> None:
    claimed = _create_run(runtime_engine)
    barrier = Barrier(2)

    class BarrierAdapter(ScriptedAgentAdapter):
        def invoke(self, context):  # type: ignore[no-untyped-def]
            barrier.wait(timeout=10)
            return super().invoke(context)

    adapter = BarrierAdapter(
        AGENT.id, AGENT.revision, (AgentOutput("Done", final=True, usage=_usage()),)
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(lambda _: execute_run(runtime_engine, claimed.lease, adapter), range(2))
        )
    assert sorted(result.status for result in results) == ["evaluation_ready", "stale_lease"]
    with Session(runtime_engine) as session:
        events = PersistenceRepository(session).fetch_events(claimed.run.run_id)
        assert sum(row.event.to_dict()["event_type"] == "agent.step" for row in events) == 1


def test_competing_executors_use_checkpoint_cas_while_run_remains_running(
    runtime_engine: Engine,
) -> None:
    claimed = _create_run(runtime_engine)
    barrier = Barrier(2)

    class BarrierAdapter(ScriptedAgentAdapter):
        def invoke(self, context):  # type: ignore[no-untyped-def]
            barrier.wait(timeout=10)
            return super().invoke(context)

    output = _call(
        "refund",
        "payments.refund",
        PAYMENTS_REFUND_V0,
        {
            "order_id": "ORD-1007",
            "payment_id": "PAY-1007",
            "amount_minor": 6000,
            "reason": "CAS approval",
            "idempotency_key": "cas-approval",
        },
    )
    adapter = BarrierAdapter(AGENT.id, AGENT.revision, (output,))
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(lambda _: execute_run(runtime_engine, claimed.lease, adapter), range(2))
        )
    assert sorted(result.status for result in results) == ["stale_lease", "waiting_for_approval"]
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        run = repository.get_run(claimed.run.run_id)
        events = [row.event.to_dict() for row in repository.fetch_events(claimed.run.run_id)]
        assert run is not None and run.status == "running"
        assert sum(event["event_type"] == "agent.step" for event in events) == 1
        sequences = [cast(int, event["sequence"]) for event in events]
        assert sequences == list(range(1, len(sequences) + 1))


def test_provider_output_is_discarded_when_lease_expires_during_call(
    runtime_engine: Engine,
) -> None:
    claimed = _create_run(runtime_engine)

    class ExpiringAdapter(ScriptedAgentAdapter):
        def invoke(self, context):  # type: ignore[no-untyped-def]
            with runtime_engine.begin() as connection:
                connection.execute(
                    text(
                        "UPDATE public.runs SET heartbeat_at = "
                        "clock_timestamp() - interval '2 seconds', lease_expires_at = "
                        "clock_timestamp() - interval '1 second' WHERE run_id = :run_id"
                    ),
                    {"run_id": context.run_id},
                )
            return super().invoke(context)

    result = execute_run(
        runtime_engine,
        claimed.lease,
        ExpiringAdapter(
            AGENT.id, AGENT.revision, (AgentOutput("stale", final=True, usage=_usage()),)
        ),
    )
    assert result.status == "stale_lease"
    with Session(runtime_engine) as session:
        events = PersistenceRepository(session).fetch_events(claimed.run.run_id)
        assert all(row.event.to_dict()["event_type"] != "agent.step" for row in events)


def test_old_provider_return_and_checkpoint_write_are_fenced_after_actual_reclaim(
    runtime_engine: Engine,
) -> None:
    claimed = _create_run(runtime_engine)
    replacement: ClaimedRun | None = None

    class ReclaimingAdapter(ScriptedAgentAdapter):
        def invoke(self, context):  # type: ignore[no-untyped-def]
            nonlocal replacement
            with runtime_engine.begin() as connection:
                connection.execute(
                    text(
                        "UPDATE public.runs SET heartbeat_at = "
                        "clock_timestamp() - interval '2 seconds', lease_expires_at = "
                        "clock_timestamp() - interval '1 second' WHERE run_id = :run_id"
                    ),
                    {"run_id": context.run_id},
                )
            with Session(runtime_engine) as session, session.begin():
                repository = PersistenceRepository(session)
                current = repository.get_run(context.run_id)
                assert current is not None
                repository.requeue_expired_run(
                    current.run_id,
                    expected_version=current.lifecycle_version,
                    evidence=_evidence("requeue-during-provider"),
                )
                replacement = repository.claim_next_run(
                    "worker-replacement",
                    lease_duration_seconds=600,
                    evidence=_evidence("reclaim-during-provider"),
                    run_id=current.run_id,
                )
                assert replacement is not None
            return super().invoke(context)

    result = execute_run(
        runtime_engine,
        claimed.lease,
        ReclaimingAdapter(
            AGENT.id, AGENT.revision, (AgentOutput("stale", final=True, usage=_usage()),)
        ),
    )
    assert result.status == "stale_lease" and replacement is not None
    with Session(runtime_engine) as session, session.begin():
        with pytest.raises(StaleLeaseError):
            PersistenceRepository(session)._store_execution_checkpoint(
                claimed.lease,
                {"schema_version": "chaosagent.execution-checkpoint/v0"},
                expected_version=0,
            )
    assert (
        execute_run(
            runtime_engine,
            replacement.lease,
            _adapter(AgentOutput("replacement", final=True, usage=_usage())),
        ).status
        == "evaluation_ready"
    )


def test_terminal_transition_racing_nonfinal_output_fences_output(
    runtime_engine: Engine,
) -> None:
    claimed = _create_run(runtime_engine)

    class TerminatingAdapter(ScriptedAgentAdapter):
        def invoke(self, context):  # type: ignore[no-untyped-def]
            with Session(runtime_engine) as session, session.begin():
                repository = PersistenceRepository(session)
                run = repository.get_run(context.run_id)
                assert run is not None
                repository.transition_owned_run(
                    claimed.lease,
                    "failed",
                    expected_version=run.lifecycle_version,
                    evidence=_evidence("terminal-race"),
                )
            return super().invoke(context)

    result = execute_run(
        runtime_engine,
        claimed.lease,
        TerminatingAdapter(AGENT.id, AGENT.revision, (AgentOutput("late", usage=_usage()),)),
    )
    assert result.status == "stale_lease"
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        run = repository.get_run(claimed.run.run_id)
        events = repository.fetch_events(claimed.run.run_id)
        assert run is not None and run.status == "failed"
        assert all(row.event.to_dict()["event_type"] != "agent.step" for row in events)


def test_crash_requeue_reclaim_resumes_from_committed_checkpoint(runtime_engine: Engine) -> None:
    claimed = _create_run(runtime_engine)

    class Crash(BaseException):
        pass

    class CrashingAdapter(ScriptedAgentAdapter):
        def invoke(self, context):  # type: ignore[no-untyped-def]
            if context.step_number == 2:
                raise Crash
            return super().invoke(context)

    adapter = CrashingAdapter(
        AGENT.id,
        AGENT.revision,
        (_call("order", "orders.get", ORDERS_GET_V0, {"order_id": "ORD-1007"}),),
    )
    with pytest.raises(Crash):
        execute_run(runtime_engine, claimed.lease, adapter)
    with Session(runtime_engine) as session, session.begin():
        session.execute(
            text(
                "UPDATE public.runs SET heartbeat_at = "
                "clock_timestamp() - interval '2 seconds', lease_expires_at = "
                "clock_timestamp() - interval '1 second' WHERE run_id = :run_id"
            ),
            {"run_id": claimed.run.run_id},
        )
    with Session(runtime_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        current = repository.get_run(claimed.run.run_id)
        assert current is not None
        repository.requeue_expired_run(
            current.run_id,
            expected_version=current.lifecycle_version,
            evidence=_evidence("requeue"),
        )
        replacement = repository.claim_next_run(
            "worker-replacement",
            lease_duration_seconds=600,
            evidence=_evidence("reclaim"),
            run_id=current.run_id,
        )
        assert replacement is not None
    resumed = execute_run(
        runtime_engine,
        replacement.lease,
        _adapter(
            _call("order", "orders.get", ORDERS_GET_V0, {"order_id": "ORD-1007"}),
            AgentOutput("Resumed", final=True, usage=_usage()),
        ),
    )
    assert resumed.status == "evaluation_ready"
    with Session(runtime_engine) as session:
        events = [
            row.event.to_dict()["event_type"]
            for row in PersistenceRepository(session).fetch_events(claimed.run.run_id)
        ]
        assert events.count("tool.requested") == 1
        assert events.count("agent.step") == 2


def test_checkpoint_write_failure_rolls_back_agent_event(
    runtime_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    claimed = _create_run(runtime_engine)

    def fail(*args: object, **kwargs: object) -> None:
        raise PersistenceIntegrityError("injected checkpoint failure")

    monkeypatch.setattr(PersistenceRepository, "_store_execution_checkpoint", fail)
    result = execute_run(
        runtime_engine,
        claimed.lease,
        _adapter(AgentOutput("not committed", final=True, usage=_usage())),
    )
    assert result.status == "infra_error"
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        assert repository.get_execution_checkpoint(claimed.run.run_id) is None
        events = [row.event.to_dict() for row in repository.fetch_events(claimed.run.run_id)]
        agent_steps = [event for event in events if event["event_type"] == "agent.step"]
        assert len(agent_steps) == 1
        assert cast(dict[str, object], agent_steps[0]["payload"])["phase"] == "failed"
        run = repository.get_run(claimed.run.run_id)
        assert run is not None and run.status == "infra_error"
        assert any(
            row.event.to_dict()["event_type"] == "run.error"
            for row in repository.fetch_events(claimed.run.run_id)
        )


def test_terminal_persistence_failure_does_not_claim_authoritative_failure(
    runtime_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    claimed = _create_run(runtime_engine)
    with Session(runtime_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        repository.transition_owned_run(
            claimed.lease,
            "running",
            expected_version=claimed.run.lifecycle_version,
            evidence=_evidence("running-before-terminal-persistence-failure"),
        )

    def fail(*args: object, **kwargs: object) -> None:
        raise PersistenceIntegrityError("injected evidence persistence failure")

    monkeypatch.setattr(PersistenceRepository, "append_event_allocated", fail)
    result = execute_run(
        runtime_engine,
        claimed.lease,
        _adapter(AgentOutput("cannot persist", final=True, usage=_usage())),
    )
    assert result.status == "run_not_ready" and result.error_code == "internal_error"
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        run = repository.get_run(claimed.run.run_id)
        assert run is not None and run.status == "running"
        assert repository.get_execution_checkpoint(claimed.run.run_id) is None
        assert (
            repository.fetch_events(claimed.run.run_id)[-1].event.to_dict()["event_type"]
            == "run.lifecycle"
        )


def test_initial_load_operational_error_is_sanitized(
    runtime_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    claimed = _create_run(runtime_engine)
    with Session(runtime_engine) as session:
        before = len(PersistenceRepository(session).fetch_events(claimed.run.run_id))

    def fail(*args: object, **kwargs: object) -> None:
        raise OperationalError(
            "SELECT secret_sql",
            {"database_host": "secret-db.internal"},
            RuntimeError("secret driver detail"),
        )

    monkeypatch.setattr(runtime_module, "_load_state", fail)
    result = execute_run(
        runtime_engine,
        claimed.lease,
        _adapter(AgentOutput("unused", final=True, usage=_usage())),
    )
    assert result.status == "run_not_ready" and result.error_code == "internal_error"
    assert "secret" not in repr(result) and "SELECT" not in repr(result)
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        run = repository.get_run(claimed.run.run_id)
        assert run is not None and run.status == "provisioning"
        assert len(repository.fetch_events(claimed.run.run_id)) == before


def test_post_provider_sqlalchemy_failure_terminalizes_cleanly(
    runtime_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    claimed = _create_run(runtime_engine)

    def fail(*args: object, **kwargs: object) -> None:
        raise SQLAlchemyError("secret post-provider SQL detail")

    monkeypatch.setattr(PersistenceRepository, "_store_execution_checkpoint", fail)
    result = execute_run(
        runtime_engine,
        claimed.lease,
        _adapter(AgentOutput("accepted but not committed", final=True, usage=_usage())),
    )
    assert result.status == "infra_error" and result.error_code == "internal_error"
    assert "secret" not in repr(result)
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        run = repository.get_run(claimed.run.run_id)
        events = [row.event.to_dict() for row in repository.fetch_events(claimed.run.run_id)]
        assert run is not None and run.status == "infra_error"
        assert repository.get_execution_checkpoint(claimed.run.run_id) is None
        assert [
            cast(dict[str, object], event["payload"])["phase"]
            for event in events
            if event["event_type"] == "agent.step"
        ] == ["failed"]


def test_pending_dispatch_sqlalchemy_failure_rolls_back_and_terminalizes(
    runtime_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    claimed = _create_run(runtime_engine)

    def fail(*args: object, **kwargs: object) -> None:
        raise SQLAlchemyError("secret tool transaction detail")

    monkeypatch.setattr(ToolGateway, "execute", fail)
    result = execute_run(
        runtime_engine,
        claimed.lease,
        _adapter(_call("order", "orders.get", ORDERS_GET_V0, {"order_id": "ORD-1007"})),
    )
    assert result.status == "infra_error" and result.error_code == "internal_error"
    assert "secret" not in repr(result)
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        run = repository.get_run(claimed.run.run_id)
        events = [row.event.to_dict() for row in repository.fetch_events(claimed.run.run_id)]
        state = repository.get_run_company_state(claimed.run.run_id)
        assert run is not None and run.status == "infra_error"
        assert state is not None and state.refunds == ()
        assert all(event["event_type"] != "tool.requested" for event in events)


def test_evidence_validation_failure_during_load_is_sanitized(
    runtime_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    claimed = _checkpoint_after_one_tool(runtime_engine)
    original = PersistenceRepository.fetch_events

    def fail(*args: object, **kwargs: object) -> None:
        raise EvidenceValidationError("run event", ["secret evidence validation detail"])

    monkeypatch.setattr(PersistenceRepository, "fetch_events", fail)

    class CountingAdapter(ScriptedAgentAdapter):
        calls = 0

        def invoke(self, context):  # type: ignore[no-untyped-def]
            self.calls += 1
            return super().invoke(context)

    adapter = CountingAdapter(
        AGENT.id, AGENT.revision, (AgentOutput("unused", final=True, usage=_usage()),)
    )
    result = execute_run(runtime_engine, claimed.lease, adapter)
    assert result.status == "run_not_ready" and result.error_code == "internal_error"
    assert adapter.calls == 0 and "secret" not in repr(result)
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        run = repository.get_run(claimed.run.run_id)
        assert run is not None and run.status == "running"
        assert original(repository, claimed.run.run_id)


def test_terminalization_sqlalchemy_failure_does_not_claim_terminal_state(
    runtime_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    claimed = _create_run(runtime_engine)
    with Session(runtime_engine) as session, session.begin():
        PersistenceRepository(session).transition_owned_run(
            claimed.lease,
            "running",
            expected_version=claimed.run.lifecycle_version,
            evidence=_evidence("running-before-raw-terminal-failure"),
        )

    def fail(*args: object, **kwargs: object) -> None:
        raise SQLAlchemyError("secret terminal SQL detail")

    monkeypatch.setattr(PersistenceRepository, "append_event_allocated", fail)
    result = execute_run(runtime_engine, claimed.lease, _adapter(object()))
    assert result.status == "run_not_ready" and result.error_code == "internal_error"
    assert "secret" not in repr(result)
    with Session(runtime_engine) as session:
        run = PersistenceRepository(session).get_run(claimed.run.run_id)
        assert run is not None and run.status == "running"


def test_post_provider_and_terminalization_sqlalchemy_failures_are_sanitized(
    runtime_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    claimed = _create_run(runtime_engine)

    def fail_checkpoint(*args: object, **kwargs: object) -> None:
        raise SQLAlchemyError("secret checkpoint SQL detail")

    def fail_terminal(*args: object, **kwargs: object) -> None:
        raise SQLAlchemyError("secret terminal evidence detail")

    monkeypatch.setattr(PersistenceRepository, "_store_execution_checkpoint", fail_checkpoint)
    monkeypatch.setattr(runtime_module, "_append_failed_agent_step", fail_terminal)
    result = execute_run(
        runtime_engine,
        claimed.lease,
        _adapter(AgentOutput("accepted but lost", final=True, usage=_usage())),
    )
    assert result.status == "run_not_ready" and result.error_code == "internal_error"
    assert "secret" not in repr(result)
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        run = repository.get_run(claimed.run.run_id)
        events = repository.fetch_events(claimed.run.run_id)
        assert run is not None and run.status == "running"
        assert repository.get_execution_checkpoint(claimed.run.run_id) is None
        assert all(row.event.to_dict()["event_type"] != "agent.step" for row in events)


def test_wall_budget_uses_monotonic_elapsed_time(
    runtime_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    claimed = _create_run(
        runtime_engine,
        budgets={
            "max_steps": 5,
            "max_tool_calls": 5,
            "max_wall_time_ms": 1,
            "max_cost_microusd": 100,
        },
    )
    ticks = iter((0, 2_000_000))
    monkeypatch.setattr(runtime_module, "monotonic_ns", lambda: next(ticks))
    result = execute_run(
        runtime_engine,
        claimed.lease,
        _adapter(AgentOutput("late final", final=True, usage=_usage())),
    )
    assert result.status == "timed_out" and result.error_code == "max_wall_time_exceeded"


def test_tool_effect_and_evidence_roll_back_if_checkpoint_fails(
    runtime_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    claimed = _create_run(runtime_engine)
    original = PersistenceRepository._store_execution_checkpoint
    calls = 0

    def fail_after_tool(
        self: PersistenceRepository,
        lease: LeaseIdentity,
        document: Mapping[str, object],
        *,
        expected_version: int,
    ) -> ExecutionCheckpointRecord:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise PersistenceIntegrityError("injected post-tool checkpoint failure")
        return original(self, lease, document, expected_version=expected_version)

    monkeypatch.setattr(PersistenceRepository, "_store_execution_checkpoint", fail_after_tool)
    result = execute_run(
        runtime_engine,
        claimed.lease,
        _adapter(
            _call(
                "refund",
                "payments.refund",
                PAYMENTS_REFUND_V0,
                {
                    "order_id": "ORD-1007",
                    "payment_id": "PAY-1007",
                    "amount_minor": 5000,
                    "reason": "Failed shipment",
                    "idempotency_key": "rollback-runtime-refund",
                },
            )
        ),
    )
    assert result.status == "infra_error" and result.error_code == "internal_error"
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        state = repository.get_run_company_state(claimed.run.run_id)
        assert state is not None and state.refunds == ()
        event_types = [
            row.event.to_dict()["event_type"] for row in repository.fetch_events(claimed.run.run_id)
        ]
        assert "tool.requested" not in event_types
        assert "state.evidence_recorded" not in event_types


def test_checkpoint_is_defensive_and_stale_cas_is_rejected(runtime_engine: Engine) -> None:
    claimed = _create_run(runtime_engine)

    class Crash(BaseException):
        pass

    class CrashingAdapter(ScriptedAgentAdapter):
        def invoke(self, context):  # type: ignore[no-untyped-def]
            if context.step_number == 2:
                raise Crash
            return super().invoke(context)

    with pytest.raises(Crash):
        execute_run(
            runtime_engine,
            claimed.lease,
            CrashingAdapter(
                AGENT.id,
                AGENT.revision,
                (_call("order", "orders.get", ORDERS_GET_V0, {"order_id": "ORD-1007"}),),
            ),
        )
    with Session(runtime_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        checkpoint = repository.get_execution_checkpoint(claimed.run.run_id)
        assert checkpoint is not None
        with pytest.raises(TypeError):
            checkpoint.document["status"] = "tampered"  # type: ignore[index]
        document = cast(dict[str, object], _plain(checkpoint.document))
        events = repository.fetch_events(claimed.run.run_id)
        document["checkpoint_version"] = checkpoint.checkpoint_version + 1
        document["last_event_sequence"] = events[-1].event.to_dict()["sequence"]
        repository._store_execution_checkpoint(
            claimed.lease, document, expected_version=checkpoint.checkpoint_version
        )
        document["checkpoint_version"] = checkpoint.checkpoint_version + 1
        with pytest.raises(CheckpointConflictError):
            repository._store_execution_checkpoint(
                claimed.lease, document, expected_version=checkpoint.checkpoint_version
            )


def test_raw_sql_rejects_missing_checkpoint_projection(runtime_engine: Engine) -> None:
    claimed = _create_run(runtime_engine)
    malformed = {
        "schema_version": "chaosagent.execution-checkpoint/v0",
        "checkpoint_version": 1,
        "lease_attempt": claimed.lease.attempt,
        "last_event_sequence": 1,
    }
    with Session(runtime_engine) as session, session.begin():
        with pytest.raises(IntegrityError), session.begin_nested():
            session.execute(
                text(
                    "INSERT INTO public.execution_checkpoints "
                    "(run_id, schema_version, checkpoint_version, lease_attempt, "
                    "last_event_sequence, document, document_digest) VALUES "
                    "(:run_id, 'chaosagent.execution-checkpoint/v0', 1, :attempt, 1, "
                    "CAST(:document AS jsonb), :digest)"
                ),
                {
                    "run_id": claimed.run.run_id,
                    "attempt": claimed.lease.attempt,
                    "document": json.dumps(malformed),
                    "digest": "sha256:" + "0" * 64,
                },
            )


def test_semantically_corrupt_checkpoint_fails_before_adapter(
    runtime_engine: Engine,
) -> None:
    claimed = _create_run(runtime_engine)

    class Crash(BaseException):
        pass

    class CrashingAdapter(ScriptedAgentAdapter):
        def invoke(self, context):  # type: ignore[no-untyped-def]
            if context.step_number == 2:
                raise Crash
            return super().invoke(context)

    with pytest.raises(Crash):
        execute_run(
            runtime_engine,
            claimed.lease,
            CrashingAdapter(
                AGENT.id,
                AGENT.revision,
                (_call("order", "orders.get", ORDERS_GET_V0, {"order_id": "ORD-1007"}),),
            ),
        )
    with Session(runtime_engine) as session:
        checkpoint = PersistenceRepository(session).get_execution_checkpoint(claimed.run.run_id)
        assert checkpoint is not None
        corrupted = cast(dict[str, object], _plain(checkpoint.document))
    corrupted["tool_attempts"] = 0
    with runtime_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE public.execution_checkpoints SET document = CAST(:document AS jsonb), "
                "document_digest = :digest WHERE run_id = :run_id"
            ),
            {
                "document": json.dumps(corrupted),
                "digest": digest_payload_v0(corrupted),
                "run_id": claimed.run.run_id,
            },
        )

    class CountingAdapter(ScriptedAgentAdapter):
        calls = 0

        def invoke(self, context):  # type: ignore[no-untyped-def]
            self.calls += 1
            return super().invoke(context)

    adapter = CountingAdapter(
        AGENT.id, AGENT.revision, (AgentOutput("unused", final=True, usage=_usage()),)
    )
    result = execute_run(runtime_engine, claimed.lease, adapter)
    assert result.status == "run_not_ready" and adapter.calls == 0


@pytest.mark.parametrize(
    "case",
    [
        "assistant_text",
        "assistant_duration",
        "fabricated_call",
        "changed_arguments",
        "repended_completed_call",
        "removed_and_repended_completed_call",
        "fabricated_observation",
        "wrong_tool_step",
        "duplicate_event_reference",
        "reset_step_count",
        "remove_committed_step",
        "reset_tool_attempts",
        "reset_known_cost",
        "change_cost_complete",
        "adapter_identity",
        "unsafe_wall_counter",
    ],
)
def test_rewritten_checkpoint_and_digest_cannot_change_authoritative_execution(
    runtime_engine: Engine, case: str
) -> None:
    claimed = _checkpoint_after_one_tool(runtime_engine, cost=10)

    def mutate(document: dict[str, object]) -> None:
        trajectory = cast(list[dict[str, object]], document["trajectory"])
        assistant = trajectory[0]
        tool = trajectory[1]
        calls = cast(list[dict[str, object]], assistant["tool_calls"])
        original = calls[0]
        if case == "assistant_text":
            assistant["text"] = "fabricated assistant output"
        elif case == "assistant_duration":
            assistant["duration_ms"] = cast(int, assistant["duration_ms"]) + 1
            document["active_wall_time_ms"] = cast(int, document["active_wall_time_ms"]) + 1
        elif case == "fabricated_call":
            step_id = cast(str, assistant["step_id"])
            call = {
                "call_id": "fabricated-refund",
                "call_index": 2,
                "logical_call_id": runtime_module._identity(
                    "logical", claimed.run.run_id, step_id, "2", "fabricated-refund"
                ),
                "tool_id": "payments.refund",
                "contract_version": PAYMENTS_REFUND_V0,
                "arguments": {
                    "order_id": "ORD-1007",
                    "payment_id": "PAY-1007",
                    "amount_minor": 5000,
                    "reason": "Fabricated",
                    "idempotency_key": "fabricated-checkpoint-refund",
                },
            }
            calls.append(call)
            document["pending_tool_calls"] = [{**call, "step_id": step_id, "attempt_number": 1}]
        elif case == "changed_arguments":
            original["arguments"] = {"order_id": "ORD-1008"}
        elif case == "repended_completed_call":
            document["pending_tool_calls"] = [
                {**original, "step_id": assistant["step_id"], "attempt_number": 2}
            ]
        elif case == "removed_and_repended_completed_call":
            removed = trajectory.pop(1)
            document["tool_attempts"] = 0
            document["active_wall_time_ms"] = cast(int, document["active_wall_time_ms"]) - cast(
                int, removed["duration_ms"]
            )
            document["pending_tool_calls"] = [
                {**original, "step_id": assistant["step_id"], "attempt_number": 1}
            ]
        elif case == "fabricated_observation":
            cast(dict[str, object], tool["output"])["status"] = "fabricated"
        elif case == "wrong_tool_step":
            tool["step_id"] = "step-wrong"
        elif case == "duplicate_event_reference":
            trajectory.append(dict(tool))
            document["tool_attempts"] = 2
        elif case == "reset_step_count":
            document["next_step_number"] = 1
        elif case == "remove_committed_step":
            trajectory.clear()
            document["next_step_number"] = 1
            document["tool_attempts"] = 0
            document["active_wall_time_ms"] = 0
            document["known_cost_microusd"] = 0
        elif case == "reset_tool_attempts":
            document["tool_attempts"] = 0
        elif case == "reset_known_cost":
            document["known_cost_microusd"] = 0
        elif case == "change_cost_complete":
            document["cost_complete"] = False
        elif case == "adapter_identity":
            document["adapter"] = {"id": "other-adapter", "version": "1"}
        elif case == "unsafe_wall_counter":
            document["active_wall_time_ms"] = 9_007_199_254_740_991
        else:  # pragma: no cover - parametrization is closed above
            raise AssertionError(case)

    _rewrite_checkpoint(runtime_engine, claimed.run.run_id, mutate)
    _assert_corruption_stops_before_progress(runtime_engine, claimed)


def test_rewritten_checkpoint_boundary_cannot_hide_later_tool_evidence(
    runtime_engine: Engine,
) -> None:
    claimed = _checkpoint_after_one_tool(runtime_engine, cost=10)
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        checkpoint = repository.get_execution_checkpoint(claimed.run.run_id)
        assert checkpoint is not None
        document = cast(dict[str, object], _plain(checkpoint.document))
        events = repository.fetch_events(claimed.run.run_id)

    trajectory = cast(list[dict[str, object]], document["trajectory"])
    assistant = trajectory[0]
    original_call = cast(list[dict[str, object]], assistant["tool_calls"])[0]
    agent_event = next(
        record.event.to_dict()
        for record in events
        if record.event.to_dict()["event_type"] == "agent.step"
        and cast(dict[str, object], record.event.to_dict()["payload"])["phase"] == "completed"
    )
    document["trajectory"] = [assistant]
    document["pending_tool_calls"] = [
        {
            **original_call,
            "step_id": assistant["step_id"],
            "attempt_number": 1,
        }
    ]
    document["tool_attempts"] = 0
    document["active_wall_time_ms"] = assistant["duration_ms"]
    document["last_event_sequence"] = agent_event["sequence"]

    with runtime_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE public.execution_checkpoints "
                "SET last_event_sequence = :sequence, document = CAST(:document AS jsonb), "
                "document_digest = :digest WHERE run_id = :run_id"
            ),
            {
                "sequence": agent_event["sequence"],
                "document": json.dumps(document),
                "digest": digest_payload_v0(document),
                "run_id": claimed.run.run_id,
            },
        )

    _assert_corruption_stops_before_progress(runtime_engine, claimed)


def test_rewritten_checkpoint_cannot_swap_evidence_between_calls(
    runtime_engine: Engine,
) -> None:
    claimed = _create_run(runtime_engine)

    class Stop(BaseException):
        pass

    class StopAtSecondStep(ScriptedAgentAdapter):
        def invoke(self, context):  # type: ignore[no-untyped-def]
            if context.step_number == 2:
                raise Stop
            return super().invoke(context)

    output = _multi_call(
        AgentToolCall("order", "orders.get", ORDERS_GET_V0, {"order_id": "ORD-1007"}),
        AgentToolCall(
            "shipping",
            "shipping.get_status",
            SHIPPING_GET_STATUS_V0,
            {"order_id": "ORD-1007"},
        ),
    )
    with pytest.raises(Stop):
        execute_run(
            runtime_engine,
            claimed.lease,
            StopAtSecondStep(AGENT.id, AGENT.revision, (output,)),
        )

    def mutate(document: dict[str, object]) -> None:
        tools = [
            turn
            for turn in cast(list[dict[str, object]], document["trajectory"])
            if turn["kind"] == "tool"
        ]
        tools[0]["request_event_id"], tools[1]["request_event_id"] = (
            tools[1]["request_event_id"],
            tools[0]["request_event_id"],
        )
        tools[0]["result_event_id"], tools[1]["result_event_id"] = (
            tools[1]["result_event_id"],
            tools[0]["result_event_id"],
        )

    _rewrite_checkpoint(runtime_engine, claimed.run.run_id, mutate)
    _assert_corruption_stops_before_progress(runtime_engine, claimed)


def test_rewritten_checkpoint_cannot_cross_bind_another_runs_evidence(
    runtime_engine: Engine,
) -> None:
    claimed = _checkpoint_after_one_tool(runtime_engine)
    other = _checkpoint_after_one_tool(runtime_engine)
    with Session(runtime_engine) as session:
        other_checkpoint = PersistenceRepository(session).get_execution_checkpoint(other.run.run_id)
        assert other_checkpoint is not None
        other_tool = next(
            turn
            for turn in cast(
                list[dict[str, object]], _plain(other_checkpoint.document["trajectory"])
            )
            if turn["kind"] == "tool"
        )

    def mutate(document: dict[str, object]) -> None:
        tool = next(
            turn
            for turn in cast(list[dict[str, object]], document["trajectory"])
            if turn["kind"] == "tool"
        )
        tool["request_event_id"] = other_tool["request_event_id"]
        tool["result_event_id"] = other_tool["result_event_id"]

    _rewrite_checkpoint(runtime_engine, claimed.run.run_id, mutate)
    _assert_corruption_stops_before_progress(runtime_engine, claimed)


def test_rewritten_checkpoint_cannot_reinterpret_read_as_mutation(
    runtime_engine: Engine,
) -> None:
    claimed = _checkpoint_after_one_tool(runtime_engine)

    def mutate(document: dict[str, object]) -> None:
        trajectory = cast(list[dict[str, object]], document["trajectory"])
        assistant = trajectory[0]
        tool = trajectory[1]
        call = cast(list[dict[str, object]], assistant["tool_calls"])[0]
        call["tool_id"] = "payments.refund"
        call["contract_version"] = PAYMENTS_REFUND_V0
        call["arguments"] = {
            "order_id": "ORD-1007",
            "payment_id": "PAY-1007",
            "amount_minor": 5000,
            "reason": "Fabricated reinterpretation",
            "idempotency_key": "fabricated-reinterpretation",
        }
        tool["tool_id"] = "payments.refund"
        tool["contract_version"] = PAYMENTS_REFUND_V0

    _rewrite_checkpoint(runtime_engine, claimed.run.run_id, mutate)
    _assert_corruption_stops_before_progress(runtime_engine, claimed)


@pytest.mark.parametrize("case", ["pending_arguments", "pending_order"])
def test_rewritten_pending_suffix_cannot_change_or_reorder_calls(
    runtime_engine: Engine, case: str
) -> None:
    claimed = _create_run(runtime_engine)
    paused = execute_run(
        runtime_engine,
        claimed.lease,
        _adapter(_approval_multi_output(), AgentOutput("unused", final=True, usage=_usage())),
    )
    assert paused.status == "waiting_for_approval"

    def mutate(document: dict[str, object]) -> None:
        pending = cast(list[dict[str, object]], document["pending_tool_calls"])
        if case == "pending_arguments":
            pending[0]["arguments"] = {
                **cast(dict[str, object], pending[0]["arguments"]),
                "amount_minor": 5000,
            }
        else:
            pending.reverse()

    _rewrite_checkpoint(runtime_engine, claimed.run.run_id, mutate)
    _assert_corruption_stops_before_progress(runtime_engine, claimed)


@pytest.mark.parametrize("case", ["final_answer", "final_pending"])
def test_rewritten_final_checkpoint_cannot_change_completion(
    runtime_engine: Engine, case: str
) -> None:
    claimed = _create_run(runtime_engine)
    assert (
        execute_run(
            runtime_engine,
            claimed.lease,
            _adapter(AgentOutput("Authoritative final", final=True, usage=_usage())),
        ).status
        == "evaluation_ready"
    )

    def mutate(document: dict[str, object]) -> None:
        if case == "final_answer":
            document["final_answer"] = "Fabricated final"
        else:
            document["pending_tool_calls"] = [
                {
                    "call_id": "fake",
                    "call_index": 1,
                    "logical_call_id": "logical-fake",
                    "tool_id": "orders.get",
                    "contract_version": ORDERS_GET_V0,
                    "arguments": {"order_id": "ORD-1007"},
                    "step_id": "step-fake",
                    "attempt_number": 1,
                }
            ]

    _rewrite_checkpoint(runtime_engine, claimed.run.run_id, mutate)
    with Session(runtime_engine) as session:
        before = len(PersistenceRepository(session).fetch_events(claimed.run.run_id))
    result = execute_run(
        runtime_engine,
        claimed.lease,
        _adapter(AgentOutput("unused", final=True, usage=_usage())),
    )
    assert result.status == "run_not_ready"
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        run = repository.get_run(claimed.run.run_id)
        assert run is not None and run.status == "evaluating"
        assert len(repository.fetch_events(claimed.run.run_id)) == before


def test_database_rejects_checkpoint_document_run_mismatch(runtime_engine: Engine) -> None:
    claimed = _checkpoint_after_one_tool(runtime_engine)
    with Session(runtime_engine) as session:
        checkpoint = PersistenceRepository(session).get_execution_checkpoint(claimed.run.run_id)
        assert checkpoint is not None
        document = cast(dict[str, object], _plain(checkpoint.document))
    document["run_id"] = _unique("other-run")
    with Session(runtime_engine) as session, session.begin():
        with pytest.raises(IntegrityError), session.begin_nested():
            session.execute(
                text(
                    "UPDATE public.execution_checkpoints "
                    "SET document = CAST(:document AS jsonb), document_digest = :digest "
                    "WHERE run_id = :run_id"
                ),
                {
                    "document": json.dumps(document),
                    "digest": digest_payload_v0(document),
                    "run_id": claimed.run.run_id,
                },
            )


def test_fake_openai_end_to_end_uses_real_runtime_gateway_and_evidence(
    runtime_engine: Engine,
) -> None:
    configuration = loads_agent_configuration(
        json.dumps(
            {
                "schema_version": "chaosagent.agent-configuration/v0",
                "agent_configuration_id": "openai-e2e-agent",
                "revision": "r1",
                "provider": "openai",
                "adapter": {"id": "openai-responses", "version": "v0"},
                "model": "gpt-4.1-2025-04-14",
                "compatibility_profile": "openai-responses-stateless-non-reasoning/v0",
                "token_accounting": {
                    "schema_version": "chaosagent.token-accounting/v0",
                    "schedule_id": "e2e-openai-rates",
                    "revision": "2026-08-28",
                    "model": "gpt-4.1-2025-04-14",
                    "unit": "microusd",
                    "tokens_per_rate_unit": 1000000,
                    "rounding": "ceiling_per_response",
                    "input_rate_microusd": 1000000,
                    "cached_input_rate_microusd": 500000,
                    "output_rate_microusd": 2000000,
                },
                "timeout_ms": 5000,
                "max_output_tokens": 256,
                "temperature": None,
                "parallel_tool_calls": True,
                "store": False,
                "max_retries": 0,
            }
        )
    )

    def response(index: int, item: dict[str, object]) -> dict[str, object]:
        return {
            "id": f"resp_e2e_{index}",
            "model": "gpt-4.1-2025-04-14",
            "status": "completed",
            "error": None,
            "output": [item],
            "usage": {
                "input_tokens": 10,
                "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
                "output_tokens": 5,
                "output_tokens_details": {"reasoning_tokens": 0},
                "total_tokens": 15,
            },
        }

    def function(name: str, call_id: str, arguments: dict[str, object]) -> dict[str, object]:
        return {
            "type": "function_call",
            "name": name,
            "call_id": call_id,
            "arguments": json.dumps(arguments),
        }

    outputs = [
        response(1, function("chaosagent_orders__get", "provider-order", {"order_id": "ORD-1007"})),
        response(
            2,
            function(
                "chaosagent_shipping__get_status",
                "provider-shipping",
                {"order_id": "ORD-1007"},
            ),
        ),
        response(
            3,
            function(
                "chaosagent_payments__refund",
                "provider-refund",
                {
                    "order_id": "ORD-1007",
                    "payment_id": "PAY-1007",
                    "amount_minor": 5000,
                    "reason": "Failed shipment",
                    "idempotency_key": "openai-e2e-refund",
                },
            ),
        ),
        response(
            4,
            function(
                "chaosagent_support__update_ticket",
                "provider-ticket",
                {
                    "ticket_id": "TKT-204",
                    "status": "closed",
                    "note": "Failed shipment verified and refund applied.",
                    "idempotency_key": "openai-e2e-ticket",
                },
            ),
        ),
        response(
            5,
            {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "Refunded and closed."}],
            },
        ),
    ]

    class Responses:
        def __init__(self) -> None:
            self.requests: list[dict[str, object]] = []

        def create(self, **kwargs: object) -> object:
            self.requests.append(dict(kwargs))
            return outputs.pop(0)

    class Client:
        def __init__(self) -> None:
            self.responses = Responses()

        def with_options(self, **kwargs: object) -> Client:
            return self

    claimed = _create_run(runtime_engine, agent_configuration=configuration)
    client = Client()
    result = execute_run(
        runtime_engine,
        claimed.lease,
        OpenAIResponsesAdapter(configuration, client=client),
    )
    assert result.status == "evaluation_ready" and result.final_answer == "Refunded and closed.", (
        result,
        len(client.responses.requests),
    )
    assert len(client.responses.requests) == 5
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        run = repository.get_run(claimed.run.run_id)
        state = repository.get_run_company_state(claimed.run.run_id)
        events = [record.event.to_dict() for record in repository.fetch_events(claimed.run.run_id)]
        assert run is not None and run.status == "evaluating"
        checkpoint = repository.get_execution_checkpoint(claimed.run.run_id)
        assert checkpoint is not None
        assert checkpoint.document["known_cost_microusd"] == 100
        assert checkpoint.document["cost_complete"] is True
        trajectory = cast(list[dict[str, object]], _plain(checkpoint.document["trajectory"]))
        assert [
            cast(dict[str, object], turn["usage"])["cost_microusd"]
            for turn in trajectory
            if turn["kind"] == "assistant"
        ] == [20, 20, 20, 20, 20]
        assert state is not None and len(state.refunds) == 1
        assert state.support_tickets[0].status == "closed"
        assert [event["event_type"] for event in events].count("tool.requested") == 4
        completed_steps = [
            event
            for event in events
            if event["event_type"] == "agent.step"
            and cast(dict[str, object], event["payload"])["phase"] == "completed"
        ]
        assert len(completed_steps) == 5
        assert [event["sequence"] for event in events] == list(range(1, len(events) + 1))
        assert sum(event["event_type"] == "state.evidence_recorded" for event in events) == 2
        first_model = cast(dict[str, object], completed_steps[0]["payload"])["model"]
        assert cast(dict[str, object], first_model)["provider_request_id"] == "resp_e2e_1"


@pytest.mark.parametrize(
    ("budget", "expected_status", "expected_error"),
    [(20, "evaluation_ready", None), (19, "timed_out", "max_cost_exceeded")],
)
def test_production_openai_first_turn_cost_budget(
    runtime_engine: Engine,
    budget: int,
    expected_status: str,
    expected_error: str | None,
) -> None:
    configuration = _hosted_configuration("openai-first-turn")
    claimed = _create_run(
        runtime_engine,
        agent_configuration=configuration,
        budgets={
            "max_steps": 2,
            "max_tool_calls": 1,
            "max_wall_time_ms": 120000,
            "max_cost_microusd": budget,
        },
    )
    client = _ProviderClient(
        _provider_response(
            "resp_first_turn",
            {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "Done"}],
            },
        )
    )
    result = execute_run(
        runtime_engine,
        claimed.lease,
        OpenAIResponsesAdapter(configuration, client=client),
    )
    assert result.status == expected_status and result.error_code == expected_error
    with Session(runtime_engine) as session:
        checkpoint = PersistenceRepository(session).get_execution_checkpoint(claimed.run.run_id)
        assert checkpoint is not None
        assert checkpoint.document["known_cost_microusd"] == 20
        assert checkpoint.document["cost_complete"] is True


@pytest.mark.parametrize(
    "usage",
    [
        None,
        {
            "input_tokens": 10,
            "input_tokens_details": {"cached_tokens": 0},
            "output_tokens": 5,
            "output_tokens_details": {"reasoning_tokens": 0},
            "total_tokens": 15,
        },
        {
            "input_tokens": 10,
            "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
            "output_tokens": 5,
            "output_tokens_details": {"reasoning_tokens": 0},
            "total_tokens": 15,
            "future_billed_tokens": 1,
        },
    ],
)
def test_production_openai_incomplete_usage_fails_closed(
    runtime_engine: Engine, usage: object
) -> None:
    configuration = _hosted_configuration("openai-missing-usage")
    claimed = _create_run(runtime_engine, agent_configuration=configuration)
    client = _ProviderClient(
        _provider_response(
            "resp_missing_usage",
            {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "Not chargeable"}],
            },
            usage=usage,
        )
    )
    result = execute_run(
        runtime_engine,
        claimed.lease,
        OpenAIResponsesAdapter(configuration, client=client),
    )
    assert result.status == "timed_out" and result.error_code == "cost_unavailable"


@pytest.mark.parametrize(
    ("reported_provider", "reported_model"),
    [
        ("fabricated-provider", "gpt-4.1-2025-04-14"),
        ("openai", "fabricated-model"),
    ],
)
def test_provider_metadata_must_match_active_hosted_adapter_before_step_persistence(
    runtime_engine: Engine, reported_provider: str, reported_model: str
) -> None:
    configuration = _hosted_configuration("openai-metadata-binding")
    document = configuration.to_dict()
    claimed = _create_run(runtime_engine, agent_configuration=configuration)

    class MismatchedMetadataAdapter:
        adapter_id = cast(str, document["agent_configuration_id"])
        adapter_version = cast(str, document["revision"])
        configuration_digest = configuration.digest
        provider_name = "openai"
        requested_model = cast(str, document["model"])

        def invoke(self, context: AgentContext) -> AgentOutput:
            return AgentOutput(
                "untrusted metadata",
                final=True,
                usage=_usage(),
                provider_metadata=AgentProviderMetadata(
                    reported_provider,
                    reported_model,
                    self.requested_model,
                    "resp_fabricated",
                ),
            )

    result = execute_run(runtime_engine, claimed.lease, MismatchedMetadataAdapter())
    assert result.status == "failed" and result.error_code == "invalid_agent_output"
    with Session(runtime_engine) as session:
        events = [
            record.event.to_dict()
            for record in PersistenceRepository(session).fetch_events(claimed.run.run_id)
        ]
    assert not any(
        event["event_type"] == "agent.step"
        and cast(dict[str, object], event["payload"])["phase"] == "completed"
        for event in events
    )


def test_openai_approval_resume_uses_fresh_adapter_and_durable_trajectory(
    runtime_engine: Engine,
) -> None:
    configuration = _hosted_configuration("openai-approval-restart")
    claimed = _create_run(runtime_engine, agent_configuration=configuration)
    first_client = _ProviderClient(
        _provider_response(
            "resp_approval_request",
            {
                "type": "function_call",
                "name": "chaosagent_payments__refund",
                "call_id": "provider-approval-refund",
                "arguments": json.dumps(
                    {
                        "order_id": "ORD-1007",
                        "payment_id": "PAY-1007",
                        "amount_minor": 6000,
                        "reason": "Approval restart test",
                        "idempotency_key": "openai-approval-restart",
                    }
                ),
            },
        )
    )
    paused = execute_run(
        runtime_engine,
        claimed.lease,
        OpenAIResponsesAdapter(configuration, client=first_client),
    )
    assert paused.status == "waiting_for_approval" and paused.approval_id is not None
    with Session(runtime_engine) as session, session.begin():
        PersistenceRepository(session).resolve_approval_request(
            paused.approval_id,
            result="approved",
            actor_id="reviewer-openai-restart",
            resolution_event_id=_unique("event-openai-approval-resolution"),
        )

    configuration_document = configuration.to_dict()
    configuration_id = cast(str, configuration_document["agent_configuration_id"])
    configuration_revision = cast(str, configuration_document["revision"])
    del configuration_document, configuration, first_client
    with Session(runtime_engine) as session:
        persisted = PersistenceRepository(session).get_agent_configuration_reference(
            configuration_id, configuration_revision
        )
        assert persisted is not None and persisted.configuration is not None
        reloaded = persisted.configuration
    second_client = _ProviderClient(
        _provider_response(
            "resp_after_approval",
            {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "Approved refund complete"}],
            },
        )
    )
    resumed = execute_run(
        runtime_engine,
        claimed.lease,
        OpenAIResponsesAdapter(reloaded, client=second_client),
    )
    assert resumed.status == "evaluation_ready"
    provider_input = cast(list[dict[str, object]], second_client.responses.requests[0]["input"])
    assert any(item.get("type") == "function_call_output" for item in provider_input)
    assert any("retry/approval outcome" in json.dumps(item) for item in provider_input)
    with Session(runtime_engine) as session:
        repository = PersistenceRepository(session)
        state = repository.get_run_company_state(claimed.run.run_id)
        checkpoint = repository.get_execution_checkpoint(claimed.run.run_id)
        assert state is not None and len(state.refunds) == 1
        assert checkpoint is not None and checkpoint.document["known_cost_microusd"] == 40


def test_agent_configuration_document_persistence_and_projection_guard(
    runtime_engine: Engine,
) -> None:
    configuration = loads_agent_configuration(
        json.dumps(
            {
                "schema_version": "chaosagent.agent-configuration/v0",
                "agent_configuration_id": _unique("openai-config"),
                "revision": "r1",
                "provider": "openai",
                "adapter": {"id": "openai-responses", "version": "v0"},
                "model": "gpt-4.1-2025-04-14",
                "compatibility_profile": "openai-responses-stateless-non-reasoning/v0",
                "token_accounting": {
                    "schema_version": "chaosagent.token-accounting/v0",
                    "schedule_id": "projection-openai-rates",
                    "revision": "2026-08-28",
                    "model": "gpt-4.1-2025-04-14",
                    "unit": "microusd",
                    "tokens_per_rate_unit": 1000000,
                    "rounding": "ceiling_per_response",
                    "input_rate_microusd": 1000000,
                    "cached_input_rate_microusd": 500000,
                    "output_rate_microusd": 2000000,
                },
                "timeout_ms": 1000,
                "max_output_tokens": 32,
                "temperature": 0,
                "parallel_tool_calls": False,
                "store": False,
                "max_retries": 0,
            }
        )
    )
    with Session(runtime_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        record = repository.insert_agent_configuration(configuration, created_by="runtime-test")
        assert record.configuration is not None
        assert record.configuration.digest == configuration.digest
    identifier = cast(str, configuration.to_dict()["agent_configuration_id"])
    with pytest.raises(DBAPIError), runtime_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE public.agent_configuration_revisions SET created_by = 'attacker' "
                "WHERE agent_configuration_id = :id AND revision = 'r1'"
            ),
            {"id": identifier},
        )
    with pytest.raises(DBAPIError), runtime_engine.begin() as connection:
        connection.execute(
            text(
                "DELETE FROM public.agent_configuration_revisions "
                "WHERE agent_configuration_id = :id AND revision = 'r1'"
            ),
            {"id": identifier},
        )
    document = configuration.to_dict()
    document.pop("revision")
    with runtime_engine.begin() as connection, pytest.raises(IntegrityError):
        connection.execute(
            text(
                "INSERT INTO public.agent_configuration_revisions "
                "(agent_configuration_id, revision, digest, schema_version, "
                "canonical_document, created_by) VALUES "
                "(:id, 'r1', :digest, 'chaosagent.agent-configuration/v0', "
                "CAST(:document AS jsonb), 'raw-test')"
            ),
            {
                "id": _unique("broken-openai-config"),
                "digest": "sha256:" + "a" * 64,
                "document": json.dumps(document),
            },
        )
    complete_document = configuration.to_dict()
    with runtime_engine.begin() as connection, pytest.raises(IntegrityError):
        connection.execute(
            text(
                "INSERT INTO public.agent_configuration_revisions "
                "(agent_configuration_id, revision, digest, schema_version, "
                "canonical_document, created_by) VALUES "
                "(:id, 'r1', :digest, NULL, CAST(:document AS jsonb), 'raw-test')"
            ),
            {
                "id": cast(str, complete_document["agent_configuration_id"]),
                "digest": configuration.digest,
                "document": json.dumps(complete_document),
            },
        )


def test_projection_valid_corrupt_agent_configuration_digest_blocks_run_creation(
    runtime_engine: Engine,
) -> None:
    configuration = _hosted_configuration("corrupt-openai-config")
    document = configuration.to_dict()
    identifier = cast(str, document["agent_configuration_id"])
    corrupt_digest = "sha256:" + "a" * 64
    assert corrupt_digest != configuration.digest
    with runtime_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO public.agent_configuration_revisions "
                "(agent_configuration_id, revision, digest, schema_version, "
                "canonical_document, created_by) VALUES "
                "(:id, 'r1', :digest, 'chaosagent.agent-configuration/v0', "
                "CAST(:document AS jsonb), 'corruption-test')"
            ),
            {
                "id": identifier,
                "digest": corrupt_digest,
                "document": json.dumps(document),
            },
        )
    with Session(runtime_engine) as session:
        with pytest.raises(PersistenceIntegrityError, match="inconsistent"):
            PersistenceRepository(session).get_agent_configuration_reference(identifier, "r1")

    scenario_document = cast(
        dict[str, object], json.loads(SCENARIO_PATH.read_text(encoding="utf-8"))
    )
    scenario_document["scenario_id"] = _unique("scenario-corrupt-config")
    scenario_document["revision"] = "1"
    scenario_document["faults"] = []
    scenario = loads_scenario(json.dumps(scenario_document))
    with Session(runtime_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        repository.insert_fixture_revision(load_fixture(FIXTURE_PATH), created_by="runtime-test")
        repository.insert_policy_revision(load_policy(POLICY_PATH), created_by="runtime-test")
        repository.insert_scenario_revision(scenario, created_by="runtime-test")
        with pytest.raises(PersistenceIntegrityError, match="inconsistent"):
            repository.create_run(
                _unique("run-corrupt-config"),
                scenario_id=cast(str, scenario_document["scenario_id"]),
                scenario_revision="1",
                agent_configuration_id=identifier,
                agent_configuration_revision="r1",
                created_by="runtime-test",
            )


def test_hosted_adapter_digest_must_match_run_frozen_configuration(
    runtime_engine: Engine,
) -> None:
    base = {
        "schema_version": "chaosagent.agent-configuration/v0",
        "agent_configuration_id": _unique("openai-binding"),
        "revision": "r1",
        "provider": "openai",
        "adapter": {"id": "openai-responses", "version": "v0"},
        "model": "gpt-4.1-2025-04-14",
        "compatibility_profile": "openai-responses-stateless-non-reasoning/v0",
        "token_accounting": {
            "schema_version": "chaosagent.token-accounting/v0",
            "schedule_id": "binding-openai-rates",
            "revision": "2026-08-28",
            "model": "gpt-4.1-2025-04-14",
            "unit": "microusd",
            "tokens_per_rate_unit": 1000000,
            "rounding": "ceiling_per_response",
            "input_rate_microusd": 1000000,
            "cached_input_rate_microusd": 500000,
            "output_rate_microusd": 2000000,
        },
        "timeout_ms": 1000,
        "max_output_tokens": 32,
        "temperature": None,
        "parallel_tool_calls": False,
        "store": False,
        "max_retries": 0,
    }
    frozen = loads_agent_configuration(json.dumps(base))
    different = loads_agent_configuration(json.dumps({**base, "timeout_ms": 2000}))

    class Responses:
        calls = 0

        def create(self, **kwargs: object) -> object:
            self.calls += 1
            raise AssertionError("provider must not be called")

    class Client:
        def __init__(self) -> None:
            self.responses = Responses()

        def with_options(self, **kwargs: object) -> Client:
            return self

    claimed = _create_run(runtime_engine, agent_configuration=frozen)
    client = Client()
    result = execute_run(
        runtime_engine,
        claimed.lease,
        OpenAIResponsesAdapter(different, client=client),
    )
    assert result.status == "run_not_ready" and result.error_code == "internal_error"
    assert client.responses.calls == 0


def test_migration_0007_downgrade_and_reupgrade_is_isolated() -> None:
    database_url = os.environ.get("CHAOSAGENT_TEST_DATABASE_URL")
    if database_url is None:
        pytest.skip("CHAOSAGENT_TEST_DATABASE_URL is not configured")
    if os.environ.get("CHAOSAGENT_ALLOW_DESTRUCTIVE_DATABASE_TESTS") != "1":
        raise RuntimeError("destructive PostgreSQL tests require explicit opt-in")
    if not (make_url(database_url).database or "").endswith("_test"):
        raise RuntimeError("PostgreSQL integration database name must end with '_test'")
    migration_database = f"chaosagent_0007_{uuid4().hex}_test"
    source_url = make_url(database_url)
    admin_engine = create_engine(source_url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    isolated_url = source_url.set(database=migration_database)
    isolated_url_text = isolated_url.render_as_string(hide_password=False)
    configuration = Config(str(ALEMBIC_INI))
    legacy = RevisionReference(_unique("legacy-before-downgrade"), "r1", "sha256:" + "b" * 64)
    hosted = _hosted_configuration("hosted-before-downgrade")
    previous_url = os.environ.get("CHAOSAGENT_DATABASE_URL")
    isolated_engine: Engine | None = None
    try:
        with admin_engine.connect() as connection:
            connection.execute(text(f'CREATE DATABASE "{migration_database}"'))
        os.environ["CHAOSAGENT_DATABASE_URL"] = isolated_url_text
        command.upgrade(configuration, "head")
        isolated_engine = create_postgres_engine(isolated_url_text)
        with Session(isolated_engine) as session, session.begin():
            repository = PersistenceRepository(session)
            repository.insert_agent_configuration_reference(legacy, created_by="migration-test")
            repository.insert_agent_configuration(hosted, created_by="migration-test")
        command.downgrade(configuration, "0006_execution_checkpoints")
        with isolated_engine.connect() as connection:
            columns = connection.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = "
                    "'agent_configuration_revisions'"
                )
            ).scalars()
            assert "canonical_document" not in set(columns)
        command.upgrade(configuration, "head")
        with isolated_engine.connect() as connection:
            columns = connection.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = "
                    "'agent_configuration_revisions'"
                )
            ).scalars()
            assert {"schema_version", "canonical_document"} <= set(columns)
        hosted_document = hosted.to_dict()
        with Session(isolated_engine) as session:
            repository = PersistenceRepository(session)
            restored_legacy = repository.get_agent_configuration_reference(
                legacy.id, legacy.revision
            )
            restored_hosted = repository.get_agent_configuration_reference(
                cast(str, hosted_document["agent_configuration_id"]),
                cast(str, hosted_document["revision"]),
            )
            assert restored_legacy is not None and restored_legacy.configuration is None
            assert restored_hosted is not None and restored_hosted.configuration is None
            assert restored_hosted.reference.digest == hosted.digest
    finally:
        if isolated_engine is not None:
            isolated_engine.dispose()
        if previous_url is None:
            os.environ.pop("CHAOSAGENT_DATABASE_URL", None)
        else:
            os.environ["CHAOSAGENT_DATABASE_URL"] = previous_url
        with admin_engine.connect() as connection:
            connection.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :database AND pid <> pg_backend_pid()"
                ),
                {"database": migration_database},
            )
            connection.execute(text(f'DROP DATABASE IF EXISTS "{migration_database}"'))
        admin_engine.dispose()


def test_migration_0006_downgrade_and_reupgrade(runtime_engine: Engine) -> None:
    database_url = os.environ["CHAOSAGENT_TEST_DATABASE_URL"]
    os.environ["CHAOSAGENT_DATABASE_URL"] = database_url
    configuration = Config(str(ALEMBIC_INI))
    command.downgrade(configuration, "0005_policy_approvals")
    with runtime_engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT to_regclass('public.execution_checkpoints')")
            ).scalar_one()
            is None
        )
    command.upgrade(configuration, "head")
    with runtime_engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT to_regclass('public.execution_checkpoints')")
            ).scalar_one()
            == "execution_checkpoints"
        )
