from __future__ import annotations

import json
import os
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC
from pathlib import Path
from threading import Barrier
from typing import cast
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from chaosagent_evidence import RunEvent, digest_payload_v0, validate_run_event_v0
from chaosagent_fixtures import load_fixture
from chaosagent_persistence import (
    ClaimedRun,
    LeaseIdentity,
    LifecycleEvidence,
    PersistenceError,
    PersistenceRepository,
    RevisionReference,
    RunEventRecord,
    create_postgres_engine,
)
from chaosagent_scenarios import loads_scenario
from chaosagent_tool_gateway import (
    ORDERS_GET_V0,
    SHIPPING_GET_STATUS_V0,
    ReadOnlyCompanyState,
    ToolExecutionResult,
    ToolGateway,
    ToolRegistry,
    default_tool_registry,
)
from sqlalchemy import Engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

pytestmark = pytest.mark.postgres

ROOT = Path(__file__).resolve().parents[2]
SCENARIO_PATH = ROOT / "benchmarks/shipment-refund/scenarios/refund-ambiguous-timeout.v0.json"
FIXTURE_PATH = ROOT / "benchmarks/shipment-refund/fixtures/failed-shipment.v0.json"
ALEMBIC_INI = ROOT / "packages/persistence/alembic.ini"
AGENT = RevisionReference("gateway-test-agent", "placeholder", "sha256:" + "a" * 64)


@pytest.fixture(scope="session")
def gateway_engine() -> Iterator[Engine]:
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


def _evidence(label: str) -> LifecycleEvidence:
    return LifecycleEvidence(_unique(f"evt-{label}"), "gateway-test", "worker-test")


def _scenario_document(*, allowed_tools: list[str] | None = None) -> dict[str, object]:
    document = cast(dict[str, object], json.loads(SCENARIO_PATH.read_text(encoding="utf-8")))
    if allowed_tools is not None:
        document["scenario_id"] = _unique("scenario")
        document["revision"] = "1"
        cast(dict[str, object], document["agent"])["allowed_tools"] = allowed_tools
    return document


def _create_running_run(
    engine: Engine,
    *,
    initialize_state: bool = True,
    allowed_tools: list[str] | None = None,
    worker_id: str = "worker-test",
) -> tuple[str, ClaimedRun]:
    run_id = _unique("run")
    with Session(engine) as session, session.begin():
        repository = PersistenceRepository(session)
        fixture = load_fixture(FIXTURE_PATH)
        scenario = loads_scenario(json.dumps(_scenario_document(allowed_tools=allowed_tools)))
        scenario_document = scenario.to_dict()
        repository.insert_fixture_revision(fixture, created_by="gateway-tests")
        repository.insert_scenario_revision(scenario, created_by="gateway-tests")
        repository.insert_agent_configuration_reference(AGENT, created_by="gateway-tests")
        repository.create_run(
            run_id,
            scenario_id=cast(str, scenario_document["scenario_id"]),
            scenario_revision=cast(str, scenario_document["revision"]),
            agent_configuration_id=AGENT.id,
            agent_configuration_revision=AGENT.revision,
            created_by="gateway-tests",
        )
        if initialize_state:
            repository.initialize_run_company_state(run_id)
        claimed = repository.claim_next_run(
            worker_id,
            lease_duration_seconds=600,
            evidence=_evidence("claim"),
            run_id=run_id,
        )
        assert claimed is not None
        running = repository.transition_owned_run(
            claimed.lease,
            "running",
            expected_version=claimed.run.lifecycle_version,
            evidence=_evidence("running"),
        )
        claimed = ClaimedRun(running, claimed.lease)
    return run_id, claimed


def _call(
    gateway: ToolGateway,
    lease: LeaseIdentity,
    *,
    tool_id: str = "orders.get",
    version: str = ORDERS_GET_V0,
    arguments: object | None = None,
    logical_call_id: str | None = None,
) -> ToolExecutionResult:
    return gateway.execute(
        lease,
        tool_id=tool_id,
        contract_version=version,
        arguments={"order_id": "ORD-1007"} if arguments is None else arguments,
        logical_call_id=logical_call_id or _unique("logical"),
        attempt_id=_unique("attempt"),
    )


def test_read_handlers_outputs_evidence_checksums_and_no_mutation(gateway_engine: Engine) -> None:
    run_id, claimed = _create_running_run(gateway_engine)
    with Session(gateway_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        before = repository.get_run_company_state(run_id)
        assert before is not None
        order = _call(ToolGateway(session), claimed.lease)
        shipping = _call(
            ToolGateway(session),
            claimed.lease,
            tool_id="shipping.get_status",
            version=SHIPPING_GET_STATUS_V0,
        )
        after = repository.get_run_company_state(run_id)

        assert order.outcome == "succeeded"
        assert dict(order.output or {}) == {
            "order_id": "ORD-1007",
            "customer_id": "CUS-042",
            "status": "paid",
            "total_minor": 12999,
            "currency": "USD",
            "placed_at": "2026-08-18T09:15:00.000Z",
        }
        assert shipping.outcome == "succeeded"
        assert dict(shipping.output or {})["shipment_id"] == "SHP-1007"
        assert dict(shipping.output or {})["status"] == "failed"
        assert after == before

        events = repository.fetch_events(run_id)
        assert [record.event.to_dict()["sequence"] for record in events] == list(
            range(1, len(events) + 1)
        )
        request, result = (record.event.to_dict() for record in events[-2:])
        assert request["event_type"] == "tool.requested"
        assert result["event_type"] == "tool.result"
        request_payload = cast(dict[str, object], request["payload"])
        result_payload = cast(dict[str, object], result["payload"])
        assert request_payload["arguments_digest"] == digest_payload_v0({"order_id": "ORD-1007"})
        assert result_payload["response_digest"] == digest_payload_v0(dict(shipping.output or {}))
        validate_run_event_v0(request)
        validate_run_event_v0(result)
        assert result_payload["request_event_id"] == request["event_id"]
        for field in ("logical_call_id", "attempt_id", "attempt_number", "tool_id"):
            assert result_payload[field] == request_payload[field]
        assert result["causation_event_id"] == request["event_id"]


def test_entity_not_found_is_evidenced_tool_error(gateway_engine: Engine) -> None:
    run_id, claimed = _create_running_run(gateway_engine)
    with Session(gateway_engine) as session, session.begin():
        result = _call(ToolGateway(session), claimed.lease, arguments={"order_id": "ORD-MISSING"})
        assert result.outcome == "failed"
        assert result.error is not None and result.error.code == "entity_not_found"
        events = PersistenceRepository(session).fetch_events(run_id)
        payload = cast(dict[str, object], events[-1].event.to_dict()["payload"])
        assert payload["outcome"] == "failed"
        assert payload["error_code"] == "entity_not_found"


def test_authorization_readiness_and_lease_fail_closed(gateway_engine: Engine) -> None:
    disallowed_run, disallowed = _create_running_run(
        gateway_engine,
        allowed_tools=["shipping.get_status", "payments.refund", "support.update_ticket"],
    )
    missing_run, missing = _create_running_run(gateway_engine, initialize_state=False)
    provisioning_run = _unique("run")
    with Session(gateway_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        fixture = load_fixture(FIXTURE_PATH)
        scenario = loads_scenario(json.dumps(_scenario_document()))
        repository.insert_fixture_revision(fixture, created_by="gateway-tests")
        repository.insert_scenario_revision(scenario, created_by="gateway-tests")
        repository.insert_agent_configuration_reference(AGENT, created_by="gateway-tests")
        doc = scenario.to_dict()
        repository.create_run(
            provisioning_run,
            scenario_id=cast(str, doc["scenario_id"]),
            scenario_revision=cast(str, doc["revision"]),
            agent_configuration_id=AGENT.id,
            agent_configuration_revision=AGENT.revision,
            created_by="gateway-tests",
        )
        repository.initialize_run_company_state(provisioning_run)
        provisioning = repository.claim_next_run(
            "worker-test",
            lease_duration_seconds=600,
            evidence=_evidence("provisioning"),
            run_id=provisioning_run,
        )
        assert provisioning is not None

    with Session(gateway_engine) as session, session.begin():
        definition = default_tool_registry().definitions[0]
        deny_registry = ToolRegistry(
            (
                replace(
                    definition,
                    handler=lambda _company, _arguments: (_ for _ in ()).throw(
                        AssertionError("disallowed handler executed")
                    ),
                ),
            )
        )
        denied = _call(ToolGateway(session, registry=deny_registry), disallowed.lease)
        not_initialized = _call(ToolGateway(session), missing.lease)
        not_running = _call(ToolGateway(session), provisioning.lease)
        wrong = _call(ToolGateway(session), replace(disallowed.lease, worker_id="wrong-worker"))
        wrong_token = _call(
            ToolGateway(session), replace(disallowed.lease, lease_token="lease-wrong")
        )
        assert denied.error is not None and denied.error.code == "tool_not_allowed"
        assert not_initialized.error is not None and not_initialized.error.code == "run_not_ready"
        assert not_running.error is not None and not_running.error.code == "run_not_ready"
        assert wrong.error is not None and wrong.error.code == "stale_lease"
        assert wrong_token.error is not None and wrong_token.error.code == "stale_lease"
        assert len(PersistenceRepository(session).fetch_events(disallowed_run)) == 2
        assert len(PersistenceRepository(session).fetch_events(missing_run)) == 2


def test_substituted_handler_receives_only_run_bound_read_capabilities(
    gateway_engine: Engine,
) -> None:
    run_id, claimed = _create_running_run(gateway_engine)
    definition = default_tool_registry().definitions[0]
    inspected = False

    def restricted_handler(
        company: ReadOnlyCompanyState, arguments: Mapping[str, object]
    ) -> dict[str, object] | None:
        nonlocal inspected
        inspected = True
        for forbidden in (
            "session",
            "execute",
            "append_event",
            "transition_owned_run",
            "commit",
            "rollback",
            "get_company_order",
        ):
            assert not hasattr(company, forbidden)
        order = company.get_order(cast(str, arguments["order_id"]))
        assert order is not None
        return {
            "order_id": order.order_id,
            "customer_id": order.customer_id,
            "status": order.status,
            "total_minor": order.total_minor,
            "currency": order.currency,
            "placed_at": order.placed_at.astimezone(UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
        }

    registry = ToolRegistry((replace(definition, handler=restricted_handler),))
    with Session(gateway_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        before = repository.get_run_company_state(run_id)
        result = _call(ToolGateway(session, registry=registry), claimed.lease)
        after = repository.get_run_company_state(run_id)
        assert inspected
        assert result.outcome == "succeeded"
        assert after == before


def test_scenario_v0_rejects_hypothetical_v1_and_unmapped_tool(
    gateway_engine: Engine,
) -> None:
    run_id, claimed = _create_running_run(gateway_engine)
    order_v0 = default_tool_registry().definitions[0]
    called = False

    def forbidden_handler(
        _company: ReadOnlyCompanyState, _arguments: Mapping[str, object]
    ) -> dict[str, object] | None:
        nonlocal called
        called = True
        return None

    order_v1 = replace(
        order_v0,
        contract_version="chaosagent.tool/orders.get/v1",
        handler=forbidden_handler,
    )
    unmapped = replace(
        order_v0,
        tool_id="payments.refund",
        contract_version="chaosagent.tool/payments.refund/v0",
        handler=forbidden_handler,
    )
    registry = ToolRegistry((order_v0, order_v1, unmapped))
    with Session(gateway_engine) as session, session.begin():
        gateway = ToolGateway(session, registry=registry)
        v1 = _call(
            gateway,
            claimed.lease,
            version="chaosagent.tool/orders.get/v1",
        )
        unknown_mapping = _call(
            gateway,
            claimed.lease,
            tool_id="payments.refund",
            version="chaosagent.tool/payments.refund/v0",
        )
        assert v1.error is not None and v1.error.code == "unsupported_tool"
        assert unknown_mapping.error is not None
        assert unknown_mapping.error.code == "unsupported_tool"
        assert not called
        assert len(PersistenceRepository(session).fetch_events(run_id)) == 2


def test_expired_and_reclaimed_worker_is_fenced(gateway_engine: Engine) -> None:
    run_id, old = _create_running_run(gateway_engine, worker_id="old-worker")
    with Session(gateway_engine) as session, session.begin():
        session.execute(
            text(
                "UPDATE public.runs SET "
                "heartbeat_at = clock_timestamp() - interval '2 hours', "
                "lease_expires_at = clock_timestamp() - interval '1 hour' "
                "WHERE run_id = :run_id"
            ),
            {"run_id": run_id},
        )
    with Session(gateway_engine) as session, session.begin():
        expired = _call(ToolGateway(session), old.lease)
        assert expired.error is not None and expired.error.code == "stale_lease"
        repository = PersistenceRepository(session)
        current = repository.get_run(run_id)
        assert current is not None
        repository.requeue_expired_run(
            run_id,
            expected_version=current.lifecycle_version,
            evidence=_evidence("requeue"),
        )
        new = repository.claim_next_run(
            "new-worker",
            lease_duration_seconds=600,
            evidence=_evidence("reclaim"),
            run_id=run_id,
        )
        assert new is not None
        running = repository.transition_owned_run(
            new.lease,
            "running",
            expected_version=new.run.lifecycle_version,
            evidence=_evidence("rerunning"),
        )
        new = ClaimedRun(running, new.lease)
    barrier = Barrier(2)

    def race(lease: LeaseIdentity) -> ToolExecutionResult:
        with Session(gateway_engine) as session, session.begin():
            barrier.wait()
            return _call(ToolGateway(session), lease)

    with ThreadPoolExecutor(max_workers=2) as pool:
        stale, current_result = list(pool.map(race, (old.lease, new.lease)))
    assert stale.error is not None and stale.error.code == "stale_lease"
    assert current_result.outcome == "succeeded"


def test_invalid_handler_output_becomes_safe_evidenced_error(gateway_engine: Engine) -> None:
    run_id, claimed = _create_running_run(gateway_engine)
    definition = default_tool_registry().definitions[0]
    registry = ToolRegistry(
        (
            replace(
                definition,
                handler=lambda _company, _arguments: {"bad": True},
            ),
        )
    )
    with Session(gateway_engine) as session, session.begin():
        result = _call(ToolGateway(session, registry=registry), claimed.lease)
        assert result.error is not None and result.error.code == "infrastructure_error"
        assert result.output is None
        events = PersistenceRepository(session).fetch_events(run_id)
        assert len(events) == 4
        result_document = events[-1].event.to_dict()
        payload = cast(dict[str, object], result_document["payload"])
        assert payload["outcome"] == "failed"
        assert payload["error_code"] == "infrastructure_error"
        assert "response_digest" not in payload
        assert result_document["payload_digest"] == digest_payload_v0(payload)
        validate_run_event_v0(result_document)


def test_caller_rollback_removes_tool_evidence(gateway_engine: Engine) -> None:
    run_id, claimed = _create_running_run(gateway_engine)
    with Session(gateway_engine) as session:
        transaction = session.begin()
        result = _call(ToolGateway(session), claimed.lease)
        assert result.outcome == "succeeded"
        transaction.rollback()
    with Session(gateway_engine) as session:
        assert len(PersistenceRepository(session).fetch_events(run_id)) == 2


def test_evidence_failure_rolls_back_request_pair(
    gateway_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id, claimed = _create_running_run(gateway_engine)
    original = PersistenceRepository.append_event
    calls = 0

    def fail_second(repository: PersistenceRepository, event: RunEvent) -> RunEventRecord:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise PersistenceError("forced result persistence failure")
        return original(repository, event)

    monkeypatch.setattr(PersistenceRepository, "append_event", fail_second)
    with Session(gateway_engine) as session, session.begin():
        result = _call(ToolGateway(session), claimed.lease)
        assert result.error is not None and result.error.code == "infrastructure_error"
    monkeypatch.undo()
    with Session(gateway_engine) as session:
        assert len(PersistenceRepository(session).fetch_events(run_id)) == 2


def test_concurrent_tool_calls_share_one_sequence_domain(gateway_engine: Engine) -> None:
    run_id, claimed = _create_running_run(gateway_engine)
    barrier = Barrier(2)

    def invoke(index: int) -> str:
        with Session(gateway_engine) as session, session.begin():
            barrier.wait()
            result = _call(
                ToolGateway(session), claimed.lease, logical_call_id=f"logical-concurrent-{index}"
            )
            return result.outcome

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(invoke, (1, 2)))
    assert outcomes == ["succeeded", "succeeded"]
    with Session(gateway_engine) as session:
        documents = [
            record.event.to_dict() for record in PersistenceRepository(session).fetch_events(run_id)
        ]
        sequences = [cast(int, document["sequence"]) for document in documents]
        assert sequences == list(range(1, 7))
        assert len({document["event_id"] for document in documents}) == 6


def test_lifecycle_and_tool_events_cannot_collide(gateway_engine: Engine) -> None:
    run_id, claimed = _create_running_run(gateway_engine)
    barrier = Barrier(2)

    def invoke_tool() -> ToolExecutionResult:
        with Session(gateway_engine) as session, session.begin():
            barrier.wait()
            return _call(ToolGateway(session), claimed.lease)

    def transition_run() -> str:
        with Session(gateway_engine) as session, session.begin():
            barrier.wait()
            PersistenceRepository(session).transition_owned_run(
                claimed.lease,
                "evaluating",
                expected_version=claimed.run.lifecycle_version,
                evidence=_evidence("evaluating-race"),
            )
            return "evaluating"

    with ThreadPoolExecutor(max_workers=2) as pool:
        tool_future = pool.submit(invoke_tool)
        transition_future = pool.submit(transition_run)
        assert transition_future.result() == "evaluating"
        tool_result = tool_future.result()

    with Session(gateway_engine) as session:
        documents = [
            record.event.to_dict() for record in PersistenceRepository(session).fetch_events(run_id)
        ]
        sequences = [cast(int, document["sequence"]) for document in documents]
        assert sequences == list(range(1, len(sequences) + 1))
        assert len(sequences) == len(set(sequences))
        tool_events = [
            document
            for document in documents
            if document["event_type"] in {"tool.requested", "tool.result"}
        ]
        if tool_result.outcome == "succeeded":
            assert len(tool_events) == 2
            assert [event["event_type"] for event in tool_events] == [
                "tool.requested",
                "tool.result",
            ]
            result_payload = cast(dict[str, object], tool_events[1]["payload"])
            assert result_payload["request_event_id"] == tool_events[0]["event_id"]
        else:
            assert tool_result.error is not None
            assert tool_result.error.code == "run_not_ready"
            assert tool_events == []


def test_two_runs_read_only_their_run_partition(gateway_engine: Engine) -> None:
    run_a, claimed_a = _create_running_run(gateway_engine)
    run_b, claimed_b = _create_running_run(gateway_engine)
    with Session(gateway_engine) as session, session.begin():
        session.execute(
            text(
                "UPDATE public.company_orders SET status = 'fulfilled' "
                "WHERE run_id = :run_id AND order_id = 'ORD-1007'"
            ),
            {"run_id": run_b},
        )
    with Session(gateway_engine) as session, session.begin():
        first = _call(ToolGateway(session), claimed_a.lease)
        second = _call(ToolGateway(session), claimed_b.lease)
        assert dict(first.output or {})["status"] == "paid"
        assert dict(second.output or {})["status"] == "fulfilled"
        assert run_a != run_b
