"""Real-PostgreSQL control-plane REST, replay, and flagship integration tests."""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Event
from time import monotonic
from typing import cast
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from chaosagent_agent_runtime import (
    AgentOutput,
    AgentToolCall,
    AgentUsage,
    ScriptedAgentAdapter,
    execute_run,
)
from chaosagent_control_plane import RunEventStream, SSEConfig, create_app, decode_cursor
from chaosagent_control_plane.service import ControlPlaneService
from chaosagent_evaluators import execute_evaluation, load_ground_truth_v0
from chaosagent_evidence import digest_payload_v0, loads_run_event
from chaosagent_faults import FaultEngine, compile_fault_plan_v0
from chaosagent_fixtures import load_fixture
from chaosagent_persistence import (
    ClaimedRun,
    LifecycleEvidence,
    PersistenceRepository,
    RunRecord,
    create_postgres_engine,
)
from chaosagent_policies import load_policy
from chaosagent_tool_gateway import PAYMENTS_REFUND_V0, SUPPORT_UPDATE_TICKET_V0
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import Engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

pytestmark = pytest.mark.postgres

ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_INI = ROOT / "packages/persistence/alembic.ini"
SCENARIO_PATH = (
    ROOT / "benchmarks/shipment-refund/scenarios/refund-ambiguous-timeout.evaluated.v0.json"
)
FIXTURE_PATH = ROOT / "benchmarks/shipment-refund/fixtures/failed-shipment.v0.json"
POLICY_PATH = ROOT / "benchmarks/shipment-refund/policies/refund-policy.v0.json"
GROUND_TRUTH_PATH = (
    ROOT / "benchmarks/shipment-refund/ground-truth/refund-once-and-close-ticket.v0.json"
)


@pytest.fixture(scope="session")
def control_plane_engine() -> Iterator[Engine]:
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


@pytest.fixture
def client(control_plane_engine: Engine) -> Iterator[TestClient]:
    truth = load_ground_truth_v0(GROUND_TRUTH_PATH)
    with TestClient(create_app(control_plane_engine, ground_truths=(truth,))) as api:
        yield api


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex}"


def _agent_document(identifier: str) -> dict[str, object]:
    return {
        "schema_version": "chaosagent.agent-configuration/v0",
        "agent_configuration_id": identifier,
        "revision": "1",
        "provider": "openai",
        "adapter": {"id": "openai-responses", "version": "v0"},
        "model": "gpt-4.1-2025-04-14",
        "compatibility_profile": "openai-responses-stateless-non-reasoning/v0",
        "token_accounting": {
            "schema_version": "chaosagent.token-accounting/v0",
            "schedule_id": "control-plane-rates",
            "revision": "2026-09-06",
            "model": "gpt-4.1-2025-04-14",
            "unit": "microusd",
            "tokens_per_rate_unit": 1_000_000,
            "rounding": "ceiling_per_response",
            "input_rate_microusd": 1_000_000,
            "cached_input_rate_microusd": 500_000,
            "output_rate_microusd": 2_000_000,
        },
        "timeout_ms": 5_000,
        "max_output_tokens": 256,
        "temperature": None,
        "parallel_tool_calls": True,
        "store": False,
        "max_retries": 0,
    }


def _seed_dependencies(engine: Engine) -> None:
    with Session(engine) as session, session.begin():
        repository = PersistenceRepository(session)
        repository.insert_fixture_revision(load_fixture(FIXTURE_PATH), created_by="api-test")
        repository.insert_policy_revision(load_policy(POLICY_PATH), created_by="api-test")


def _create_api_run(
    client: TestClient,
    engine: Engine,
    *,
    faults: list[dict[str, object]] | None = None,
) -> tuple[str, dict[str, object], dict[str, object]]:
    _seed_dependencies(engine)
    scenario = cast(dict[str, object], json.loads(SCENARIO_PATH.read_text(encoding="utf-8")))
    scenario["scenario_id"] = _unique("scenario")
    if faults is not None:
        scenario["faults"] = faults
    scenario_response = client.post(
        "/api/v1/scenarios", json={"scenario": scenario, "created_by": "api-test"}
    )
    assert scenario_response.status_code == 201, scenario_response.text
    agent_document = _agent_document(_unique("agent"))
    agent_response = client.post(
        "/api/v1/agent-configs",
        json={"agent_configuration": agent_document, "created_by": "api-test"},
    )
    assert agent_response.status_code == 201, agent_response.text
    run_id = _unique("run")
    created = client.post(
        "/api/v1/runs",
        json={
            "run_id": run_id,
            "scenario": {
                "id": scenario_response.json()["scenario_id"],
                "revision": scenario_response.json()["revision"],
                "digest": scenario_response.json()["digest"],
            },
            "agent_configuration": {
                "id": agent_response.json()["agent_configuration_id"],
                "revision": agent_response.json()["revision"],
                "digest": agent_response.json()["digest"],
            },
            "created_by": "api-test",
        },
    )
    assert created.status_code == 201, created.text
    return run_id, scenario_response.json(), agent_response.json()


def _evidence(label: str) -> LifecycleEvidence:
    return LifecycleEvidence(_unique(f"event-{label}"), "control-plane-test", "worker-api")


def _claim(engine: Engine, run_id: str) -> ClaimedRun:
    with Session(engine) as session, session.begin():
        repository = PersistenceRepository(session)
        repository.initialize_run_company_state(run_id)
        claimed = repository.claim_next_run(
            "worker-api", lease_duration_seconds=600, evidence=_evidence("claim"), run_id=run_id
        )
        assert claimed is not None
        return claimed


def _wait_for_blocked_requests(engine: Engine, blocker_pid: int) -> None:
    deadline = monotonic() + 10
    while monotonic() < deadline:
        with engine.connect() as observer:
            count = cast(
                int,
                observer.scalar(
                    text(
                        "SELECT count(*) FROM pg_stat_activity "
                        "WHERE :blocker_pid = ANY(pg_blocking_pids(pid))"
                    ),
                    {"blocker_pid": blocker_pid},
                ),
            )
        if count >= 1:
            return
        Event().wait(0.01)
    raise AssertionError("concurrent API requests never reached the PostgreSQL row lock")


def _post_in_independent_client(
    engine: Engine, path: str, body: dict[str, object] | None = None
) -> tuple[int, dict[str, object]]:
    truth = load_ground_truth_v0(GROUND_TRUTH_PATH)
    with TestClient(create_app(engine, ground_truths=(truth,))) as independent:
        response = independent.post(path, json=body)
    return response.status_code, cast(dict[str, object], response.json())


def _create_run_references(
    client: TestClient, engine: Engine, *, agent_count: int
) -> tuple[dict[str, object], tuple[dict[str, object], ...]]:
    _seed_dependencies(engine)
    scenario_document = cast(
        dict[str, object], json.loads(SCENARIO_PATH.read_text(encoding="utf-8"))
    )
    scenario_document["scenario_id"] = _unique("scenario")
    scenario_document["faults"] = []
    scenario_response = client.post(
        "/api/v1/scenarios",
        json={"scenario": scenario_document, "created_by": "api-test"},
    )
    assert scenario_response.status_code == 201, scenario_response.text
    agents: list[dict[str, object]] = []
    for _ in range(agent_count):
        response = client.post(
            "/api/v1/agent-configs",
            json={
                "agent_configuration": _agent_document(_unique("agent")),
                "created_by": "api-test",
            },
        )
        assert response.status_code == 201, response.text
        agents.append(cast(dict[str, object], response.json()))
    return cast(dict[str, object], scenario_response.json()), tuple(agents)


def _run_create_body(
    run_id: str,
    scenario: dict[str, object],
    agent: dict[str, object],
) -> dict[str, object]:
    return {
        "run_id": run_id,
        "scenario": {
            "id": scenario["scenario_id"],
            "revision": scenario["revision"],
            "digest": scenario["digest"],
        },
        "agent_configuration": {
            "id": agent["agent_configuration_id"],
            "revision": agent["revision"],
            "digest": agent["digest"],
        },
        "created_by": "api-test",
    }


def _synchronize_run_inserts(monkeypatch: pytest.MonkeyPatch) -> None:
    barrier = Barrier(2)
    original = PersistenceRepository.create_run

    def synchronized_create_run(
        repository: PersistenceRepository,
        run_id: str,
        *,
        scenario_id: str,
        scenario_revision: str,
        agent_configuration_id: str,
        agent_configuration_revision: str,
        created_by: str,
    ) -> RunRecord:
        barrier.wait(timeout=10)
        return original(
            repository,
            run_id,
            scenario_id=scenario_id,
            scenario_revision=scenario_revision,
            agent_configuration_id=agent_configuration_id,
            agent_configuration_revision=agent_configuration_revision,
            created_by=created_by,
        )

    monkeypatch.setattr(PersistenceRepository, "create_run", synchronized_create_run)


def test_scenario_agent_run_create_read_and_retry(
    client: TestClient, control_plane_engine: Engine
) -> None:
    run_id, scenario, agent = _create_api_run(client, control_plane_engine, faults=[])
    fetched_scenario = client.get(
        f"/api/v1/scenarios/{scenario['scenario_id']}/revisions/{scenario['revision']}"
    )
    fetched_agent = client.get(
        f"/api/v1/agent-configs/{agent['agent_configuration_id']}/revisions/{agent['revision']}"
    )
    fetched_run = client.get(f"/api/v1/runs/{run_id}")
    assert fetched_scenario.json()["digest"] == scenario["digest"]
    assert fetched_agent.json()["digest"] == agent["digest"]
    assert fetched_run.status_code == 200 and fetched_run.json()["status"] == "queued"
    assert "lease_token" not in fetched_run.text and "lease_owner" not in fetched_run.text

    repeated = client.post(
        "/api/v1/scenarios", json={"scenario": scenario["document"], "created_by": "api-test"}
    )
    assert repeated.status_code == 201 and repeated.json()["digest"] == scenario["digest"]
    changed = cast(dict[str, object], json.loads(json.dumps(scenario["document"])))
    cast(dict[str, object], changed["metadata"])["title"] = "Different immutable content"
    conflict = client.post(
        "/api/v1/scenarios", json={"scenario": changed, "created_by": "api-test"}
    )
    assert conflict.status_code == 409


def test_identical_concurrent_run_creation_is_one_durable_run(
    client: TestClient,
    control_plane_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario, (agent,) = _create_run_references(client, control_plane_engine, agent_count=1)
    run_id = _unique("run-concurrent-identical")
    body = _run_create_body(run_id, scenario, agent)
    _synchronize_run_inserts(monkeypatch)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(_post_in_independent_client, control_plane_engine, "/api/v1/runs", body)
        second = pool.submit(
            _post_in_independent_client, control_plane_engine, "/api/v1/runs", body
        )
        responses = (first.result(timeout=15), second.result(timeout=15))

    assert [status for status, _ in responses] == [201, 201]
    assert responses[0][1] == responses[1][1]
    with Session(control_plane_engine) as session:
        count = cast(
            int,
            session.scalar(
                text("SELECT count(*) FROM public.runs WHERE run_id = :run_id"),
                {"run_id": run_id},
            ),
        )
        events = PersistenceRepository(session).fetch_events(run_id)
    assert count == 1
    assert events == ()


def test_conflicting_concurrent_run_creation_has_one_winner(
    client: TestClient,
    control_plane_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario, agents = _create_run_references(client, control_plane_engine, agent_count=2)
    run_id = _unique("run-concurrent-conflict")
    bodies = tuple(_run_create_body(run_id, scenario, agent) for agent in agents)
    _synchronize_run_inserts(monkeypatch)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = tuple(
            pool.submit(
                _post_in_independent_client,
                control_plane_engine,
                "/api/v1/runs",
                body,
            )
            for body in bodies
        )
        responses = tuple(future.result(timeout=15) for future in futures)

    assert sorted(status for status, _ in responses) == [201, 409]
    winner_index = next(index for index, response in enumerate(responses) if response[0] == 201)
    winner_agent = cast(dict[str, object], bodies[winner_index]["agent_configuration"])
    with Session(control_plane_engine) as session:
        repository = PersistenceRepository(session)
        stored = repository.get_run(run_id)
        count = cast(
            int,
            session.scalar(
                text("SELECT count(*) FROM public.runs WHERE run_id = :run_id"),
                {"run_id": run_id},
            ),
        )
        events = repository.fetch_events(run_id)
    assert stored is not None
    assert stored.agent_configuration.id == winner_agent["id"]
    assert stored.agent_configuration.revision == winner_agent["revision"]
    assert stored.agent_configuration.digest == winner_agent["digest"]
    assert count == 1
    assert events == ()


def test_invalid_reference_not_found_report_and_cancellation_semantics(
    client: TestClient, control_plane_engine: Engine
) -> None:
    run_id, scenario, agent = _create_api_run(client, control_plane_engine, faults=[])
    missing = client.get(f"/api/v1/runs/{_unique('missing')}")
    report = client.get(f"/api/v1/runs/{run_id}/report")
    assert missing.status_code == 404 and report.status_code == 409
    bad_run = client.post(
        "/api/v1/runs",
        json={
            "run_id": _unique("run"),
            "scenario": {
                "id": scenario["scenario_id"],
                "revision": scenario["revision"],
                "digest": "sha256:" + "0" * 64,
            },
            "agent_configuration": {
                "id": agent["agent_configuration_id"],
                "revision": agent["revision"],
                "digest": agent["digest"],
            },
            "created_by": "api-test",
        },
    )
    assert bad_run.status_code == 409
    lock_session = Session(control_plane_engine)
    lock_transaction = lock_session.begin()
    try:
        blocker_pid = cast(int, lock_session.scalar(text("SELECT pg_backend_pid()")))
        lock_session.execute(
            text("SELECT run_id FROM public.runs WHERE run_id = :run_id FOR UPDATE"),
            {"run_id": run_id},
        )
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(
                _post_in_independent_client,
                control_plane_engine,
                f"/api/v1/runs/{run_id}/cancel",
            )
            second = pool.submit(
                _post_in_independent_client,
                control_plane_engine,
                f"/api/v1/runs/{run_id}/cancel",
            )
            try:
                _wait_for_blocked_requests(control_plane_engine, blocker_pid)
            finally:
                lock_transaction.commit()
            responses = (first.result(timeout=10), second.result(timeout=10))
    finally:
        if lock_transaction.is_active:
            lock_transaction.rollback()
        lock_session.close()
    assert {status for status, _ in responses} == {200}
    assert sorted(cast(bool, body["already_cancelled"]) for _, body in responses) == [False, True]


def test_event_pagination_cursor_foreign_reconnect_and_active_cancel(
    client: TestClient, control_plane_engine: Engine
) -> None:
    first, _, _ = _create_api_run(client, control_plane_engine, faults=[])
    second, _, _ = _create_api_run(client, control_plane_engine, faults=[])
    claimed = _claim(control_plane_engine, first)
    with Session(control_plane_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        running = repository.transition_owned_run(
            claimed.lease,
            "running",
            expected_version=claimed.run.lifecycle_version,
            evidence=_evidence("running"),
        )
        repository.transition_owned_run(
            claimed.lease,
            "failed",
            expected_version=running.lifecycle_version,
            evidence=_evidence("failed"),
        )
    assert client.post(f"/api/v1/runs/{first}/cancel").status_code == 409
    client.post(f"/api/v1/runs/{second}/cancel")

    first_page = client.get(f"/api/v1/runs/{first}/events", params={"limit": 1})
    cursor = first_page.json()["next_cursor"]
    second_page = client.get(f"/api/v1/runs/{first}/events", params={"limit": 1, "cursor": cursor})
    assert first_page.json()["events"][0]["sequence"] == 1
    assert second_page.json()["events"][0]["sequence"] == 2
    assert client.get(f"/api/v1/runs/{second}/events", params={"cursor": cursor}).status_code == 400
    assert client.get(f"/api/v1/runs/{first}/events", params={"cursor": "bad"}).status_code == 400

    restarted = TestClient(
        create_app(control_plane_engine, ground_truths=(load_ground_truth_v0(GROUND_TRUTH_PATH),))
    )
    with restarted:
        stream = restarted.get(
            f"/api/v1/runs/{first}/events/stream", headers={"Last-Event-ID": cursor}
        )
    assert stream.status_code == 200
    assert 'sequence":2' in stream.text and 'sequence":1' not in stream.text
    assert stream.text.count("id: ") == 2


def test_event_committed_between_polls_is_seen_and_rolled_back_event_is_not(
    client: TestClient, control_plane_engine: Engine
) -> None:
    run_id, _, _ = _create_api_run(client, control_plane_engine, faults=[])
    rolled_back_id = _unique("event-rolled-back")
    payload: dict[str, object] = {
        "previous_state": "queued",
        "state": "cancelled",
        "reason_code": "rolled_back",
    }
    document: dict[str, object] = {
        "schema_version": "chaosagent.run-event/v0",
        "event_id": rolled_back_id,
        "run_id": run_id,
        "sequence": 1,
        "occurred_at": "2026-09-06T10:00:00.000Z",
        "recorded_at": "2026-09-06T10:00:00.000Z",
        "event_type": "run.lifecycle",
        "producer": {"component": "control-plane-test"},
        "correlation_id": run_id,
        "payload": payload,
        "payload_digest": digest_payload_v0(payload),
    }
    with Session(control_plane_engine) as session:
        transaction = session.begin()
        PersistenceRepository(session).append_event(loads_run_event(json.dumps(document)))
        transaction.rollback()

    service = cast(ControlPlaneService, cast(FastAPI, client.app).state.control_plane_service)
    calls = 0

    async def sleeper(_seconds: float) -> None:
        nonlocal calls
        calls += 1
        assert client.post(f"/api/v1/runs/{run_id}/cancel").status_code == 200

    async def collect() -> list[bytes]:
        streamer = RunEventStream(
            service,
            SSEConfig(page_size=1, poll_interval_seconds=0.01, keepalive_seconds=10),
            sleeper=sleeper,
        )

        async def connected() -> bool:
            return False

        return [frame async for frame in streamer.frames(run_id, None, connected)]

    frames = asyncio.run(collect())
    assert calls == 1 and len(frames) == 1
    assert rolled_back_id.encode() not in frames[0]
    assert b'"state":"cancelled"' in frames[0]


def test_approval_read_resolution_retry_and_conflict(
    client: TestClient, control_plane_engine: Engine
) -> None:
    run_id, _, agent = _create_api_run(client, control_plane_engine, faults=[])
    claimed = _claim(control_plane_engine, run_id)
    output = AgentOutput(
        "Requesting refund",
        (
            AgentToolCall(
                "refund",
                "payments.refund",
                PAYMENTS_REFUND_V0,
                {
                    "order_id": "ORD-1007",
                    "payment_id": "PAY-1007",
                    "amount_minor": 6000,
                    "reason": "Failed shipment",
                    "idempotency_key": "approval-refund",
                },
            ),
        ),
        usage=AgentUsage(input_tokens=1, output_tokens=1, cost_microusd=1),
    )
    execution = execute_run(
        control_plane_engine,
        claimed.lease,
        ScriptedAgentAdapter(
            cast(str, agent["agent_configuration_id"]), cast(str, agent["revision"]), (output,)
        ),
    )
    assert execution.status == "waiting_for_approval"
    listed = client.get(f"/api/v1/runs/{run_id}/approvals")
    approval_id = listed.json()["approvals"][0]["approval_id"]
    lock_session = Session(control_plane_engine)
    lock_transaction = lock_session.begin()
    try:
        blocker_pid = cast(int, lock_session.scalar(text("SELECT pg_backend_pid()")))
        lock_session.execute(
            text("SELECT run_id FROM public.runs WHERE run_id = :run_id FOR UPDATE"),
            {"run_id": run_id},
        )
        body: dict[str, object] = {"result": "approved", "actor_id": "operator-1"}
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(
                _post_in_independent_client,
                control_plane_engine,
                f"/api/v1/approvals/{approval_id}/resolve",
                body,
            )
            second = pool.submit(
                _post_in_independent_client,
                control_plane_engine,
                f"/api/v1/approvals/{approval_id}/resolve",
                body,
            )
            try:
                _wait_for_blocked_requests(control_plane_engine, blocker_pid)
            finally:
                lock_transaction.commit()
            resolutions = (first.result(timeout=10), second.result(timeout=10))
    finally:
        if lock_transaction.is_active:
            lock_transaction.rollback()
        lock_session.close()
    conflict = client.post(
        f"/api/v1/approvals/{approval_id}/resolve",
        json={"result": "denied", "actor_id": "operator-2"},
    )
    assert {status for status, _ in resolutions} == {200}
    assert sorted(cast(bool, body["already_resolved"]) for _, body in resolutions) == [False, True]
    assert conflict.status_code == 409


def test_flagship_api_flow_campaign_statistics_exports_and_sse_replay(
    client: TestClient, control_plane_engine: Engine
) -> None:
    fault: dict[str, object] = {
        "id": "refund-ack-lost",
        "kind": "ambiguous_post_commit_timeout",
        "match": {"tool_id": "payments.refund", "phase": "after_commit"},
        "activation": {"probability_ppm": 1_000_000, "max_occurrences": 1},
        "parameters": {"duration_ms": 1},
    }
    run_id, scenario, agent = _create_api_run(client, control_plane_engine, faults=[fault])
    campaign_id = _unique("campaign")
    planned = client.post(
        "/api/v1/campaigns",
        json={
            "campaign_id": campaign_id,
            "arm": "faulted",
            "selected_fault_ids": ["refund-ack-lost"],
            "assignments": [{"trial_index": 0, "run_id": run_id}],
        },
    )
    assert planned.status_code == 201, planned.text
    claimed = _claim(control_plane_engine, run_id)
    with Session(control_plane_engine) as session:
        stored = PersistenceRepository(session).get_scenario_revision(
            cast(str, scenario["scenario_id"]), cast(str, scenario["revision"])
        )
        assert stored is not None
        fault_engine = FaultEngine(compile_fault_plan_v0(stored.scenario), run_seed=1616)
    refund = AgentOutput(
        "Refund",
        (
            AgentToolCall(
                "refund-first",
                "payments.refund",
                PAYMENTS_REFUND_V0,
                {
                    "order_id": "ORD-1007",
                    "payment_id": "PAY-1007",
                    "amount_minor": 5000,
                    "reason": "Failed shipment",
                    "idempotency_key": "flagship-refund",
                },
            ),
        ),
        usage=AgentUsage(input_tokens=1, output_tokens=1, cost_microusd=1),
    )
    replay = AgentOutput(
        "Retry refund",
        (
            AgentToolCall(
                "refund-replay",
                "payments.refund",
                PAYMENTS_REFUND_V0,
                cast(dict[str, object], refund.tool_calls[0].arguments),
            ),
        ),
        usage=AgentUsage(input_tokens=1, output_tokens=1, cost_microusd=1),
    )
    ticket = AgentOutput(
        "Close ticket",
        (
            AgentToolCall(
                "ticket",
                "support.update_ticket",
                SUPPORT_UPDATE_TICKET_V0,
                {
                    "ticket_id": "TKT-204",
                    "status": "closed",
                    "note": "Refund completed after shipment failure.",
                    "idempotency_key": "flagship-ticket",
                },
            ),
        ),
        usage=AgentUsage(input_tokens=1, output_tokens=1, cost_microusd=1),
    )
    final = AgentOutput(
        "Refund confirmed exactly once.",
        final=True,
        usage=AgentUsage(input_tokens=1, output_tokens=1, cost_microusd=1),
    )
    runtime = execute_run(
        control_plane_engine,
        claimed.lease,
        ScriptedAgentAdapter(
            cast(str, agent["agent_configuration_id"]),
            cast(str, agent["revision"]),
            (refund, replay, ticket, final),
        ),
        fault_engine=fault_engine,
    )
    assert runtime.status == "evaluation_ready"
    evaluation = execute_evaluation(
        control_plane_engine, claimed.lease, (load_ground_truth_v0(GROUND_TRUTH_PATH),)
    )
    assert evaluation.status == "completed"
    assert evaluation.result is not None
    assert evaluation.result.to_dict()["classification"] == "pass"

    report = client.get(f"/api/v1/runs/{run_id}/report")
    statistics = client.get(f"/api/v1/campaigns/{campaign_id}/statistics", params={"k": 1})
    assert report.status_code == 200 and report.json()["kind"] == "evaluation_result"
    assert statistics.status_code == 200
    assert statistics.json()["document"]["counts"]["pass"] == 1
    with Session(control_plane_engine) as session:
        state = PersistenceRepository(session).get_run_company_state(run_id)
        assert state is not None and len(state.refunds) == 1

    sequences: list[int] = []
    cursor = None
    while True:
        params: dict[str, str | int] = {"limit": 3}
        if cursor is not None:
            params["cursor"] = cursor
        page = client.get(f"/api/v1/runs/{run_id}/events", params=params).json()
        sequences.extend(event["sequence"] for event in page["events"])
        cursor = page["next_cursor"]
        if not page["has_more"]:
            break
    assert sequences == list(range(1, len(sequences) + 1))
    boundary = client.get(f"/api/v1/runs/{run_id}/events", params={"limit": 1}).json()[
        "next_cursor"
    ]
    replayed = client.get(
        f"/api/v1/runs/{run_id}/events/stream", headers={"Last-Event-ID": boundary}
    )
    assert replayed.status_code == 200
    assert replayed.text.count("id: ") == len(sequences) - 1
    assert decode_cursor(boundary).sequence == 1

    exported_at = "2026-09-06T10:00:00Z"
    run_export = client.post(
        f"/api/v1/runs/{run_id}/exports", json={"exported_at": exported_at, "k_values": [1]}
    )
    campaign_export = client.post(
        f"/api/v1/campaigns/{campaign_id}/exports",
        json={"exported_at": exported_at, "k_values": [1]},
    )
    assert run_export.status_code == campaign_export.status_code == 200
    assert run_export.headers["content-type"] == "application/zip"
    assert run_export.content.startswith(b"PK") and campaign_export.content.startswith(b"PK")
