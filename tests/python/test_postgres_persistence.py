from __future__ import annotations

import json
import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from threading import Barrier, Event
from time import monotonic
from typing import Any, cast
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from chaosagent_evaluators import (
    CampaignValidationError,
    authenticated_campaign_plan,
    authenticated_campaign_trial,
)
from chaosagent_evidence import (
    RunEvent,
    RunReport,
    loads_run_event,
    loads_run_report,
)
from chaosagent_fixtures import Fixture, load_fixture, loads_fixture
from chaosagent_persistence import (
    CampaignMembershipConflictError,
    ClaimedRun,
    CompanyStateInitializationError,
    DuplicateEventIDError,
    EventIdentityAndSequenceConflictError,
    EventSequenceConflictError,
    FinalReportConflictError,
    IllegalRunTransitionError,
    LeaseExpiredError,
    LifecycleConflictError,
    LifecycleEvidence,
    PersistenceConflictError,
    PersistenceIntegrityError,
    PersistenceProfileError,
    PersistenceRepository,
    ReferenceNotFoundError,
    RevisionConflictError,
    RevisionReference,
    StaleLeaseError,
    create_postgres_engine,
)
from chaosagent_policies import Policy, load_policy, loads_policy
from chaosagent_scenarios import (
    Scenario,
    load_scenario,
    loads_scenario,
)
from sqlalchemy import Connection, Engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.orm import Session

pytestmark = pytest.mark.postgres

ROOT = Path(__file__).resolve().parents[2]
SCENARIO_PATH = ROOT / "benchmarks/shipment-refund/scenarios/refund-ambiguous-timeout.v0.json"
FIXTURE_PATH = ROOT / "benchmarks/shipment-refund/fixtures/failed-shipment.v0.json"
POLICY_PATH = ROOT / "benchmarks/shipment-refund/policies/refund-policy.v0.json"
EVENT_PATH = ROOT / "benchmarks/shipment-refund/evidence/v0/001-run-started.json"
REPORT_PATH = ROOT / "benchmarks/shipment-refund/evidence/v0/run-report.json"
ALEMBIC_INI = ROOT / "packages/persistence/alembic.ini"
AGENT_REFERENCE = RevisionReference(
    "example-agent-configuration",
    "unresolved-example",
    "sha256:" + "0" * 64,
)


def _require_disposable_database(database_url: str) -> None:
    if os.environ.get("CHAOSAGENT_ALLOW_DESTRUCTIVE_DATABASE_TESTS") != "1":
        raise RuntimeError(
            "set CHAOSAGENT_ALLOW_DESTRUCTIVE_DATABASE_TESTS=1 to permit migration teardown"
        )
    database_name = make_url(database_url).database
    if database_name is None or not database_name.endswith("_test"):
        raise RuntimeError("PostgreSQL integration database name must end with '_test'")


@pytest.fixture(scope="session")
def postgres_url() -> str:
    value = os.environ.get("CHAOSAGENT_TEST_DATABASE_URL")
    if value is None:
        pytest.skip("CHAOSAGENT_TEST_DATABASE_URL is not configured")
    return value


@pytest.fixture(scope="session")
def migrated_engine(postgres_url: str) -> Iterator[Engine]:
    _require_disposable_database(postgres_url)
    os.environ["CHAOSAGENT_DATABASE_URL"] = postgres_url
    configuration = Config(str(ALEMBIC_INI))
    command.downgrade(configuration, "base")
    command.upgrade(configuration, "head")
    engine = create_postgres_engine(postgres_url)
    yield engine
    engine.dispose()
    command.downgrade(configuration, "base")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex}"


def _scenario() -> Scenario:
    return load_scenario(SCENARIO_PATH)


def _fixture() -> Fixture:
    return load_fixture(FIXTURE_PATH)


def _policy() -> Policy:
    return load_policy(POLICY_PATH)


def _event(run_id: str, sequence: int, *, event_id: str | None = None) -> RunEvent:
    document = cast(dict[str, object], json.loads(EVENT_PATH.read_text(encoding="utf-8")))
    document["event_id"] = event_id or _unique("event")
    document["run_id"] = run_id
    document["sequence"] = sequence
    return loads_run_event(json.dumps(document))


def _report(run_id: str, *, report_id: str | None = None) -> RunReport:
    document = cast(dict[str, object], json.loads(REPORT_PATH.read_text(encoding="utf-8")))
    document["report_id"] = report_id or _unique("report")
    document["run_id"] = run_id
    return loads_run_report(json.dumps(document))


def _seed_run(session: Session, run_id: str) -> None:
    repository = PersistenceRepository(session)
    scenario = _scenario()
    fixture = _fixture()
    scenario_document = scenario.to_dict()
    repository.insert_fixture_revision(fixture, created_by="test-suite")
    repository.insert_policy_revision(_policy(), created_by="test-suite")
    repository.insert_scenario_revision(scenario, created_by="test-suite")
    repository.insert_agent_configuration_reference(AGENT_REFERENCE, created_by="test-suite")
    repository.create_run(
        run_id,
        scenario_id=cast(str, scenario_document["scenario_id"]),
        scenario_revision=cast(str, scenario_document["revision"]),
        agent_configuration_id=AGENT_REFERENCE.id,
        agent_configuration_revision=AGENT_REFERENCE.revision,
        created_by="test-suite",
    )


def _create_campaign_plan(
    repository: PersistenceRepository, run_id: str, campaign_id: str
) -> object:
    return repository.create_campaign_plan(
        campaign_id=campaign_id,
        arm="baseline",
        selected_fault_ids=(),
        fault_plan_digest="sha256:" + "a" * 64,
        assignments=((0, run_id),),
    )


def test_campaign_membership_is_durable_immutable_and_rollback_safe(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("campaign-member")
    campaign_id = _unique("campaign")
    with Session(migrated_engine) as session, session.begin():
        _seed_isolated_run(session, run_id)
        plan = _create_campaign_plan(PersistenceRepository(session), run_id, campaign_id)
        assert getattr(plan, "assignments") == ((0, run_id),)
    with Session(migrated_engine) as session:
        repository = PersistenceRepository(session)
        assert repository.get_campaign_plan(campaign_id) is not None
        membership = repository.get_campaign_membership(run_id)
        assert membership is not None and membership.campaign_id == campaign_id
    with migrated_engine.begin() as connection:
        with pytest.raises(DBAPIError):
            connection.execute(
                text(
                    "UPDATE public.campaign_trial_memberships "
                    "SET trial_index = 1 WHERE run_id = :run_id"
                ),
                {"run_id": run_id},
            )
    with migrated_engine.begin() as connection:
        with pytest.raises(DBAPIError):
            connection.execute(
                text("DELETE FROM public.campaign_trial_memberships WHERE run_id = :run_id"),
                {"run_id": run_id},
            )
    with migrated_engine.begin() as connection:
        with pytest.raises(DBAPIError):
            connection.execute(
                text(
                    "UPDATE public.campaign_plans SET planned_trials = 2 "
                    "WHERE campaign_id = :campaign_id"
                ),
                {"campaign_id": campaign_id},
            )
    with migrated_engine.begin() as connection:
        with pytest.raises(DBAPIError):
            connection.execute(
                text("DELETE FROM public.campaign_plans WHERE campaign_id = :campaign_id"),
                {"campaign_id": campaign_id},
            )
    with migrated_engine.connect() as connection:
        connection_transaction = connection.begin()
        try:
            connection.execute(
                text(
                    "ALTER TABLE public.campaign_trial_memberships "
                    "DISABLE TRIGGER campaign_trial_memberships_immutable"
                )
            )
            connection.execute(
                text(
                    "UPDATE public.campaign_trial_memberships "
                    "SET membership_digest = :digest WHERE run_id = :run_id"
                ),
                {"digest": "sha256:" + "f" * 64, "run_id": run_id},
            )
            connection.execute(
                text(
                    "ALTER TABLE public.campaign_trial_memberships "
                    "ENABLE TRIGGER campaign_trial_memberships_immutable"
                )
            )
            with Session(bind=connection) as session:
                with pytest.raises(PersistenceIntegrityError, match="membership digest"):
                    PersistenceRepository(session).get_campaign_plan(campaign_id)
        finally:
            connection_transaction.rollback()

    rolled_back_run = _unique("campaign-rollback-run")
    rolled_back_campaign = _unique("campaign-rollback")
    with Session(migrated_engine) as session, session.begin():
        _seed_isolated_run(session, rolled_back_run)
    with Session(migrated_engine) as session:
        session_transaction = session.begin()
        rolled_back_wrapper = authenticated_campaign_plan(
            PersistenceRepository(session),
            campaign_id=rolled_back_campaign,
            arm="baseline",
            selected_fault_ids=(),
            assignments={0: rolled_back_run},
        )
        session_transaction.rollback()
    with Session(migrated_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        assert repository.get_campaign_plan(rolled_back_campaign) is None
        assert repository.get_campaign_membership(rolled_back_run) is None
        with pytest.raises(CampaignValidationError, match="committed durable authority"):
            authenticated_campaign_trial(
                repository,
                rolled_back_wrapper,
                rolled_back_run,
                ground_truths=(),
            )
        assert _create_campaign_plan(repository, rolled_back_run, rolled_back_campaign) is not None


def test_concurrent_campaign_planners_cannot_assign_one_run_twice(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("campaign-race-run")
    campaigns = (_unique("campaign-a"), _unique("campaign-b"))
    with Session(migrated_engine) as session, session.begin():
        _seed_isolated_run(session, run_id)
    barrier = Barrier(2)

    def plan(campaign_id: str) -> str:
        with Session(migrated_engine) as session, session.begin():
            barrier.wait()
            try:
                _create_campaign_plan(PersistenceRepository(session), run_id, campaign_id)
            except CampaignMembershipConflictError:
                return "conflict"
            return "created"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(plan, campaigns))
    assert sorted(results) == ["conflict", "created"]
    with Session(migrated_engine) as session:
        membership = PersistenceRepository(session).get_campaign_membership(run_id)
        assert membership is not None and membership.campaign_id in campaigns


def test_concurrent_plans_cannot_reuse_one_campaign_index(
    migrated_engine: Engine,
) -> None:
    run_ids = (_unique("campaign-index-a"), _unique("campaign-index-b"))
    campaign_id = _unique("campaign-shared-index")
    with Session(migrated_engine) as session, session.begin():
        for run_id in run_ids:
            _seed_isolated_run(session, run_id)
    barrier = Barrier(2)

    def plan(run_id: str) -> str:
        with Session(migrated_engine) as session, session.begin():
            barrier.wait()
            try:
                _create_campaign_plan(PersistenceRepository(session), run_id, campaign_id)
            except CampaignMembershipConflictError:
                return "conflict"
            return "created"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(plan, run_ids))
    assert sorted(results) == ["conflict", "created"]
    with Session(migrated_engine) as session:
        plan_record = PersistenceRepository(session).get_campaign_plan(campaign_id)
        assert plan_record is not None
        assert len(plan_record.assignments) == 1
        assert plan_record.assignments[0][1] in run_ids


def test_campaign_planning_rejects_stale_orm_state_and_claimed_run(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("campaign-stale-run")
    with Session(migrated_engine) as session, session.begin():
        _seed_isolated_run(session, run_id)
    stale_session = Session(migrated_engine, expire_on_commit=False)
    try:
        stale_repository = PersistenceRepository(stale_session)
        stale = stale_repository.get_run(run_id)
        assert stale is not None and stale.status == "queued"
        with Session(migrated_engine) as claimant, claimant.begin():
            claimed = PersistenceRepository(claimant).claim_next_run(
                "campaign-claim-worker",
                lease_duration_seconds=60,
                evidence=_lifecycle_evidence(1),
                run_id=run_id,
            )
            assert claimed is not None
        with pytest.raises(CampaignMembershipConflictError, match="queued"):
            _create_campaign_plan(stale_repository, run_id, _unique("campaign-stale"))
        stale_session.rollback()
        with Session(migrated_engine) as session:
            assert PersistenceRepository(session).get_campaign_membership(run_id) is None
    finally:
        stale_session.close()


@pytest.mark.parametrize(
    "status",
    [
        "provisioning",
        "running",
        "evaluating",
        "completed",
        "failed",
        "timed_out",
        "cancelled",
        "infra_error",
    ],
)
def test_campaign_planning_rejects_every_nonqueued_state(
    migrated_engine: Engine, status: str
) -> None:
    run_id = _unique(f"campaign-{status}")
    with Session(migrated_engine) as session, session.begin():
        _seed_isolated_run(session, run_id)
        repository = PersistenceRepository(session)
        if status == "cancelled":
            repository.cancel_queued_run(
                run_id, expected_version=0, evidence=_lifecycle_evidence(1)
            )
        else:
            claimed = repository.claim_next_run(
                "campaign-state-worker",
                lease_duration_seconds=60,
                evidence=_lifecycle_evidence(1),
                run_id=run_id,
            )
            assert claimed is not None
            current = claimed.run
            if status != "provisioning":
                current = repository.transition_owned_run(
                    claimed.lease,
                    "running",
                    expected_version=current.lifecycle_version,
                    evidence=_lifecycle_evidence(2),
                )
            if status in {"evaluating", "completed"}:
                current = repository.transition_owned_run(
                    claimed.lease,
                    "evaluating",
                    expected_version=current.lifecycle_version,
                    evidence=_lifecycle_evidence(3),
                )
            if status == "completed":
                repository.transition_owned_run(
                    claimed.lease,
                    "completed",
                    expected_version=current.lifecycle_version,
                    evidence=_lifecycle_evidence(4),
                )
            elif status in {"failed", "timed_out", "infra_error"}:
                repository.transition_owned_run(
                    claimed.lease,
                    cast(Any, status),
                    expected_version=current.lifecycle_version,
                    evidence=_lifecycle_evidence(3),
                )
    with Session(migrated_engine) as session, session.begin():
        with pytest.raises(CampaignMembershipConflictError, match="queued"):
            _create_campaign_plan(
                PersistenceRepository(session), run_id, _unique("campaign-nonqueued")
            )


def test_campaign_planning_and_claiming_serialize_on_the_run_row(
    migrated_engine: Engine,
) -> None:
    planner_first_run = _unique("campaign-planner-first")
    with Session(migrated_engine) as session, session.begin():
        _seed_isolated_run(session, planner_first_run)
    planner_session = Session(migrated_engine)
    planner_transaction = planner_session.begin()
    try:
        _create_campaign_plan(
            PersistenceRepository(planner_session),
            planner_first_run,
            _unique("campaign-planner-wins"),
        )
        with Session(migrated_engine) as claimant, claimant.begin():
            assert (
                PersistenceRepository(claimant).claim_next_run(
                    "campaign-skipped-claim",
                    lease_duration_seconds=60,
                    evidence=_lifecycle_evidence(1),
                    run_id=planner_first_run,
                )
                is None
            )
        planner_transaction.commit()
    finally:
        planner_session.close()
    with Session(migrated_engine) as claimant, claimant.begin():
        assert (
            PersistenceRepository(claimant).claim_next_run(
                "campaign-after-plan-claim",
                lease_duration_seconds=60,
                evidence=_lifecycle_evidence(1),
                run_id=planner_first_run,
            )
            is not None
        )

    claimant_first_run = _unique("campaign-claimant-first")
    with Session(migrated_engine) as session, session.begin():
        _seed_isolated_run(session, claimant_first_run)
    claiming_session = Session(migrated_engine)
    claiming_transaction = claiming_session.begin()
    try:
        claimed = PersistenceRepository(claiming_session).claim_next_run(
            "campaign-lock-holder",
            lease_duration_seconds=60,
            evidence=_lifecycle_evidence(1),
            run_id=claimant_first_run,
        )
        assert claimed is not None
        entered = Event()
        backend: dict[str, int] = {}

        def blocked_plan() -> str:
            with Session(migrated_engine) as session, session.begin():
                backend["pid"] = cast(int, session.scalar(text("SELECT pg_backend_pid()")))
                entered.set()
                try:
                    _create_campaign_plan(
                        PersistenceRepository(session),
                        claimant_first_run,
                        _unique("campaign-claim-wins"),
                    )
                except CampaignMembershipConflictError:
                    return "conflict"
                return "created"

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(blocked_plan)
            assert entered.wait(timeout=5)
            deadline = monotonic() + 5
            waiting = False
            while monotonic() < deadline:
                with migrated_engine.connect() as observer:
                    waiting = bool(
                        observer.scalar(
                            text(
                                "SELECT wait_event_type = 'Lock' FROM pg_stat_activity "
                                "WHERE pid = :pid"
                            ),
                            {"pid": backend["pid"]},
                        )
                    )
                if waiting:
                    break
                Event().wait(0.05)
            completed_while_locked = future.done()
            claiming_transaction.commit()
            assert waiting and not completed_while_locked
            assert future.result(timeout=5) == "conflict"
    finally:
        if claiming_transaction.is_active:
            claiming_transaction.rollback()
        claiming_session.close()


def _seed_isolated_run(session: Session, run_id: str) -> None:
    repository = PersistenceRepository(session)
    fixture_document = _fixture().to_dict()
    fixture_document["fixture_id"] = _unique("fault-seed-fixture")
    fixture = loads_fixture(json.dumps(fixture_document))
    policy_document = _policy().to_dict()
    policy_document["policy_id"] = _unique("fault-seed-policy")
    policy = loads_policy(json.dumps(policy_document))
    scenario_document = _scenario().to_dict()
    scenario_document["scenario_id"] = _unique("fault-seed-scenario")
    scenario_document["fixture"] = {
        "id": fixture_document["fixture_id"],
        "revision": fixture_document["revision"],
        "digest": fixture.digest,
    }
    scenario_document["policy"] = {
        "id": policy_document["policy_id"],
        "revision": policy_document["revision"],
        "digest": policy.digest,
    }
    scenario = loads_scenario(json.dumps(scenario_document))
    agent = RevisionReference(_unique("fault-seed-agent"), "1", "sha256:" + "c" * 64)
    repository.insert_fixture_revision(fixture, created_by="fault-seed-test")
    repository.insert_policy_revision(policy, created_by="fault-seed-test")
    repository.insert_scenario_revision(scenario, created_by="fault-seed-test")
    repository.insert_agent_configuration_reference(agent, created_by="fault-seed-test")
    repository.create_run(
        run_id,
        scenario_id=cast(str, scenario_document["scenario_id"]),
        scenario_revision=cast(str, scenario_document["revision"]),
        agent_configuration_id=agent.id,
        agent_configuration_revision=agent.revision,
        created_by="fault-seed-test",
    )


def _lifecycle_evidence(sequence: int) -> LifecycleEvidence:
    return LifecycleEvidence(
        event_id=_unique(f"lifecycle-event-{sequence}"),
        producer_component="run-controller",
        producer_instance_id="test-worker",
    )


def _complete_run(
    repository: PersistenceRepository, run_id: str, worker_id: str = "test-worker"
) -> ClaimedRun:
    claimed = repository.claim_next_run(
        worker_id,
        lease_duration_seconds=60,
        evidence=_lifecycle_evidence(1),
        run_id=run_id,
    )
    assert claimed is not None
    running = repository.transition_owned_run(
        claimed.lease,
        "running",
        expected_version=claimed.run.lifecycle_version,
        evidence=_lifecycle_evidence(2),
    )
    evaluating = repository.transition_owned_run(
        claimed.lease,
        "evaluating",
        expected_version=running.lifecycle_version,
        evidence=_lifecycle_evidence(3),
    )
    completed = repository.transition_owned_run(
        claimed.lease,
        "completed",
        expected_version=evaluating.lifecycle_version,
        evidence=_lifecycle_evidence(4),
    )
    return ClaimedRun(completed, claimed.lease)


def _assert_raw_insert_rejected(
    engine: Engine, statement: str, parameters: dict[str, object]
) -> None:
    with engine.connect() as connection:
        transaction = connection.begin()
        with pytest.raises(IntegrityError):
            connection.execute(text(statement), parameters)
        transaction.rollback()


_RAW_EVENT_INSERT = """
    INSERT INTO public.run_events (
        event_id, run_id, sequence, schema_version, event_type,
        occurred_at, recorded_at, document, payload_digest
    ) VALUES (
        :event_id, :run_id, :sequence, :schema_version, :event_type,
        CAST(:occurred_at AS timestamptz), CAST(:recorded_at AS timestamptz),
        CAST(:document AS jsonb), :payload_digest
    )
"""


def _raw_event_parameters(document: dict[str, object]) -> dict[str, object]:
    return {
        "event_id": document["event_id"],
        "run_id": document["run_id"],
        "sequence": document["sequence"],
        "schema_version": "chaosagent.run-event/v0",
        "event_type": document["event_type"],
        "occurred_at": "2026-08-24T10:00:00Z",
        "recorded_at": "2026-08-24T10:00:00.01Z",
        "document": json.dumps(document),
        "payload_digest": document["payload_digest"],
    }


_RAW_REPORT_INSERT = """
    INSERT INTO public.run_reports (
        report_id, run_id, schema_version,
        scenario_id, scenario_revision, scenario_digest,
        agent_configuration_id, agent_configuration_revision,
        agent_configuration_digest, run_status, classification,
        generated_at, document
    ) VALUES (
        :report_id, :run_id, :schema_version,
        :scenario_id, :scenario_revision, :scenario_digest,
        :agent_configuration_id, :agent_configuration_revision,
        :agent_configuration_digest, :run_status, :classification,
        CAST(:generated_at AS timestamptz), CAST(:document AS jsonb)
    )
"""


def _raw_report_parameters(document: dict[str, object]) -> dict[str, object]:
    scenario = cast(dict[str, object], document["scenario"])
    agent = cast(dict[str, object], document["agent_configuration"])
    return {
        "report_id": document["report_id"],
        "run_id": document["run_id"],
        "schema_version": "chaosagent.run-report/v0",
        "scenario_id": scenario["id"],
        "scenario_revision": scenario["revision"],
        "scenario_digest": scenario["digest"],
        "agent_configuration_id": agent["id"],
        "agent_configuration_revision": agent["revision"],
        "agent_configuration_digest": agent["digest"],
        "run_status": document["run_status"],
        "classification": document["classification"],
        "generated_at": document["generated_at"],
        "document": json.dumps(document),
    }


def _execute_raw_insert(
    connection: Connection, statement: str, parameters: dict[str, object]
) -> None:
    connection.execute(text(statement), parameters)


def _wait_for_database_block(engine: Engine, application_name: str) -> None:
    """Wait until PostgreSQL proves the named test session is lock-blocked."""
    deadline = monotonic() + 10
    while monotonic() < deadline:
        with engine.connect() as connection:
            blocked = connection.scalar(
                text(
                    "SELECT EXISTS ("
                    "SELECT 1 FROM pg_stat_activity "
                    "WHERE application_name = :application_name "
                    "AND cardinality(pg_blocking_pids(pid)) > 0)"
                ),
                {"application_name": application_name},
            )
        if blocked:
            return
        Event().wait(0.01)
    raise AssertionError(f"session {application_name!r} never became lock-blocked")


def test_migration_up_down_and_model_metadata_match(migrated_engine: Engine) -> None:
    configuration = Config(str(ALEMBIC_INI))
    command.downgrade(configuration, "base")
    assert "scenario_revisions" not in inspect(migrated_engine).get_table_names(schema="public")
    with migrated_engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT to_regprocedure('public.chaosagent_reject_immutable_change()')")
            )
            is None
        )
    command.upgrade(configuration, "head")
    assert {
        "agent_configuration_revisions",
        "approval_requests",
        "approval_resolutions",
        "company_customers",
        "company_effects",
        "company_orders",
        "company_payments",
        "company_refunds",
        "company_shipments",
        "company_support_tickets",
        "fixture_revisions",
        "policy_revisions",
        "post_commit_acknowledgements",
        "run_company_state",
        "run_events",
        "run_reports",
        "runs",
        "scenario_revisions",
    }.issubset(inspect(migrated_engine).get_table_names(schema="public"))
    with migrated_engine.connect() as connection:
        assert connection.scalar(
            text("SELECT to_regprocedure('public.chaosagent_reject_immutable_change()')")
        )
    command.check(configuration)


def test_lifecycle_migration_round_trips_to_issue_5(migrated_engine: Engine) -> None:
    configuration = Config(str(ALEMBIC_INI))
    command.downgrade(configuration, "0001_persistence_v0")
    issue_5_columns = {
        column["name"] for column in inspect(migrated_engine).get_columns("runs", schema="public")
    }
    assert "lifecycle_version" not in issue_5_columns
    assert "lease_token" not in issue_5_columns
    legacy_statuses = (
        "queued",
        "provisioning",
        "running",
        "evaluating",
        "completed",
        "failed",
        "timed_out",
        "cancelled",
        "infra_error",
    )
    terminal_statuses = ("completed", "failed", "timed_out", "cancelled", "infra_error")
    unreported_runs = {status: _unique(f"legacy-unreported-{status}") for status in legacy_statuses}
    reported_runs = {status: _unique(f"legacy-reported-{status}") for status in terminal_statuses}
    scenario_id = _unique("pre-issue6-scenario")
    digest = "sha256:" + "1" * 64
    scenario_document = {
        "scenario_id": scenario_id,
        "revision": "1",
        "schema_version": "chaosagent.scenario/v0",
    }
    with migrated_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO public.scenario_revisions "
                "(scenario_id, revision, schema_version, canonical_document, "
                "canonical_digest, created_by) VALUES "
                "(:scenario_id, '1', 'chaosagent.scenario/v0', CAST(:document AS jsonb), "
                ":digest, 'migration-test')"
            ),
            {
                "scenario_id": scenario_id,
                "document": json.dumps(scenario_document),
                "digest": digest,
            },
        )
        connection.execute(
            text(
                "INSERT INTO public.agent_configuration_revisions "
                "(agent_configuration_id, revision, digest, created_by) VALUES "
                "(:id, :revision, :digest, 'migration-test')"
            ),
            {
                "id": AGENT_REFERENCE.id,
                "revision": AGENT_REFERENCE.revision,
                "digest": AGENT_REFERENCE.digest,
            },
        )
        legacy_rows = list(unreported_runs.items()) + [
            ("queued", run_id) for run_id in reported_runs.values()
        ]
        for status, run_id in legacy_rows:
            connection.execute(
                text(
                    "INSERT INTO public.runs "
                    "(run_id, scenario_id, scenario_revision, scenario_digest, "
                    "agent_configuration_id, agent_configuration_revision, "
                    "agent_configuration_digest, status, created_by) VALUES "
                    "(:run_id, :scenario_id, '1', :digest, :agent_id, :agent_revision, "
                    ":agent_digest, :status, 'migration-test')"
                ),
                {
                    "run_id": run_id,
                    "scenario_id": scenario_id,
                    "digest": digest,
                    "agent_id": AGENT_REFERENCE.id,
                    "agent_revision": AGENT_REFERENCE.revision,
                    "agent_digest": AGENT_REFERENCE.digest,
                    "status": status,
                },
            )
        for terminal_status, reported_run_id in reported_runs.items():
            report_document: dict[str, object] = {
                "report_id": _unique(f"legacy-report-{terminal_status}"),
                "run_id": reported_run_id,
                "schema_version": "chaosagent.run-report/v0",
                "scenario": {"id": scenario_id, "revision": "1", "digest": digest},
                "agent_configuration": {
                    "id": AGENT_REFERENCE.id,
                    "revision": AGENT_REFERENCE.revision,
                    "digest": AGENT_REFERENCE.digest,
                },
                "run_status": terminal_status,
                "classification": "not_evaluated",
                "generated_at": "2026-08-24T10:00:00Z",
            }
            connection.execute(
                text(_RAW_REPORT_INSERT),
                _raw_report_parameters(report_document),
            )
    command.upgrade(configuration, "head")
    issue_6_columns = {
        column["name"] for column in inspect(migrated_engine).get_columns("runs", schema="public")
    }
    assert {
        "lifecycle_version",
        "lease_owner",
        "lease_token",
        "lease_expires_at",
        "heartbeat_at",
        "attempt",
    }.issubset(issue_6_columns)
    with migrated_engine.connect() as connection:
        for run_id in unreported_runs.values():
            assert (
                connection.scalar(
                    text("SELECT status FROM public.runs WHERE run_id = :run_id"),
                    {"run_id": run_id},
                )
                == "queued"
            )
        for expected_status, run_id in reported_runs.items():
            assert (
                connection.scalar(
                    text("SELECT status FROM public.runs WHERE run_id = :run_id"),
                    {"run_id": run_id},
                )
                == expected_status
            )
    command.check(configuration)


def test_fixture_migration_round_trips_to_issue_6(migrated_engine: Engine) -> None:
    configuration = Config(str(ALEMBIC_INI))
    command.downgrade(configuration, "0002_run_lifecycle_leases")
    tables = set(inspect(migrated_engine).get_table_names(schema="public"))
    assert "fixture_revisions" not in tables
    assert "run_company_state" not in tables
    run_columns = {
        column["name"] for column in inspect(migrated_engine).get_columns("runs", schema="public")
    }
    assert "fixture_id" not in run_columns
    command.upgrade(configuration, "head")
    tables = set(inspect(migrated_engine).get_table_names(schema="public"))
    assert {
        "fixture_revisions",
        "run_company_state",
        "company_customers",
        "company_orders",
        "company_shipments",
        "company_payments",
        "company_refunds",
        "company_support_tickets",
    }.issubset(tables)
    command.check(configuration)


def test_mutation_ledger_migration_round_trips_to_issue_8(migrated_engine: Engine) -> None:
    configuration = Config(str(ALEMBIC_INI))
    command.downgrade(configuration, "0003_fixture_company_state")
    inspector = inspect(migrated_engine)
    assert "company_effects" not in inspector.get_table_names(schema="public")
    assert "effect_id" not in {
        column["name"] for column in inspector.get_columns("company_refunds", schema="public")
    }
    assert "origin" not in {
        column["name"] for column in inspector.get_columns("company_refunds", schema="public")
    }
    assert "last_effect_id" not in {
        column["name"]
        for column in inspector.get_columns("company_support_tickets", schema="public")
    }
    command.upgrade(configuration, "head")
    inspector = inspect(migrated_engine)
    assert "company_effects" in inspector.get_table_names(schema="public")
    assert {"effect_id", "origin"}.issubset(
        {column["name"] for column in inspector.get_columns("company_refunds", schema="public")}
    )
    assert "ix_company_refunds_run_payment_succeeded" in {
        index["name"] for index in inspector.get_indexes("company_refunds", schema="public")
    }
    assert "last_effect_id" in {
        column["name"]
        for column in inspector.get_columns("company_support_tickets", schema="public")
    }
    command.check(configuration)


def test_policy_approval_migration_round_trips_to_issue_9(migrated_engine: Engine) -> None:
    configuration = Config(str(ALEMBIC_INI))
    command.downgrade(configuration, "0004_mutation_effect_ledger")
    tables = set(inspect(migrated_engine).get_table_names(schema="public"))
    assert {"policy_revisions", "approval_requests", "approval_resolutions"}.isdisjoint(tables)
    command.upgrade(configuration, "head")
    tables = set(inspect(migrated_engine).get_table_names(schema="public"))
    assert {"policy_revisions", "approval_requests", "approval_resolutions"}.issubset(tables)
    command.check(configuration)


def test_fault_seed_migration_round_trip_is_honest(migrated_engine: Engine) -> None:
    run_id = _unique("fault-seed-migration")
    with Session(migrated_engine) as session, session.begin():
        _seed_isolated_run(session, run_id)
        repository = PersistenceRepository(session)
        claimed = repository.claim_next_run(
            "fault-seed-migration-worker",
            lease_duration_seconds=60,
            evidence=_lifecycle_evidence(1),
            run_id=run_id,
        )
        assert claimed is not None
        assert repository.bind_run_fault_seed(claimed.lease, 1616).fault_seed == 1616

    configuration = Config(str(ALEMBIC_INI))
    command.downgrade(configuration, "0008_post_commit_ack")
    columns = {
        column["name"] for column in inspect(migrated_engine).get_columns("runs", schema="public")
    }
    assert "fault_seed" not in columns
    with migrated_engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT to_regprocedure('public.chaosagent_freeze_run_fault_seed()')")
            )
            is None
        )
    command.upgrade(configuration, "head")
    with migrated_engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT fault_seed FROM public.runs WHERE run_id = :run_id"),
                {"run_id": run_id},
            )
            is None
        )
    command.check(configuration)


def test_campaign_membership_migration_downgrade_is_honest(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("campaign-migration-run")
    campaign_id = _unique("campaign-migration")
    with Session(migrated_engine) as session, session.begin():
        _seed_isolated_run(session, run_id)
        _create_campaign_plan(PersistenceRepository(session), run_id, campaign_id)
    configuration = Config(str(ALEMBIC_INI))
    command.downgrade(configuration, "0009_run_fault_seed")
    inspector = inspect(migrated_engine)
    assert {
        "campaign_plans",
        "campaign_trial_memberships",
    }.isdisjoint(inspector.get_table_names(schema="public"))
    columns = {column["name"] for column in inspector.get_columns("runs", schema="public")}
    assert "fault_plan_digest" not in columns
    with migrated_engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT count(*) FROM public.runs WHERE run_id = :run_id"),
                {"run_id": run_id},
            )
            == 1
        )
    command.upgrade(configuration, "head")
    with migrated_engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM public.campaign_plans")) == 0
        assert (
            connection.scalar(
                text("SELECT fault_plan_digest FROM public.runs WHERE run_id = :run_id"),
                {"run_id": run_id},
            )
            is None
        )
    command.check(configuration)


def test_run_fault_seed_binds_once_and_database_rejects_rewrite(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("fault-seed-binding")
    with Session(migrated_engine) as session, session.begin():
        _seed_isolated_run(session, run_id)
        repository = PersistenceRepository(session)
        claimed = repository.claim_next_run(
            "fault-seed-worker",
            lease_duration_seconds=60,
            evidence=_lifecycle_evidence(1),
            run_id=run_id,
        )
        assert claimed is not None
        first = repository.bind_run_fault_seed(claimed.lease, 2026)
        repeated = repository.bind_run_fault_seed(claimed.lease, 2026)
        assert first.fault_seed == repeated.fault_seed == 2026
        with pytest.raises(PersistenceIntegrityError, match="differs"):
            repository.bind_run_fault_seed(claimed.lease, 2027)

    with migrated_engine.connect() as connection:
        transaction = connection.begin()
        with pytest.raises(DBAPIError, match="immutable"):
            connection.execute(
                text("UPDATE public.runs SET fault_seed = 2027 WHERE run_id = :run_id"),
                {"run_id": run_id},
            )
        transaction.rollback()


def test_insert_fetch_fixture_revision_conflict_and_database_immutability(
    migrated_engine: Engine,
) -> None:
    fixture = _fixture()
    document = fixture.to_dict()
    cast(dict[str, object], document["metadata"])["title"] = "Conflicting fixture title"
    conflicting = loads_fixture(json.dumps(document))
    with Session(migrated_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        inserted = repository.insert_fixture_revision(fixture, created_by="fixture-author")
        repeated = repository.insert_fixture_revision(fixture, created_by="ignored")
        fetched = repository.get_fixture_revision(
            cast(str, fixture.to_dict()["fixture_id"]), cast(str, fixture.to_dict()["revision"])
        )
        assert inserted.fixture.digest == repeated.fixture.digest
        assert repeated.created_by == "fixture-author"
        assert fetched is not None
        assert fetched.fixture.canonical_bytes == fixture.canonical_bytes
        with pytest.raises(RevisionConflictError):
            repository.insert_fixture_revision(conflicting, created_by="other")

    with migrated_engine.connect() as connection:
        transaction = connection.begin()
        with pytest.raises(DBAPIError, match="append-only"):
            connection.execute(
                text(
                    "UPDATE public.fixture_revisions SET created_by = 'mutated' "
                    "WHERE fixture_id = :fixture_id AND revision = :revision"
                ),
                {
                    "fixture_id": fixture.to_dict()["fixture_id"],
                    "revision": fixture.to_dict()["revision"],
                },
            )
        transaction.rollback()


def test_run_requires_and_freezes_exact_scenario_fixture_reference(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("fixture-bound-run")
    fixture_document = _fixture().to_dict()
    fixture_document["fixture_id"] = _unique("fixture-binding")
    fixture = loads_fixture(json.dumps(fixture_document))
    scenario_document = _scenario().to_dict()
    scenario_document["scenario_id"] = _unique("fixture-binding-scenario")
    scenario_document["fixture"] = {
        "id": fixture.to_dict()["fixture_id"],
        "revision": fixture.to_dict()["revision"],
        "digest": fixture.digest,
    }
    scenario = loads_scenario(json.dumps(scenario_document))
    scenario_document = scenario.to_dict()
    with Session(migrated_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        repository.insert_scenario_revision(scenario, created_by="test-suite")
        repository.insert_policy_revision(_policy(), created_by="test-suite")
        repository.insert_agent_configuration_reference(AGENT_REFERENCE, created_by="test-suite")
        with pytest.raises(ReferenceNotFoundError, match="fixture reference"):
            repository.create_run(
                run_id,
                scenario_id=cast(str, scenario_document["scenario_id"]),
                scenario_revision=cast(str, scenario_document["revision"]),
                agent_configuration_id=AGENT_REFERENCE.id,
                agent_configuration_revision=AGENT_REFERENCE.revision,
                created_by="test-suite",
            )
        inserted_fixture = repository.insert_fixture_revision(fixture, created_by="test-suite")
        run = repository.create_run(
            run_id,
            scenario_id=cast(str, scenario_document["scenario_id"]),
            scenario_revision=cast(str, scenario_document["revision"]),
            agent_configuration_id=AGENT_REFERENCE.id,
            agent_configuration_revision=AGENT_REFERENCE.revision,
            created_by="test-suite",
        )
        assert run.fixture is not None
        assert run.fixture.digest == inserted_fixture.fixture.digest


def test_run_creation_requires_exact_valid_policy_revision(migrated_engine: Engine) -> None:
    fixture = _fixture()
    base_policy = _policy()

    def scenario_for(policy_reference: dict[str, object]) -> Scenario:
        document = _scenario().to_dict()
        document["scenario_id"] = _unique("policy-bound-scenario")
        document["policy"] = policy_reference
        return loads_scenario(json.dumps(document))

    missing_policy_id = _unique("missing-policy")
    missing = scenario_for(
        {"id": missing_policy_id, "revision": "1", "digest": "sha256:" + "8" * 64}
    )
    mismatch = scenario_for(
        {
            "id": base_policy.to_dict()["policy_id"],
            "revision": base_policy.to_dict()["revision"],
            "digest": "sha256:" + "9" * 64,
        }
    )
    with Session(migrated_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        repository.insert_fixture_revision(fixture, created_by="policy-run-test")
        repository.insert_policy_revision(base_policy, created_by="policy-run-test")
        repository.insert_agent_configuration_reference(
            AGENT_REFERENCE, created_by="policy-run-test"
        )
        for scenario in (missing, mismatch):
            repository.insert_scenario_revision(scenario, created_by="policy-run-test")
            document = scenario.to_dict()
            with pytest.raises(ReferenceNotFoundError, match="policy reference"):
                repository.create_run(
                    _unique("invalid-policy-run"),
                    scenario_id=cast(str, document["scenario_id"]),
                    scenario_revision=cast(str, document["revision"]),
                    agent_configuration_id=AGENT_REFERENCE.id,
                    agent_configuration_revision=AGENT_REFERENCE.revision,
                    created_by="policy-run-test",
                )

    corrupt_document = base_policy.to_dict()
    corrupt_document["policy_id"] = _unique("corrupt-policy")
    corrupt_policy = loads_policy(json.dumps(corrupt_document))
    corrupt_scenario = scenario_for(
        {
            "id": corrupt_document["policy_id"],
            "revision": corrupt_document["revision"],
            "digest": corrupt_policy.digest,
        }
    )
    with Session(migrated_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        repository.insert_policy_revision(corrupt_policy, created_by="policy-run-test")
        repository.insert_scenario_revision(corrupt_scenario, created_by="policy-run-test")
    with migrated_engine.begin() as connection:
        connection.execute(
            text("ALTER TABLE public.policy_revisions DISABLE TRIGGER policy_revisions_immutable")
        )
        connection.execute(
            text(
                "UPDATE public.policy_revisions SET canonical_document = "
                "canonical_document - 'tools' WHERE policy_id = :policy_id AND revision = '1'"
            ),
            {"policy_id": corrupt_document["policy_id"]},
        )
        connection.execute(
            text("ALTER TABLE public.policy_revisions ENABLE TRIGGER policy_revisions_immutable")
        )
    with Session(migrated_engine) as session, session.begin():
        document = corrupt_scenario.to_dict()
        with pytest.raises(PersistenceIntegrityError, match="stored policy document"):
            PersistenceRepository(session).create_run(
                _unique("corrupt-policy-run"),
                scenario_id=cast(str, document["scenario_id"]),
                scenario_revision=cast(str, document["revision"]),
                agent_configuration_id=AGENT_REFERENCE.id,
                agent_configuration_revision=AGENT_REFERENCE.revision,
                created_by="policy-run-test",
            )


def test_run_company_initialization_is_deterministic_idempotent_and_isolated(
    migrated_engine: Engine,
) -> None:
    run_a = _unique("company-run-a")
    run_b = _unique("company-run-b")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_a)
        _seed_run(session, run_b)
        repository = PersistenceRepository(session)
        state_a = repository.initialize_run_company_state(run_a)
        state_b = repository.initialize_run_company_state(run_b)
        repeated_a = repository.initialize_run_company_state(run_a)
        assert state_a.fixture == state_b.fixture
        assert state_a.customers == state_b.customers
        assert state_a.orders == state_b.orders
        assert state_a.shipments == state_b.shipments
        assert state_a.payments == state_b.payments
        assert state_a.refunds == state_b.refunds == ()
        assert state_a.support_tickets == state_b.support_tickets
        assert repeated_a.reference_time == state_a.reference_time == state_b.reference_time

    with migrated_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE public.company_support_tickets SET status = 'closed', note = 'Run A only' "
                "WHERE run_id = :run_id AND ticket_id = 'TKT-204'"
            ),
            {"run_id": run_a},
        )
    with Session(migrated_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        repeated_a = repository.initialize_run_company_state(run_a)
        unchanged_b = repository.get_run_company_state(run_b)
        assert repeated_a.support_tickets[0].status == "closed"
        assert repeated_a.support_tickets[0].note == "Run A only"
        assert unchanged_b is not None
        assert unchanged_b.support_tickets[0].status == "open"
        assert unchanged_b.support_tickets[0].note != "Run A only"


def test_company_initialization_rejects_started_run_and_participates_in_rollback(
    migrated_engine: Engine,
) -> None:
    started_run = _unique("started-company-run")
    rollback_run = _unique("rollback-company-run")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, started_run)
        claimed = PersistenceRepository(session).claim_next_run(
            "fixture-worker",
            lease_duration_seconds=60,
            evidence=_lifecycle_evidence(1),
            run_id=started_run,
        )
        assert claimed is not None
    with Session(migrated_engine) as session, session.begin():
        with pytest.raises(CompanyStateInitializationError, match="before its first claim"):
            PersistenceRepository(session).initialize_run_company_state(started_run)

    with Session(migrated_engine) as session:
        transaction = session.begin()
        _seed_run(session, rollback_run)
        PersistenceRepository(session).initialize_run_company_state(rollback_run)
        transaction.rollback()
    with Session(migrated_engine) as session:
        assert PersistenceRepository(session).get_run(rollback_run) is None
        assert PersistenceRepository(session).get_run_company_state(rollback_run) is None


def test_concurrent_company_initialization_is_idempotent(migrated_engine: Engine) -> None:
    run_id = _unique("concurrent-company-init")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
    barrier = Barrier(2)

    def initialize() -> tuple[str, int]:
        with Session(migrated_engine) as session, session.begin():
            barrier.wait(timeout=10)
            state = PersistenceRepository(session).initialize_run_company_state(run_id)
            return state.fixture.digest, len(state.orders)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: initialize(), range(2)))
    assert results == [(_fixture().digest, 1), (_fixture().digest, 1)]
    with migrated_engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT count(*) FROM public.run_company_state WHERE run_id = :run_id"),
                {"run_id": run_id},
            )
            == 1
        )


def test_insert_fetch_scenario_revision_and_idempotent_reinsert(
    migrated_engine: Engine,
) -> None:
    document = _scenario().to_dict()
    document["scenario_id"] = _unique("idempotent-scenario")
    scenario = loads_scenario(json.dumps(document))
    with Session(migrated_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        inserted = repository.insert_scenario_revision(scenario, created_by="author-1")
        repeated = repository.insert_scenario_revision(scenario, created_by="ignored-author")
        fetched = repository.get_scenario_revision(
            cast(str, scenario.to_dict()["scenario_id"]),
            cast(str, scenario.to_dict()["revision"]),
        )
        assert inserted.scenario.digest == scenario.digest
        assert repeated.created_by == "author-1"
        assert fetched is not None
        assert fetched.scenario.canonical_bytes == scenario.canonical_bytes


def test_scenario_revision_rejects_content_and_digest_conflict(
    migrated_engine: Engine,
) -> None:
    original = _scenario()
    changed_document = original.to_dict()
    cast(dict[str, object], changed_document["metadata"])["title"] = "Different title"
    changed = loads_scenario(json.dumps(changed_document))
    with Session(migrated_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        repository.insert_scenario_revision(original, created_by="author-1")
        with pytest.raises(RevisionConflictError):
            repository.insert_scenario_revision(changed, created_by="author-2")


def test_policy_revision_insert_fetch_conflict_and_database_immutability(
    migrated_engine: Engine,
) -> None:
    original_document = _policy().to_dict()
    original_document["policy_id"] = _unique("policy")
    original = loads_policy(json.dumps(original_document))
    changed_document = original.to_dict()
    cast(dict[str, object], changed_document["metadata"])["title"] = "Changed policy"
    changed = loads_policy(json.dumps(changed_document))
    with Session(migrated_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        inserted = repository.insert_policy_revision(original, created_by="policy-author")
        repeated = repository.insert_policy_revision(original, created_by="ignored")
        fetched = repository.get_policy_revision(
            cast(str, original.to_dict()["policy_id"]),
            cast(str, original.to_dict()["revision"]),
        )
        assert inserted.policy.digest == original.digest
        assert repeated.created_by == "policy-author"
        assert fetched is not None and fetched.policy.canonical_bytes == original.canonical_bytes
        with pytest.raises(RevisionConflictError):
            repository.insert_policy_revision(changed, created_by="other")
    with migrated_engine.connect() as connection:
        transaction = connection.begin()
        with pytest.raises(DBAPIError, match="immutable"):
            connection.execute(
                text(
                    "UPDATE public.policy_revisions SET created_by = 'attacker' "
                    "WHERE policy_id = :policy_id AND revision = :revision"
                ),
                {
                    "policy_id": original.to_dict()["policy_id"],
                    "revision": original.to_dict()["revision"],
                },
            )
        transaction.rollback()


def test_policy_projection_constraints_fail_closed(migrated_engine: Engine) -> None:
    statement = """
        INSERT INTO public.policy_revisions (
            policy_id, revision, schema_version, canonical_document,
            canonical_digest, created_by
        ) VALUES (
            :policy_id, '1', 'chaosagent.policy/v0', CAST(:document AS jsonb),
            :digest, 'raw-test'
        )
    """
    policy_id = _unique("raw-policy")
    for document in (
        {},
        {"policy_id": None, "revision": "1", "schema_version": "chaosagent.policy/v0"},
        {"policy_id": policy_id, "revision": "1", "schema_version": "chaosagent.policy/v1"},
    ):
        _assert_raw_insert_rejected(
            migrated_engine,
            statement,
            {
                "policy_id": policy_id,
                "document": json.dumps(document),
                "digest": "sha256:" + "7" * 64,
            },
        )


def test_raw_approval_cannot_bind_another_valid_scenario_to_run(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("approval-run")
    alternate_document = _scenario().to_dict()
    alternate_document["scenario_id"] = _unique("alternate-scenario")
    alternate = loads_scenario(json.dumps(alternate_document))
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
        PersistenceRepository(session).insert_scenario_revision(
            alternate, created_by="raw-approval-test"
        )
    policy = _policy()
    policy_document = policy.to_dict()
    with migrated_engine.begin() as connection, pytest.raises(IntegrityError):
        connection.execute(
            text(
                "INSERT INTO public.approval_requests ("
                "approval_id, run_id, scenario_id, scenario_revision, scenario_digest, "
                "policy_id, policy_revision, policy_digest, tool_id, contract_version, "
                "request_digest, idempotency_key_digest, arguments_document, logical_call_id, "
                "requested_attempt_id, lease_attempt, decision_id, decision_event_id, "
                "request_event_id) VALUES ("
                ":approval_id, :run_id, :scenario_id, :scenario_revision, :scenario_digest, "
                ":policy_id, :policy_revision, :policy_digest, 'payments.refund', "
                "'chaosagent.tool/payments.refund/v0', :request_digest, :key_digest, "
                "CAST(:arguments AS jsonb), 'logical-raw', 'attempt-raw', 1, "
                "'decision-raw', 'evt-decision-raw', 'evt-request-raw')"
            ),
            {
                "approval_id": _unique("approval"),
                "run_id": run_id,
                "scenario_id": alternate_document["scenario_id"],
                "scenario_revision": alternate_document["revision"],
                "scenario_digest": alternate.digest,
                "policy_id": policy_document["policy_id"],
                "policy_revision": policy_document["revision"],
                "policy_digest": policy.digest,
                "request_digest": "sha256:" + "1" * 64,
                "key_digest": "sha256:" + "2" * 64,
                "arguments": json.dumps(
                    {
                        "order_id": "ORD-1007",
                        "payment_id": "PAY-1007",
                        "amount_minor": 6000,
                        "reason": "raw",
                        "idempotency_key": "raw-key",
                    }
                ),
            },
        )


def test_scenario_jsonb_persistence_profile_rejects_u0000(migrated_engine: Engine) -> None:
    document = _scenario().to_dict()
    scenario_id = _unique("nul-scenario")
    document["scenario_id"] = scenario_id
    cast(dict[str, object], document["metadata"])["title"] = "Cannot\u0000persist"
    scenario = loads_scenario(json.dumps(document))
    with Session(migrated_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        with pytest.raises(PersistenceProfileError, match=r"U\+0000"):
            repository.insert_scenario_revision(scenario, created_by="author-1")
        assert repository.get_scenario_revision(scenario_id, "1") is None


def test_scenario_projection_constraints_reject_missing_null_and_mismatch(
    migrated_engine: Engine,
) -> None:
    scenario_id = _unique("raw-scenario")
    documents: list[dict[str, object]] = [
        {},
        {
            "scenario_id": None,
            "revision": "1",
            "schema_version": "chaosagent.scenario/v0",
        },
        {
            "scenario_id": scenario_id,
            "revision": "1",
            "schema_version": "chaosagent.scenario/v1",
        },
    ]
    statement = """
        INSERT INTO public.scenario_revisions (
            scenario_id, revision, schema_version, canonical_document,
            canonical_digest, created_by
        ) VALUES (
            :scenario_id, '1', 'chaosagent.scenario/v0',
            CAST(:document AS jsonb), :digest, 'raw-test'
        )
    """
    for document in documents:
        _assert_raw_insert_rejected(
            migrated_engine,
            statement,
            {
                "scenario_id": scenario_id,
                "document": json.dumps(document),
                "digest": "sha256:" + "1" * 64,
            },
        )


def test_scenario_revision_is_database_immutable(migrated_engine: Engine) -> None:
    scenario = _scenario()
    with Session(migrated_engine) as session, session.begin():
        PersistenceRepository(session).insert_scenario_revision(scenario, created_by="author-1")
    with migrated_engine.connect() as connection:
        transaction = connection.begin()
        with pytest.raises(DBAPIError, match="immutable"):
            connection.execute(
                text(
                    "UPDATE public.scenario_revisions SET created_by = 'attacker' "
                    "WHERE scenario_id = :scenario_id AND revision = :revision"
                ),
                {
                    "scenario_id": scenario.to_dict()["scenario_id"],
                    "revision": scenario.to_dict()["revision"],
                },
            )
        transaction.rollback()


def test_create_and_fetch_run_with_frozen_references(migrated_engine: Engine) -> None:
    run_id = _unique("run")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
        fetched = PersistenceRepository(session).get_run(run_id)
        assert fetched is not None
        assert fetched.run_id == run_id
        assert fetched.status == "queued"
        assert fetched.scenario.digest == _scenario().digest
        assert fetched.agent_configuration == AGENT_REFERENCE
        with pytest.raises(PersistenceConflictError):
            _seed_run(session, run_id)


def test_append_fetch_order_and_duplicate_constraints(migrated_engine: Engine) -> None:
    run_id = _unique("run")
    shared_event_id = _unique("event")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
        repository = PersistenceRepository(session)
        repository.append_event(_event(run_id, 3))
        repository.append_event(_event(run_id, 1, event_id=shared_event_id))
        repository.append_event(_event(run_id, 2))
        assert [
            record.event.to_dict()["sequence"] for record in repository.fetch_events(run_id)
        ] == [
            1,
            2,
            3,
        ]
        with pytest.raises(DuplicateEventIDError):
            repository.append_event(_event(run_id, 4, event_id=shared_event_id))
        with pytest.raises(EventSequenceConflictError):
            repository.append_event(_event(run_id, 2))
        with pytest.raises(EventIdentityAndSequenceConflictError):
            repository.append_event(_event(run_id, 1, event_id=shared_event_id))


def test_event_projection_constraints_reject_missing_schema_and_timestamp_mismatch(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("raw-event-run")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)

    mutations = ("missing_event_id", "schema_mismatch", "timestamp_mismatch")
    for sequence, mutation in enumerate(mutations, start=100):
        document = _event(run_id, sequence).to_dict()
        parameters = _raw_event_parameters(document)
        if mutation == "missing_event_id":
            del document["event_id"]
        elif mutation == "schema_mismatch":
            document["schema_version"] = "chaosagent.run-event/v1"
        else:
            document["occurred_at"] = "2026-08-24T10:00:01Z"
        parameters["document"] = json.dumps(document)
        _assert_raw_insert_rejected(migrated_engine, _RAW_EVENT_INSERT, parameters)


def test_event_is_immutable_in_api_and_database(migrated_engine: Engine) -> None:
    run_id = _unique("run")
    event = _event(run_id, 1)
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
        record = PersistenceRepository(session).append_event(event)
        with pytest.raises(FrozenInstanceError):
            setattr(record, "inserted_at", record.inserted_at)
    with migrated_engine.connect() as connection:
        transaction = connection.begin()
        with pytest.raises(DBAPIError, match="append-only"):
            connection.execute(
                text("DELETE FROM public.run_events WHERE event_id = :event_id"),
                {"event_id": event.to_dict()["event_id"]},
            )
        transaction.rollback()


def test_final_report_is_one_immutable_document_per_run(migrated_engine: Engine) -> None:
    run_id = _unique("run")
    report = _report(run_id)
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
        repository = PersistenceRepository(session)
        _complete_run(repository, run_id)
        inserted = repository.store_final_report(report)
        repeated = repository.store_final_report(report)
        fetched = repository.get_final_report(run_id)
        assert inserted.report.canonical_bytes == repeated.report.canonical_bytes
        assert fetched is not None
        assert fetched.report.canonical_bytes == report.canonical_bytes
        run = repository.get_run(run_id)
        assert run is not None
        assert run.status == "completed"
        assert fetched.report.to_dict()["run_status"] == "completed"
        with pytest.raises(FinalReportConflictError):
            repository.store_final_report(_report(run_id))


def test_report_projection_constraints_reject_schema_and_nested_reference_mismatch(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("raw-report-run")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
        _complete_run(PersistenceRepository(session), run_id)

    mutations = ("missing_scenario_id", "schema_mismatch", "agent_digest_mismatch")
    for mutation in mutations:
        document = _report(run_id).to_dict()
        parameters = _raw_report_parameters(document)
        if mutation == "missing_scenario_id":
            del cast(dict[str, object], document["scenario"])["id"]
        elif mutation == "schema_mismatch":
            document["schema_version"] = "chaosagent.run-report/v1"
        else:
            cast(dict[str, object], document["agent_configuration"])["digest"] = (
                "sha256:" + "f" * 64
            )
        parameters["document"] = json.dumps(document)
        _assert_raw_insert_rejected(migrated_engine, _RAW_REPORT_INSERT, parameters)


def test_corrupted_event_and_report_reads_raise_persistence_integrity(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("corrupt-read-run")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
        _complete_run(PersistenceRepository(session), run_id)

    event_document = _event(run_id, 5).to_dict()
    del event_document["producer"]
    report_document = _report(run_id).to_dict()
    del report_document["critical_gates"]
    with migrated_engine.begin() as connection:
        _execute_raw_insert(
            connection,
            _RAW_EVENT_INSERT,
            _raw_event_parameters(event_document),
        )
        _execute_raw_insert(
            connection,
            _RAW_REPORT_INSERT,
            _raw_report_parameters(report_document),
        )

    with Session(migrated_engine) as session:
        repository = PersistenceRepository(session)
        with pytest.raises(PersistenceIntegrityError, match="event document"):
            repository.fetch_events(run_id)
        with pytest.raises(PersistenceIntegrityError, match="report document"):
            repository.get_final_report(run_id)


def test_repository_participates_in_caller_rollback(migrated_engine: Engine) -> None:
    run_id = _unique("rollback-run")
    with Session(migrated_engine) as session:
        transaction = session.begin()
        _seed_run(session, run_id)
        transaction.rollback()
    with Session(migrated_engine) as session:
        assert PersistenceRepository(session).get_run(run_id) is None


def test_concurrent_same_sequence_has_one_winner(migrated_engine: Engine) -> None:
    run_id = _unique("concurrent-run")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)

    barrier = Barrier(2)

    def append(event: RunEvent) -> str:
        try:
            with Session(migrated_engine) as session, session.begin():
                barrier.wait()
                PersistenceRepository(session).append_event(event)
            return "inserted"
        except EventSequenceConflictError:
            return "sequence_conflict"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(append, [_event(run_id, 1), _event(run_id, 1)]))
    assert sorted(outcomes) == ["inserted", "sequence_conflict"]
    with Session(migrated_engine) as session:
        assert len(PersistenceRepository(session).fetch_events(run_id)) == 1


def test_legal_lifecycle_path_versions_and_terminal_claim_rejection(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("lifecycle-run")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
        repository = PersistenceRepository(session)
        claimed = repository.claim_next_run(
            "worker-a",
            lease_duration_seconds=60,
            evidence=_lifecycle_evidence(1),
            run_id=run_id,
        )
        assert claimed is not None
        assert claimed.run.status == "provisioning"
        assert claimed.run.lifecycle_version == 1
        assert claimed.run.attempt == 1
        with pytest.raises(IllegalRunTransitionError):
            repository.transition_owned_run(
                claimed.lease,
                "evaluating",
                expected_version=1,
                evidence=_lifecycle_evidence(2),
            )
        running = repository.transition_owned_run(
            claimed.lease,
            "running",
            expected_version=1,
            evidence=_lifecycle_evidence(2),
        )
        evaluating = repository.transition_owned_run(
            claimed.lease,
            "evaluating",
            expected_version=2,
            evidence=_lifecycle_evidence(3),
        )
        completed = repository.transition_owned_run(
            claimed.lease,
            "completed",
            expected_version=3,
            evidence=_lifecycle_evidence(4),
        )
        assert (running.lifecycle_version, evaluating.lifecycle_version) == (2, 3)
        assert completed.lifecycle_version == 4
        assert completed.lease_owner is None
        assert completed.lease_token is None
        assert (
            repository.claim_next_run(
                "worker-b",
                lease_duration_seconds=60,
                evidence=_lifecycle_evidence(5),
                run_id=run_id,
            )
            is None
        )
        events = repository.fetch_events(run_id)
        assert [record.event.to_dict()["payload"] for record in events] == [
            {"previous_state": "queued", "state": "provisioning"},
            {"previous_state": "provisioning", "state": "running"},
            {"previous_state": "running", "state": "evaluating"},
            {"previous_state": "evaluating", "state": "completed"},
        ]


def test_lifecycle_sequence_follows_existing_caller_sequenced_evidence(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("lifecycle-sequence-run")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
        repository = PersistenceRepository(session)
        repository.append_event(_event(run_id, 7))
        claimed = repository.claim_next_run(
            "sequence-worker",
            lease_duration_seconds=60,
            evidence=_lifecycle_evidence(8),
            run_id=run_id,
        )
        assert claimed is not None
        sequences = [
            record.event.to_dict()["sequence"] for record in repository.fetch_events(run_id)
        ]
        assert sequences == [
            7,
            8,
        ]


def test_heartbeat_requires_current_owner_version_and_unexpired_lease(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("heartbeat-run")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
        repository = PersistenceRepository(session)
        claimed = repository.claim_next_run(
            "worker-a",
            lease_duration_seconds=30,
            evidence=_lifecycle_evidence(1),
            run_id=run_id,
        )
        assert claimed is not None
        original_expiry = claimed.run.lease_expires_at
        heartbeat = repository.heartbeat(
            claimed.lease,
            expected_version=claimed.run.lifecycle_version,
            lease_duration_seconds=60,
        )
        assert heartbeat.run.lifecycle_version == 2
        assert heartbeat.run.lease_expires_at is not None
        assert original_expiry is not None
        assert heartbeat.run.lease_expires_at > original_expiry
        with pytest.raises(StaleLeaseError):
            repository.heartbeat(
                replace(claimed.lease, worker_id="worker-b"),
                expected_version=2,
                lease_duration_seconds=60,
            )
        with pytest.raises(LifecycleConflictError):
            repository.heartbeat(
                claimed.lease,
                expected_version=1,
                lease_duration_seconds=60,
            )


def test_expiry_requeue_reclaim_fences_stale_worker(migrated_engine: Engine) -> None:
    run_id = _unique("reclaim-run")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
        repository = PersistenceRepository(session)
        first = repository.claim_next_run(
            "worker-a",
            lease_duration_seconds=60,
            evidence=_lifecycle_evidence(1),
            run_id=run_id,
        )
        assert first is not None

    with migrated_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE public.runs SET "
                "heartbeat_at = clock_timestamp() - interval '2 hours', "
                "lease_expires_at = clock_timestamp() - interval '1 hour' "
                "WHERE run_id = :run_id"
            ),
            {"run_id": run_id},
        )

    with Session(migrated_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        with pytest.raises(LeaseExpiredError):
            repository.heartbeat(
                first.lease,
                expected_version=first.run.lifecycle_version,
                lease_duration_seconds=60,
            )
        queued = repository.requeue_expired_run(
            run_id,
            expected_version=first.run.lifecycle_version,
            evidence=_lifecycle_evidence(2),
        )
        assert queued.status == "queued"
        assert queued.lifecycle_version == 2

    with Session(migrated_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        second = repository.claim_next_run(
            "worker-b",
            lease_duration_seconds=60,
            evidence=_lifecycle_evidence(3),
            run_id=run_id,
        )
        assert second is not None
        assert second.run.attempt == 2
        assert second.lease.lease_token != first.lease.lease_token

    with Session(migrated_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        with pytest.raises(StaleLeaseError):
            repository.transition_owned_run(
                first.lease,
                "failed",
                expected_version=first.run.lifecycle_version,
                evidence=_lifecycle_evidence(4),
            )

    with Session(migrated_engine) as session, session.begin():
        repository = PersistenceRepository(session)
        running = repository.transition_owned_run(
            second.lease,
            "running",
            expected_version=second.run.lifecycle_version,
            evidence=_lifecycle_evidence(4),
        )
        assert running.status == "running"


def test_competing_claims_have_exactly_one_owner(migrated_engine: Engine) -> None:
    run_id = _unique("claim-race-run")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
    barrier = Barrier(2)

    def claim(worker_id: str) -> str:
        with Session(migrated_engine) as session, session.begin():
            barrier.wait()
            claimed = PersistenceRepository(session).claim_next_run(
                worker_id,
                lease_duration_seconds=60,
                evidence=_lifecycle_evidence(1),
                run_id=run_id,
            )
            return "none" if claimed is None else claimed.lease.worker_id

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(claim, ["worker-a", "worker-b"]))
    assert outcomes.count("none") == 1
    winners = [outcome for outcome in outcomes if outcome != "none"]
    assert len(winners) == 1
    with Session(migrated_engine) as session:
        run = PersistenceRepository(session).get_run(run_id)
        assert run is not None
        assert run.lease_owner == winners[0]
        assert run.attempt == 1


def test_generic_queue_skips_locked_run_and_claims_next_visible_run(
    migrated_engine: Engine,
) -> None:
    run_ids = (_unique("generic-queue-a"), _unique("generic-queue-b"))
    with Session(migrated_engine) as session, session.begin():
        for run_id in run_ids:
            _seed_run(session, run_id)
        session.execute(
            text(
                "UPDATE public.runs SET created_at = TIMESTAMPTZ '2000-01-01 00:00:00+00' "
                "WHERE run_id = ANY(:run_ids)"
            ),
            {"run_ids": list(run_ids)},
        )

    first_claimed = Event()
    release_first = Event()
    results: dict[str, str] = {}

    def hold_first_claim() -> None:
        with Session(migrated_engine) as session:
            transaction = session.begin()
            claimed = PersistenceRepository(session).claim_next_run(
                "generic-worker-a",
                lease_duration_seconds=60,
                evidence=_lifecycle_evidence(1),
            )
            assert claimed is not None
            results["first"] = claimed.run.run_id
            first_claimed.set()
            assert release_first.wait(timeout=10)
            transaction.commit()

    def claim_while_first_is_locked() -> None:
        assert first_claimed.wait(timeout=10)
        try:
            with Session(migrated_engine) as session, session.begin():
                claimed = PersistenceRepository(session).claim_next_run(
                    "generic-worker-b",
                    lease_duration_seconds=60,
                    evidence=_lifecycle_evidence(1),
                )
                assert claimed is not None
                results["second"] = claimed.run.run_id
        finally:
            release_first.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(hold_first_claim)
        second = executor.submit(claim_while_first_is_locked)
        first.result(timeout=15)
        second.result(timeout=15)

    assert {results["first"], results["second"]} == set(run_ids)
    with Session(migrated_engine) as session:
        for run_id in run_ids:
            events = PersistenceRepository(session).fetch_events(run_id)
            assert [record.event.to_dict()["sequence"] for record in events] == [1]


def test_expired_requeue_wins_race_against_heartbeat(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("heartbeat-race-run")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
        claimed = PersistenceRepository(session).claim_next_run(
            "heartbeat-race-worker",
            lease_duration_seconds=60,
            evidence=_lifecycle_evidence(1),
            run_id=run_id,
        )
        assert claimed is not None

    recovery_done = Event()
    release_recovery = Event()
    heartbeat_started = Event()

    def hold_expired_recovery() -> None:
        with Session(migrated_engine) as session:
            transaction = session.begin()
            session.execute(
                text(
                    "UPDATE public.runs SET "
                    "heartbeat_at = clock_timestamp() - interval '2 hours', "
                    "lease_expires_at = clock_timestamp() - interval '1 hour' "
                    "WHERE run_id = :run_id"
                ),
                {"run_id": run_id},
            )
            PersistenceRepository(session).requeue_expired_run(
                run_id,
                expected_version=claimed.run.lifecycle_version,
                evidence=_lifecycle_evidence(2),
            )
            recovery_done.set()
            assert release_recovery.wait(timeout=10)
            transaction.commit()

    def lose_heartbeat_race() -> str:
        assert recovery_done.wait(timeout=10)
        try:
            with Session(migrated_engine) as session, session.begin():
                session.execute(text("SET LOCAL application_name = 'issue6-heartbeat-requeue'"))
                heartbeat_started.set()
                PersistenceRepository(session).heartbeat(
                    claimed.lease,
                    expected_version=claimed.run.lifecycle_version,
                    lease_duration_seconds=120,
                )
        except LifecycleConflictError:
            return "conflict"
        return "unexpected"

    with ThreadPoolExecutor(max_workers=2) as executor:
        winner = executor.submit(hold_expired_recovery)
        loser = executor.submit(lose_heartbeat_race)
        assert heartbeat_started.wait(timeout=10)
        _wait_for_database_block(migrated_engine, "issue6-heartbeat-requeue")
        release_recovery.set()
        assert loser.result(timeout=15) == "conflict"
        winner.result(timeout=15)

    with Session(migrated_engine) as session:
        run = PersistenceRepository(session).get_run(run_id)
        assert run is not None
        assert run.status == "queued"
        assert run.lifecycle_version == 2


def test_terminal_transition_wins_race_against_requeue(migrated_engine: Engine) -> None:
    run_id = _unique("terminal-requeue-race")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
        claimed = PersistenceRepository(session).claim_next_run(
            "terminal-race-worker",
            lease_duration_seconds=60,
            evidence=_lifecycle_evidence(1),
            run_id=run_id,
        )
        assert claimed is not None

    transition_done = Event()
    release_transition = Event()
    terminal_requeue_started = Event()

    def hold_terminal_transition() -> None:
        with Session(migrated_engine) as session:
            transaction = session.begin()
            PersistenceRepository(session).transition_owned_run(
                claimed.lease,
                "failed",
                expected_version=claimed.run.lifecycle_version,
                evidence=_lifecycle_evidence(2),
            )
            transition_done.set()
            assert release_transition.wait(timeout=10)
            transaction.commit()

    def lose_requeue_to_terminal() -> str:
        assert transition_done.wait(timeout=10)
        try:
            with Session(migrated_engine) as session, session.begin():
                session.execute(text("SET LOCAL application_name = 'issue6-terminal-requeue'"))
                terminal_requeue_started.set()
                PersistenceRepository(session).requeue_expired_run(
                    run_id,
                    expected_version=claimed.run.lifecycle_version,
                    evidence=_lifecycle_evidence(3),
                )
        except LifecycleConflictError:
            return "conflict"
        finally:
            release_transition.set()
        return "unexpected"

    with ThreadPoolExecutor(max_workers=2) as executor:
        winner = executor.submit(hold_terminal_transition)
        loser = executor.submit(lose_requeue_to_terminal)
        assert terminal_requeue_started.wait(timeout=10)
        assert loser.result(timeout=15) == "conflict"
        winner.result(timeout=15)


def test_two_expired_recovery_attempts_with_same_version_have_one_winner(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("recovery-race-run")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
        claimed = PersistenceRepository(session).claim_next_run(
            "recovery-race-worker",
            lease_duration_seconds=60,
            evidence=_lifecycle_evidence(1),
            run_id=run_id,
        )
        assert claimed is not None
    with migrated_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE public.runs SET heartbeat_at = clock_timestamp() - interval '2 hours', "
                "lease_expires_at = clock_timestamp() - interval '1 hour' WHERE run_id = :run_id"
            ),
            {"run_id": run_id},
        )

    barrier = Barrier(2)

    def recover(worker_number: int) -> str:
        try:
            with Session(migrated_engine) as session, session.begin():
                barrier.wait(timeout=10)
                PersistenceRepository(session).requeue_expired_run(
                    run_id,
                    expected_version=claimed.run.lifecycle_version,
                    evidence=_lifecycle_evidence(worker_number + 1),
                )
            return "requeued"
        except LifecycleConflictError:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(recover, (1, 2)))
    assert sorted(outcomes) == ["conflict", "requeued"]


def test_heartbeat_and_terminal_transition_use_same_coordination_cas(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("heartbeat-terminal-race")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
        claimed = PersistenceRepository(session).claim_next_run(
            "coordination-worker",
            lease_duration_seconds=60,
            evidence=_lifecycle_evidence(1),
            run_id=run_id,
        )
        assert claimed is not None

    heartbeat_done = Event()
    release_heartbeat = Event()
    transition_started = Event()

    def hold_heartbeat() -> None:
        with Session(migrated_engine) as session:
            transaction = session.begin()
            PersistenceRepository(session).heartbeat(
                claimed.lease,
                expected_version=1,
                lease_duration_seconds=120,
            )
            heartbeat_done.set()
            assert release_heartbeat.wait(timeout=10)
            transaction.commit()

    def stale_terminal_transition() -> str:
        assert heartbeat_done.wait(timeout=10)
        try:
            with Session(migrated_engine) as session, session.begin():
                session.execute(text("SET LOCAL application_name = 'issue6-heartbeat-terminal'"))
                transition_started.set()
                PersistenceRepository(session).transition_owned_run(
                    claimed.lease,
                    "failed",
                    expected_version=1,
                    evidence=_lifecycle_evidence(2),
                )
        except LifecycleConflictError:
            return "conflict"
        return "unexpected"

    with ThreadPoolExecutor(max_workers=2) as executor:
        winner = executor.submit(hold_heartbeat)
        loser = executor.submit(stale_terminal_transition)
        assert transition_started.wait(timeout=10)
        _wait_for_database_block(migrated_engine, "issue6-heartbeat-terminal")
        release_heartbeat.set()
        assert loser.result(timeout=15) == "conflict"
        winner.result(timeout=15)


def test_lifecycle_evidence_failure_rolls_back_state_but_not_outer_transaction(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("evidence-rollback-run")
    first_evidence = _lifecycle_evidence(1)
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
        repository = PersistenceRepository(session)
        claimed = repository.claim_next_run(
            "worker-a",
            lease_duration_seconds=60,
            evidence=first_evidence,
            run_id=run_id,
        )
        assert claimed is not None
        duplicate = first_evidence
        with pytest.raises(DuplicateEventIDError):
            repository.transition_owned_run(
                claimed.lease,
                "running",
                expected_version=claimed.run.lifecycle_version,
                evidence=duplicate,
            )
        unchanged = repository.get_run(run_id)
        assert unchanged is not None
        assert unchanged.status == "provisioning"
        assert unchanged.lifecycle_version == 1
        running = repository.transition_owned_run(
            claimed.lease,
            "running",
            expected_version=1,
            evidence=_lifecycle_evidence(2),
        )
        assert running.status == "running"


def test_failed_claim_evidence_releases_savepoint_lock_for_another_transaction(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("claim-lock-release-run")
    duplicate_event_id = _unique("claim-lock-duplicate")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
        PersistenceRepository(session).append_event(_event(run_id, 1, event_id=duplicate_event_id))

    with Session(migrated_engine) as failed_session:
        outer = failed_session.begin()
        with pytest.raises(DuplicateEventIDError):
            PersistenceRepository(failed_session).claim_next_run(
                "failed-claim-worker",
                lease_duration_seconds=60,
                evidence=LifecycleEvidence(
                    event_id=duplicate_event_id,
                    producer_component="run-controller",
                ),
                run_id=run_id,
            )
        with Session(migrated_engine) as winner_session, winner_session.begin():
            claimed = PersistenceRepository(winner_session).claim_next_run(
                "replacement-worker",
                lease_duration_seconds=60,
                evidence=_lifecycle_evidence(2),
                run_id=run_id,
            )
            assert claimed is not None
        assert failed_session.in_transaction()
        outer.rollback()


def test_claim_rollback_restores_queue_eligibility(migrated_engine: Engine) -> None:
    run_id = _unique("claim-rollback-run")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
    with Session(migrated_engine) as session:
        transaction = session.begin()
        first = PersistenceRepository(session).claim_next_run(
            "worker-a",
            lease_duration_seconds=60,
            evidence=_lifecycle_evidence(1),
            run_id=run_id,
        )
        assert first is not None
        transaction.rollback()
    with Session(migrated_engine) as session, session.begin():
        second = PersistenceRepository(session).claim_next_run(
            "worker-b",
            lease_duration_seconds=60,
            evidence=_lifecycle_evidence(1),
            run_id=run_id,
        )
        assert second is not None
        assert second.run.attempt == 1


def test_queued_cancellation_is_terminal_and_report_requires_matching_terminal_status(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("cancel-run")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)
        repository = PersistenceRepository(session)
        with pytest.raises(PersistenceIntegrityError, match="cannot be stored"):
            repository.store_final_report(_report(run_id))
        cancelled = repository.cancel_queued_run(
            run_id,
            expected_version=0,
            evidence=_lifecycle_evidence(1),
        )
        assert cancelled.status == "cancelled"
        assert cancelled.lifecycle_version == 1
        assert (
            repository.claim_next_run(
                "worker-a",
                lease_duration_seconds=60,
                evidence=_lifecycle_evidence(2),
                run_id=run_id,
            )
            is None
        )
        with pytest.raises(PersistenceIntegrityError, match="does not match"):
            repository.store_final_report(_report(run_id))


def test_non_postgresql_url_fails_closed() -> None:
    with pytest.raises(ValueError, match="PostgreSQL"):
        create_postgres_engine("sqlite+pysqlite:///:memory:")


def test_destructive_database_guard_requires_opt_in_and_test_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CHAOSAGENT_ALLOW_DESTRUCTIVE_DATABASE_TESTS", raising=False)
    with pytest.raises(RuntimeError, match="ALLOW_DESTRUCTIVE"):
        _require_disposable_database("postgresql+psycopg://localhost/chaosagent_test")
    monkeypatch.setenv("CHAOSAGENT_ALLOW_DESTRUCTIVE_DATABASE_TESTS", "1")
    with pytest.raises(RuntimeError, match="must end with '_test'"):
        _require_disposable_database("postgresql+psycopg://localhost/chaosagent")
