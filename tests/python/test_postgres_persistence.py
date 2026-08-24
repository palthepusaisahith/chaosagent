from __future__ import annotations

import json
import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from pathlib import Path
from threading import Barrier
from typing import cast
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from chaosagent_evidence import (
    RunEvent,
    RunReport,
    loads_run_event,
    loads_run_report,
)
from chaosagent_persistence import (
    DuplicateEventIDError,
    EventIdentityAndSequenceConflictError,
    EventSequenceConflictError,
    FinalReportConflictError,
    PersistenceConflictError,
    PersistenceIntegrityError,
    PersistenceProfileError,
    PersistenceRepository,
    RevisionConflictError,
    RevisionReference,
    create_postgres_engine,
)
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
    scenario_document = scenario.to_dict()
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


def test_insert_fetch_scenario_revision_and_idempotent_reinsert(
    migrated_engine: Engine,
) -> None:
    scenario = _scenario()
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
        inserted = repository.store_final_report(report)
        repeated = repository.store_final_report(report)
        fetched = repository.get_final_report(run_id)
        assert inserted.report.canonical_bytes == repeated.report.canonical_bytes
        assert fetched is not None
        assert fetched.report.canonical_bytes == report.canonical_bytes
        run = repository.get_run(run_id)
        assert run is not None
        assert run.status == "queued"
        assert fetched.report.to_dict()["run_status"] == "completed"
        with pytest.raises(FinalReportConflictError):
            repository.store_final_report(_report(run_id))


def test_report_projection_constraints_reject_schema_and_nested_reference_mismatch(
    migrated_engine: Engine,
) -> None:
    run_id = _unique("raw-report-run")
    with Session(migrated_engine) as session, session.begin():
        _seed_run(session, run_id)

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

    event_document = _event(run_id, 1).to_dict()
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
