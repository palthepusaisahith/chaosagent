"""Typed transactional repository for immutable ChaosAgent contract documents."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import NoReturn, cast

from chaosagent_evidence import (
    EvidenceValidationError,
    RunEvent,
    RunReport,
    loads_run_event,
    loads_run_report,
)
from chaosagent_scenarios import Scenario, ScenarioValidationError, loads_scenario
from sqlalchemy import Engine, Select, create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .models import (
    IDENTIFIER_CHECK,
    REVISION_CHECK,
    AgentConfigurationRevisionModel,
    RunEventModel,
    RunModel,
    RunReportModel,
    ScenarioRevisionModel,
)

_IDENTIFIER_RE = re.compile(IDENTIFIER_CHECK)
_REVISION_RE = re.compile(REVISION_CHECK)
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


class PersistenceError(RuntimeError):
    """Base class for persistence-boundary failures."""


class PersistenceConflictError(PersistenceError):
    """Raised when a stable identity is reused for different content."""


class RevisionConflictError(PersistenceConflictError):
    """Raised when an immutable revision identity conflicts."""


class DuplicateEventIDError(PersistenceConflictError):
    """Raised when an event ID already exists."""


class EventSequenceConflictError(PersistenceConflictError):
    """Raised when a run sequence number already exists."""


class EventIdentityAndSequenceConflictError(DuplicateEventIDError, EventSequenceConflictError):
    """Raised when both an event ID and its run sequence already exist."""


class FinalReportConflictError(PersistenceConflictError):
    """Raised when a run already has a different final report."""


class ReferenceNotFoundError(PersistenceError):
    """Raised when a required immutable reference does not exist."""


class PersistenceIntegrityError(PersistenceError):
    """Raised when stored relational and contract data disagree."""


class PersistenceProfileError(PersistenceError):
    """Raised when valid contract JSON cannot be represented by the V0 JSONB profile."""


@dataclass(frozen=True, slots=True)
class RevisionReference:
    id: str
    revision: str
    digest: str


@dataclass(frozen=True, slots=True)
class ScenarioRevisionRecord:
    scenario: Scenario
    created_at: datetime
    created_by: str


@dataclass(frozen=True, slots=True)
class AgentConfigurationRevisionRecord:
    reference: RevisionReference
    created_at: datetime
    created_by: str


@dataclass(frozen=True, slots=True)
class RunRecord:
    run_id: str
    scenario: RevisionReference
    agent_configuration: RevisionReference
    status: str
    created_at: datetime
    created_by: str


@dataclass(frozen=True, slots=True)
class RunEventRecord:
    event: RunEvent
    inserted_at: datetime


@dataclass(frozen=True, slots=True)
class RunReportRecord:
    report: RunReport
    inserted_at: datetime


def create_postgres_engine(database_url: str, *, echo: bool = False) -> Engine:
    """Create a SQLAlchemy engine and fail closed for non-PostgreSQL URLs."""
    engine = create_engine(database_url, echo=echo, pool_pre_ping=True)
    if engine.dialect.name != "postgresql":
        engine.dispose()
        raise ValueError("ChaosAgent persistence requires a PostgreSQL database URL")
    return engine


def _json_text(document: dict[str, object]) -> str:
    return json.dumps(document, ensure_ascii=False, separators=(",", ":"))


def _require_identifier(value: str, field: str) -> None:
    if len(value) > 128 or _IDENTIFIER_RE.fullmatch(value) is None:
        raise ValueError(f"{field} is not a valid ChaosAgent identifier")


def _require_revision(value: str) -> None:
    if len(value) > 64 or _REVISION_RE.fullmatch(value) is None:
        raise ValueError("revision is not a valid ChaosAgent revision")


def _require_digest(value: str) -> None:
    if _DIGEST_RE.fullmatch(value) is None:
        raise ValueError("digest must be a lowercase sha256 digest")


def _validate_reference(reference: RevisionReference, field: str) -> None:
    _require_identifier(reference.id, f"{field}.id")
    _require_revision(reference.revision)
    _require_digest(reference.digest)


def _validate_jsonb_persistence_profile(value: object, contract: str, path: str = "$") -> None:
    if isinstance(value, str):
        if "\x00" in value:
            raise PersistenceProfileError(
                f"{contract} {path} contains U+0000, which PostgreSQL JSONB cannot store"
            )
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_jsonb_persistence_profile(item, contract, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if "\x00" in key:
                raise PersistenceProfileError(
                    f"{contract} {path} contains an object key with U+0000, "
                    "which PostgreSQL JSONB cannot store"
                )
            _validate_jsonb_persistence_profile(item, contract, f"{path}.{key}")


def _constraint_name(error: IntegrityError) -> str | None:
    diagnostic = getattr(error.orig, "diag", None)
    return cast(str | None, getattr(diagnostic, "constraint_name", None))


def _raise_integrity(error: IntegrityError, operation: str) -> NoReturn:
    raise PersistenceIntegrityError(
        f"database rejected {operation} at constraint {_constraint_name(error)!r}"
    ) from error


class PersistenceRepository:
    """Repository bound to a caller-owned SQLAlchemy transaction.

    Methods flush but never commit. The caller controls the transaction boundary.
    Immutable-return records expose validated wrappers or frozen scalar values, never ORM rows.
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    def insert_scenario_revision(
        self, scenario: Scenario, *, created_by: str
    ) -> ScenarioRevisionRecord:
        document = scenario.to_dict()
        _validate_jsonb_persistence_profile(document, "scenario")
        scenario_id = cast(str, document["scenario_id"])
        revision = cast(str, document["revision"])
        schema_version = cast(str, document["schema_version"])
        existing = self._session.get(ScenarioRevisionModel, (scenario_id, revision))
        if existing is not None:
            return self._same_scenario_or_conflict(existing, scenario)

        model = ScenarioRevisionModel(
            scenario_id=scenario_id,
            revision=revision,
            schema_version=schema_version,
            canonical_document=document,
            canonical_digest=scenario.digest,
            created_by=created_by,
        )
        try:
            with self._session.begin_nested():
                self._session.add(model)
                self._session.flush()
        except IntegrityError as error:
            if _constraint_name(error) == "pk_scenario_revisions":
                concurrent = self._session.get(ScenarioRevisionModel, (scenario_id, revision))
                if concurrent is not None:
                    return self._same_scenario_or_conflict(concurrent, scenario)
            _raise_integrity(error, "scenario revision insert")
        return self._scenario_record(model)

    def get_scenario_revision(
        self, scenario_id: str, revision: str
    ) -> ScenarioRevisionRecord | None:
        model = self._session.get(ScenarioRevisionModel, (scenario_id, revision))
        return None if model is None else self._scenario_record(model)

    def insert_agent_configuration_reference(
        self, reference: RevisionReference, *, created_by: str
    ) -> AgentConfigurationRevisionRecord:
        _validate_reference(reference, "agent_configuration")
        existing = self._session.get(
            AgentConfigurationRevisionModel, (reference.id, reference.revision)
        )
        if existing is not None:
            return self._same_agent_reference_or_conflict(existing, reference)
        model = AgentConfigurationRevisionModel(
            agent_configuration_id=reference.id,
            revision=reference.revision,
            digest=reference.digest,
            created_by=created_by,
        )
        try:
            with self._session.begin_nested():
                self._session.add(model)
                self._session.flush()
        except IntegrityError as error:
            if _constraint_name(error) == "pk_agent_configuration_revisions":
                concurrent = self._session.get(
                    AgentConfigurationRevisionModel, (reference.id, reference.revision)
                )
                if concurrent is not None:
                    return self._same_agent_reference_or_conflict(concurrent, reference)
            _raise_integrity(error, "agent configuration reference insert")
        return self._agent_configuration_record(model)

    def get_agent_configuration_reference(
        self, agent_configuration_id: str, revision: str
    ) -> AgentConfigurationRevisionRecord | None:
        model = self._session.get(
            AgentConfigurationRevisionModel, (agent_configuration_id, revision)
        )
        return None if model is None else self._agent_configuration_record(model)

    def create_run(
        self,
        run_id: str,
        *,
        scenario_id: str,
        scenario_revision: str,
        agent_configuration_id: str,
        agent_configuration_revision: str,
        created_by: str,
    ) -> RunRecord:
        _require_identifier(run_id, "run_id")
        scenario = self._session.get(ScenarioRevisionModel, (scenario_id, scenario_revision))
        if scenario is None:
            raise ReferenceNotFoundError(
                f"scenario revision {(scenario_id, scenario_revision)!r} does not exist"
            )
        agent = self._session.get(
            AgentConfigurationRevisionModel,
            (agent_configuration_id, agent_configuration_revision),
        )
        if agent is None:
            raise ReferenceNotFoundError(
                "agent configuration revision "
                f"{(agent_configuration_id, agent_configuration_revision)!r} does not exist"
            )
        model = RunModel(
            run_id=run_id,
            scenario_id=scenario.scenario_id,
            scenario_revision=scenario.revision,
            scenario_digest=scenario.canonical_digest,
            agent_configuration_id=agent.agent_configuration_id,
            agent_configuration_revision=agent.revision,
            agent_configuration_digest=agent.digest,
            created_by=created_by,
        )
        try:
            with self._session.begin_nested():
                self._session.add(model)
                self._session.flush()
        except IntegrityError as error:
            if _constraint_name(error) == "pk_runs":
                raise PersistenceConflictError(f"run_id {run_id!r} already exists") from error
            _raise_integrity(error, "run insert")
        return self._run_record(model)

    def get_run(self, run_id: str) -> RunRecord | None:
        model = self._session.get(RunModel, run_id)
        return None if model is None else self._run_record(model)

    def append_event(self, event: RunEvent) -> RunEventRecord:
        document = event.to_dict()
        _validate_jsonb_persistence_profile(document, "run event")
        event_id = cast(str, document["event_id"])
        run_id = cast(str, document["run_id"])
        if self._session.get(RunModel, run_id) is None:
            raise ReferenceNotFoundError(f"run {run_id!r} does not exist")
        model = RunEventModel(
            event_id=event_id,
            run_id=run_id,
            sequence=cast(int, document["sequence"]),
            schema_version=cast(str, document["schema_version"]),
            event_type=cast(str, document["event_type"]),
            occurred_at=_timestamp(cast(str, document["occurred_at"])),
            recorded_at=_timestamp(cast(str, document["recorded_at"])),
            document=document,
            payload_digest=cast(str, document["payload_digest"]),
        )
        try:
            with self._session.begin_nested():
                self._session.add(model)
                self._session.flush()
        except IntegrityError as error:
            self._raise_event_conflict_or_integrity(
                error, event_id=event_id, run_id=run_id, sequence=model.sequence
            )
        return self._event_record(model)

    def fetch_events(
        self, run_id: str, *, after_sequence: int = 0, through_sequence: int | None = None
    ) -> tuple[RunEventRecord, ...]:
        if after_sequence < 0:
            raise ValueError("after_sequence must be non-negative")
        query: Select[tuple[RunEventModel]] = (
            select(RunEventModel)
            .where(RunEventModel.run_id == run_id, RunEventModel.sequence > after_sequence)
            .order_by(RunEventModel.sequence)
        )
        if through_sequence is not None:
            if through_sequence < 1:
                raise ValueError("through_sequence must be positive")
            query = query.where(RunEventModel.sequence <= through_sequence)
        return tuple(self._event_record(model) for model in self._session.scalars(query))

    def store_final_report(self, report: RunReport) -> RunReportRecord:
        document = report.to_dict()
        _validate_jsonb_persistence_profile(document, "run report")
        report_id = cast(str, document["report_id"])
        run_id = cast(str, document["run_id"])
        run = self._session.get(RunModel, run_id)
        if run is None:
            raise ReferenceNotFoundError(f"run {run_id!r} does not exist")
        self._verify_report_references(document, run)

        existing_run_report = self._session.scalar(
            select(RunReportModel).where(RunReportModel.run_id == run_id)
        )
        if existing_run_report is not None:
            return self._same_report_or_conflict(existing_run_report, report)
        existing_id = self._session.get(RunReportModel, report_id)
        if existing_id is not None:
            raise FinalReportConflictError(f"report_id {report_id!r} already exists")

        model = RunReportModel(
            report_id=report_id,
            run_id=run_id,
            schema_version=cast(str, document["schema_version"]),
            scenario_id=run.scenario_id,
            scenario_revision=run.scenario_revision,
            scenario_digest=run.scenario_digest,
            agent_configuration_id=run.agent_configuration_id,
            agent_configuration_revision=run.agent_configuration_revision,
            agent_configuration_digest=run.agent_configuration_digest,
            run_status=cast(str, document["run_status"]),
            classification=cast(str, document["classification"]),
            generated_at=_timestamp(cast(str, document["generated_at"])),
            document=document,
        )
        try:
            with self._session.begin_nested():
                self._session.add(model)
                self._session.flush()
        except IntegrityError as error:
            constraint = _constraint_name(error)
            if constraint in {"pk_run_reports", "uq_run_reports_run_id"}:
                concurrent = self._session.scalar(
                    select(RunReportModel).where(RunReportModel.run_id == run_id)
                )
                if concurrent is not None:
                    return self._same_report_or_conflict(concurrent, report)
                raise FinalReportConflictError(f"report_id {report_id!r} already exists") from error
            _raise_integrity(error, "final report insert")
        return self._report_record(model)

    def get_final_report(self, run_id: str) -> RunReportRecord | None:
        model = self._session.scalar(select(RunReportModel).where(RunReportModel.run_id == run_id))
        return None if model is None else self._report_record(model)

    def _same_scenario_or_conflict(
        self, model: ScenarioRevisionModel, scenario: Scenario
    ) -> ScenarioRevisionRecord:
        if (
            model.canonical_digest != scenario.digest
            or model.canonical_document != scenario.to_dict()
        ):
            raise RevisionConflictError(
                f"scenario revision {(model.scenario_id, model.revision)!r} has different content"
            )
        return self._scenario_record(model)

    def _raise_event_conflict_or_integrity(
        self, error: IntegrityError, *, event_id: str, run_id: str, sequence: int
    ) -> NoReturn:
        event_id_exists = self._session.get(RunEventModel, event_id) is not None
        sequence_exists = (
            self._session.scalar(
                select(RunEventModel.event_id).where(
                    RunEventModel.run_id == run_id, RunEventModel.sequence == sequence
                )
            )
            is not None
        )
        if event_id_exists and sequence_exists:
            raise EventIdentityAndSequenceConflictError(
                f"event_id {event_id!r} and run {run_id!r} sequence {sequence} already exist"
            ) from error
        if event_id_exists:
            raise DuplicateEventIDError(f"event_id {event_id!r} already exists") from error
        if sequence_exists:
            raise EventSequenceConflictError(
                f"run {run_id!r} already has sequence {sequence}"
            ) from error
        _raise_integrity(error, "run event append")

    def _same_agent_reference_or_conflict(
        self, model: AgentConfigurationRevisionModel, reference: RevisionReference
    ) -> AgentConfigurationRevisionRecord:
        if model.digest != reference.digest:
            raise RevisionConflictError(
                "agent configuration revision "
                f"{(model.agent_configuration_id, model.revision)!r} has a different digest"
            )
        return self._agent_configuration_record(model)

    def _same_report_or_conflict(self, model: RunReportModel, report: RunReport) -> RunReportRecord:
        if model.document != report.to_dict():
            raise FinalReportConflictError(f"run {model.run_id!r} already has a different report")
        return self._report_record(model)

    @staticmethod
    def _verify_report_references(document: dict[str, object], run: RunModel) -> None:
        scenario = cast(dict[str, object], document["scenario"])
        agent = cast(dict[str, object], document["agent_configuration"])
        expected_scenario = (run.scenario_id, run.scenario_revision, run.scenario_digest)
        actual_scenario = (scenario["id"], scenario["revision"], scenario["digest"])
        if actual_scenario != expected_scenario:
            raise PersistenceIntegrityError("report scenario reference does not match the run")
        expected_agent = (
            run.agent_configuration_id,
            run.agent_configuration_revision,
            run.agent_configuration_digest,
        )
        actual_agent = (agent["id"], agent["revision"], agent["digest"])
        if actual_agent != expected_agent:
            raise PersistenceIntegrityError(
                "report agent configuration reference does not match the run"
            )

    @staticmethod
    def _scenario_record(model: ScenarioRevisionModel) -> ScenarioRevisionRecord:
        try:
            scenario = loads_scenario(_json_text(model.canonical_document))
        except ScenarioValidationError as error:
            raise PersistenceIntegrityError(
                "stored scenario document violates its contract"
            ) from error
        if scenario.digest != model.canonical_digest:
            raise PersistenceIntegrityError("stored scenario document does not match its digest")
        return ScenarioRevisionRecord(scenario, model.created_at, model.created_by)

    @staticmethod
    def _agent_configuration_record(
        model: AgentConfigurationRevisionModel,
    ) -> AgentConfigurationRevisionRecord:
        return AgentConfigurationRevisionRecord(
            RevisionReference(model.agent_configuration_id, model.revision, model.digest),
            model.created_at,
            model.created_by,
        )

    @staticmethod
    def _run_record(model: RunModel) -> RunRecord:
        return RunRecord(
            run_id=model.run_id,
            scenario=RevisionReference(
                model.scenario_id, model.scenario_revision, model.scenario_digest
            ),
            agent_configuration=RevisionReference(
                model.agent_configuration_id,
                model.agent_configuration_revision,
                model.agent_configuration_digest,
            ),
            status=model.status,
            created_at=model.created_at,
            created_by=model.created_by,
        )

    @staticmethod
    def _event_record(model: RunEventModel) -> RunEventRecord:
        try:
            event = loads_run_event(_json_text(model.document))
        except EvidenceValidationError as error:
            raise PersistenceIntegrityError(
                "stored run event document violates its contract"
            ) from error
        return RunEventRecord(event, model.inserted_at)

    @staticmethod
    def _report_record(model: RunReportModel) -> RunReportRecord:
        try:
            report = loads_run_report(_json_text(model.document))
        except EvidenceValidationError as error:
            raise PersistenceIntegrityError(
                "stored run report document violates its contract"
            ) from error
        return RunReportRecord(report, model.inserted_at)


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("contract timestamp must include a timezone")
    return parsed
