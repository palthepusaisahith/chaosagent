"""Typed transactional repository for immutable ChaosAgent contract documents."""

from __future__ import annotations

import json
import re
import secrets
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import NoReturn, cast

from chaosagent_evidence import (
    EvidenceValidationError,
    RunEvent,
    RunReport,
    digest_payload_v0,
    loads_run_event,
    loads_run_report,
)
from chaosagent_fixtures import Fixture, FixtureValidationError, loads_fixture
from chaosagent_scenarios import Scenario, ScenarioValidationError, loads_scenario
from sqlalchemy import Engine, Select, create_engine, func, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .lifecycle import (
    ACTIVE_STATUSES,
    TERMINAL_STATUSES,
    RunStatus,
    parse_run_status,
    require_claim_transition,
    require_owned_transition,
    require_recovery_transition,
    require_unleased_transition,
)
from .models import (
    IDENTIFIER_CHECK,
    REVISION_CHECK,
    AgentConfigurationRevisionModel,
    CompanyCustomerModel,
    CompanyOrderModel,
    CompanyPaymentModel,
    CompanyRefundModel,
    CompanyShipmentModel,
    CompanySupportTicketModel,
    FixtureRevisionModel,
    RunCompanyStateModel,
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


class LifecycleConflictError(PersistenceConflictError):
    """Raised when a lifecycle compare-and-swap predicate is stale."""


class StaleLeaseError(LifecycleConflictError):
    """Raised when a worker no longer owns the current fencing generation."""


class LeaseExpiredError(LifecycleConflictError):
    """Raised when the otherwise-current lease is already expired."""


class LeaseNotExpiredError(LifecycleConflictError):
    """Raised when recovery is attempted before the database lease expires."""


class CompanyStateInitializationError(PersistenceError):
    """Raised when a Run-local company state cannot be initialized safely."""


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
class FixtureRevisionRecord:
    fixture: Fixture
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
    fixture: RevisionReference | None
    status: RunStatus
    lifecycle_version: int
    lease_owner: str | None
    lease_token: str | None
    lease_expires_at: datetime | None
    heartbeat_at: datetime | None
    attempt: int
    created_at: datetime
    created_by: str


@dataclass(frozen=True, slots=True)
class LeaseIdentity:
    """Opaque worker credentials plus the transactional attempt generation."""

    run_id: str
    worker_id: str
    lease_token: str
    attempt: int


@dataclass(frozen=True, slots=True)
class ClaimedRun:
    """A Run snapshot paired with credentials needed for later mutations."""

    run: RunRecord
    lease: LeaseIdentity


@dataclass(frozen=True, slots=True)
class LifecycleEvidence:
    """Caller-owned Event v0 fields excluding repository-allocated Run sequence."""

    event_id: str
    producer_component: str
    producer_instance_id: str | None = None
    correlation_id: str | None = None
    causation_event_id: str | None = None
    reason_code: str | None = None


@dataclass(frozen=True, slots=True)
class RunEventRecord:
    event: RunEvent
    inserted_at: datetime


@dataclass(frozen=True, slots=True)
class RunReportRecord:
    report: RunReport
    inserted_at: datetime


@dataclass(frozen=True, slots=True)
class CompanyCustomer:
    customer_id: str
    name: str
    email: str
    status: str


@dataclass(frozen=True, slots=True)
class CompanyOrder:
    order_id: str
    customer_id: str
    status: str
    currency: str
    total_minor: int
    placed_at: datetime


@dataclass(frozen=True, slots=True)
class CompanyShipment:
    shipment_id: str
    order_id: str
    status: str
    carrier: str
    tracking_number: str
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class CompanyPayment:
    payment_id: str
    order_id: str
    status: str
    currency: str
    amount_minor: int
    captured_at: datetime


@dataclass(frozen=True, slots=True)
class CompanyRefund:
    refund_id: str
    payment_id: str
    order_id: str
    status: str
    amount_minor: int
    reason: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class CompanySupportTicket:
    ticket_id: str
    customer_id: str
    order_id: str
    status: str
    subject: str
    note: str
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class SyntheticCompanyState:
    run_id: str
    fixture: RevisionReference
    reference_time: datetime
    customers: tuple[CompanyCustomer, ...]
    orders: tuple[CompanyOrder, ...]
    shipments: tuple[CompanyShipment, ...]
    payments: tuple[CompanyPayment, ...]
    refunds: tuple[CompanyRefund, ...]
    support_tickets: tuple[CompanySupportTicket, ...]


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

    def insert_fixture_revision(
        self, fixture: Fixture, *, created_by: str
    ) -> FixtureRevisionRecord:
        document = fixture.to_dict()
        _validate_jsonb_persistence_profile(document, "fixture")
        fixture_id = cast(str, document["fixture_id"])
        revision = cast(str, document["revision"])
        existing = self._session.get(FixtureRevisionModel, (fixture_id, revision))
        if existing is not None:
            return self._same_fixture_or_conflict(existing, fixture)
        model = FixtureRevisionModel(
            fixture_id=fixture_id,
            revision=revision,
            schema_version=cast(str, document["schema_version"]),
            canonical_document=document,
            canonical_digest=fixture.digest,
            created_by=created_by,
        )
        try:
            with self._session.begin_nested():
                self._session.add(model)
                self._session.flush()
        except IntegrityError as error:
            if _constraint_name(error) == "pk_fixture_revisions":
                concurrent = self._session.get(FixtureRevisionModel, (fixture_id, revision))
                if concurrent is not None:
                    return self._same_fixture_or_conflict(concurrent, fixture)
            _raise_integrity(error, "fixture revision insert")
        return self._fixture_record(model)

    def get_fixture_revision(self, fixture_id: str, revision: str) -> FixtureRevisionRecord | None:
        model = self._session.get(FixtureRevisionModel, (fixture_id, revision))
        return None if model is None else self._fixture_record(model)

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
        scenario_fixture = cast(
            dict[str, object],
            scenario.canonical_document["fixture"],
        )
        fixture_key = (
            cast(str, scenario_fixture["id"]),
            cast(str, scenario_fixture["revision"]),
        )
        fixture = self._session.get(FixtureRevisionModel, fixture_key)
        if fixture is None or fixture.canonical_digest != scenario_fixture["digest"]:
            raise ReferenceNotFoundError(
                "scenario fixture reference "
                f"{(fixture_key[0], fixture_key[1], scenario_fixture['digest'])!r} "
                "does not resolve to an immutable Fixture revision"
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
            fixture_id=fixture.fixture_id,
            fixture_revision=fixture.revision,
            fixture_digest=fixture.canonical_digest,
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

    def lock_current_lease(self, lease: LeaseIdentity) -> RunRecord:
        """Lock a Run and prove the caller holds its current unexpired lease.

        The row lock is retained by the caller-owned transaction. This is the
        fencing primitive for non-lifecycle work that must remain coherent with
        requeue, reclaim, heartbeat, and lifecycle transitions.
        """
        _validate_lease_identity(lease)
        model = self._session.scalar(
            select(RunModel)
            .where(RunModel.run_id == lease.run_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if model is None:
            raise ReferenceNotFoundError(f"run {lease.run_id!r} does not exist")
        if (
            model.status not in ACTIVE_STATUSES
            or model.lease_owner != lease.worker_id
            or model.lease_token != lease.lease_token
            or model.attempt != lease.attempt
        ):
            raise StaleLeaseError(
                f"worker {lease.worker_id!r} no longer owns run {lease.run_id!r} "
                f"attempt {lease.attempt}"
            )
        if model.lease_expires_at is None or model.lease_expires_at <= self.database_time():
            raise LeaseExpiredError(f"run {lease.run_id!r} lease has expired")
        return self._run_record(model)

    def initialize_run_company_state(self, run_id: str) -> SyntheticCompanyState:
        """Materialize one deterministic Run-local copy before its first claim."""
        _require_identifier(run_id, "run_id")
        existing = self._session.get(RunCompanyStateModel, run_id)
        if existing is not None:
            return self._company_state_record(existing)

        with self._session.begin_nested():
            run = self._session.scalar(
                select(RunModel)
                .where(RunModel.run_id == run_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if run is None:
                raise ReferenceNotFoundError(f"run {run_id!r} does not exist")
            concurrently_initialized = self._session.scalar(
                select(RunCompanyStateModel)
                .where(RunCompanyStateModel.run_id == run_id)
                .execution_options(populate_existing=True)
            )
            if concurrently_initialized is not None:
                return self._company_state_record(concurrently_initialized)
            if run.status != "queued" or run.attempt != 0:
                raise CompanyStateInitializationError(
                    f"run {run_id!r} company state may only be initialized before its first claim"
                )
            if run.fixture_id is None or run.fixture_revision is None or run.fixture_digest is None:
                raise CompanyStateInitializationError(
                    f"legacy run {run_id!r} has no immutable Fixture binding"
                )
            fixture_model = self._session.get(
                FixtureRevisionModel, (run.fixture_id, run.fixture_revision)
            )
            if fixture_model is None or fixture_model.canonical_digest != run.fixture_digest:
                raise PersistenceIntegrityError(f"run {run_id!r} Fixture binding does not resolve")
            fixture = self._fixture_record(fixture_model).fixture
            document = fixture.to_dict()
            state = RunCompanyStateModel(
                run_id=run_id,
                fixture_id=fixture_model.fixture_id,
                fixture_revision=fixture_model.revision,
                fixture_digest=fixture_model.canonical_digest,
                reference_time=_timestamp(cast(str, document["reference_time"])),
            )
            self._session.add(state)
            self._session.flush()
            self._insert_company_entities(run_id, document)
            self._session.flush()
        return self._company_state_record(state)

    def get_run_company_state(self, run_id: str) -> SyntheticCompanyState | None:
        """Read a deterministic immutable snapshot of one Run-local company state."""
        state = self._session.get(RunCompanyStateModel, run_id)
        return None if state is None else self._company_state_record(state)

    def has_run_company_state(self, run_id: str) -> bool:
        """Check materialization without reading any business entity as a tool result."""
        return self._session.get(RunCompanyStateModel, run_id) is not None

    def get_company_order(self, run_id: str, order_id: str) -> CompanyOrder | None:
        """Read one Run-scoped synthetic order without exposing an ORM row."""
        model = self._session.get(CompanyOrderModel, (run_id, order_id))
        if model is None:
            return None
        return CompanyOrder(
            model.order_id,
            model.customer_id,
            model.status,
            model.currency,
            model.total_minor,
            model.placed_at,
        )

    def get_company_shipment_for_order(self, run_id: str, order_id: str) -> CompanyShipment | None:
        """Read the sole Fixture v0 shipment associated with a Run-scoped order."""
        model = self._session.scalar(
            select(CompanyShipmentModel).where(
                CompanyShipmentModel.run_id == run_id,
                CompanyShipmentModel.order_id == order_id,
            )
        )
        if model is None:
            return None
        return CompanyShipment(
            model.shipment_id,
            model.order_id,
            model.status,
            model.carrier,
            model.tracking_number,
            model.updated_at,
        )

    def claim_next_run(
        self,
        worker_id: str,
        *,
        lease_duration_seconds: int,
        evidence: LifecycleEvidence,
        run_id: str | None = None,
    ) -> ClaimedRun | None:
        """Claim the oldest queued Run using ``FOR UPDATE SKIP LOCKED``.

        The row lock and all writes remain part of the caller-owned transaction.
        A matching lifecycle Event v0 insert is required in the same savepoint.
        Omitting ``run_id`` selects from the global visible queue; supplying it
        returns ``None`` when that specific Run is absent, locked, or ineligible.
        """
        _require_identifier(worker_id, "worker_id")
        _require_lease_duration(lease_duration_seconds)
        if run_id is not None:
            _require_identifier(run_id, "run_id")
        model: RunModel | None = None
        lease_token = f"lease-{secrets.token_hex(16)}"
        with self._session.begin_nested():
            query = (
                select(RunModel)
                .where(RunModel.status == "queued")
                .order_by(RunModel.created_at, RunModel.run_id)
                .with_for_update(skip_locked=True)
                .limit(1)
                .execution_options(populate_existing=True)
            )
            if run_id is not None:
                query = query.where(RunModel.run_id == run_id)
            candidate = self._session.scalar(query)
            if candidate is None:
                return None
            source = parse_run_status(candidate.status)
            require_claim_transition(source, "provisioning")
            model = self._session.scalar(
                update(RunModel)
                .where(
                    RunModel.run_id == candidate.run_id,
                    RunModel.status == source,
                    RunModel.lifecycle_version == candidate.lifecycle_version,
                    RunModel.lease_owner.is_(None),
                    RunModel.lease_token.is_(None),
                )
                .values(
                    status="provisioning",
                    lifecycle_version=RunModel.lifecycle_version + 1,
                    lease_owner=worker_id,
                    lease_token=lease_token,
                    heartbeat_at=func.clock_timestamp(),
                    lease_expires_at=text(
                        "clock_timestamp() + make_interval(secs => :lease_duration_seconds)"
                    ),
                    attempt=RunModel.attempt + 1,
                )
                .returning(RunModel),
                {"lease_duration_seconds": lease_duration_seconds},
            )
            if model is None:
                raise LifecycleConflictError(
                    f"queued run {candidate.run_id!r} changed while being claimed"
                )
            occurred_at = self._database_clock()
            self._append_lifecycle_event(
                model.run_id,
                previous_state=source,
                state="provisioning",
                occurred_at=occurred_at,
                evidence=evidence,
            )
        record = self._run_record(model)
        return ClaimedRun(
            record,
            LeaseIdentity(model.run_id, worker_id, lease_token, model.attempt),
        )

    def heartbeat(
        self,
        lease: LeaseIdentity,
        *,
        expected_version: int,
        lease_duration_seconds: int,
    ) -> ClaimedRun:
        """Extend only the current, unexpired lease generation using database time."""
        _validate_lease_identity(lease)
        _require_expected_version(expected_version)
        _require_lease_duration(lease_duration_seconds)
        model = self._session.scalar(
            update(RunModel)
            .where(
                RunModel.run_id == lease.run_id,
                RunModel.status.in_(ACTIVE_STATUSES),
                RunModel.lifecycle_version == expected_version,
                RunModel.lease_owner == lease.worker_id,
                RunModel.lease_token == lease.lease_token,
                RunModel.attempt == lease.attempt,
                RunModel.lease_expires_at > func.clock_timestamp(),
            )
            .values(
                lifecycle_version=RunModel.lifecycle_version + 1,
                heartbeat_at=func.clock_timestamp(),
                lease_expires_at=text(
                    "clock_timestamp() + make_interval(secs => :lease_duration_seconds)"
                ),
            )
            .returning(RunModel),
            {"lease_duration_seconds": lease_duration_seconds},
        )
        if model is None:
            self._raise_lease_mutation_failure(lease, expected_version)
        return ClaimedRun(self._run_record(model), lease)

    def transition_owned_run(
        self,
        lease: LeaseIdentity,
        target_status: RunStatus,
        *,
        expected_version: int,
        evidence: LifecycleEvidence,
    ) -> RunRecord:
        """CAS-transition a Run while proving the current, unexpired lease."""
        _validate_lease_identity(lease)
        _require_expected_version(expected_version)
        current = self._fresh_run(lease.run_id)
        source = parse_run_status(current.status)
        require_owned_transition(source, target_status)
        clearing_lease = target_status in TERMINAL_STATUSES
        values: dict[str, object] = {
            "status": target_status,
            "lifecycle_version": RunModel.lifecycle_version + 1,
        }
        if clearing_lease:
            values.update(
                lease_owner=None,
                lease_token=None,
                lease_expires_at=None,
                heartbeat_at=None,
            )
        with self._session.begin_nested():
            model = self._session.scalar(
                update(RunModel)
                .where(
                    RunModel.run_id == lease.run_id,
                    RunModel.status == source,
                    RunModel.lifecycle_version == expected_version,
                    RunModel.lease_owner == lease.worker_id,
                    RunModel.lease_token == lease.lease_token,
                    RunModel.attempt == lease.attempt,
                    RunModel.lease_expires_at > func.clock_timestamp(),
                )
                .values(**values)
                .returning(RunModel)
            )
            if model is None:
                self._raise_lease_mutation_failure(lease, expected_version)
            self._append_lifecycle_event(
                model.run_id,
                previous_state=source,
                state=target_status,
                occurred_at=self._database_clock(),
                evidence=evidence,
            )
        return self._run_record(model)

    def cancel_queued_run(
        self,
        run_id: str,
        *,
        expected_version: int,
        evidence: LifecycleEvidence,
    ) -> RunRecord:
        """Cancel an unclaimed queued Run with a lifecycle CAS and evidence."""
        _require_expected_version(expected_version)
        current = self._fresh_run(run_id)
        source = parse_run_status(current.status)
        require_unleased_transition(source, "cancelled")
        with self._session.begin_nested():
            model = self._session.scalar(
                update(RunModel)
                .where(
                    RunModel.run_id == run_id,
                    RunModel.status == source,
                    RunModel.lifecycle_version == expected_version,
                    RunModel.lease_owner.is_(None),
                    RunModel.lease_token.is_(None),
                )
                .values(status="cancelled", lifecycle_version=RunModel.lifecycle_version + 1)
                .returning(RunModel)
            )
            if model is None:
                self._raise_unleased_mutation_failure(run_id, expected_version)
            self._append_lifecycle_event(
                model.run_id,
                previous_state=source,
                state="cancelled",
                occurred_at=self._database_clock(),
                evidence=evidence,
            )
        return self._run_record(model)

    def requeue_expired_run(
        self,
        run_id: str,
        *,
        expected_version: int,
        evidence: LifecycleEvidence,
    ) -> RunRecord:
        """Fence and requeue one expired active Run; no scheduler is implied."""
        _require_expected_version(expected_version)
        current = self._fresh_run(run_id)
        source = parse_run_status(current.status)
        require_recovery_transition(source, "queued")
        with self._session.begin_nested():
            model = self._session.scalar(
                update(RunModel)
                .where(
                    RunModel.run_id == run_id,
                    RunModel.status == source,
                    RunModel.lifecycle_version == expected_version,
                    RunModel.lease_expires_at <= func.clock_timestamp(),
                )
                .values(
                    status="queued",
                    lifecycle_version=RunModel.lifecycle_version + 1,
                    lease_owner=None,
                    lease_token=None,
                    lease_expires_at=None,
                    heartbeat_at=None,
                )
                .returning(RunModel)
            )
            if model is None:
                refreshed = self._fresh_run(run_id)
                if refreshed.lifecycle_version != expected_version:
                    raise LifecycleConflictError(
                        f"run {run_id!r} lifecycle version is now "
                        f"{refreshed.lifecycle_version}, expected {expected_version}"
                    )
                raise LeaseNotExpiredError(f"run {run_id!r} lease has not expired")
            self._append_lifecycle_event(
                model.run_id,
                previous_state=source,
                state="queued",
                occurred_at=self._database_clock(),
                evidence=evidence,
            )
        return self._run_record(model)

    def append_event(self, event: RunEvent) -> RunEventRecord:
        document = event.to_dict()
        _validate_jsonb_persistence_profile(document, "run event")
        event_id = cast(str, document["event_id"])
        run_id = cast(str, document["run_id"])
        # Serializing all evidence appends on the Run row makes the bounded
        # lifecycle MAX(sequence)+1 allocator safe relative to caller-sequenced
        # Event v0 appends. This does not allocate ordinary event sequences.
        run_exists = self._session.scalar(
            select(RunModel.run_id).where(RunModel.run_id == run_id).with_for_update()
        )
        if run_exists is None:
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

    def append_event_allocated(
        self, run_id: str, event_factory: Callable[[int], RunEvent]
    ) -> RunEventRecord:
        """Allocate and append one Event v0 while serializing on the Run row.

        The sequence is never exposed without its event append. Lifecycle and
        tool producers therefore cannot abandon a reservation or separate
        allocation from persistence. Caller-owned transaction semantics remain
        unchanged.
        """
        run_exists = self._session.scalar(
            select(RunModel.run_id).where(RunModel.run_id == run_id).with_for_update()
        )
        if run_exists is None:
            raise ReferenceNotFoundError(f"run {run_id!r} does not exist")
        current = self._session.scalar(
            select(func.coalesce(func.max(RunEventModel.sequence), 0)).where(
                RunEventModel.run_id == run_id
            )
        )
        sequence = cast(int, current) + 1
        if sequence > 9_007_199_254_740_991:
            raise PersistenceIntegrityError(
                f"run {run_id!r} exhausted the Event v0 safe-integer sequence range"
            )
        event = event_factory(sequence)
        document = event.to_dict()
        if document["run_id"] != run_id or document["sequence"] != sequence:
            raise PersistenceIntegrityError(
                "allocated Event factory changed the authoritative run or sequence"
            )
        return self.append_event(event)

    def database_time(self) -> datetime:
        """Return PostgreSQL wall time for lease-sensitive orchestration."""
        value = self._session.scalar(select(func.clock_timestamp()))
        if value is None:
            raise PersistenceIntegrityError("PostgreSQL did not return its current timestamp")
        return cast(datetime, value)

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
        run = self._fresh_run(run_id)
        if parse_run_status(run.status) not in TERMINAL_STATUSES:
            raise PersistenceIntegrityError(
                f"final report cannot be stored while run {run_id!r} is {run.status!r}"
            )
        if document["run_status"] != run.status:
            raise PersistenceIntegrityError("report run_status does not match the terminal run")
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

    def _insert_company_entities(self, run_id: str, document: dict[str, object]) -> None:
        customers = cast(list[dict[str, object]], document["customers"])
        orders = cast(list[dict[str, object]], document["orders"])
        shipments = cast(list[dict[str, object]], document["shipments"])
        payments = cast(list[dict[str, object]], document["payments"])
        refunds = cast(list[dict[str, object]], document["refunds"])
        tickets = cast(list[dict[str, object]], document["support_tickets"])
        self._session.add_all(
            CompanyCustomerModel(
                run_id=run_id,
                customer_id=cast(str, row["customer_id"]),
                name=cast(str, row["name"]),
                email=cast(str, row["email"]),
                status=cast(str, row["status"]),
            )
            for row in customers
        )
        self._session.flush()
        self._session.add_all(
            CompanyOrderModel(
                run_id=run_id,
                order_id=cast(str, row["order_id"]),
                customer_id=cast(str, row["customer_id"]),
                status=cast(str, row["status"]),
                currency=cast(str, row["currency"]),
                total_minor=cast(int, row["total_minor"]),
                placed_at=_timestamp(cast(str, row["placed_at"])),
            )
            for row in orders
        )
        self._session.flush()
        self._session.add_all(
            CompanyShipmentModel(
                run_id=run_id,
                shipment_id=cast(str, row["shipment_id"]),
                order_id=cast(str, row["order_id"]),
                status=cast(str, row["status"]),
                carrier=cast(str, row["carrier"]),
                tracking_number=cast(str, row["tracking_number"]),
                updated_at=_timestamp(cast(str, row["updated_at"])),
            )
            for row in shipments
        )
        self._session.add_all(
            CompanyPaymentModel(
                run_id=run_id,
                payment_id=cast(str, row["payment_id"]),
                order_id=cast(str, row["order_id"]),
                status=cast(str, row["status"]),
                currency=cast(str, row["currency"]),
                amount_minor=cast(int, row["amount_minor"]),
                captured_at=_timestamp(cast(str, row["captured_at"])),
            )
            for row in payments
        )
        self._session.flush()
        self._session.add_all(
            CompanyRefundModel(
                run_id=run_id,
                refund_id=cast(str, row["refund_id"]),
                payment_id=cast(str, row["payment_id"]),
                order_id=cast(str, row["order_id"]),
                status=cast(str, row["status"]),
                amount_minor=cast(int, row["amount_minor"]),
                reason=cast(str, row["reason"]),
                created_at=_timestamp(cast(str, row["created_at"])),
            )
            for row in refunds
        )
        self._session.add_all(
            CompanySupportTicketModel(
                run_id=run_id,
                ticket_id=cast(str, row["ticket_id"]),
                customer_id=cast(str, row["customer_id"]),
                order_id=cast(str, row["order_id"]),
                status=cast(str, row["status"]),
                subject=cast(str, row["subject"]),
                note=cast(str, row["note"]),
                updated_at=_timestamp(cast(str, row["updated_at"])),
            )
            for row in tickets
        )

    def _company_state_record(self, state: RunCompanyStateModel) -> SyntheticCompanyState:
        run_id = state.run_id
        customers = self._session.scalars(
            select(CompanyCustomerModel)
            .where(CompanyCustomerModel.run_id == run_id)
            .order_by(CompanyCustomerModel.customer_id)
        )
        orders = self._session.scalars(
            select(CompanyOrderModel)
            .where(CompanyOrderModel.run_id == run_id)
            .order_by(CompanyOrderModel.order_id)
        )
        shipments = self._session.scalars(
            select(CompanyShipmentModel)
            .where(CompanyShipmentModel.run_id == run_id)
            .order_by(CompanyShipmentModel.shipment_id)
        )
        payments = self._session.scalars(
            select(CompanyPaymentModel)
            .where(CompanyPaymentModel.run_id == run_id)
            .order_by(CompanyPaymentModel.payment_id)
        )
        refunds = self._session.scalars(
            select(CompanyRefundModel)
            .where(CompanyRefundModel.run_id == run_id)
            .order_by(CompanyRefundModel.refund_id)
        )
        tickets = self._session.scalars(
            select(CompanySupportTicketModel)
            .where(CompanySupportTicketModel.run_id == run_id)
            .order_by(CompanySupportTicketModel.ticket_id)
        )
        return SyntheticCompanyState(
            run_id=run_id,
            fixture=RevisionReference(
                state.fixture_id, state.fixture_revision, state.fixture_digest
            ),
            reference_time=state.reference_time,
            customers=tuple(
                CompanyCustomer(row.customer_id, row.name, row.email, row.status)
                for row in customers
            ),
            orders=tuple(
                CompanyOrder(
                    row.order_id,
                    row.customer_id,
                    row.status,
                    row.currency,
                    row.total_minor,
                    row.placed_at,
                )
                for row in orders
            ),
            shipments=tuple(
                CompanyShipment(
                    row.shipment_id,
                    row.order_id,
                    row.status,
                    row.carrier,
                    row.tracking_number,
                    row.updated_at,
                )
                for row in shipments
            ),
            payments=tuple(
                CompanyPayment(
                    row.payment_id,
                    row.order_id,
                    row.status,
                    row.currency,
                    row.amount_minor,
                    row.captured_at,
                )
                for row in payments
            ),
            refunds=tuple(
                CompanyRefund(
                    row.refund_id,
                    row.payment_id,
                    row.order_id,
                    row.status,
                    row.amount_minor,
                    row.reason,
                    row.created_at,
                )
                for row in refunds
            ),
            support_tickets=tuple(
                CompanySupportTicket(
                    row.ticket_id,
                    row.customer_id,
                    row.order_id,
                    row.status,
                    row.subject,
                    row.note,
                    row.updated_at,
                )
                for row in tickets
            ),
        )

    def _same_fixture_or_conflict(
        self, model: FixtureRevisionModel, fixture: Fixture
    ) -> FixtureRevisionRecord:
        if (
            model.canonical_digest != fixture.digest
            or model.canonical_document != fixture.to_dict()
        ):
            raise RevisionConflictError(
                f"fixture revision {(model.fixture_id, model.revision)!r} has different content"
            )
        return self._fixture_record(model)

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

    def _append_lifecycle_event(
        self,
        run_id: str,
        *,
        previous_state: RunStatus,
        state: RunStatus,
        occurred_at: datetime,
        evidence: LifecycleEvidence,
    ) -> None:
        producer: dict[str, object] = {"component": evidence.producer_component}
        if evidence.producer_instance_id is not None:
            producer["instance_id"] = evidence.producer_instance_id
        payload: dict[str, object] = {
            "state": state,
            "previous_state": previous_state,
        }
        if evidence.reason_code is not None:
            payload["reason_code"] = evidence.reason_code

        def event_factory(sequence: int) -> RunEvent:
            document: dict[str, object] = {
                "schema_version": "chaosagent.run-event/v0",
                "event_id": evidence.event_id,
                "run_id": run_id,
                "sequence": sequence,
                "occurred_at": _event_timestamp(occurred_at),
                "recorded_at": _event_timestamp(occurred_at),
                "event_type": "run.lifecycle",
                "producer": producer,
                "correlation_id": evidence.correlation_id or run_id,
                "payload": payload,
                "payload_digest": digest_payload_v0(payload),
            }
            if evidence.causation_event_id is not None:
                document["causation_event_id"] = evidence.causation_event_id
            return loads_run_event(json.dumps(document))

        self.append_event_allocated(run_id, event_factory)

    def _raise_lease_mutation_failure(
        self, lease: LeaseIdentity, expected_version: int
    ) -> NoReturn:
        current = self._fresh_run(lease.run_id)
        if (
            current.status not in ACTIVE_STATUSES
            or current.lease_owner != lease.worker_id
            or current.lease_token != lease.lease_token
            or current.attempt != lease.attempt
        ):
            raise StaleLeaseError(
                f"worker {lease.worker_id!r} no longer owns run {lease.run_id!r} "
                f"attempt {lease.attempt}"
            )
        if current.lease_expires_at is None or current.lease_expires_at <= self._database_clock():
            raise LeaseExpiredError(f"run {lease.run_id!r} lease has expired")
        if current.lifecycle_version != expected_version:
            raise LifecycleConflictError(
                f"run {lease.run_id!r} lifecycle version is now "
                f"{current.lifecycle_version}, expected {expected_version}"
            )
        raise LifecycleConflictError(f"run {lease.run_id!r} lifecycle mutation lost its CAS")

    def _raise_unleased_mutation_failure(self, run_id: str, expected_version: int) -> NoReturn:
        current = self._fresh_run(run_id)
        if current.lifecycle_version != expected_version:
            raise LifecycleConflictError(
                f"run {run_id!r} lifecycle version is now "
                f"{current.lifecycle_version}, expected {expected_version}"
            )
        raise LifecycleConflictError(f"run {run_id!r} is no longer eligible for mutation")

    def _fresh_run(self, run_id: str) -> RunModel:
        model = self._session.scalar(
            select(RunModel)
            .where(RunModel.run_id == run_id)
            .execution_options(populate_existing=True)
        )
        if model is None:
            raise ReferenceNotFoundError(f"run {run_id!r} does not exist")
        return model

    def _database_clock(self) -> datetime:
        return self.database_time()

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
    def _fixture_record(model: FixtureRevisionModel) -> FixtureRevisionRecord:
        try:
            fixture = loads_fixture(_json_text(model.canonical_document))
        except FixtureValidationError as error:
            raise PersistenceIntegrityError(
                "stored fixture document violates its contract"
            ) from error
        if fixture.digest != model.canonical_digest:
            raise PersistenceIntegrityError("stored fixture document does not match its digest")
        return FixtureRevisionRecord(fixture, model.created_at, model.created_by)

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
        fixture = None
        if (
            model.fixture_id is not None
            and model.fixture_revision is not None
            and model.fixture_digest is not None
        ):
            fixture = RevisionReference(
                model.fixture_id, model.fixture_revision, model.fixture_digest
            )
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
            fixture=fixture,
            status=parse_run_status(model.status),
            lifecycle_version=model.lifecycle_version,
            lease_owner=model.lease_owner,
            lease_token=model.lease_token,
            lease_expires_at=model.lease_expires_at,
            heartbeat_at=model.heartbeat_at,
            attempt=model.attempt,
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


def _require_expected_version(value: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError("expected_version must be a non-negative integer")


def _require_lease_duration(value: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= 86_400:
        raise ValueError("lease_duration_seconds must be between 1 and 86400")


def _validate_lease_identity(lease: LeaseIdentity) -> None:
    _require_identifier(lease.run_id, "lease.run_id")
    _require_identifier(lease.worker_id, "lease.worker_id")
    _require_identifier(lease.lease_token, "lease.lease_token")
    if isinstance(lease.attempt, bool) or lease.attempt < 1:
        raise ValueError("lease.attempt must be a positive integer")


def _event_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise PersistenceIntegrityError("database timestamp unexpectedly lacks a timezone")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
