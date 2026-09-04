"""Typed transactional repository for immutable ChaosAgent contract documents."""

from __future__ import annotations

import hashlib
import json
import re
import secrets
from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Literal, NoReturn, cast

import rfc8785
from chaosagent_agent_configurations import (
    AgentConfiguration,
    AgentConfigurationValidationError,
    loads_agent_configuration,
)
from chaosagent_evidence import (
    EvidenceValidationError,
    RunEvent,
    RunReport,
    digest_payload_v0,
    loads_run_event,
    loads_run_report,
)
from chaosagent_fixtures import Fixture, FixtureValidationError, loads_fixture
from chaosagent_policies import Policy, PolicyValidationError, loads_policy
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
    ApprovalRequestModel,
    ApprovalResolutionModel,
    CampaignPlanModel,
    CampaignTrialMembershipModel,
    CompanyCustomerModel,
    CompanyEffectModel,
    CompanyOrderModel,
    CompanyPaymentModel,
    CompanyRefundModel,
    CompanyShipmentModel,
    CompanySupportTicketModel,
    ExecutionCheckpointModel,
    FixtureRevisionModel,
    PolicyRevisionModel,
    PostCommitAcknowledgementModel,
    RunCompanyStateModel,
    RunEventModel,
    RunModel,
    RunReportModel,
    ScenarioRevisionModel,
)

_IDENTIFIER_RE = re.compile(IDENTIFIER_CHECK)
_REVISION_RE = re.compile(REVISION_CHECK)
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_CATALOG_ID_RE = re.compile(r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$")


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


class IdempotencyConflictError(PersistenceConflictError):
    """Raised when an idempotency identity is reused for a different request."""


class BusinessRuleViolationError(PersistenceError):
    """Raised when a valid mutation request violates synthetic business state."""


class ApprovalConflictError(PersistenceConflictError):
    """Raised when an approval identity is reused for another frozen request."""


class ApprovalAlreadyResolvedError(PersistenceConflictError):
    """Raised when an immutable approval resolution already exists."""


class CheckpointConflictError(PersistenceConflictError):
    """Raised when a stale executor attempts to replace a newer checkpoint."""


class CampaignMembershipConflictError(PersistenceConflictError):
    """Raised when a Run or Campaign index already has another assignment."""


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
class PolicyRevisionRecord:
    policy: Policy
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
    configuration: AgentConfiguration | None
    created_at: datetime
    created_by: str


@dataclass(frozen=True, slots=True)
class RunRecord:
    run_id: str
    scenario: RevisionReference
    agent_configuration: RevisionReference
    fixture: RevisionReference | None
    fault_seed: int | None
    fault_plan_digest: str | None
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
class CampaignPlanRecord:
    campaign_id: str
    arm: Literal["baseline", "faulted"]
    planned_trials: int
    scenario: RevisionReference
    agent_configuration: RevisionReference
    selected_fault_ids: tuple[str, ...]
    fault_plan_digest: str
    assignments: tuple[tuple[int, str], ...]
    canonical_digest: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class CampaignTrialMembershipRecord:
    run_id: str
    campaign_id: str
    campaign_plan_digest: str
    trial_index: int
    scenario: RevisionReference
    agent_configuration: RevisionReference
    membership_digest: str
    created_at: datetime


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


@dataclass(frozen=True, slots=True)
class CompanyEffect:
    run_id: str
    tool_id: str
    contract_version: str
    idempotency_key_digest: str
    request_digest: str
    effect_id: str
    effect_kind: str
    subject_type: str
    subject_id: str
    result: Mapping[str, object]
    logical_call_id: str
    first_attempt_id: str
    lease_attempt: int
    created_at: datetime
    newly_applied: bool


@dataclass(frozen=True, slots=True)
class PostCommitAcknowledgement:
    """Immutable effect-commit marker used to finish or replay an acknowledgement."""

    run_id: str
    attempt_id: str
    logical_call_id: str
    attempt_number: int
    call_ordinal: int
    tool_id: str
    contract_version: str
    idempotency_key_digest: str
    request_digest: str
    arguments_digest: str
    effect_id: str
    lease_attempt: int
    request_event_id: str
    state_evidence_event_id: str
    policy_decision_event_id: str
    approval_id: str | None
    fault_id: str
    activation_id: str
    timeout_duration_ms: int
    matched_event_id: str
    applied_event_id: str
    result_event_id: str
    observed_event_id: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ApprovalRequestRecord:
    approval_id: str
    run_id: str
    scenario: RevisionReference
    policy: RevisionReference
    tool_id: str
    contract_version: str
    request_digest: str
    idempotency_key_digest: str
    arguments: Mapping[str, object]
    logical_call_id: str
    requested_attempt_id: str
    lease_attempt: int
    decision_id: str
    decision_event_id: str
    request_event_id: str
    status: Literal["pending", "approved", "denied"]
    created_at: datetime
    resolved_at: datetime | None
    actor_id: str | None
    resolution_event_id: str | None
    newly_created: bool = False


@dataclass(frozen=True, slots=True)
class ExecutionCheckpointRecord:
    run_id: str
    checkpoint_version: int
    lease_attempt: int
    last_event_sequence: int
    document: Mapping[str, object]
    document_digest: str
    updated_at: datetime


def create_postgres_engine(database_url: str, *, echo: bool = False) -> Engine:
    """Create a SQLAlchemy engine and fail closed for non-PostgreSQL URLs."""
    engine = create_engine(database_url, echo=echo, pool_pre_ping=True)
    if engine.dialect.name != "postgresql":
        engine.dispose()
        raise ValueError("ChaosAgent persistence requires a PostgreSQL database URL")
    return engine


def _json_text(document: dict[str, object]) -> str:
    return json.dumps(document, ensure_ascii=False, separators=(",", ":"))


def _freeze_json_value(value: object) -> object:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json_value(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_json_value(item) for item in value)
    return value


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


def approval_identity(
    *,
    run_id: str,
    scenario_id: str,
    scenario_revision: str,
    scenario_digest: str,
    policy_id: str,
    policy_revision: str,
    policy_digest: str,
    tool_id: str,
    contract_version: str,
    request_digest: str,
    idempotency_key_digest: str,
) -> str:
    """Return the stable identity for one fully frozen approval request."""
    binding = {
        "run_id": run_id,
        "scenario_id": scenario_id,
        "scenario_revision": scenario_revision,
        "scenario_digest": scenario_digest,
        "policy_id": policy_id,
        "policy_revision": policy_revision,
        "policy_digest": policy_digest,
        "tool_id": tool_id,
        "contract_version": contract_version,
        "request_digest": request_digest,
        "idempotency_key_digest": idempotency_key_digest,
    }
    encoded = json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()
    return f"approval-{hashlib.sha256(encoded).hexdigest()}"


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


def validate_jsonb_persistence_profile(value: object, contract: str) -> None:
    """Fail before SQL when a JSON value is not representable by PostgreSQL JSONB."""
    _validate_jsonb_persistence_profile(value, contract)


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

    def insert_policy_revision(self, policy: Policy, *, created_by: str) -> PolicyRevisionRecord:
        document = policy.to_dict()
        _validate_jsonb_persistence_profile(document, "policy")
        policy_id = cast(str, document["policy_id"])
        revision = cast(str, document["revision"])
        existing = self._session.get(PolicyRevisionModel, (policy_id, revision))
        if existing is not None:
            return self._same_policy_or_conflict(existing, policy)
        model = PolicyRevisionModel(
            policy_id=policy_id,
            revision=revision,
            schema_version=cast(str, document["schema_version"]),
            canonical_document=document,
            canonical_digest=policy.digest,
            created_by=created_by,
        )
        try:
            with self._session.begin_nested():
                self._session.add(model)
                self._session.flush()
        except IntegrityError as error:
            if _constraint_name(error) == "pk_policy_revisions":
                concurrent = self._session.get(PolicyRevisionModel, (policy_id, revision))
                if concurrent is not None:
                    return self._same_policy_or_conflict(concurrent, policy)
            _raise_integrity(error, "policy revision insert")
        return self._policy_record(model)

    def get_policy_revision(self, policy_id: str, revision: str) -> PolicyRevisionRecord | None:
        model = self._session.get(PolicyRevisionModel, (policy_id, revision))
        return None if model is None else self._policy_record(model)

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

    def insert_agent_configuration(
        self, configuration: AgentConfiguration, *, created_by: str
    ) -> AgentConfigurationRevisionRecord:
        document = configuration.to_dict()
        _validate_jsonb_persistence_profile(document, "agent configuration")
        identifier = cast(str, document["agent_configuration_id"])
        revision = cast(str, document["revision"])
        existing = self._session.get(AgentConfigurationRevisionModel, (identifier, revision))
        if existing is not None:
            record = self._agent_configuration_record(existing)
            if (
                record.reference.digest != configuration.digest
                or record.configuration is None
                or record.configuration.canonical_bytes != configuration.canonical_bytes
            ):
                raise RevisionConflictError(
                    f"agent configuration revision {(identifier, revision)!r} has different content"
                )
            return record
        model = AgentConfigurationRevisionModel(
            agent_configuration_id=identifier,
            revision=revision,
            digest=configuration.digest,
            schema_version=cast(str, document["schema_version"]),
            canonical_document=document,
            created_by=created_by,
        )
        try:
            with self._session.begin_nested():
                self._session.add(model)
                self._session.flush()
        except IntegrityError as error:
            if _constraint_name(error) == "pk_agent_configuration_revisions":
                concurrent = self._session.get(
                    AgentConfigurationRevisionModel, (identifier, revision)
                )
                if concurrent is not None:
                    record = self._agent_configuration_record(concurrent)
                    if (
                        record.reference.digest == configuration.digest
                        and record.configuration is not None
                        and record.configuration.canonical_bytes == configuration.canonical_bytes
                    ):
                        return record
                    raise RevisionConflictError(
                        "agent configuration revision "
                        f"{(identifier, revision)!r} has different content"
                    ) from error
            _raise_integrity(error, "agent configuration revision insert")
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
        scenario_document = self._scenario_record(scenario).scenario.to_dict()
        scenario_fixture = cast(
            dict[str, object],
            scenario_document["fixture"],
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
        scenario_policy = cast(dict[str, object], scenario_document["policy"])
        policy_key = (
            cast(str, scenario_policy["id"]),
            cast(str, scenario_policy["revision"]),
        )
        policy = self._session.get(PolicyRevisionModel, policy_key)
        if policy is None or policy.canonical_digest != scenario_policy["digest"]:
            raise ReferenceNotFoundError(
                "scenario policy reference "
                f"{(policy_key[0], policy_key[1], scenario_policy['digest'])!r} "
                "does not resolve to an immutable Policy revision"
            )
        self._policy_record(policy)
        agent = self._session.get(
            AgentConfigurationRevisionModel,
            (agent_configuration_id, agent_configuration_revision),
        )
        if agent is None:
            raise ReferenceNotFoundError(
                "agent configuration revision "
                f"{(agent_configuration_id, agent_configuration_revision)!r} does not exist"
            )
        agent_record = self._agent_configuration_record(agent)
        model = RunModel(
            run_id=run_id,
            scenario_id=scenario.scenario_id,
            scenario_revision=scenario.revision,
            scenario_digest=scenario.canonical_digest,
            agent_configuration_id=agent_record.reference.id,
            agent_configuration_revision=agent_record.reference.revision,
            agent_configuration_digest=agent_record.reference.digest,
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

    def create_campaign_plan(
        self,
        *,
        campaign_id: str,
        arm: Literal["baseline", "faulted"],
        selected_fault_ids: tuple[str, ...],
        fault_plan_digest: str,
        assignments: tuple[tuple[int, str], ...],
    ) -> CampaignPlanRecord:
        """Atomically freeze one Campaign plan while every assigned Run is queued.

        Run rows are locked in stable Run-ID order. The locks, plan, and all
        memberships remain in the caller-owned transaction.
        """
        _require_identifier(campaign_id, "campaign_id")
        _require_digest(fault_plan_digest)
        if not isinstance(arm, str) or arm not in {"baseline", "faulted"}:
            raise ValueError("arm must be baseline or faulted")
        if not assignments or tuple(index for index, _ in assignments) != tuple(
            range(len(assignments))
        ):
            raise ValueError("Campaign assignments must be contiguous from zero")
        run_ids = tuple(run_id for _, run_id in assignments)
        if len(set(run_ids)) != len(run_ids):
            raise ValueError("Campaign assignments contain duplicate Runs")
        for run_id in run_ids:
            _require_identifier(run_id, "run_id")
        if (
            any(not isinstance(item, str) for item in selected_fault_ids)
            or tuple(sorted(selected_fault_ids)) != selected_fault_ids
            or len(set(selected_fault_ids)) != len(selected_fault_ids)
            or any(_CATALOG_ID_RE.fullmatch(item) is None for item in selected_fault_ids)
        ):
            raise ValueError("selected fault IDs must be unique and canonically ordered")
        if (arm == "baseline" and selected_fault_ids) or (
            arm == "faulted" and not selected_fault_ids
        ):
            raise ValueError("Campaign arm contradicts selected faults")

        locked = tuple(
            self._session.scalars(
                select(RunModel)
                .where(RunModel.run_id.in_(sorted(run_ids)))
                .order_by(RunModel.run_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )
        if len(locked) != len(run_ids) or any(model.status != "queued" for model in locked):
            raise CampaignMembershipConflictError(
                "Campaign membership requires existing queued Runs"
            )
        by_id = {model.run_id: model for model in locked}
        first = by_id[run_ids[0]]
        if any(
            (
                model.scenario_id,
                model.scenario_revision,
                model.scenario_digest,
                model.agent_configuration_id,
                model.agent_configuration_revision,
                model.agent_configuration_digest,
            )
            != (
                first.scenario_id,
                first.scenario_revision,
                first.scenario_digest,
                first.agent_configuration_id,
                first.agent_configuration_revision,
                first.agent_configuration_digest,
            )
            for model in locked
        ):
            raise CampaignMembershipConflictError("Campaign Runs use incompatible frozen revisions")
        scenario = RevisionReference(
            first.scenario_id, first.scenario_revision, first.scenario_digest
        )
        agent = RevisionReference(
            first.agent_configuration_id,
            first.agent_configuration_revision,
            first.agent_configuration_digest,
        )
        document: dict[str, object] = {
            "schema_version": "chaosagent.campaign-plan/v0",
            "campaign_id": campaign_id,
            "arm": arm,
            "planned_trials": len(assignments),
            "scenario": {
                "id": scenario.id,
                "revision": scenario.revision,
                "digest": scenario.digest,
            },
            "agent_configuration": {
                "id": agent.id,
                "revision": agent.revision,
                "digest": agent.digest,
            },
            "selected_fault_ids": list(selected_fault_ids),
            "fault_plan_digest": fault_plan_digest,
            "assignments": [
                {"trial_index": index, "run_id": run_id} for index, run_id in assignments
            ],
        }
        canonical = rfc8785.dumps(cast(object, document))  # type: ignore[arg-type]
        plan_digest = "sha256:" + hashlib.sha256(canonical).hexdigest()
        existing = self._session.get(CampaignPlanModel, campaign_id)
        if existing is not None:
            return self._same_campaign_plan_or_conflict(existing, canonical, plan_digest)
        plan = CampaignPlanModel(
            campaign_id=campaign_id,
            arm=arm,
            planned_trials=len(assignments),
            scenario_id=scenario.id,
            scenario_revision=scenario.revision,
            scenario_digest=scenario.digest,
            agent_configuration_id=agent.id,
            agent_configuration_revision=agent.revision,
            agent_configuration_digest=agent.digest,
            selected_fault_ids=list(selected_fault_ids),
            fault_plan_digest=fault_plan_digest,
            schema_version="chaosagent.campaign-plan/v0",
            canonical_document=document,
            canonical_digest=plan_digest,
        )
        memberships = []
        for index, run_id in assignments:
            membership_document = {
                "campaign_plan_digest": plan_digest,
                "run_id": run_id,
                "trial_index": index,
            }
            membership_digest = (
                "sha256:"
                + hashlib.sha256(
                    rfc8785.dumps(cast(object, membership_document))  # type: ignore[arg-type]
                ).hexdigest()
            )
            memberships.append(
                CampaignTrialMembershipModel(
                    run_id=run_id,
                    campaign_id=campaign_id,
                    campaign_plan_digest=plan_digest,
                    trial_index=index,
                    scenario_id=scenario.id,
                    scenario_revision=scenario.revision,
                    scenario_digest=scenario.digest,
                    agent_configuration_id=agent.id,
                    agent_configuration_revision=agent.revision,
                    agent_configuration_digest=agent.digest,
                    membership_digest=membership_digest,
                )
            )
        try:
            with self._session.begin_nested():
                self._session.add(plan)
                self._session.add_all(memberships)
                self._session.flush()
        except IntegrityError as error:
            constraint = _constraint_name(error)
            if constraint in {
                "pk_campaign_plans",
                "pk_campaign_trial_memberships",
                "uq_campaign_memberships_campaign_index",
            }:
                concurrent = self._session.get(
                    CampaignPlanModel, campaign_id, populate_existing=True
                )
                if concurrent is not None:
                    return self._same_campaign_plan_or_conflict(concurrent, canonical, plan_digest)
                raise CampaignMembershipConflictError(
                    "Run or Campaign index already has another membership"
                ) from error
            _raise_integrity(error, "Campaign plan insert")
        return self._campaign_plan_record(plan)

    def get_campaign_plan(self, campaign_id: str) -> CampaignPlanRecord | None:
        model = self._session.scalar(
            select(CampaignPlanModel)
            .where(CampaignPlanModel.campaign_id == campaign_id)
            .execution_options(populate_existing=True)
        )
        return None if model is None else self._campaign_plan_record(model)

    def get_campaign_membership(self, run_id: str) -> CampaignTrialMembershipRecord | None:
        model = self._session.scalar(
            select(CampaignTrialMembershipModel)
            .where(CampaignTrialMembershipModel.run_id == run_id)
            .execution_options(populate_existing=True)
        )
        return None if model is None else self._campaign_membership_record(model)

    def bind_run_fault_plan(
        self,
        lease: LeaseIdentity,
        *,
        selected_fault_ids: tuple[str, ...],
        fault_plan_digest: str | None,
    ) -> RunRecord:
        """Bind the actual runtime fault plan and enforce Campaign assignment."""
        run = self.lock_current_lease(lease)
        membership = self.get_campaign_membership(run.run_id)
        effective_digest = fault_plan_digest
        if membership is not None:
            plan = self.get_campaign_plan(membership.campaign_id)
            if plan is None or plan.canonical_digest != membership.campaign_plan_digest:
                raise PersistenceIntegrityError("Campaign membership plan binding is corrupt")
            if selected_fault_ids != plan.selected_fault_ids:
                raise PersistenceIntegrityError("fault engine differs from Campaign assignment")
            if selected_fault_ids and fault_plan_digest != plan.fault_plan_digest:
                raise PersistenceIntegrityError(
                    "fault plan digest differs from Campaign assignment"
                )
            effective_digest = plan.fault_plan_digest
        if effective_digest is None:
            return run
        _require_digest(effective_digest)
        model = self._session.get(RunModel, run.run_id)
        if model is None:
            raise ReferenceNotFoundError("Run does not exist")
        if model.fault_plan_digest is None:
            model.fault_plan_digest = effective_digest
            self._session.flush()
        elif model.fault_plan_digest != effective_digest:
            raise PersistenceIntegrityError("Run fault plan binding is immutable")
        return self._run_record(model)

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

    def bind_run_fault_seed(self, lease: LeaseIdentity, run_seed: int) -> RunRecord:
        """Bind the deterministic fault seed once under the current Run lease.

        The database trigger permits only NULL-to-value binding. Subsequent
        callers must present the exact same seed, so recovery can authenticate
        Issue #13 activation identities without trusting process memory.
        """
        if isinstance(run_seed, bool) or not isinstance(run_seed, int):
            raise ValueError("run_seed must be an exact integer")
        if not 0 <= run_seed <= 9_007_199_254_740_991:
            raise ValueError("run_seed must be a nonnegative JSON-safe integer")
        self.lock_current_lease(lease)
        model = self._fresh_run(lease.run_id)
        if model.fault_seed is None:
            with self._session.begin_nested():
                model.fault_seed = run_seed
                self._session.flush()
        elif model.fault_seed != run_seed:
            raise PersistenceIntegrityError("fault engine seed differs from the frozen Run seed")
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

    def _lock_company_mutation_run(self, run_id: str) -> None:
        _require_identifier(run_id, "run_id")
        locked = self._session.scalar(
            select(RunModel.run_id).where(RunModel.run_id == run_id).with_for_update()
        )
        if locked is None:
            raise ReferenceNotFoundError(f"run {run_id!r} does not exist")

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

    def get_company_payment(self, run_id: str, payment_id: str) -> CompanyPayment | None:
        """Read one Run-scoped payment without exposing mutable ORM state."""
        model = self._session.get(CompanyPaymentModel, (run_id, payment_id))
        if model is None:
            return None
        return CompanyPayment(
            model.payment_id,
            model.order_id,
            model.status,
            model.currency,
            model.amount_minor,
            model.captured_at,
        )

    def get_company_support_ticket(
        self, run_id: str, ticket_id: str
    ) -> CompanySupportTicket | None:
        """Read one Run-scoped support ticket without exposing mutable ORM state."""
        model = self._session.get(CompanySupportTicketModel, (run_id, ticket_id))
        if model is None:
            return None
        return CompanySupportTicket(
            model.ticket_id,
            model.customer_id,
            model.order_id,
            model.status,
            model.subject,
            model.note,
            model.updated_at,
        )

    def get_company_effect(
        self,
        run_id: str,
        tool_id: str,
        contract_version: str,
        idempotency_key_digest: str,
    ) -> CompanyEffect | None:
        """Fetch an immutable effect by its complete idempotency identity."""
        model = self._session.get(
            CompanyEffectModel,
            (run_id, tool_id, contract_version, idempotency_key_digest),
            populate_existing=True,
        )
        return None if model is None else self._company_effect_record(model, newly_applied=False)

    def list_company_effects(self, run_id: str) -> tuple[CompanyEffect, ...]:
        """Read all immutable effects for one Run in stable effect-ID order."""
        _require_identifier(run_id, "run_id")
        models = self._session.scalars(
            select(CompanyEffectModel)
            .where(CompanyEffectModel.run_id == run_id)
            .order_by(CompanyEffectModel.effect_id)
            .execution_options(populate_existing=True)
        )
        return tuple(self._company_effect_record(model, newly_applied=False) for model in models)

    def get_post_commit_acknowledgement(
        self, run_id: str, attempt_id: str
    ) -> PostCommitAcknowledgement | None:
        """Fetch one immutable post-commit recovery marker."""
        model = self._session.get(
            PostCommitAcknowledgementModel, (run_id, attempt_id), populate_existing=True
        )
        return None if model is None else self._post_commit_acknowledgement_record(model)

    def create_post_commit_acknowledgement(
        self,
        *,
        run_id: str,
        attempt_id: str,
        logical_call_id: str,
        attempt_number: int,
        call_ordinal: int,
        tool_id: str,
        contract_version: str,
        idempotency_key_digest: str,
        request_digest: str,
        arguments_digest: str,
        effect_id: str,
        lease_attempt: int,
        request_event_id: str,
        state_evidence_event_id: str,
        policy_decision_event_id: str,
        approval_id: str | None,
        fault_id: str,
        activation_id: str,
        timeout_duration_ms: int,
        matched_event_id: str,
        applied_event_id: str,
        result_event_id: str,
        observed_event_id: str,
    ) -> PostCommitAcknowledgement:
        """Insert the immutable marker in the same transaction as its effect evidence."""
        planned_event_ids = (
            matched_event_id,
            applied_event_id,
            result_event_id,
            observed_event_id,
        )
        if len(set(planned_event_ids)) != len(planned_event_ids):
            raise PersistenceIntegrityError(
                "post-commit acknowledgement event identities must be distinct"
            )
        existing = self.get_post_commit_acknowledgement(run_id, attempt_id)
        expected = (
            logical_call_id,
            attempt_number,
            call_ordinal,
            tool_id,
            contract_version,
            idempotency_key_digest,
            request_digest,
            arguments_digest,
            effect_id,
            lease_attempt,
            request_event_id,
            state_evidence_event_id,
            policy_decision_event_id,
            approval_id,
            fault_id,
            activation_id,
            timeout_duration_ms,
            matched_event_id,
            applied_event_id,
            result_event_id,
            observed_event_id,
        )
        if existing is not None:
            actual = (
                existing.logical_call_id,
                existing.attempt_number,
                existing.call_ordinal,
                existing.tool_id,
                existing.contract_version,
                existing.idempotency_key_digest,
                existing.request_digest,
                existing.arguments_digest,
                existing.effect_id,
                existing.lease_attempt,
                existing.request_event_id,
                existing.state_evidence_event_id,
                existing.policy_decision_event_id,
                existing.approval_id,
                existing.fault_id,
                existing.activation_id,
                existing.timeout_duration_ms,
                existing.matched_event_id,
                existing.applied_event_id,
                existing.result_event_id,
                existing.observed_event_id,
            )
            if actual != expected:
                raise PersistenceIntegrityError(
                    "post-commit acknowledgement identity maps to different content"
                )
            return existing
        model = PostCommitAcknowledgementModel(
            run_id=run_id,
            attempt_id=attempt_id,
            logical_call_id=logical_call_id,
            attempt_number=attempt_number,
            call_ordinal=call_ordinal,
            tool_id=tool_id,
            contract_version=contract_version,
            idempotency_key_digest=idempotency_key_digest,
            request_digest=request_digest,
            arguments_digest=arguments_digest,
            effect_id=effect_id,
            lease_attempt=lease_attempt,
            request_event_id=request_event_id,
            state_evidence_event_id=state_evidence_event_id,
            policy_decision_event_id=policy_decision_event_id,
            approval_id=approval_id,
            fault_id=fault_id,
            activation_id=activation_id,
            timeout_duration_ms=timeout_duration_ms,
            matched_event_id=matched_event_id,
            applied_event_id=applied_event_id,
            result_event_id=result_event_id,
            observed_event_id=observed_event_id,
        )
        try:
            with self._session.begin_nested():
                self._session.add(model)
                self._session.flush()
                self._session.refresh(model)
        except IntegrityError as error:
            raise PersistenceConflictError(
                "post-commit acknowledgement marker conflicts with durable state"
            ) from error
        return self._post_commit_acknowledgement_record(model)

    def verify_company_effect(
        self,
        claimed: CompanyEffect,
        *,
        expected_arguments: Mapping[str, object],
    ) -> CompanyEffect:
        """Independently verify the ledger and transaction-visible business projection."""
        model = self._session.get(
            CompanyEffectModel,
            (
                claimed.run_id,
                claimed.tool_id,
                claimed.contract_version,
                claimed.idempotency_key_digest,
            ),
            populate_existing=True,
        )
        if model is None:
            raise PersistenceIntegrityError("mutation effect ledger row is missing")
        trusted = self._company_effect_record(model, newly_applied=claimed.newly_applied)
        expected_request_digest = digest_payload_v0(
            {
                "tool_id": trusted.tool_id,
                "contract_version": trusted.contract_version,
                "arguments": dict(expected_arguments),
            }
        )
        key = expected_arguments.get("idempotency_key")
        if (
            not isinstance(key, str)
            or trusted.request_digest != expected_request_digest
            or trusted.idempotency_key_digest != digest_payload_v0(key)
        ):
            raise PersistenceIntegrityError("mutation effect request identity is inconsistent")
        if (
            trusted.run_id,
            trusted.tool_id,
            trusted.contract_version,
            trusted.idempotency_key_digest,
            trusted.request_digest,
            trusted.effect_id,
            trusted.effect_kind,
            trusted.subject_type,
            trusted.subject_id,
            dict(trusted.result),
            trusted.logical_call_id,
            trusted.first_attempt_id,
            trusted.lease_attempt,
            trusted.created_at,
        ) != (
            claimed.run_id,
            claimed.tool_id,
            claimed.contract_version,
            claimed.idempotency_key_digest,
            claimed.request_digest,
            claimed.effect_id,
            claimed.effect_kind,
            claimed.subject_type,
            claimed.subject_id,
            dict(claimed.result),
            claimed.logical_call_id,
            claimed.first_attempt_id,
            claimed.lease_attempt,
            claimed.created_at,
        ):
            raise PersistenceIntegrityError("mutation handler effect disagrees with the ledger")

        result = trusted.result
        if trusted.tool_id == "payments.refund":
            expected = (
                expected_arguments.get("order_id"),
                expected_arguments.get("payment_id"),
                expected_arguments.get("amount_minor"),
            )
            actual = (result["order_id"], result["payment_id"], result["amount_minor"])
            if actual != expected:
                raise PersistenceIntegrityError("refund effect disagrees with its request")
            refund = self._session.get(
                CompanyRefundModel, (trusted.run_id, trusted.subject_id), populate_existing=True
            )
            payment = self._session.get(
                CompanyPaymentModel,
                (trusted.run_id, cast(str, result["payment_id"])),
                populate_existing=True,
            )
            total_refunded = self._session.scalar(
                select(func.coalesce(func.sum(CompanyRefundModel.amount_minor), 0)).where(
                    CompanyRefundModel.run_id == trusted.run_id,
                    CompanyRefundModel.payment_id == result["payment_id"],
                    CompanyRefundModel.status == "succeeded",
                )
            )
            expected_payment_status = (
                "refunded"
                if payment is not None and cast(int, total_refunded) == payment.amount_minor
                else "partially_refunded"
            )
            if (
                refund is None
                or payment is None
                or not 1 <= cast(int, total_refunded) <= payment.amount_minor
                or (
                    refund.origin,
                    refund.effect_id,
                    refund.refund_id,
                    refund.order_id,
                    refund.payment_id,
                    refund.amount_minor,
                    refund.status,
                    refund.reason,
                    payment.order_id,
                    payment.currency,
                    payment.status,
                )
                != (
                    "mutation",
                    trusted.effect_id,
                    result["refund_id"],
                    result["order_id"],
                    result["payment_id"],
                    result["amount_minor"],
                    result["status"],
                    expected_arguments.get("reason"),
                    result["order_id"],
                    result["currency"],
                    expected_payment_status,
                )
            ):
                raise PersistenceIntegrityError("refund effect has no matching business projection")
        elif trusted.tool_id == "support.update_ticket":
            if (
                result["ticket_id"],
                result["status"],
                result["note"],
            ) != (
                expected_arguments.get("ticket_id"),
                expected_arguments.get("status"),
                expected_arguments.get("note"),
            ):
                raise PersistenceIntegrityError("ticket effect disagrees with its request")
            ticket = self._session.get(
                CompanySupportTicketModel,
                (trusted.run_id, trusted.subject_id),
                populate_existing=True,
            )
            if ticket is None:
                raise PersistenceIntegrityError("ticket effect has no business subject")
            if trusted.newly_applied and (
                ticket.last_effect_id,
                ticket.status,
                ticket.note,
                _event_timestamp(ticket.updated_at),
            ) != (
                trusted.effect_id,
                result["status"],
                result["note"],
                result["updated_at"],
            ):
                raise PersistenceIntegrityError("new ticket effect is not the current projection")
        else:
            raise PersistenceIntegrityError("stored mutation tool is unsupported")
        return trusted

    def apply_refund_effect(
        self,
        run_id: str,
        *,
        contract_version: str,
        idempotency_key_digest: str,
        request_digest: str,
        order_id: str,
        payment_id: str,
        amount_minor: int,
        reason: str,
        logical_call_id: str,
        attempt_id: str,
        lease_attempt: int,
    ) -> CompanyEffect:
        """Serialize a refund mutation on its Run without owning the outer transaction."""
        try:
            with self._session.begin_nested():
                self._lock_company_mutation_run(run_id)
                return self._apply_refund_effect_locked(
                    run_id,
                    contract_version=contract_version,
                    idempotency_key_digest=idempotency_key_digest,
                    request_digest=request_digest,
                    order_id=order_id,
                    payment_id=payment_id,
                    amount_minor=amount_minor,
                    reason=reason,
                    logical_call_id=logical_call_id,
                    attempt_id=attempt_id,
                    lease_attempt=lease_attempt,
                )
        except IntegrityError as error:
            raise PersistenceIntegrityError("refund effect could not be persisted") from error

    def _apply_refund_effect_locked(
        self,
        run_id: str,
        *,
        contract_version: str,
        idempotency_key_digest: str,
        request_digest: str,
        order_id: str,
        payment_id: str,
        amount_minor: int,
        reason: str,
        logical_call_id: str,
        attempt_id: str,
        lease_attempt: int,
    ) -> CompanyEffect:
        """Create or replay one idempotent refund while locking its payment row."""
        if type(amount_minor) is not int or not 1 <= amount_minor <= 9_007_199_254_740_991:
            raise ValueError("amount_minor must be an exact positive safe integer")
        existing = self.get_company_effect(
            run_id, "payments.refund", contract_version, idempotency_key_digest
        )
        if existing is not None:
            if existing.request_digest != request_digest:
                raise IdempotencyConflictError(
                    "idempotency key is already bound to a different payments.refund request"
                )
            return existing

        payment = self._session.scalar(
            select(CompanyPaymentModel)
            .where(
                CompanyPaymentModel.run_id == run_id,
                CompanyPaymentModel.payment_id == payment_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if payment is None or payment.order_id != order_id:
            raise ReferenceNotFoundError("Run-local order/payment reference was not found")
        if payment.status not in {"captured", "partially_refunded"}:
            raise BusinessRuleViolationError("payment is not refundable")
        refunded = self._session.scalar(
            select(func.coalesce(func.sum(CompanyRefundModel.amount_minor), 0)).where(
                CompanyRefundModel.run_id == run_id,
                CompanyRefundModel.payment_id == payment_id,
                CompanyRefundModel.status == "succeeded",
            )
        )
        total_after = cast(int, refunded) + amount_minor
        if total_after > payment.amount_minor:
            raise BusinessRuleViolationError("refund would exceed captured payment amount")

        effect_id = _effect_identity(
            run_id, "payments.refund", contract_version, idempotency_key_digest
        )
        refund_id = f"RFD-{effect_id.removeprefix('effect-')}"
        applied_at = self.database_time()
        result: dict[str, object] = {
            "effect_id": effect_id,
            "refund_id": refund_id,
            "order_id": order_id,
            "payment_id": payment_id,
            "status": "succeeded",
            "amount_minor": amount_minor,
            "currency": payment.currency,
            "application": "newly_applied",
        }
        effect = CompanyEffectModel(
            run_id=run_id,
            tool_id="payments.refund",
            contract_version=contract_version,
            idempotency_key_digest=idempotency_key_digest,
            request_digest=request_digest,
            effect_id=effect_id,
            effect_kind="refund.created",
            subject_type="refund",
            subject_id=refund_id,
            effect_state="applied",
            result_document=result,
            logical_call_id=logical_call_id,
            first_attempt_id=attempt_id,
            lease_attempt=lease_attempt,
            created_at=applied_at,
        )
        self._session.add(effect)
        self._session.flush()
        self._session.add(
            CompanyRefundModel(
                run_id=run_id,
                refund_id=refund_id,
                payment_id=payment_id,
                order_id=order_id,
                status="succeeded",
                amount_minor=amount_minor,
                reason=reason,
                created_at=applied_at,
                origin="mutation",
                effect_id=effect_id,
            )
        )
        payment.status = "refunded" if total_after == payment.amount_minor else "partially_refunded"
        self._session.flush()
        return self._company_effect_record(effect, newly_applied=True)

    def apply_support_ticket_effect(
        self,
        run_id: str,
        *,
        contract_version: str,
        idempotency_key_digest: str,
        request_digest: str,
        ticket_id: str,
        status: str,
        note: str,
        logical_call_id: str,
        attempt_id: str,
        lease_attempt: int,
    ) -> CompanyEffect:
        """Serialize a ticket mutation on its Run without owning the outer transaction."""
        try:
            with self._session.begin_nested():
                self._lock_company_mutation_run(run_id)
                return self._apply_support_ticket_effect_locked(
                    run_id,
                    contract_version=contract_version,
                    idempotency_key_digest=idempotency_key_digest,
                    request_digest=request_digest,
                    ticket_id=ticket_id,
                    status=status,
                    note=note,
                    logical_call_id=logical_call_id,
                    attempt_id=attempt_id,
                    lease_attempt=lease_attempt,
                )
        except IntegrityError as error:
            raise PersistenceIntegrityError("ticket effect could not be persisted") from error

    def _apply_support_ticket_effect_locked(
        self,
        run_id: str,
        *,
        contract_version: str,
        idempotency_key_digest: str,
        request_digest: str,
        ticket_id: str,
        status: str,
        note: str,
        logical_call_id: str,
        attempt_id: str,
        lease_attempt: int,
    ) -> CompanyEffect:
        """Create or replay one idempotent support-ticket state update."""
        existing = self.get_company_effect(
            run_id, "support.update_ticket", contract_version, idempotency_key_digest
        )
        if existing is not None:
            if existing.request_digest != request_digest:
                raise IdempotencyConflictError(
                    "idempotency key is already bound to a different support.update_ticket request"
                )
            return existing

        ticket = self._session.scalar(
            select(CompanySupportTicketModel)
            .where(
                CompanySupportTicketModel.run_id == run_id,
                CompanySupportTicketModel.ticket_id == ticket_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if ticket is None:
            raise ReferenceNotFoundError("Run-local support ticket was not found")

        effect_id = _effect_identity(
            run_id, "support.update_ticket", contract_version, idempotency_key_digest
        )
        applied_at = self.database_time()
        result: dict[str, object] = {
            "effect_id": effect_id,
            "ticket_id": ticket_id,
            "status": status,
            "note": note,
            "updated_at": _event_timestamp(applied_at),
            "application": "newly_applied",
        }
        effect = CompanyEffectModel(
            run_id=run_id,
            tool_id="support.update_ticket",
            contract_version=contract_version,
            idempotency_key_digest=idempotency_key_digest,
            request_digest=request_digest,
            effect_id=effect_id,
            effect_kind="support_ticket.updated",
            subject_type="support_ticket",
            subject_id=ticket_id,
            effect_state="applied",
            result_document=result,
            logical_call_id=logical_call_id,
            first_attempt_id=attempt_id,
            lease_attempt=lease_attempt,
            created_at=applied_at,
        )
        self._session.add(effect)
        self._session.flush()
        ticket.status = status
        ticket.note = note
        ticket.updated_at = applied_at
        ticket.last_effect_id = effect_id
        self._session.flush()
        return self._company_effect_record(effect, newly_applied=True)

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

    def get_execution_checkpoint(self, run_id: str) -> ExecutionCheckpointRecord | None:
        """Return an immutable checkpoint snapshot and verify its stored digest."""
        _require_identifier(run_id, "run_id")
        model = self._session.get(ExecutionCheckpointModel, run_id)
        return None if model is None else self._checkpoint_record(model)

    def _store_execution_checkpoint(
        self,
        lease: LeaseIdentity,
        document: Mapping[str, object],
        *,
        expected_version: int,
    ) -> ExecutionCheckpointRecord:
        """Internal CAS-write for the runtime's already-validated checkpoint.

        The runtime owns JSON Schema and evidence-semantic validation. The
        caller must include every Event committed in this transaction in
        ``last_event_sequence``.  The Run row lock shares the evidence sequence
        serialization and fences lifecycle/reclaim operations.
        """
        if expected_version < 0:
            raise ValueError("expected checkpoint version must be non-negative")
        snapshot = deepcopy(dict(document))
        _validate_jsonb_persistence_profile(snapshot, "execution checkpoint")
        run = self.lock_current_lease(lease)
        if run.status != "running":
            raise LifecycleConflictError("execution checkpoints require a running Run")
        if snapshot.get("schema_version") != "chaosagent.execution-checkpoint/v0":
            raise PersistenceIntegrityError("unsupported execution checkpoint version")
        if snapshot.get("run_id") != run.run_id:
            raise PersistenceIntegrityError("execution checkpoint belongs to another Run")
        next_version = expected_version + 1
        if snapshot.get("checkpoint_version") != next_version:
            raise PersistenceIntegrityError("checkpoint document version does not match CAS")
        if snapshot.get("lease_attempt") != lease.attempt:
            raise PersistenceIntegrityError("checkpoint document lease attempt is stale")
        last_sequence = snapshot.get("last_event_sequence")
        if type(last_sequence) is not int or last_sequence < 1:
            raise PersistenceIntegrityError("checkpoint last event sequence is invalid")
        latest = cast(
            int,
            self._session.scalar(
                select(func.coalesce(func.max(RunEventModel.sequence), 0)).where(
                    RunEventModel.run_id == run.run_id
                )
            ),
        )
        if last_sequence != latest:
            raise PersistenceIntegrityError(
                "checkpoint does not point at the latest committed Run evidence"
            )
        digest = digest_payload_v0(snapshot)
        existing = self._session.scalar(
            select(ExecutionCheckpointModel)
            .where(ExecutionCheckpointModel.run_id == run.run_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        model: ExecutionCheckpointModel
        with self._session.begin_nested():
            if existing is None:
                if expected_version != 0:
                    raise CheckpointConflictError("execution checkpoint does not yet exist")
                model = ExecutionCheckpointModel(
                    run_id=run.run_id,
                    schema_version="chaosagent.execution-checkpoint/v0",
                    checkpoint_version=next_version,
                    lease_attempt=lease.attempt,
                    last_event_sequence=last_sequence,
                    document=snapshot,
                    document_digest=digest,
                )
                self._session.add(model)
                self._session.flush()
            else:
                updated_model = self._session.scalar(
                    update(ExecutionCheckpointModel)
                    .where(
                        ExecutionCheckpointModel.run_id == run.run_id,
                        ExecutionCheckpointModel.checkpoint_version == expected_version,
                    )
                    .values(
                        checkpoint_version=next_version,
                        lease_attempt=lease.attempt,
                        last_event_sequence=last_sequence,
                        document=snapshot,
                        document_digest=digest,
                        updated_at=func.clock_timestamp(),
                    )
                    .returning(ExecutionCheckpointModel)
                )
                if updated_model is None:
                    raise CheckpointConflictError(
                        "execution checkpoint was advanced by another executor"
                    )
                model = updated_model
        return self._checkpoint_record(model)

    def get_approval_request(self, approval_id: str) -> ApprovalRequestRecord | None:
        model = self._session.get(ApprovalRequestModel, approval_id)
        return None if model is None else self._approval_record(model)

    def list_approval_requests(self, run_id: str) -> tuple[ApprovalRequestRecord, ...]:
        """Return every Run approval after full durable-row/evidence revalidation."""
        _require_identifier(run_id, "run_id")
        models = self._session.scalars(
            select(ApprovalRequestModel)
            .where(ApprovalRequestModel.run_id == run_id)
            .order_by(ApprovalRequestModel.approval_id)
            .execution_options(populate_existing=True)
        )
        return tuple(self._approval_record(model) for model in models)

    def get_approval_request_for_authorization(
        self,
        approval_id: str,
        *,
        run: RunRecord,
        policy: RevisionReference,
        tool_id: str,
        contract_version: str,
        request_digest: str,
        idempotency_key_digest: str,
        arguments: Mapping[str, object],
    ) -> ApprovalRequestRecord | None:
        """Load one approval and prove it authorizes this exact frozen request."""
        record = self.get_approval_request(approval_id)
        if record is None:
            return None
        if not self._approval_matches(
            record,
            run=run,
            policy=policy,
            tool_id=tool_id,
            contract_version=contract_version,
            request_digest=request_digest,
            idempotency_key_digest=idempotency_key_digest,
            arguments=arguments,
        ):
            raise PersistenceIntegrityError(
                "persisted approval does not match the current frozen request"
            )
        return record

    def create_approval_request(
        self,
        *,
        run: RunRecord,
        policy: RevisionReference,
        tool_id: str,
        contract_version: str,
        request_digest: str,
        idempotency_key_digest: str,
        arguments: Mapping[str, object],
        logical_call_id: str,
        requested_attempt_id: str,
        lease_attempt: int,
        decision_id: str,
        decision_event_id: str,
        request_event_id: str,
        producer_component: str,
        producer_instance_id: str | None = None,
    ) -> ApprovalRequestRecord:
        """Atomically persist one exact request and its approval.requested evidence."""
        if tool_id != "payments.refund":
            raise ValueError("Policy v0 approvals are only defined for payments.refund")
        for value, field in (
            (logical_call_id, "logical_call_id"),
            (requested_attempt_id, "requested_attempt_id"),
            (decision_id, "decision_id"),
            (decision_event_id, "decision_event_id"),
            (request_event_id, "request_event_id"),
        ):
            _require_identifier(value, field)
        _validate_reference(policy, "policy")
        _require_digest(request_digest)
        _require_digest(idempotency_key_digest)
        arguments_document = deepcopy(dict(arguments))
        _validate_jsonb_persistence_profile(arguments_document, "approval request")
        with self._session.begin_nested():
            authoritative_model = self._session.scalar(
                select(RunModel)
                .where(RunModel.run_id == run.run_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if authoritative_model is None:
                raise ReferenceNotFoundError(f"run {run.run_id!r} does not exist")
            authoritative_run = self._run_record(authoritative_model)
            if authoritative_run.scenario != run.scenario:
                raise PersistenceIntegrityError(
                    "caller Run snapshot does not match the authoritative Run Scenario"
                )
            if authoritative_run.status != "running" or lease_attempt != authoritative_run.attempt:
                raise PersistenceIntegrityError(
                    "approval request does not match the authoritative active Run attempt"
                )
            scenario_model = self._session.get(
                ScenarioRevisionModel,
                (authoritative_run.scenario.id, authoritative_run.scenario.revision),
            )
            if (
                scenario_model is None
                or scenario_model.canonical_digest != authoritative_run.scenario.digest
            ):
                raise PersistenceIntegrityError("authoritative Run Scenario does not resolve")
            scenario_document = self._scenario_record(scenario_model).scenario.to_dict()
            policy_document = cast(dict[str, object], scenario_document["policy"])
            authoritative_policy = RevisionReference(
                cast(str, policy_document["id"]),
                cast(str, policy_document["revision"]),
                cast(str, policy_document["digest"]),
            )
            if policy != authoritative_policy:
                raise PersistenceIntegrityError(
                    "caller Policy does not match the authoritative Scenario Policy"
                )
            policy_model = self._session.get(
                PolicyRevisionModel, (authoritative_policy.id, authoritative_policy.revision)
            )
            if policy_model is None or policy_model.canonical_digest != authoritative_policy.digest:
                raise PersistenceIntegrityError("authoritative Scenario Policy does not resolve")
            self._policy_record(policy_model)
            computed_request_digest = digest_payload_v0(
                {
                    "tool_id": tool_id,
                    "contract_version": contract_version,
                    "arguments": arguments_document,
                }
            )
            key = arguments_document.get("idempotency_key")
            if not isinstance(key, str):
                raise ValueError("approval arguments require an idempotency_key")
            computed_key_digest = digest_payload_v0(key)
            if (
                request_digest != computed_request_digest
                or idempotency_key_digest != computed_key_digest
            ):
                raise PersistenceIntegrityError(
                    "caller approval fingerprints do not match the frozen request"
                )
            self._require_policy_decision_provenance(
                run_id=authoritative_run.run_id,
                policy=authoritative_policy,
                tool_id=tool_id,
                arguments=arguments_document,
                logical_call_id=logical_call_id,
                requested_attempt_id=requested_attempt_id,
                decision_id=decision_id,
                decision_event_id=decision_event_id,
                idempotency_key_digest=computed_key_digest,
            )
            approval_id = approval_identity(
                run_id=authoritative_run.run_id,
                scenario_id=authoritative_run.scenario.id,
                scenario_revision=authoritative_run.scenario.revision,
                scenario_digest=authoritative_run.scenario.digest,
                policy_id=authoritative_policy.id,
                policy_revision=authoritative_policy.revision,
                policy_digest=authoritative_policy.digest,
                tool_id=tool_id,
                contract_version=contract_version,
                request_digest=computed_request_digest,
                idempotency_key_digest=computed_key_digest,
            )
            existing = self._session.get(ApprovalRequestModel, approval_id)
            if existing is not None:
                record = self._approval_record(existing)
                if not self._approval_matches(
                    record,
                    run=authoritative_run,
                    policy=authoritative_policy,
                    tool_id=tool_id,
                    contract_version=contract_version,
                    request_digest=computed_request_digest,
                    idempotency_key_digest=computed_key_digest,
                    arguments=arguments_document,
                ):
                    raise ApprovalConflictError(
                        f"approval_id {approval_id!r} is bound to different content"
                    )
                return record
            model = ApprovalRequestModel(
                approval_id=approval_id,
                run_id=authoritative_run.run_id,
                scenario_id=authoritative_run.scenario.id,
                scenario_revision=authoritative_run.scenario.revision,
                scenario_digest=authoritative_run.scenario.digest,
                policy_id=authoritative_policy.id,
                policy_revision=authoritative_policy.revision,
                policy_digest=authoritative_policy.digest,
                tool_id=tool_id,
                contract_version=contract_version,
                request_digest=computed_request_digest,
                idempotency_key_digest=computed_key_digest,
                arguments_document=arguments_document,
                logical_call_id=logical_call_id,
                requested_attempt_id=requested_attempt_id,
                lease_attempt=lease_attempt,
                decision_id=decision_id,
                decision_event_id=decision_event_id,
                request_event_id=request_event_id,
            )
            self._session.add(model)
            self._session.flush()
            observed = self.database_time()
            producer: dict[str, object] = {"component": producer_component}
            if producer_instance_id is not None:
                producer["instance_id"] = producer_instance_id
            payload: dict[str, object] = {
                "approval_id": approval_id,
                "decision_id": decision_id,
                "action_digest": computed_request_digest,
            }

            def event_factory(sequence: int) -> RunEvent:
                document: dict[str, object] = {
                    "schema_version": "chaosagent.run-event/v0",
                    "event_id": request_event_id,
                    "run_id": authoritative_run.run_id,
                    "sequence": sequence,
                    "occurred_at": _event_timestamp(observed),
                    "recorded_at": _event_timestamp(observed),
                    "event_type": "approval.requested",
                    "producer": producer,
                    "correlation_id": logical_call_id,
                    "causation_event_id": decision_event_id,
                    "payload": payload,
                    "payload_digest": digest_payload_v0(payload),
                }
                return loads_run_event(json.dumps(document))

            self.append_event_allocated(authoritative_run.run_id, event_factory)
            record = self._approval_record(model)
            return replace(record, newly_created=True)

    def resolve_approval_request(
        self,
        approval_id: str,
        *,
        result: Literal["approved", "denied"],
        actor_id: str,
        resolution_event_id: str,
        producer_component: str = "approval-service",
        producer_instance_id: str | None = None,
    ) -> ApprovalRequestRecord:
        """Resolve once and append approval.resolved without touching Run lifecycle."""
        _require_identifier(approval_id, "approval_id")
        _require_identifier(actor_id, "actor_id")
        _require_identifier(resolution_event_id, "resolution_event_id")
        if result not in {"approved", "denied"}:
            raise ValueError("result must be approved or denied")
        with self._session.begin_nested():
            request = self._session.get(ApprovalRequestModel, approval_id)
            if request is None:
                raise ReferenceNotFoundError(f"approval {approval_id!r} does not exist")
            self._session.scalar(
                select(RunModel.run_id).where(RunModel.run_id == request.run_id).with_for_update()
            )
            request = self._session.scalar(
                select(ApprovalRequestModel)
                .where(ApprovalRequestModel.approval_id == approval_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            assert request is not None
            self._approval_record(request)
            existing = self._session.get(ApprovalResolutionModel, approval_id)
            if existing is not None:
                raise ApprovalAlreadyResolvedError(
                    f"approval {approval_id!r} is already {existing.result}"
                )
            resolution = ApprovalResolutionModel(
                approval_id=approval_id,
                run_id=request.run_id,
                result=result,
                actor_id=actor_id,
                responder_type="human",
                resolution_event_id=resolution_event_id,
            )
            self._session.add(resolution)
            self._session.flush()
            observed = self.database_time()
            producer: dict[str, object] = {"component": producer_component}
            if producer_instance_id is not None:
                producer["instance_id"] = producer_instance_id
            payload: dict[str, object] = {
                "approval_id": approval_id,
                "request_event_id": request.request_event_id,
                "result": result,
                "responder_type": "human",
            }

            def event_factory(sequence: int) -> RunEvent:
                document: dict[str, object] = {
                    "schema_version": "chaosagent.run-event/v0",
                    "event_id": resolution_event_id,
                    "run_id": request.run_id,
                    "sequence": sequence,
                    "occurred_at": _event_timestamp(observed),
                    "recorded_at": _event_timestamp(observed),
                    "event_type": "approval.resolved",
                    "producer": producer,
                    "correlation_id": approval_id,
                    "causation_event_id": request.request_event_id,
                    "payload": payload,
                    "payload_digest": digest_payload_v0(payload),
                }
                return loads_run_event(json.dumps(document))

            self.append_event_allocated(request.run_id, event_factory)
            return self._approval_record(request)

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

    def latest_event_projection(self, run_id: str) -> tuple[str, int] | None:
        """Read only the constraint-protected identity/sequence projection.

        Evaluator error handling uses this after a corrupt immutable document
        fails the normal contract loader; it never treats the projection as
        semantic evidence.
        """
        _require_identifier(run_id, "run_id")
        row = self._session.execute(
            select(RunEventModel.event_id, RunEventModel.sequence)
            .where(RunEventModel.run_id == run_id)
            .order_by(RunEventModel.sequence.desc())
            .limit(1)
        ).one_or_none()
        return None if row is None else (row[0], row[1])

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
                origin="fixture",
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

    def _same_policy_or_conflict(
        self, model: PolicyRevisionModel, policy: Policy
    ) -> PolicyRevisionRecord:
        if model.canonical_digest != policy.digest or model.canonical_document != policy.to_dict():
            raise RevisionConflictError(
                f"policy revision {(model.policy_id, model.revision)!r} has different content"
            )
        return self._policy_record(model)

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
    def _policy_record(model: PolicyRevisionModel) -> PolicyRevisionRecord:
        try:
            policy = loads_policy(_json_text(model.canonical_document))
        except PolicyValidationError as error:
            raise PersistenceIntegrityError(
                "stored policy document violates its contract"
            ) from error
        if policy.digest != model.canonical_digest:
            raise PersistenceIntegrityError("stored policy document does not match its digest")
        return PolicyRevisionRecord(policy, model.created_at, model.created_by)

    def _require_policy_decision_provenance(
        self,
        *,
        run_id: str,
        policy: RevisionReference,
        tool_id: str,
        arguments: Mapping[str, object],
        logical_call_id: str,
        requested_attempt_id: str,
        decision_id: str,
        decision_event_id: str,
        idempotency_key_digest: str,
    ) -> None:
        decision_model = self._session.get(RunEventModel, decision_event_id)
        if decision_model is None:
            raise PersistenceIntegrityError("approval policy-decision event does not exist")
        decision = self._event_record(decision_model).event.to_dict()
        payload = cast(dict[str, object], decision["payload"])
        policy_value = payload.get("policy")
        if not isinstance(policy_value, dict):
            raise PersistenceIntegrityError("approval policy-decision reference is malformed")
        if not (
            decision["run_id"] == run_id
            and decision["event_type"] == "policy.decision"
            and payload.get("decision") == "require_approval"
            and payload.get("decision_id") == decision_id
            and payload.get("logical_call_id") == logical_call_id
            and decision.get("correlation_id") == logical_call_id
            and (
                policy_value.get("id"),
                policy_value.get("revision"),
                policy_value.get("digest"),
            )
            == (policy.id, policy.revision, policy.digest)
        ):
            raise PersistenceIntegrityError("approval policy-decision provenance does not match")
        request_event_id = decision.get("causation_event_id")
        request_model = (
            self._session.get(RunEventModel, request_event_id)
            if isinstance(request_event_id, str)
            else None
        )
        if request_model is None:
            raise PersistenceIntegrityError("approval tool-request provenance does not exist")
        request = self._event_record(request_model).event.to_dict()
        request_payload = cast(dict[str, object], request["payload"])
        if not (
            request["run_id"] == run_id
            and request["event_type"] == "tool.requested"
            and request_payload.get("logical_call_id") == logical_call_id
            and request_payload.get("attempt_id") == requested_attempt_id
            and request_payload.get("tool_id") == tool_id
            and request_payload.get("arguments_digest") == digest_payload_v0(arguments)
            and request_payload.get("idempotency_key_digest") == idempotency_key_digest
            and request.get("correlation_id") == logical_call_id
            and cast(int, request["sequence"]) < cast(int, decision["sequence"])
        ):
            raise PersistenceIntegrityError("approval tool-request provenance does not match")

    def _approval_record(self, model: ApprovalRequestModel) -> ApprovalRequestRecord:
        resolution = self._session.get(ApprovalResolutionModel, model.approval_id)
        self._validate_approval_integrity(model, resolution)
        status: Literal["pending", "approved", "denied"] = (
            "pending"
            if resolution is None
            else cast(Literal["approved", "denied"], resolution.result)
        )
        return ApprovalRequestRecord(
            approval_id=model.approval_id,
            run_id=model.run_id,
            scenario=RevisionReference(
                model.scenario_id, model.scenario_revision, model.scenario_digest
            ),
            policy=RevisionReference(model.policy_id, model.policy_revision, model.policy_digest),
            tool_id=model.tool_id,
            contract_version=model.contract_version,
            request_digest=model.request_digest,
            idempotency_key_digest=model.idempotency_key_digest,
            arguments=MappingProxyType(deepcopy(model.arguments_document)),
            logical_call_id=model.logical_call_id,
            requested_attempt_id=model.requested_attempt_id,
            lease_attempt=model.lease_attempt,
            decision_id=model.decision_id,
            decision_event_id=model.decision_event_id,
            request_event_id=model.request_event_id,
            status=status,
            created_at=model.created_at,
            resolved_at=None if resolution is None else resolution.resolved_at,
            actor_id=None if resolution is None else resolution.actor_id,
            resolution_event_id=None if resolution is None else resolution.resolution_event_id,
        )

    def _validate_approval_integrity(
        self,
        model: ApprovalRequestModel,
        resolution: ApprovalResolutionModel | None,
    ) -> None:
        """Recompute every authoritative approval binding and evidence reference."""
        arguments = model.arguments_document
        if type(arguments) is not dict:
            raise PersistenceIntegrityError("stored approval arguments are not an object")
        try:
            request_digest = digest_payload_v0(
                {
                    "tool_id": model.tool_id,
                    "contract_version": model.contract_version,
                    "arguments": arguments,
                }
            )
        except EvidenceValidationError as error:
            raise PersistenceIntegrityError(
                "stored approval request cannot be fingerprinted"
            ) from error
        key = arguments.get("idempotency_key")
        if not isinstance(key, str):
            raise PersistenceIntegrityError("stored approval has no idempotency identity")
        try:
            key_digest = digest_payload_v0(key)
        except EvidenceValidationError as error:
            raise PersistenceIntegrityError(
                "stored approval idempotency identity cannot be fingerprinted"
            ) from error
        expected_id = approval_identity(
            run_id=model.run_id,
            scenario_id=model.scenario_id,
            scenario_revision=model.scenario_revision,
            scenario_digest=model.scenario_digest,
            policy_id=model.policy_id,
            policy_revision=model.policy_revision,
            policy_digest=model.policy_digest,
            tool_id=model.tool_id,
            contract_version=model.contract_version,
            request_digest=request_digest,
            idempotency_key_digest=key_digest,
        )
        if (
            model.approval_id != expected_id
            or model.request_digest != request_digest
            or model.idempotency_key_digest != key_digest
        ):
            raise PersistenceIntegrityError("stored approval identity or request digest is corrupt")

        run = self._fresh_run(model.run_id)
        run_scenario = (run.scenario_id, run.scenario_revision, run.scenario_digest)
        approval_scenario = (model.scenario_id, model.scenario_revision, model.scenario_digest)
        if approval_scenario != run_scenario:
            raise PersistenceIntegrityError("stored approval Scenario does not match its Run")
        if model.lease_attempt > run.attempt:
            raise PersistenceIntegrityError("stored approval refers to an impossible Run attempt")
        scenario_model = self._session.get(
            ScenarioRevisionModel, (model.scenario_id, model.scenario_revision)
        )
        if scenario_model is None or scenario_model.canonical_digest != model.scenario_digest:
            raise PersistenceIntegrityError("stored approval Scenario reference does not resolve")
        scenario = self._scenario_record(scenario_model).scenario.to_dict()
        scenario_policy = cast(dict[str, object], scenario["policy"])
        approval_policy = (model.policy_id, model.policy_revision, model.policy_digest)
        expected_policy = (
            scenario_policy["id"],
            scenario_policy["revision"],
            scenario_policy["digest"],
        )
        if approval_policy != expected_policy:
            raise PersistenceIntegrityError("stored approval Policy does not match its Scenario")
        policy_model = self._session.get(
            PolicyRevisionModel, (model.policy_id, model.policy_revision)
        )
        if policy_model is None or policy_model.canonical_digest != model.policy_digest:
            raise PersistenceIntegrityError("stored approval Policy reference does not resolve")
        self._policy_record(policy_model)

        decision_model = self._session.get(RunEventModel, model.decision_event_id)
        request_model = self._session.get(RunEventModel, model.request_event_id)
        if decision_model is None or request_model is None:
            raise PersistenceIntegrityError("stored approval evidence reference does not resolve")
        decision = self._event_record(decision_model).event.to_dict()
        approval_requested = self._event_record(request_model).event.to_dict()
        decision_payload = cast(dict[str, object], decision["payload"])
        requested_payload = cast(dict[str, object], approval_requested["payload"])
        policy_value = decision_payload.get("policy")
        if not isinstance(policy_value, dict):
            raise PersistenceIntegrityError("stored approval Policy evidence is malformed")
        if not (
            decision["run_id"] == model.run_id
            and decision["event_type"] == "policy.decision"
            and decision_payload.get("decision") == "require_approval"
            and decision_payload.get("decision_id") == model.decision_id
            and decision_payload.get("logical_call_id") == model.logical_call_id
            and decision.get("correlation_id") == model.logical_call_id
            and (
                policy_value.get("id"),
                policy_value.get("revision"),
                policy_value.get("digest"),
            )
            == approval_policy
        ):
            raise PersistenceIntegrityError("stored approval policy-decision provenance is corrupt")
        tool_request_id = decision.get("causation_event_id")
        tool_request_model = (
            self._session.get(RunEventModel, tool_request_id)
            if isinstance(tool_request_id, str)
            else None
        )
        if tool_request_model is None:
            raise PersistenceIntegrityError("stored approval tool-request provenance is missing")
        tool_request = self._event_record(tool_request_model).event.to_dict()
        tool_payload = cast(dict[str, object], tool_request["payload"])
        if not (
            tool_request["run_id"] == model.run_id
            and tool_request["event_type"] == "tool.requested"
            and tool_payload.get("logical_call_id") == model.logical_call_id
            and tool_payload.get("attempt_id") == model.requested_attempt_id
            and tool_payload.get("tool_id") == model.tool_id
            and tool_payload.get("arguments_digest") == digest_payload_v0(arguments)
            and tool_payload.get("idempotency_key_digest") == key_digest
            and tool_request.get("correlation_id") == model.logical_call_id
            and cast(int, tool_request["sequence"]) < cast(int, decision["sequence"])
        ):
            raise PersistenceIntegrityError("stored approval tool-request provenance is corrupt")
        if not (
            approval_requested["run_id"] == model.run_id
            and approval_requested["event_type"] == "approval.requested"
            and approval_requested.get("causation_event_id") == model.decision_event_id
            and requested_payload.get("approval_id") == model.approval_id
            and requested_payload.get("decision_id") == model.decision_id
            and requested_payload.get("action_digest") == model.request_digest
            and approval_requested.get("correlation_id") == model.logical_call_id
            and cast(int, decision["sequence"]) < cast(int, approval_requested["sequence"])
        ):
            raise PersistenceIntegrityError("stored approval-request evidence is corrupt")

        if resolution is None:
            return
        resolution_model = self._session.get(RunEventModel, resolution.resolution_event_id)
        if resolution_model is None:
            raise PersistenceIntegrityError("stored approval resolution evidence is missing")
        resolved = self._event_record(resolution_model).event.to_dict()
        resolved_payload = cast(dict[str, object], resolved["payload"])
        if not (
            resolution.run_id == model.run_id
            and resolution.result in {"approved", "denied"}
            and resolved["run_id"] == model.run_id
            and resolved["event_type"] == "approval.resolved"
            and resolved.get("causation_event_id") == model.request_event_id
            and resolved_payload.get("approval_id") == model.approval_id
            and resolved_payload.get("request_event_id") == model.request_event_id
            and resolved_payload.get("result") == resolution.result
            and resolved_payload.get("responder_type") == resolution.responder_type
            and resolved.get("correlation_id") == model.approval_id
            and cast(int, approval_requested["sequence"]) < cast(int, resolved["sequence"])
        ):
            raise PersistenceIntegrityError("stored approval resolution provenance is corrupt")

    @staticmethod
    def _approval_matches(
        record: ApprovalRequestRecord,
        *,
        run: RunRecord,
        policy: RevisionReference,
        tool_id: str,
        contract_version: str,
        request_digest: str,
        idempotency_key_digest: str,
        arguments: Mapping[str, object],
    ) -> bool:
        return (
            record.run_id == run.run_id
            and record.scenario == run.scenario
            and record.policy == policy
            and record.tool_id == tool_id
            and record.contract_version == contract_version
            and record.request_digest == request_digest
            and record.idempotency_key_digest == idempotency_key_digest
            and dict(record.arguments) == dict(arguments)
        )

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
    def _company_effect_record(model: CompanyEffectModel, *, newly_applied: bool) -> CompanyEffect:
        _validate_company_effect_model(model)
        result = deepcopy(model.result_document)
        result["application"] = "newly_applied" if newly_applied else "already_applied"
        return CompanyEffect(
            run_id=model.run_id,
            tool_id=model.tool_id,
            contract_version=model.contract_version,
            idempotency_key_digest=model.idempotency_key_digest,
            request_digest=model.request_digest,
            effect_id=model.effect_id,
            effect_kind=model.effect_kind,
            subject_type=model.subject_type,
            subject_id=model.subject_id,
            result=MappingProxyType(result),
            logical_call_id=model.logical_call_id,
            first_attempt_id=model.first_attempt_id,
            lease_attempt=model.lease_attempt,
            created_at=model.created_at,
            newly_applied=newly_applied,
        )

    @staticmethod
    def _post_commit_acknowledgement_record(
        model: PostCommitAcknowledgementModel,
    ) -> PostCommitAcknowledgement:
        return PostCommitAcknowledgement(
            run_id=model.run_id,
            attempt_id=model.attempt_id,
            logical_call_id=model.logical_call_id,
            attempt_number=model.attempt_number,
            call_ordinal=model.call_ordinal,
            tool_id=model.tool_id,
            contract_version=model.contract_version,
            idempotency_key_digest=model.idempotency_key_digest,
            request_digest=model.request_digest,
            arguments_digest=model.arguments_digest,
            effect_id=model.effect_id,
            lease_attempt=model.lease_attempt,
            request_event_id=model.request_event_id,
            state_evidence_event_id=model.state_evidence_event_id,
            policy_decision_event_id=model.policy_decision_event_id,
            approval_id=model.approval_id,
            fault_id=model.fault_id,
            activation_id=model.activation_id,
            timeout_duration_ms=model.timeout_duration_ms,
            matched_event_id=model.matched_event_id,
            applied_event_id=model.applied_event_id,
            result_event_id=model.result_event_id,
            observed_event_id=model.observed_event_id,
            created_at=model.created_at,
        )

    @staticmethod
    def _agent_configuration_record(
        model: AgentConfigurationRevisionModel,
    ) -> AgentConfigurationRevisionRecord:
        configuration = None
        if model.canonical_document is not None:
            try:
                configuration = loads_agent_configuration(_json_text(model.canonical_document))
            except AgentConfigurationValidationError as error:
                raise PersistenceIntegrityError(
                    "stored Agent Configuration document is invalid"
                ) from error
            document = configuration.to_dict()
            if (
                configuration.digest != model.digest
                or document["agent_configuration_id"] != model.agent_configuration_id
                or document["revision"] != model.revision
                or document["schema_version"] != model.schema_version
            ):
                raise PersistenceIntegrityError(
                    "stored Agent Configuration projections or digest are inconsistent"
                )
        return AgentConfigurationRevisionRecord(
            RevisionReference(model.agent_configuration_id, model.revision, model.digest),
            configuration,
            model.created_at,
            model.created_by,
        )

    def _same_campaign_plan_or_conflict(
        self, model: CampaignPlanModel, canonical: bytes, digest: str
    ) -> CampaignPlanRecord:
        record = self._campaign_plan_record(model)
        try:
            stored = rfc8785.dumps(cast(object, deepcopy(model.canonical_document)))  # type: ignore[arg-type]
        except (TypeError, rfc8785.CanonicalizationError) as error:
            raise PersistenceIntegrityError("stored Campaign plan is malformed") from error
        if record.canonical_digest != digest or stored != canonical:
            raise CampaignMembershipConflictError(
                "Campaign identity already has a different immutable plan"
            )
        return record

    def _campaign_plan_record(self, model: CampaignPlanModel) -> CampaignPlanRecord:
        try:
            document = deepcopy(model.canonical_document)
            canonical = rfc8785.dumps(cast(object, document))  # type: ignore[arg-type]
            digest = "sha256:" + hashlib.sha256(canonical).hexdigest()
            scenario = cast(dict[str, object], document["scenario"])
            agent = cast(dict[str, object], document["agent_configuration"])
            assignments_document = cast(list[dict[str, object]], document["assignments"])
            assignments = tuple(
                (cast(int, item["trial_index"]), cast(str, item["run_id"]))
                for item in assignments_document
            )
            selected = tuple(cast(list[str], document["selected_fault_ids"]))
        except (KeyError, TypeError, ValueError, rfc8785.CanonicalizationError) as error:
            raise PersistenceIntegrityError("stored Campaign plan is malformed") from error
        if (
            digest != model.canonical_digest
            or document.get("schema_version") != "chaosagent.campaign-plan/v0"
            or document.get("campaign_id") != model.campaign_id
            or document.get("arm") != model.arm
            or document.get("planned_trials") != model.planned_trials
            or document.get("fault_plan_digest") != model.fault_plan_digest
            or list(selected) != model.selected_fault_ids
            or scenario
            != {
                "id": model.scenario_id,
                "revision": model.scenario_revision,
                "digest": model.scenario_digest,
            }
            or agent
            != {
                "id": model.agent_configuration_id,
                "revision": model.agent_configuration_revision,
                "digest": model.agent_configuration_digest,
            }
            or len(assignments) != model.planned_trials
            or tuple(index for index, _ in assignments) != tuple(range(model.planned_trials))
            or len({run_id for _, run_id in assignments}) != len(assignments)
        ):
            raise PersistenceIntegrityError("stored Campaign plan projections are inconsistent")
        membership_models = tuple(
            self._session.scalars(
                select(CampaignTrialMembershipModel)
                .where(CampaignTrialMembershipModel.campaign_id == model.campaign_id)
                .order_by(CampaignTrialMembershipModel.trial_index)
            )
        )
        membership_assignments = tuple(
            (membership.trial_index, membership.run_id) for membership in membership_models
        )
        if membership_assignments != assignments or any(
            membership.campaign_plan_digest != model.canonical_digest
            or membership.scenario_id != model.scenario_id
            or membership.scenario_revision != model.scenario_revision
            or membership.scenario_digest != model.scenario_digest
            or membership.agent_configuration_id != model.agent_configuration_id
            or membership.agent_configuration_revision != model.agent_configuration_revision
            or membership.agent_configuration_digest != model.agent_configuration_digest
            for membership in membership_models
        ):
            raise PersistenceIntegrityError("stored Campaign memberships contradict their plan")
        for membership in membership_models:
            self._campaign_membership_record(membership)
        return CampaignPlanRecord(
            model.campaign_id,
            cast(Literal["baseline", "faulted"], model.arm),
            model.planned_trials,
            RevisionReference(model.scenario_id, model.scenario_revision, model.scenario_digest),
            RevisionReference(
                model.agent_configuration_id,
                model.agent_configuration_revision,
                model.agent_configuration_digest,
            ),
            selected,
            model.fault_plan_digest,
            assignments,
            model.canonical_digest,
            model.created_at,
        )

    @staticmethod
    def _campaign_membership_record(
        model: CampaignTrialMembershipModel,
    ) -> CampaignTrialMembershipRecord:
        material = {
            "campaign_plan_digest": model.campaign_plan_digest,
            "run_id": model.run_id,
            "trial_index": model.trial_index,
        }
        try:
            canonical = rfc8785.dumps(cast(object, material))  # type: ignore[arg-type]
        except (TypeError, rfc8785.CanonicalizationError) as error:
            raise PersistenceIntegrityError("stored Campaign membership is malformed") from error
        digest = "sha256:" + hashlib.sha256(canonical).hexdigest()
        if digest != model.membership_digest:
            raise PersistenceIntegrityError("stored Campaign membership digest is corrupt")
        return CampaignTrialMembershipRecord(
            model.run_id,
            model.campaign_id,
            model.campaign_plan_digest,
            model.trial_index,
            RevisionReference(model.scenario_id, model.scenario_revision, model.scenario_digest),
            RevisionReference(
                model.agent_configuration_id,
                model.agent_configuration_revision,
                model.agent_configuration_digest,
            ),
            model.membership_digest,
            model.created_at,
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
            fault_seed=model.fault_seed,
            fault_plan_digest=model.fault_plan_digest,
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
    def _checkpoint_record(model: ExecutionCheckpointModel) -> ExecutionCheckpointRecord:
        expected = digest_payload_v0(model.document)
        if expected != model.document_digest:
            raise PersistenceIntegrityError("stored execution checkpoint digest is corrupt")
        return ExecutionCheckpointRecord(
            model.run_id,
            model.checkpoint_version,
            model.lease_attempt,
            model.last_event_sequence,
            cast(Mapping[str, object], _freeze_json_value(deepcopy(model.document))),
            model.document_digest,
            model.updated_at,
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


def _validate_company_effect_model(model: CompanyEffectModel) -> None:
    result = model.result_document
    if type(result) is not dict:
        raise PersistenceIntegrityError("stored effect result is not an object")
    common_valid = (
        isinstance(model.run_id, str)
        and isinstance(model.tool_id, str)
        and isinstance(model.contract_version, str)
        and _DIGEST_RE.fullmatch(model.idempotency_key_digest) is not None
        and _DIGEST_RE.fullmatch(model.request_digest) is not None
        and isinstance(model.effect_id, str)
        and model.effect_id
        == _effect_identity(
            model.run_id,
            model.tool_id,
            model.contract_version,
            model.idempotency_key_digest,
        )
        and result.get("effect_id") == model.effect_id
        and result.get("application") == "newly_applied"
        and type(model.lease_attempt) is int
        and model.lease_attempt >= 1
    )
    if not common_valid:
        raise PersistenceIntegrityError("stored effect identity/result projection is corrupt")
    if model.tool_id == "payments.refund":
        if set(result) != {
            "effect_id",
            "refund_id",
            "order_id",
            "payment_id",
            "status",
            "amount_minor",
            "currency",
            "application",
        } or not (
            model.contract_version == "chaosagent.tool/payments.refund/v0"
            and model.effect_kind == "refund.created"
            and model.subject_type == "refund"
            and result.get("refund_id") == model.subject_id
            and model.subject_id == f"RFD-{model.effect_id.removeprefix('effect-')}"
            and all(isinstance(result.get(key), str) for key in ("order_id", "payment_id"))
            and result.get("status") == "succeeded"
            and type(result.get("amount_minor")) is int
            and 1 <= cast(int, result["amount_minor"]) <= 9_007_199_254_740_991
            and isinstance(result.get("currency"), str)
            and re.fullmatch(r"[A-Z]{3}", cast(str, result["currency"])) is not None
        ):
            raise PersistenceIntegrityError("stored refund effect result is corrupt")
        return
    if model.tool_id == "support.update_ticket":
        if set(result) != {
            "effect_id",
            "ticket_id",
            "status",
            "note",
            "updated_at",
            "application",
        } or not (
            model.contract_version == "chaosagent.tool/support.update_ticket/v0"
            and model.effect_kind == "support_ticket.updated"
            and model.subject_type == "support_ticket"
            and result.get("ticket_id") == model.subject_id
            and result.get("status") in {"open", "pending", "closed"}
            and isinstance(result.get("note"), str)
            and 1 <= len(cast(str, result["note"])) <= 4000
            and isinstance(result.get("updated_at"), str)
        ):
            raise PersistenceIntegrityError("stored support-ticket effect result is corrupt")
        try:
            _timestamp(cast(str, result["updated_at"]))
        except (TypeError, ValueError) as error:
            raise PersistenceIntegrityError("stored support-ticket timestamp is corrupt") from error
        return
    raise PersistenceIntegrityError("stored effect tool is unsupported")


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


def _effect_identity(
    run_id: str, tool_id: str, contract_version: str, idempotency_key_digest: str
) -> str:
    identity = "\x1f".join((run_id, tool_id, contract_version, idempotency_key_digest))
    return f"effect-{hashlib.sha256(identity.encode('utf-8')).hexdigest()}"
