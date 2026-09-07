"""Thin transactional adapter over authoritative ChaosAgent domain services."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Literal, cast

from chaosagent_agent_configurations import (
    AgentConfigurationValidationError,
    loads_agent_configuration,
)
from chaosagent_evaluators import (
    CampaignCohort,
    CampaignComparison,
    CampaignStatistics,
    CampaignValidationError,
    GroundTruth,
    aggregate_campaign_v0,
    authenticated_campaign_plan,
    authenticated_campaign_trial,
    campaign_cohort_v0,
    compare_campaigns_v0,
    load_authoritative_evaluation_snapshot,
)
from chaosagent_exports import ExportIntegrityError, export_campaign_bundle, export_run_bundle
from chaosagent_persistence import (
    ApprovalAlreadyResolvedError,
    ApprovalConflictError,
    ApprovalRequestRecord,
    CampaignPlanRecord,
    IllegalRunTransitionError,
    LifecycleConflictError,
    LifecycleEvidence,
    PersistenceConflictError,
    PersistenceError,
    PersistenceIntegrityError,
    PersistenceRepository,
    ReferenceNotFoundError,
    RevisionConflictError,
    RevisionReference,
    RunEventRecord,
    RunRecord,
)
from chaosagent_scenarios import ScenarioValidationError, loads_scenario
from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from .cursor import EventCursor, decode_cursor, encode_cursor
from .errors import ControlPlaneError, bad_request, conflict, internal_error, not_found, unavailable
from .models import (
    AgentConfigurationCreateRequest,
    AgentConfigurationResponse,
    ApprovalResponse,
    CampaignAssignment,
    CampaignCreateRequest,
    CampaignResponse,
    CancellationResponse,
    EventPageResponse,
    ReportResponse,
    RevisionReferenceModel,
    RunCreateRequest,
    RunResponse,
    ScenarioCreateRequest,
    ScenarioResponse,
)

_RFC3339_TIMESTAMP_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]+)?(?:Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])$"
)


def _timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _reference(value: RevisionReference) -> RevisionReferenceModel:
    return RevisionReferenceModel(id=value.id, revision=value.revision, digest=value.digest)


def _run(value: RunRecord) -> RunResponse:
    return RunResponse(
        run_id=value.run_id,
        status=value.status,
        lifecycle_version=value.lifecycle_version,
        attempt=value.attempt,
        scenario=_reference(value.scenario),
        agent_configuration=_reference(value.agent_configuration),
        fixture=None if value.fixture is None else _reference(value.fixture),
        fault_seed=value.fault_seed,
        fault_plan_digest=value.fault_plan_digest,
        lease_expires_at=_timestamp(value.lease_expires_at),
        heartbeat_at=_timestamp(value.heartbeat_at),
        created_at=cast(str, _timestamp(value.created_at)),
        created_by=value.created_by,
    )


def _approval(value: ApprovalRequestRecord) -> ApprovalResponse:
    return ApprovalResponse(
        approval_id=value.approval_id,
        run_id=value.run_id,
        scenario=_reference(value.scenario),
        policy=_reference(value.policy),
        tool_id=value.tool_id,
        contract_version=value.contract_version,
        request_digest=value.request_digest,
        idempotency_key_digest=value.idempotency_key_digest,
        logical_call_id=value.logical_call_id,
        requested_attempt_id=value.requested_attempt_id,
        lease_attempt=value.lease_attempt,
        decision_id=value.decision_id,
        decision_event_id=value.decision_event_id,
        request_event_id=value.request_event_id,
        status=value.status,
        created_at=cast(str, _timestamp(value.created_at)),
        resolved_at=_timestamp(value.resolved_at),
        actor_id=value.actor_id,
        resolution_event_id=value.resolution_event_id,
    )


def _campaign(value: CampaignPlanRecord) -> CampaignResponse:
    return CampaignResponse(
        schema_version="chaosagent.campaign-plan/v0",
        campaign_id=value.campaign_id,
        arm=value.arm,
        planned_trials=value.planned_trials,
        scenario=_reference(value.scenario),
        agent_configuration=_reference(value.agent_configuration),
        selected_fault_ids=list(value.selected_fault_ids),
        fault_plan_digest=value.fault_plan_digest,
        assignments=[
            CampaignAssignment(trial_index=index, run_id=run_id)
            for index, run_id in value.assignments
        ],
        digest=value.canonical_digest,
        created_at=cast(str, _timestamp(value.created_at)),
    )


class ControlPlaneService:
    """Short-lived transaction facade used by REST and SSE transports."""

    def __init__(self, engine: Engine, *, ground_truths: Iterable[GroundTruth] = ()) -> None:
        if not isinstance(engine, Engine):
            raise TypeError("control plane requires a SQLAlchemy Engine")
        truths = tuple(ground_truths)
        if any(not isinstance(item, GroundTruth) for item in truths):
            raise TypeError("ground_truths must be validated Ground Truth values")
        self.engine = engine
        self._ground_truths = truths

    def ready(self) -> None:
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
        except SQLAlchemyError as error:
            raise unavailable("postgres_unavailable", "PostgreSQL is not ready.") from error

    def create_scenario(self, request: ScenarioCreateRequest) -> ScenarioResponse:
        try:
            scenario = loads_scenario(json.dumps(request.scenario, separators=(",", ":")))
            with Session(self.engine) as session, session.begin():
                record = PersistenceRepository(session).insert_scenario_revision(
                    scenario, created_by=request.created_by
                )
        except ScenarioValidationError as error:
            raise ControlPlaneError(
                422, "invalid_scenario", "The Scenario document is invalid."
            ) from error
        except RevisionConflictError as error:
            raise conflict(
                "scenario_revision_conflict", "The Scenario revision already differs."
            ) from error
        document = record.scenario.to_dict()
        return ScenarioResponse(
            scenario_id=cast(str, document["scenario_id"]),
            revision=cast(str, document["revision"]),
            digest=record.scenario.digest,
            schema_version=cast(str, document["schema_version"]),
            created_at=cast(str, _timestamp(record.created_at)),
            created_by=record.created_by,
            document=document,
        )

    def get_scenario(self, scenario_id: str, revision: str) -> ScenarioResponse:
        try:
            with Session(self.engine) as session:
                record = PersistenceRepository(session).get_scenario_revision(scenario_id, revision)
        except (PersistenceIntegrityError, SQLAlchemyError) as error:
            raise internal_error() from error
        if record is None:
            raise not_found("scenario_not_found", "The Scenario revision does not exist.")
        document = record.scenario.to_dict()
        return ScenarioResponse(
            scenario_id=scenario_id,
            revision=revision,
            digest=record.scenario.digest,
            schema_version=cast(str, document["schema_version"]),
            created_at=cast(str, _timestamp(record.created_at)),
            created_by=record.created_by,
            document=document,
        )

    def create_agent_configuration(
        self, request: AgentConfigurationCreateRequest
    ) -> AgentConfigurationResponse:
        try:
            configuration = loads_agent_configuration(
                json.dumps(request.agent_configuration, separators=(",", ":"))
            )
            with Session(self.engine) as session, session.begin():
                record = PersistenceRepository(session).insert_agent_configuration(
                    configuration, created_by=request.created_by
                )
        except AgentConfigurationValidationError as error:
            raise ControlPlaneError(
                422,
                "invalid_agent_configuration",
                "The Agent Configuration document is invalid.",
            ) from error
        except RevisionConflictError as error:
            raise conflict(
                "agent_configuration_revision_conflict",
                "The Agent Configuration revision already differs.",
            ) from error
        if record.configuration is None:
            raise internal_error()
        document = record.configuration.to_dict()
        return AgentConfigurationResponse(
            agent_configuration_id=record.reference.id,
            revision=record.reference.revision,
            digest=record.reference.digest,
            schema_version=cast(str, document["schema_version"]),
            created_at=cast(str, _timestamp(record.created_at)),
            created_by=record.created_by,
            document=document,
        )

    def get_agent_configuration(
        self, configuration_id: str, revision: str
    ) -> AgentConfigurationResponse:
        try:
            with Session(self.engine) as session:
                record = PersistenceRepository(session).get_agent_configuration_reference(
                    configuration_id, revision
                )
        except (PersistenceIntegrityError, SQLAlchemyError) as error:
            raise internal_error() from error
        if record is None:
            raise not_found(
                "agent_configuration_not_found", "The Agent Configuration revision does not exist."
            )
        if record.configuration is None:
            raise conflict(
                "agent_configuration_unavailable",
                "The Agent Configuration content is not available.",
            )
        document = record.configuration.to_dict()
        return AgentConfigurationResponse(
            agent_configuration_id=record.reference.id,
            revision=record.reference.revision,
            digest=record.reference.digest,
            schema_version=cast(str, document["schema_version"]),
            created_at=cast(str, _timestamp(record.created_at)),
            created_by=record.created_by,
            document=document,
        )

    def create_run(self, request: RunCreateRequest) -> RunResponse:
        try:
            with Session(self.engine) as session, session.begin():
                repository = PersistenceRepository(session)
                existing = repository.get_run(request.run_id)
                if existing is not None:
                    expected_scenario = RevisionReference(**request.scenario.model_dump())
                    expected_agent = RevisionReference(**request.agent_configuration.model_dump())
                    if (
                        existing.scenario == expected_scenario
                        and existing.agent_configuration == expected_agent
                        and existing.created_by == request.created_by
                    ):
                        return _run(existing)
                    raise conflict("run_conflict", "The Run identity is already bound differently.")
                scenario = repository.get_scenario_revision(
                    request.scenario.id, request.scenario.revision
                )
                agent = repository.get_agent_configuration_reference(
                    request.agent_configuration.id, request.agent_configuration.revision
                )
                if scenario is None or scenario.scenario.digest != request.scenario.digest:
                    raise conflict(
                        "scenario_reference_mismatch",
                        "The frozen Scenario reference does not resolve.",
                    )
                if agent is None or agent.reference.digest != request.agent_configuration.digest:
                    raise conflict(
                        "agent_configuration_reference_mismatch",
                        "The frozen Agent Configuration reference does not resolve.",
                    )
                record = repository.create_run(
                    request.run_id,
                    scenario_id=request.scenario.id,
                    scenario_revision=request.scenario.revision,
                    agent_configuration_id=request.agent_configuration.id,
                    agent_configuration_revision=request.agent_configuration.revision,
                    created_by=request.created_by,
                )
                return _run(record)
        except ControlPlaneError:
            raise
        except ReferenceNotFoundError as error:
            raise conflict(
                "frozen_reference_unavailable", "A frozen Run reference is unavailable."
            ) from error
        except PersistenceConflictError as error:
            repeated = self._matching_run_retry(request)
            if repeated is not None:
                return repeated
            raise conflict("run_conflict", "The Run identity already exists.") from error

    def get_run(self, run_id: str) -> RunResponse:
        try:
            with Session(self.engine) as session:
                record = PersistenceRepository(session).get_run(run_id)
        except (PersistenceIntegrityError, SQLAlchemyError) as error:
            raise internal_error() from error
        if record is None:
            raise not_found("run_not_found", "The Run does not exist.")
        return _run(record)

    def cancel_run(self, run_id: str) -> CancellationResponse:
        try:
            with Session(self.engine) as session, session.begin():
                repository = PersistenceRepository(session)
                current = repository.get_run(run_id)
                if current is None:
                    raise not_found("run_not_found", "The Run does not exist.")
                if current.status == "cancelled":
                    return CancellationResponse(run=_run(current), already_cancelled=True)
                if current.status != "queued":
                    raise conflict(
                        "run_not_cancellable",
                        "Only an unleased queued Run can be cancelled through this endpoint.",
                    )
                event_id = self._identity(
                    "event-control-plane-cancel", run_id, current.lifecycle_version
                )
                cancelled = repository.cancel_queued_run(
                    run_id,
                    expected_version=current.lifecycle_version,
                    evidence=LifecycleEvidence(
                        event_id=event_id,
                        producer_component="control-plane",
                        reason_code="operator_cancelled",
                    ),
                )
                return CancellationResponse(run=_run(cancelled), already_cancelled=False)
        except ControlPlaneError:
            raise
        except (IllegalRunTransitionError, LifecycleConflictError) as error:
            reloaded = self.get_run(run_id)
            if reloaded.status == "cancelled":
                return CancellationResponse(run=reloaded, already_cancelled=True)
            raise conflict(
                "run_cancellation_conflict", "The Run changed before cancellation."
            ) from error

    def list_approvals(self, run_id: str) -> list[ApprovalResponse]:
        try:
            with Session(self.engine) as session:
                repository = PersistenceRepository(session)
                if repository.get_run(run_id) is None:
                    raise not_found("run_not_found", "The Run does not exist.")
                return [_approval(item) for item in repository.list_approval_requests(run_id)]
        except ControlPlaneError:
            raise
        except (PersistenceIntegrityError, SQLAlchemyError) as error:
            raise internal_error() from error

    def resolve_approval(
        self, approval_id: str, *, result: str, actor_id: str
    ) -> tuple[ApprovalResponse, bool]:
        if result not in {"approved", "denied"}:
            raise bad_request("invalid_approval_result", "The approval result is invalid.")
        try:
            with Session(self.engine) as session, session.begin():
                repository = PersistenceRepository(session)
                current = repository.get_approval_request(approval_id)
                if current is None:
                    raise not_found("approval_not_found", "The approval request does not exist.")
                if current.status != "pending":
                    if current.status == result and current.actor_id == actor_id:
                        return _approval(current), True
                    raise conflict(
                        "approval_already_resolved",
                        "The approval request was already resolved differently.",
                    )
                event_id = self._identity(
                    "event-approval-resolution", approval_id, result, actor_id
                )
                resolved = repository.resolve_approval_request(
                    approval_id,
                    result=cast(Literal["approved", "denied"], result),
                    actor_id=actor_id,
                    resolution_event_id=event_id,
                    producer_component="control-plane",
                )
                return _approval(resolved), False
        except ControlPlaneError:
            raise
        except ApprovalAlreadyResolvedError as error:
            try:
                with Session(self.engine) as session:
                    current = PersistenceRepository(session).get_approval_request(approval_id)
            except (PersistenceIntegrityError, SQLAlchemyError) as reload_error:
                raise internal_error() from reload_error
            if current is not None and current.status == result and current.actor_id == actor_id:
                return _approval(current), True
            raise conflict(
                "approval_resolution_conflict", "The approval was resolved concurrently."
            ) from error
        except ApprovalConflictError as error:
            raise conflict("approval_conflict", "The approval binding is inconsistent.") from error

    def create_campaign(self, request: CampaignCreateRequest) -> CampaignResponse:
        indexes = [item.trial_index for item in request.assignments]
        run_ids = [item.run_id for item in request.assignments]
        if len(set(indexes)) != len(indexes) or len(set(run_ids)) != len(run_ids):
            raise ControlPlaneError(
                422, "invalid_campaign_assignments", "Campaign assignments must be unique."
            )
        try:
            with Session(self.engine) as session, session.begin():
                repository = PersistenceRepository(session)
                plan = authenticated_campaign_plan(
                    repository,
                    campaign_id=request.campaign_id,
                    arm=request.arm,
                    selected_fault_ids=request.selected_fault_ids,
                    assignments={item.trial_index: item.run_id for item in request.assignments},
                )
                record = repository.get_campaign_plan(plan.campaign_id)
                if record is None:
                    raise PersistenceIntegrityError("committed Campaign plan disappeared")
                return _campaign(record)
        except CampaignValidationError as error:
            raise conflict(
                "campaign_plan_conflict", "The Campaign plan could not be frozen."
            ) from error

    def get_campaign(self, campaign_id: str) -> CampaignResponse:
        try:
            with Session(self.engine) as session:
                record = PersistenceRepository(session).get_campaign_plan(campaign_id)
        except (PersistenceIntegrityError, SQLAlchemyError) as error:
            raise internal_error() from error
        if record is None:
            raise not_found("campaign_not_found", "The Campaign does not exist.")
        return _campaign(record)

    def campaign_statistics(
        self, campaign_id: str, k_values: tuple[int, ...]
    ) -> CampaignStatistics:
        try:
            with Session(self.engine) as session:
                repository = PersistenceRepository(session)
                cohort = self._campaign_cohort(repository, campaign_id)
                return aggregate_campaign_v0(cohort, k_values=k_values)
        except CampaignValidationError as error:
            raise conflict(
                "campaign_statistics_unavailable", "Campaign statistics are not available."
            ) from error
        except (PersistenceError, SQLAlchemyError) as error:
            raise internal_error() from error

    def campaign_comparison(
        self, baseline_campaign_id: str, faulted_campaign_id: str, k_values: tuple[int, ...]
    ) -> CampaignComparison:
        try:
            with Session(self.engine) as session:
                repository = PersistenceRepository(session)
                baseline = self._campaign_cohort(repository, baseline_campaign_id)
                faulted = self._campaign_cohort(repository, faulted_campaign_id)
                return compare_campaigns_v0(baseline, faulted, k_values=k_values)
        except CampaignValidationError as error:
            raise conflict(
                "campaign_comparison_unavailable", "Campaign comparison is not available."
            ) from error
        except (PersistenceError, SQLAlchemyError) as error:
            raise internal_error() from error

    def event_page(self, run_id: str, cursor_value: str | None, limit: int) -> EventPageResponse:
        cursor = self.validate_cursor(run_id, cursor_value)
        after = 0 if cursor is None else cursor.sequence
        try:
            with Session(self.engine) as session:
                repository = PersistenceRepository(session)
                if repository.get_run(run_id) is None:
                    raise not_found("run_not_found", "The Run does not exist.")
                records = repository.fetch_event_page(run_id, after_sequence=after, limit=limit + 1)
        except ControlPlaneError:
            raise
        except (PersistenceIntegrityError, SQLAlchemyError) as error:
            raise internal_error() from error
        page = records[:limit]
        next_cursor = None
        if page:
            document = page[-1].event.to_dict()
            next_cursor = encode_cursor(
                run_id, cast(int, document["sequence"]), cast(str, document["event_id"])
            )
        return EventPageResponse(
            run_id=run_id,
            events=[item.event.to_dict() for item in page],
            next_cursor=next_cursor,
            has_more=len(records) > limit,
        )

    def validate_cursor(self, run_id: str, cursor_value: str | None) -> EventCursor | None:
        if cursor_value is None:
            return None
        cursor = decode_cursor(cursor_value)
        if cursor.run_id != run_id:
            raise bad_request("foreign_event_cursor", "The event cursor belongs to another Run.")
        try:
            with Session(self.engine) as session:
                record = PersistenceRepository(session).get_event(cursor.event_id)
        except (PersistenceIntegrityError, SQLAlchemyError) as error:
            raise internal_error() from error
        if record is None:
            raise bad_request(
                "unknown_event_cursor", "The event cursor does not identify an event."
            )
        document = record.event.to_dict()
        if document["run_id"] != run_id or document["sequence"] != cursor.sequence:
            raise bad_request("invalid_event_cursor", "The event cursor binding is invalid.")
        return cursor

    def preflight_event_stream(self, run_id: str, cursor_value: str | None) -> EventCursor | None:
        """Validate an SSE replay position and Run before response streaming starts."""
        cursor = self.validate_cursor(run_id, cursor_value)
        self.get_run(run_id)
        return cursor

    def get_report(self, run_id: str) -> ReportResponse:
        try:
            with Session(self.engine) as session:
                repository = PersistenceRepository(session)
                run = repository.get_run(run_id)
                if run is None:
                    raise not_found("run_not_found", "The Run does not exist.")
                record = repository.get_final_report(run_id)
                if record is None and run.status == "completed":
                    truths = self._truths_for_reference(repository, run.scenario)
                    snapshot = load_authoritative_evaluation_snapshot(repository, run_id, truths)
                else:
                    snapshot = None
        except ControlPlaneError:
            raise
        except (PersistenceIntegrityError, SQLAlchemyError) as error:
            raise internal_error() from error
        if record is None and snapshot is None:
            raise conflict("report_not_ready", "The final Run Report is not available.")
        if snapshot is not None:
            document = snapshot.result.to_dict()
            return ReportResponse(
                run_id=run_id,
                kind="evaluation_result",
                document_id=cast(str, document["evaluation_id"]),
                digest=snapshot.result.digest,
                inserted_at=None,
                document=document,
            )
        assert record is not None
        document = record.report.to_dict()
        return ReportResponse(
            run_id=run_id,
            kind="run_report",
            document_id=cast(str, document["report_id"]),
            digest="sha256:" + hashlib.sha256(record.report.canonical_bytes).hexdigest(),
            inserted_at=cast(str, _timestamp(record.inserted_at)),
            document=document,
        )

    def export_run(self, run_id: str, exported_at: str) -> bytes:
        timestamp = self._parse_exported_at(exported_at)
        truths = self._truths_for_run(run_id)
        try:
            return export_run_bundle(
                self.engine, run_id, ground_truths=truths, exported_at=timestamp
            ).to_zip_bytes()
        except ExportIntegrityError as error:
            raise conflict("run_export_unavailable", "The Run export is not available.") from error

    def export_campaign(
        self, campaign_id: str, exported_at: str, k_values: tuple[int, ...]
    ) -> bytes:
        timestamp = self._parse_exported_at(exported_at)
        truths = self._truths_for_campaign(campaign_id)
        try:
            return export_campaign_bundle(
                self.engine,
                campaign_id,
                ground_truths=truths,
                k_values=k_values,
                exported_at=timestamp,
            ).to_zip_bytes()
        except ExportIntegrityError as error:
            raise conflict(
                "campaign_export_unavailable", "The Campaign export is not available."
            ) from error

    def poll_events(
        self, run_id: str, *, after_sequence: int, limit: int
    ) -> tuple[RunRecord, tuple[RunEventRecord, ...]]:
        """Take one short READ COMMITTED snapshot, reading Run before events."""
        try:
            with Session(self.engine) as session, session.begin():
                repository = PersistenceRepository(session)
                run = repository.get_run(run_id)
                if run is None:
                    raise not_found("run_not_found", "The Run does not exist.")
                events = repository.fetch_event_page(
                    run_id, after_sequence=after_sequence, limit=limit
                )
                return run, events
        except ControlPlaneError:
            raise
        except (PersistenceIntegrityError, SQLAlchemyError) as error:
            raise internal_error() from error

    def _campaign_cohort(
        self, repository: PersistenceRepository, campaign_id: str
    ) -> CampaignCohort:
        record = repository.get_campaign_plan(campaign_id)
        if record is None:
            raise CampaignValidationError(["Campaign does not exist"])
        plan = authenticated_campaign_plan(
            repository,
            campaign_id=record.campaign_id,
            arm=record.arm,
            selected_fault_ids=record.selected_fault_ids,
            assignments=dict(record.assignments),
        )
        truths = self._truths_for_reference(repository, record.scenario)
        trials = tuple(
            authenticated_campaign_trial(repository, plan, run_id, ground_truths=truths)
            for _, run_id in record.assignments
        )
        scenario_record = repository.get_scenario_revision(
            record.scenario.id, record.scenario.revision
        )
        if scenario_record is None or scenario_record.scenario.digest != record.scenario.digest:
            raise CampaignValidationError(["Campaign Scenario binding is unavailable"])
        document = scenario_record.scenario.to_dict()
        available = tuple(
            sorted(
                cast(str, item["id"]) for item in cast(list[dict[str, object]], document["faults"])
            )
        )
        return campaign_cohort_v0(
            campaign_id=record.campaign_id,
            arm=record.arm,
            scenario={
                "id": record.scenario.id,
                "revision": record.scenario.revision,
                "digest": record.scenario.digest,
            },
            agent_configuration={
                "id": record.agent_configuration.id,
                "revision": record.agent_configuration.revision,
                "digest": record.agent_configuration.digest,
            },
            available_fault_ids=available,
            selected_fault_ids=record.selected_fault_ids,
            planned_trials=record.planned_trials,
            trials=trials,
        )

    def _truths_for_run(self, run_id: str) -> tuple[GroundTruth, ...]:
        try:
            with Session(self.engine) as session:
                repository = PersistenceRepository(session)
                run = repository.get_run(run_id)
                if run is None:
                    raise not_found("run_not_found", "The Run does not exist.")
                return self._truths_for_reference(repository, run.scenario)
        except ControlPlaneError:
            raise
        except (PersistenceIntegrityError, SQLAlchemyError) as error:
            raise internal_error() from error

    def _matching_run_retry(self, request: RunCreateRequest) -> RunResponse | None:
        try:
            with Session(self.engine) as session:
                existing = PersistenceRepository(session).get_run(request.run_id)
        except (PersistenceIntegrityError, SQLAlchemyError) as error:
            raise internal_error() from error
        if existing is None:
            return None
        expected_scenario = RevisionReference(**request.scenario.model_dump())
        expected_agent = RevisionReference(**request.agent_configuration.model_dump())
        if (
            existing.scenario == expected_scenario
            and existing.agent_configuration == expected_agent
            and existing.created_by == request.created_by
        ):
            return _run(existing)
        return None

    def _truths_for_campaign(self, campaign_id: str) -> tuple[GroundTruth, ...]:
        try:
            with Session(self.engine) as session:
                repository = PersistenceRepository(session)
                record = repository.get_campaign_plan(campaign_id)
                if record is None:
                    raise not_found("campaign_not_found", "The Campaign does not exist.")
                return self._truths_for_reference(repository, record.scenario)
        except ControlPlaneError:
            raise
        except (PersistenceIntegrityError, SQLAlchemyError) as error:
            raise internal_error() from error

    def _truths_for_reference(
        self, repository: PersistenceRepository, reference: RevisionReference
    ) -> tuple[GroundTruth, ...]:
        scenario = repository.get_scenario_revision(reference.id, reference.revision)
        if scenario is None or scenario.scenario.digest != reference.digest:
            raise PersistenceIntegrityError("Scenario Ground Truth binding is unavailable")
        expected = cast(list[dict[str, object]], scenario.scenario.to_dict()["expected_outcomes"])
        available = {
            (
                truth.to_dict()["ground_truth_id"],
                truth.to_dict()["revision"],
                truth.digest,
            ): truth
            for truth in self._ground_truths
        }
        resolved: list[GroundTruth] = []
        for item in expected:
            key = (
                item["id"],
                item["revision"],
                cast(str, item["digest"]),
            )
            truth = available.get(key)
            if truth is None:
                raise conflict(
                    "ground_truth_unavailable",
                    "A required Ground Truth revision is not configured.",
                )
            resolved.append(truth)
        return tuple(resolved)

    @staticmethod
    def _parse_exported_at(value: str) -> datetime:
        if _RFC3339_TIMESTAMP_RE.fullmatch(value) is None:
            raise ControlPlaneError(
                422,
                "invalid_export_timestamp",
                "exported_at must use RFC 3339 YYYY-MM-DDTHH:MM:SS[.fraction](Z|+/-HH:MM).",
            )
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise ControlPlaneError(
                422,
                "invalid_export_timestamp",
                "exported_at must use RFC 3339 YYYY-MM-DDTHH:MM:SS[.fraction](Z|+/-HH:MM).",
            ) from error
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ControlPlaneError(
                422, "invalid_export_timestamp", "exported_at must include a timezone."
            )
        return parsed.astimezone(UTC)

    @staticmethod
    def _identity(prefix: str, *parts: object) -> str:
        material = "\x00".join(str(item) for item in parts).encode()
        return f"{prefix}-{hashlib.sha256(material).hexdigest()[:32]}"


def map_unhandled_error(error: Exception) -> ControlPlaneError:
    """Translate known infrastructure/domain failures without exposing their text."""
    if isinstance(error, ControlPlaneError):
        return error
    if isinstance(error, PersistenceIntegrityError):
        return internal_error()
    if isinstance(error, SQLAlchemyError | PersistenceError):
        return unavailable("persistence_unavailable", "PostgreSQL request processing failed.")
    return internal_error()
