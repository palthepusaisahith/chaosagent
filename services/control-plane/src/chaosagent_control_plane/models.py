"""Strict HTTP request and response models for control-plane API v1."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

Identifier = Annotated[
    str,
    Field(strict=True, min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"),
]
Revision = Annotated[
    str,
    Field(strict=True, min_length=1, max_length=64, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"),
]
Digest = Annotated[str, Field(strict=True, pattern=r"^sha256:[0-9a-f]{64}$")]
ContractVersion = Annotated[str, Field(strict=True, min_length=1, max_length=128)]
NonNegativeInt = Annotated[int, Field(strict=True, ge=0, le=9_007_199_254_740_991)]
PositiveInt = Annotated[int, Field(strict=True, ge=1, le=9_007_199_254_740_991)]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class RevisionReferenceModel(StrictModel):
    id: Identifier
    revision: Revision
    digest: Digest


class ScenarioCreateRequest(StrictModel):
    scenario: dict[str, object]
    created_by: Identifier


class ScenarioResponse(StrictModel):
    scenario_id: Identifier
    revision: Revision
    digest: Digest
    schema_version: str
    created_at: str
    created_by: Identifier
    document: dict[str, object]


class AgentConfigurationCreateRequest(StrictModel):
    agent_configuration: dict[str, object]
    created_by: Identifier


class AgentConfigurationResponse(StrictModel):
    agent_configuration_id: Identifier
    revision: Revision
    digest: Digest
    schema_version: str
    created_at: str
    created_by: Identifier
    document: dict[str, object]


class RunCreateRequest(StrictModel):
    run_id: Identifier
    scenario: RevisionReferenceModel
    agent_configuration: RevisionReferenceModel
    created_by: Identifier


class RunResponse(StrictModel):
    run_id: Identifier
    status: Literal[
        "queued",
        "provisioning",
        "running",
        "evaluating",
        "completed",
        "failed",
        "timed_out",
        "cancelled",
        "infra_error",
    ]
    lifecycle_version: NonNegativeInt
    attempt: NonNegativeInt
    scenario: RevisionReferenceModel
    agent_configuration: RevisionReferenceModel
    fixture: RevisionReferenceModel | None
    fault_seed: NonNegativeInt | None
    fault_plan_digest: Digest | None
    lease_expires_at: str | None
    heartbeat_at: str | None
    created_at: str
    created_by: Identifier


class EventPageResponse(StrictModel):
    run_id: Identifier
    events: list[dict[str, object]]
    next_cursor: str | None
    has_more: bool


class ReportResponse(StrictModel):
    run_id: Identifier
    kind: Literal["run_report", "evaluation_result"]
    document_id: Identifier
    digest: Digest
    inserted_at: str | None
    document: dict[str, object]


class CancellationResponse(StrictModel):
    run: RunResponse
    already_cancelled: bool


class ApprovalResponse(StrictModel):
    approval_id: Identifier
    run_id: Identifier
    scenario: RevisionReferenceModel
    policy: RevisionReferenceModel
    tool_id: Identifier
    contract_version: ContractVersion
    request_digest: Digest
    idempotency_key_digest: Digest
    logical_call_id: Identifier
    requested_attempt_id: Identifier
    lease_attempt: NonNegativeInt
    decision_id: Identifier
    decision_event_id: Identifier
    request_event_id: Identifier
    status: Literal["pending", "approved", "denied"]
    created_at: str
    resolved_at: str | None
    actor_id: Identifier | None
    resolution_event_id: Identifier | None


class ApprovalListResponse(StrictModel):
    run_id: Identifier
    approvals: list[ApprovalResponse]


class ApprovalResolveRequest(StrictModel):
    result: Literal["approved", "denied"]
    actor_id: Identifier


class ApprovalResolutionResponse(StrictModel):
    approval: ApprovalResponse
    already_resolved: bool


class CampaignAssignment(StrictModel):
    trial_index: NonNegativeInt
    run_id: Identifier


class CampaignCreateRequest(StrictModel):
    campaign_id: Identifier
    arm: Literal["baseline", "faulted"]
    selected_fault_ids: Annotated[list[Identifier], Field(max_length=64)]
    assignments: Annotated[list[CampaignAssignment], Field(min_length=1, max_length=1_000)]


class CampaignResponse(StrictModel):
    schema_version: str
    campaign_id: Identifier
    arm: Literal["baseline", "faulted"]
    planned_trials: PositiveInt
    scenario: RevisionReferenceModel
    agent_configuration: RevisionReferenceModel
    selected_fault_ids: list[Identifier]
    fault_plan_digest: Digest
    assignments: list[CampaignAssignment]
    digest: Digest
    created_at: str


class CampaignStatisticsResponse(StrictModel):
    campaign_id: Identifier
    digest: Digest
    document: dict[str, object]


class CampaignComparisonResponse(StrictModel):
    baseline_campaign_id: Identifier
    faulted_campaign_id: Identifier
    digest: Digest
    document: dict[str, object]


class ExportRequest(StrictModel):
    exported_at: Annotated[str, Field(strict=True, min_length=20, max_length=40)]
    k_values: Annotated[list[PositiveInt], Field(min_length=1, max_length=32)] = Field(
        default_factory=lambda: [1]
    )


class HealthResponse(StrictModel):
    status: Literal["ok"]


class ReadyResponse(StrictModel):
    status: Literal["ready"]


class ErrorBody(StrictModel):
    code: str
    message: str
    details: dict[str, object] | None = None


class ErrorResponse(StrictModel):
    error: ErrorBody
