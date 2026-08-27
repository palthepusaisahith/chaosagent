"""SQLAlchemy mappings for the ChaosAgent PostgreSQL persistence boundary."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    MetaData,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .lifecycle import RUN_STATUSES, RunStatus

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

IDENTIFIER_CHECK = r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"
REVISION_CHECK = r"^[A-Za-z0-9]+([._-][A-Za-z0-9]+)*$"
DIGEST_CHECK = r"^sha256:[0-9a-f]{64}$"


class Base(DeclarativeBase):
    """Declarative base shared by the persistence models and Alembic."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class ScenarioRevisionModel(Base):
    """Immutable, validated Scenario contract revision."""

    __tablename__ = "scenario_revisions"
    __table_args__ = (
        CheckConstraint("scenario_id ~ '" + IDENTIFIER_CHECK + "'", name="scenario_id_format"),
        CheckConstraint("revision ~ '" + REVISION_CHECK + "'", name="revision_format"),
        CheckConstraint("canonical_digest ~ '" + DIGEST_CHECK + "'", name="digest_format"),
        CheckConstraint("jsonb_typeof(canonical_document) = 'object'", name="document_object"),
        CheckConstraint(
            "(canonical_document ->> 'scenario_id') IS NOT DISTINCT FROM scenario_id",
            name="document_scenario_id",
        ),
        CheckConstraint(
            "(canonical_document ->> 'revision') IS NOT DISTINCT FROM revision",
            name="document_revision",
        ),
        CheckConstraint(
            "(canonical_document ->> 'schema_version') IS NOT DISTINCT FROM schema_version",
            name="document_schema_version",
        ),
        UniqueConstraint(
            "scenario_id", "revision", "canonical_digest", name="uq_scenario_revision_digest"
        ),
        {"schema": "public"},
    )

    scenario_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    revision: Mapped[str] = mapped_column(String(64), primary_key=True)
    schema_version: Mapped[str] = mapped_column(String(128), nullable=False)
    canonical_document: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    canonical_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    created_by: Mapped[str] = mapped_column(String(128), nullable=False)


class PolicyRevisionModel(Base):
    """Immutable, validated Policy contract revision."""

    __tablename__ = "policy_revisions"
    __table_args__ = (
        CheckConstraint("policy_id ~ '" + IDENTIFIER_CHECK + "'", name="policy_id_format"),
        CheckConstraint("revision ~ '" + REVISION_CHECK + "'", name="revision_format"),
        CheckConstraint("canonical_digest ~ '" + DIGEST_CHECK + "'", name="digest_format"),
        CheckConstraint("jsonb_typeof(canonical_document) = 'object'", name="document_object"),
        CheckConstraint(
            "(canonical_document ->> 'policy_id') IS NOT DISTINCT FROM policy_id",
            name="document_policy_id",
        ),
        CheckConstraint(
            "(canonical_document ->> 'revision') IS NOT DISTINCT FROM revision",
            name="document_revision",
        ),
        CheckConstraint(
            "(canonical_document ->> 'schema_version') IS NOT DISTINCT FROM schema_version",
            name="document_schema_version",
        ),
        UniqueConstraint(
            "policy_id", "revision", "canonical_digest", name="uq_policy_revision_digest"
        ),
        {"schema": "public"},
    )

    policy_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    revision: Mapped[str] = mapped_column(String(64), primary_key=True)
    schema_version: Mapped[str] = mapped_column(String(128), nullable=False)
    canonical_document: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    canonical_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    created_by: Mapped[str] = mapped_column(String(128), nullable=False)


class FixtureRevisionModel(Base):
    """Immutable, validated synthetic-company Fixture contract revision."""

    __tablename__ = "fixture_revisions"
    __table_args__ = (
        CheckConstraint("fixture_id ~ '" + IDENTIFIER_CHECK + "'", name="fixture_id_format"),
        CheckConstraint("revision ~ '" + REVISION_CHECK + "'", name="revision_format"),
        CheckConstraint("canonical_digest ~ '" + DIGEST_CHECK + "'", name="digest_format"),
        CheckConstraint("jsonb_typeof(canonical_document) = 'object'", name="document_object"),
        CheckConstraint(
            "(canonical_document ->> 'fixture_id') IS NOT DISTINCT FROM fixture_id",
            name="document_fixture_id",
        ),
        CheckConstraint(
            "(canonical_document ->> 'revision') IS NOT DISTINCT FROM revision",
            name="document_revision",
        ),
        CheckConstraint(
            "(canonical_document ->> 'schema_version') IS NOT DISTINCT FROM schema_version",
            name="document_schema_version",
        ),
        UniqueConstraint(
            "fixture_id", "revision", "canonical_digest", name="uq_fixture_revision_digest"
        ),
        {"schema": "public"},
    )

    fixture_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    revision: Mapped[str] = mapped_column(String(64), primary_key=True)
    schema_version: Mapped[str] = mapped_column(String(128), nullable=False)
    canonical_document: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    canonical_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    created_by: Mapped[str] = mapped_column(String(128), nullable=False)


class AgentConfigurationRevisionModel(Base):
    """Immutable identity/digest placeholder until an Agent Configuration contract exists."""

    __tablename__ = "agent_configuration_revisions"
    __table_args__ = (
        CheckConstraint(
            "agent_configuration_id ~ '" + IDENTIFIER_CHECK + "'",
            name="agent_configuration_id_format",
        ),
        CheckConstraint("revision ~ '" + REVISION_CHECK + "'", name="revision_format"),
        CheckConstraint("digest ~ '" + DIGEST_CHECK + "'", name="digest_format"),
        UniqueConstraint(
            "agent_configuration_id",
            "revision",
            "digest",
            name="uq_agent_configuration_revision_digest",
        ),
        {"schema": "public"},
    )

    agent_configuration_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    revision: Mapped[str] = mapped_column(String(64), primary_key=True)
    digest: Mapped[str] = mapped_column(String(71), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    created_by: Mapped[str] = mapped_column(String(128), nullable=False)


class RunModel(Base):
    """Run identity, frozen references, and Issue #6 coordination state."""

    __tablename__ = "runs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["scenario_id", "scenario_revision", "scenario_digest"],
            [
                "public.scenario_revisions.scenario_id",
                "public.scenario_revisions.revision",
                "public.scenario_revisions.canonical_digest",
            ],
            name="fk_runs_scenario_revision",
        ),
        ForeignKeyConstraint(
            [
                "agent_configuration_id",
                "agent_configuration_revision",
                "agent_configuration_digest",
            ],
            [
                "public.agent_configuration_revisions.agent_configuration_id",
                "public.agent_configuration_revisions.revision",
                "public.agent_configuration_revisions.digest",
            ],
            name="fk_runs_agent_configuration_revision",
        ),
        ForeignKeyConstraint(
            ["fixture_id", "fixture_revision", "fixture_digest"],
            [
                "public.fixture_revisions.fixture_id",
                "public.fixture_revisions.revision",
                "public.fixture_revisions.canonical_digest",
            ],
            name="fk_runs_fixture_revision",
        ),
        CheckConstraint("run_id ~ '" + IDENTIFIER_CHECK + "'", name="run_id_format"),
        CheckConstraint("status IN " + repr(RUN_STATUSES), name="status_value"),
        CheckConstraint("lifecycle_version >= 0", name="lifecycle_version_nonnegative"),
        CheckConstraint("attempt >= 0", name="attempt_nonnegative"),
        CheckConstraint(
            "(fixture_id IS NULL AND fixture_revision IS NULL AND fixture_digest IS NULL) OR "
            "(fixture_id IS NOT NULL AND fixture_revision IS NOT NULL "
            "AND fixture_digest IS NOT NULL)",
            name="fixture_binding_complete",
        ),
        CheckConstraint(
            "lease_owner IS NULL OR lease_owner ~ '" + IDENTIFIER_CHECK + "'",
            name="lease_owner_format",
        ),
        CheckConstraint(
            "lease_token IS NULL OR lease_token ~ '" + IDENTIFIER_CHECK + "'",
            name="lease_token_format",
        ),
        CheckConstraint(
            "((status IN ('provisioning', 'running', 'evaluating')) "
            "AND lease_owner IS NOT NULL AND lease_token IS NOT NULL "
            "AND lease_expires_at IS NOT NULL AND heartbeat_at IS NOT NULL "
            "AND lease_expires_at > heartbeat_at) OR "
            "((status IN ('queued', 'completed', 'failed', 'timed_out', "
            "'cancelled', 'infra_error')) AND lease_owner IS NULL "
            "AND lease_token IS NULL AND lease_expires_at IS NULL "
            "AND heartbeat_at IS NULL)",
            name="lease_state_consistency",
        ),
        UniqueConstraint(
            "run_id",
            "scenario_id",
            "scenario_revision",
            "scenario_digest",
            "agent_configuration_id",
            "agent_configuration_revision",
            "agent_configuration_digest",
            "status",
            name="uq_run_frozen_references",
        ),
        UniqueConstraint(
            "run_id", "fixture_id", "fixture_revision", "fixture_digest", name="uq_run_fixture"
        ),
        UniqueConstraint(
            "run_id",
            "scenario_id",
            "scenario_revision",
            "scenario_digest",
            name="uq_runs_approval_scenario",
        ),
        Index("ix_runs_scenario_revision", "scenario_id", "scenario_revision"),
        Index(
            "ix_runs_claim_queue",
            "created_at",
            "run_id",
            postgresql_where=text("status = 'queued'"),
        ),
        {"schema": "public"},
    )

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    scenario_id: Mapped[str] = mapped_column(String(128), nullable=False)
    scenario_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    scenario_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    agent_configuration_id: Mapped[str] = mapped_column(String(128), nullable=False)
    agent_configuration_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    agent_configuration_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    fixture_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    fixture_revision: Mapped[str | None] = mapped_column(String(64), nullable=True)
    fixture_digest: Mapped[str | None] = mapped_column(String(71), nullable=True)
    status: Mapped[RunStatus] = mapped_column(String(32), nullable=False, server_default="queued")
    lifecycle_version: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_token: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    attempt: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    created_by: Mapped[str] = mapped_column(String(128), nullable=False)


class ExecutionCheckpointModel(Base):
    """Mutable, CAS-protected Issue #11 trajectory for one Run."""

    __tablename__ = "execution_checkpoints"
    __table_args__ = (
        CheckConstraint(
            "schema_version = 'chaosagent.execution-checkpoint/v0'",
            name="schema_version_value",
        ),
        CheckConstraint(
            "checkpoint_version BETWEEN 1 AND 9007199254740991",
            name="checkpoint_version_positive",
        ),
        CheckConstraint(
            "lease_attempt BETWEEN 1 AND 9007199254740991", name="lease_attempt_positive"
        ),
        CheckConstraint(
            "last_event_sequence BETWEEN 1 AND 9007199254740991",
            name="last_event_sequence_positive",
        ),
        CheckConstraint("document_digest ~ '" + DIGEST_CHECK + "'", name="document_digest"),
        CheckConstraint("jsonb_typeof(document) = 'object'", name="document_object"),
        CheckConstraint(
            "(document ->> 'schema_version') IS NOT DISTINCT FROM schema_version",
            name="document_schema_version",
        ),
        CheckConstraint(
            "(document ->> 'run_id') IS NOT DISTINCT FROM run_id", name="document_run_id"
        ),
        CheckConstraint(
            "jsonb_typeof(document -> 'checkpoint_version') IS NOT DISTINCT FROM 'number' "
            "AND (document ->> 'checkpoint_version') ~ '^[1-9][0-9]*$' "
            "AND (document ->> 'checkpoint_version')::bigint "
            "IS NOT DISTINCT FROM checkpoint_version",
            name="document_checkpoint_version",
        ),
        CheckConstraint(
            "jsonb_typeof(document -> 'lease_attempt') IS NOT DISTINCT FROM 'number' "
            "AND (document ->> 'lease_attempt') ~ '^[1-9][0-9]*$' "
            "AND (document ->> 'lease_attempt')::bigint IS NOT DISTINCT FROM lease_attempt",
            name="document_lease_attempt",
        ),
        CheckConstraint(
            "jsonb_typeof(document -> 'last_event_sequence') IS NOT DISTINCT FROM 'number' "
            "AND (document ->> 'last_event_sequence') ~ '^[1-9][0-9]*$' "
            "AND (document ->> 'last_event_sequence')::bigint "
            "IS NOT DISTINCT FROM last_event_sequence",
            name="document_last_event_sequence",
        ),
        ForeignKeyConstraint(
            ["run_id"], ["public.runs.run_id"], name="fk_execution_checkpoints_run"
        ),
        {"schema": "public"},
    )

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    schema_version: Mapped[str] = mapped_column(String(128), nullable=False)
    checkpoint_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    lease_attempt: Mapped[int] = mapped_column(BigInteger, nullable=False)
    last_event_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    document: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    document_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()")
    )


class RunEventModel(Base):
    """Append-only Run Event contract document and query/index columns."""

    __tablename__ = "run_events"
    __table_args__ = (
        UniqueConstraint("run_id", "sequence", name="uq_run_events_run_sequence"),
        CheckConstraint("event_id ~ '" + IDENTIFIER_CHECK + "'", name="event_id_format"),
        CheckConstraint("sequence BETWEEN 1 AND 9007199254740991", name="sequence_safe_integer"),
        CheckConstraint("jsonb_typeof(document) = 'object'", name="document_object"),
        CheckConstraint(
            "(document ->> 'event_id') IS NOT DISTINCT FROM event_id",
            name="document_event_id",
        ),
        CheckConstraint(
            "(document ->> 'run_id') IS NOT DISTINCT FROM run_id", name="document_run_id"
        ),
        CheckConstraint(
            "((document ->> 'sequence')::bigint) IS NOT DISTINCT FROM sequence",
            name="document_sequence",
        ),
        CheckConstraint(
            "(document ->> 'schema_version') IS NOT DISTINCT FROM schema_version",
            name="document_schema_version",
        ),
        CheckConstraint(
            "(document ->> 'event_type') IS NOT DISTINCT FROM event_type",
            name="document_event_type",
        ),
        CheckConstraint(
            "((document ->> 'occurred_at')::timestamptz) IS NOT DISTINCT FROM occurred_at",
            name="document_occurred_at",
        ),
        CheckConstraint(
            "((document ->> 'recorded_at')::timestamptz) IS NOT DISTINCT FROM recorded_at",
            name="document_recorded_at",
        ),
        CheckConstraint(
            "(document ->> 'payload_digest') IS NOT DISTINCT FROM payload_digest",
            name="document_payload_digest",
        ),
        CheckConstraint("payload_digest ~ '" + DIGEST_CHECK + "'", name="payload_digest_format"),
        Index("ix_run_events_run_recorded", "run_id", "recorded_at"),
        {"schema": "public"},
    )

    event_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("public.runs.run_id", ondelete="RESTRICT"), nullable=False
    )
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    schema_version: Mapped[str] = mapped_column(String(128), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    document: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    payload_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    inserted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )


class ApprovalRequestModel(Base):
    """Immutable authorization request for one exact logical mutation."""

    __tablename__ = "approval_requests"
    __table_args__ = (
        CheckConstraint("approval_id ~ '" + IDENTIFIER_CHECK + "'", name="approval_id_format"),
        CheckConstraint("decision_id ~ '" + IDENTIFIER_CHECK + "'", name="decision_id_format"),
        CheckConstraint("tool_id = 'payments.refund'", name="tool_id_value"),
        CheckConstraint(
            "contract_version = 'chaosagent.tool/payments.refund/v0'",
            name="contract_version_value",
        ),
        CheckConstraint("request_digest ~ '" + DIGEST_CHECK + "'", name="request_digest"),
        CheckConstraint("idempotency_key_digest ~ '" + DIGEST_CHECK + "'", name="key_digest"),
        CheckConstraint("jsonb_typeof(arguments_document) = 'object'", name="arguments_object"),
        CheckConstraint(
            "jsonb_typeof(arguments_document -> 'amount_minor') "
            "IS NOT DISTINCT FROM 'number' AND "
            "(arguments_document ->> 'amount_minor') ~ '^[1-9][0-9]*$'",
            name="amount_integer",
        ),
        CheckConstraint(
            "jsonb_typeof(arguments_document -> 'order_id') IS NOT DISTINCT FROM 'string' AND "
            "jsonb_typeof(arguments_document -> 'payment_id') IS NOT DISTINCT FROM 'string' AND "
            "jsonb_typeof(arguments_document -> 'idempotency_key') "
            "IS NOT DISTINCT FROM 'string'",
            name="argument_identity",
        ),
        CheckConstraint("lease_attempt >= 1", name="lease_attempt_positive"),
        ForeignKeyConstraint(
            ["run_id", "scenario_id", "scenario_revision", "scenario_digest"],
            [
                "public.runs.run_id",
                "public.runs.scenario_id",
                "public.runs.scenario_revision",
                "public.runs.scenario_digest",
            ],
            name="fk_approval_requests_run_scenario",
        ),
        ForeignKeyConstraint(
            ["scenario_id", "scenario_revision", "scenario_digest"],
            [
                "public.scenario_revisions.scenario_id",
                "public.scenario_revisions.revision",
                "public.scenario_revisions.canonical_digest",
            ],
            name="fk_approval_requests_scenario",
        ),
        ForeignKeyConstraint(
            ["policy_id", "policy_revision", "policy_digest"],
            [
                "public.policy_revisions.policy_id",
                "public.policy_revisions.revision",
                "public.policy_revisions.canonical_digest",
            ],
            name="fk_approval_requests_policy",
        ),
        UniqueConstraint(
            "run_id",
            "scenario_id",
            "scenario_revision",
            "scenario_digest",
            "policy_id",
            "policy_revision",
            "policy_digest",
            "tool_id",
            "contract_version",
            "request_digest",
            "idempotency_key_digest",
            name="uq_approval_request_binding",
        ),
        UniqueConstraint("approval_id", "run_id", name="uq_approval_request_run"),
        {"schema": "public"},
    )

    approval_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(128), nullable=False)
    scenario_id: Mapped[str] = mapped_column(String(128), nullable=False)
    scenario_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    scenario_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    policy_id: Mapped[str] = mapped_column(String(128), nullable=False)
    policy_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    tool_id: Mapped[str] = mapped_column(String(128), nullable=False)
    contract_version: Mapped[str] = mapped_column(String(256), nullable=False)
    request_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    idempotency_key_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    arguments_document: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    logical_call_id: Mapped[str] = mapped_column(String(128), nullable=False)
    requested_attempt_id: Mapped[str] = mapped_column(String(128), nullable=False)
    lease_attempt: Mapped[int] = mapped_column(BigInteger, nullable=False)
    decision_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    decision_event_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    request_event_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()")
    )


class ApprovalResolutionModel(Base):
    """One immutable authoritative resolution for an approval request."""

    __tablename__ = "approval_resolutions"
    __table_args__ = (
        CheckConstraint("result IN ('approved', 'denied')", name="result_value"),
        CheckConstraint("responder_type IN ('human', 'simulated')", name="responder_type_value"),
        CheckConstraint("actor_id ~ '" + IDENTIFIER_CHECK + "'", name="actor_id_format"),
        ForeignKeyConstraint(
            ["approval_id", "run_id"],
            ["public.approval_requests.approval_id", "public.approval_requests.run_id"],
            name="fk_approval_resolutions_request",
        ),
        {"schema": "public"},
    )

    approval_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(128), nullable=False)
    result: Mapped[str] = mapped_column(String(16), nullable=False)
    actor_id: Mapped[str] = mapped_column(String(128), nullable=False)
    responder_type: Mapped[str] = mapped_column(String(16), nullable=False)
    resolution_event_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    resolved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()")
    )


class RunReportModel(Base):
    """One immutable final Run Report v0 document per run."""

    __tablename__ = "run_reports"
    __table_args__ = (
        ForeignKeyConstraint(
            [
                "run_id",
                "scenario_id",
                "scenario_revision",
                "scenario_digest",
                "agent_configuration_id",
                "agent_configuration_revision",
                "agent_configuration_digest",
                "run_status",
            ],
            [
                "public.runs.run_id",
                "public.runs.scenario_id",
                "public.runs.scenario_revision",
                "public.runs.scenario_digest",
                "public.runs.agent_configuration_id",
                "public.runs.agent_configuration_revision",
                "public.runs.agent_configuration_digest",
                "public.runs.status",
            ],
            name="fk_run_reports_frozen_run",
        ),
        CheckConstraint("report_id ~ '" + IDENTIFIER_CHECK + "'", name="report_id_format"),
        CheckConstraint("jsonb_typeof(document) = 'object'", name="document_object"),
        CheckConstraint(
            "(document ->> 'report_id') IS NOT DISTINCT FROM report_id",
            name="document_report_id",
        ),
        CheckConstraint(
            "(document ->> 'run_id') IS NOT DISTINCT FROM run_id", name="document_run_id"
        ),
        CheckConstraint(
            "(document ->> 'schema_version') IS NOT DISTINCT FROM schema_version",
            name="document_schema_version",
        ),
        CheckConstraint(
            "(document ->> 'run_status') IS NOT DISTINCT FROM run_status",
            name="document_run_status",
        ),
        CheckConstraint(
            "(document ->> 'classification') IS NOT DISTINCT FROM classification",
            name="document_classification",
        ),
        CheckConstraint(
            "run_status IN ('completed', 'failed', 'timed_out', 'cancelled', 'infra_error')",
            name="run_status_value",
        ),
        CheckConstraint(
            "classification IN ('pass', 'fail', 'invalid', 'not_evaluated')",
            name="classification_value",
        ),
        CheckConstraint(
            "((document ->> 'generated_at')::timestamptz) IS NOT DISTINCT FROM generated_at",
            name="document_generated_at",
        ),
        CheckConstraint(
            "(document #>> '{scenario,id}') IS NOT DISTINCT FROM scenario_id",
            name="document_scenario_id",
        ),
        CheckConstraint(
            "(document #>> '{scenario,revision}') IS NOT DISTINCT FROM scenario_revision",
            name="document_scenario_revision",
        ),
        CheckConstraint(
            "(document #>> '{scenario,digest}') IS NOT DISTINCT FROM scenario_digest",
            name="document_scenario_digest",
        ),
        CheckConstraint(
            "(document #>> '{agent_configuration,id}') IS NOT DISTINCT FROM agent_configuration_id",
            name="document_agent_configuration_id",
        ),
        CheckConstraint(
            "(document #>> '{agent_configuration,revision}') "
            "IS NOT DISTINCT FROM agent_configuration_revision",
            name="document_agent_configuration_revision",
        ),
        CheckConstraint(
            "(document #>> '{agent_configuration,digest}') "
            "IS NOT DISTINCT FROM agent_configuration_digest",
            name="document_agent_configuration_digest",
        ),
        {"schema": "public"},
    )

    report_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    schema_version: Mapped[str] = mapped_column(String(128), nullable=False)
    scenario_id: Mapped[str] = mapped_column(String(128), nullable=False)
    scenario_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    scenario_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    agent_configuration_id: Mapped[str] = mapped_column(String(128), nullable=False)
    agent_configuration_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    agent_configuration_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    run_status: Mapped[str] = mapped_column(String(32), nullable=False)
    classification: Mapped[str] = mapped_column(String(32), nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    document: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    inserted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )


class RunCompanyStateModel(Base):
    """One initialization marker binding a Run-local state to its Fixture revision."""

    __tablename__ = "run_company_state"
    __table_args__ = (
        ForeignKeyConstraint(
            ["run_id", "fixture_id", "fixture_revision", "fixture_digest"],
            [
                "public.runs.run_id",
                "public.runs.fixture_id",
                "public.runs.fixture_revision",
                "public.runs.fixture_digest",
            ],
            name="fk_run_company_state_frozen_run_fixture",
        ),
        {"schema": "public"},
    )

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    fixture_id: Mapped[str] = mapped_column(String(128), nullable=False)
    fixture_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    fixture_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    reference_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class CompanyCustomerModel(Base):
    __tablename__ = "company_customers"
    __table_args__ = (
        CheckConstraint("status IN ('active', 'suspended')", name="status_value"),
        ForeignKeyConstraint(
            ["run_id"], ["public.run_company_state.run_id"], name="fk_company_customers_state"
        ),
        {"schema": "public"},
    )

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    customer_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    email: Mapped[str] = mapped_column(String(320), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)


class CompanyOrderModel(Base):
    __tablename__ = "company_orders"
    __table_args__ = (
        CheckConstraint(
            "status IN ('placed', 'paid', 'fulfilled', 'cancelled')", name="status_value"
        ),
        CheckConstraint("currency ~ '^[A-Z]{3}$'", name="currency_format"),
        CheckConstraint("total_minor BETWEEN 1 AND 9007199254740991", name="total_minor_safe"),
        ForeignKeyConstraint(
            ["run_id", "customer_id"],
            ["public.company_customers.run_id", "public.company_customers.customer_id"],
            name="fk_company_orders_customer",
        ),
        UniqueConstraint("run_id", "order_id", "customer_id", name="uq_company_order_customer"),
        {"schema": "public"},
    )

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    order_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    customer_id: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    total_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    placed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class CompanyShipmentModel(Base):
    __tablename__ = "company_shipments"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'in_transit', 'delivered', 'failed', 'returned')",
            name="status_value",
        ),
        ForeignKeyConstraint(
            ["run_id", "order_id"],
            ["public.company_orders.run_id", "public.company_orders.order_id"],
            name="fk_company_shipments_order",
        ),
        UniqueConstraint("run_id", "order_id", name="uq_company_shipment_order"),
        {"schema": "public"},
    )

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    shipment_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    order_id: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    carrier: Mapped[str] = mapped_column(String(100), nullable=False)
    tracking_number: Mapped[str] = mapped_column(String(128), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class CompanyPaymentModel(Base):
    __tablename__ = "company_payments"
    __table_args__ = (
        CheckConstraint(
            "status IN ('captured', 'partially_refunded', 'refunded')",
            name="status_value",
        ),
        CheckConstraint("currency ~ '^[A-Z]{3}$'", name="currency_format"),
        CheckConstraint("amount_minor BETWEEN 1 AND 9007199254740991", name="amount_minor_safe"),
        ForeignKeyConstraint(
            ["run_id", "order_id"],
            ["public.company_orders.run_id", "public.company_orders.order_id"],
            name="fk_company_payments_order",
        ),
        UniqueConstraint("run_id", "order_id", name="uq_company_payment_order"),
        UniqueConstraint(
            "run_id", "payment_id", "order_id", name="uq_company_payment_order_reference"
        ),
        {"schema": "public"},
    )

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    payment_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    order_id: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    amount_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class CompanyRefundModel(Base):
    __tablename__ = "company_refunds"
    __table_args__ = (
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="status_value"),
        CheckConstraint("origin IN ('fixture', 'mutation')", name="origin_value"),
        CheckConstraint(
            "(origin = 'fixture' AND effect_id IS NULL) OR "
            "(origin = 'mutation' AND effect_id IS NOT NULL)",
            name="origin_effect",
        ),
        CheckConstraint("amount_minor BETWEEN 1 AND 9007199254740991", name="amount_minor_safe"),
        ForeignKeyConstraint(
            ["run_id", "payment_id", "order_id"],
            [
                "public.company_payments.run_id",
                "public.company_payments.payment_id",
                "public.company_payments.order_id",
            ],
            name="fk_company_refunds_payment_order",
        ),
        ForeignKeyConstraint(
            ["run_id", "effect_id"],
            ["public.company_effects.run_id", "public.company_effects.effect_id"],
            name="fk_company_refunds_effect",
        ),
        Index(
            "ix_company_refunds_run_payment_succeeded",
            "run_id",
            "payment_id",
            postgresql_where=text("status = 'succeeded'"),
        ),
        {"schema": "public"},
    )

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    refund_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    payment_id: Mapped[str] = mapped_column(String(128), nullable=False)
    order_id: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    amount_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    reason: Mapped[str] = mapped_column(String(500), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    origin: Mapped[str] = mapped_column(String(16), nullable=False)
    effect_id: Mapped[str | None] = mapped_column(String(128), nullable=True)


class CompanySupportTicketModel(Base):
    __tablename__ = "company_support_tickets"
    __table_args__ = (
        CheckConstraint("status IN ('open', 'pending', 'closed')", name="status_value"),
        ForeignKeyConstraint(
            ["run_id", "order_id", "customer_id"],
            [
                "public.company_orders.run_id",
                "public.company_orders.order_id",
                "public.company_orders.customer_id",
            ],
            name="fk_company_support_tickets_order_customer",
        ),
        ForeignKeyConstraint(
            ["run_id", "last_effect_id"],
            ["public.company_effects.run_id", "public.company_effects.effect_id"],
            name="fk_company_support_tickets_effect",
        ),
        {"schema": "public"},
    )

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    ticket_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    customer_id: Mapped[str] = mapped_column(String(128), nullable=False)
    order_id: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    subject: Mapped[str] = mapped_column(String(500), nullable=False)
    note: Mapped[str] = mapped_column(String(4000), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_effect_id: Mapped[str | None] = mapped_column(String(128), nullable=True)


class CompanyEffectModel(Base):
    """Immutable idempotency/effect record for one applied synthetic mutation."""

    __tablename__ = "company_effects"
    __table_args__ = (
        CheckConstraint("tool_id IN ('payments.refund', 'support.update_ticket')", name="tool_id"),
        CheckConstraint(
            "contract_version IN ("
            "'chaosagent.tool/payments.refund/v0', "
            "'chaosagent.tool/support.update_ticket/v0')",
            name="contract_version",
        ),
        CheckConstraint("idempotency_key_digest ~ '" + DIGEST_CHECK + "'", name="key_digest"),
        CheckConstraint("request_digest ~ '" + DIGEST_CHECK + "'", name="request_digest"),
        CheckConstraint("effect_id ~ '" + IDENTIFIER_CHECK + "'", name="effect_id"),
        CheckConstraint(
            "effect_kind IN ('refund.created', 'support_ticket.updated')", name="effect_kind"
        ),
        CheckConstraint("subject_type IN ('refund', 'support_ticket')", name="subject_type"),
        CheckConstraint("subject_id ~ '" + IDENTIFIER_CHECK + "'", name="subject_id"),
        CheckConstraint("effect_state = 'applied'", name="effect_state"),
        CheckConstraint("jsonb_typeof(result_document) = 'object'", name="result_object"),
        CheckConstraint(
            "(result_document ->> 'effect_id') IS NOT DISTINCT FROM effect_id",
            name="result_effect_id",
        ),
        CheckConstraint(
            "(result_document ->> 'application') IS NOT DISTINCT FROM 'newly_applied'",
            name="result_application",
        ),
        CheckConstraint(
            "(tool_id <> 'payments.refund') OR ("
            "jsonb_typeof(result_document -> 'order_id') IS NOT DISTINCT FROM 'string' AND "
            "jsonb_typeof(result_document -> 'payment_id') IS NOT DISTINCT FROM 'string' AND "
            "jsonb_typeof(result_document -> 'refund_id') IS NOT DISTINCT FROM 'string' AND "
            "(result_document ->> 'status') IS NOT DISTINCT FROM 'succeeded' AND "
            "jsonb_typeof(result_document -> 'amount_minor') IS NOT DISTINCT FROM 'number' AND "
            "(result_document ->> 'amount_minor') ~ '^[1-9][0-9]*$' AND "
            "jsonb_typeof(result_document -> 'currency') IS NOT DISTINCT FROM 'string' AND "
            "(result_document ->> 'currency') ~ '^[A-Z]{3}$')",
            name="refund_result_shape",
        ),
        CheckConstraint(
            "(tool_id <> 'support.update_ticket') OR ("
            "jsonb_typeof(result_document -> 'ticket_id') IS NOT DISTINCT FROM 'string' AND "
            "jsonb_typeof(result_document -> 'status') IS NOT DISTINCT FROM 'string' AND "
            "jsonb_typeof(result_document -> 'note') IS NOT DISTINCT FROM 'string' AND "
            "jsonb_typeof(result_document -> 'updated_at') IS NOT DISTINCT FROM 'string')",
            name="ticket_result_shape",
        ),
        CheckConstraint(
            "(tool_id = 'payments.refund' "
            "AND contract_version = 'chaosagent.tool/payments.refund/v0' "
            "AND effect_kind = 'refund.created' AND subject_type = 'refund' "
            "AND (result_document ->> 'refund_id') IS NOT DISTINCT FROM subject_id) OR "
            "(tool_id = 'support.update_ticket' "
            "AND contract_version = 'chaosagent.tool/support.update_ticket/v0' "
            "AND effect_kind = 'support_ticket.updated' AND subject_type = 'support_ticket' "
            "AND (result_document ->> 'ticket_id') IS NOT DISTINCT FROM subject_id)",
            name="tool_subject_projection",
        ),
        CheckConstraint("lease_attempt >= 1", name="lease_attempt_positive"),
        ForeignKeyConstraint(
            ["run_id"], ["public.run_company_state.run_id"], name="fk_company_effects_state"
        ),
        UniqueConstraint("run_id", "effect_id", name="uq_company_effect_run_effect_id"),
        {"schema": "public"},
    )

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    tool_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    contract_version: Mapped[str] = mapped_column(String(256), primary_key=True)
    idempotency_key_digest: Mapped[str] = mapped_column(String(71), primary_key=True)
    request_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    effect_id: Mapped[str] = mapped_column(String(128), nullable=False)
    effect_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    subject_type: Mapped[str] = mapped_column(String(64), nullable=False)
    subject_id: Mapped[str] = mapped_column(String(128), nullable=False)
    effect_state: Mapped[str] = mapped_column(String(32), nullable=False)
    result_document: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    logical_call_id: Mapped[str] = mapped_column(String(128), nullable=False)
    first_attempt_id: Mapped[str] = mapped_column(String(128), nullable=False)
    lease_attempt: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()")
    )
