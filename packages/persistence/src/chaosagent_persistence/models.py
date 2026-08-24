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
        CheckConstraint("run_id ~ '" + IDENTIFIER_CHECK + "'", name="run_id_format"),
        CheckConstraint("status IN " + repr(RUN_STATUSES), name="status_value"),
        CheckConstraint("lifecycle_version >= 0", name="lifecycle_version_nonnegative"),
        CheckConstraint("attempt >= 0", name="attempt_nonnegative"),
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
