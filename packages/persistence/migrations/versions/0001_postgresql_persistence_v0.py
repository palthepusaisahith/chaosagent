"""Create immutable revisions, structural runs, events, and final reports.

Revision ID: 0001_persistence_v0
Revises: None
Create Date: 2026-08-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_persistence_v0"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_IDENTIFIER = r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"
_REVISION = r"^[A-Za-z0-9]+([._-][A-Za-z0-9]+)*$"
_DIGEST = r"^sha256:[0-9a-f]{64}$"


def upgrade() -> None:
    op.create_table(
        "scenario_revisions",
        sa.Column("scenario_id", sa.String(length=128), nullable=False),
        sa.Column("revision", sa.String(length=64), nullable=False),
        sa.Column("schema_version", sa.String(length=128), nullable=False),
        sa.Column("canonical_document", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("canonical_digest", sa.String(length=71), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("created_by", sa.String(length=128), nullable=False),
        sa.CheckConstraint(f"scenario_id ~ '{_IDENTIFIER}'", name="scenario_id_format"),
        sa.CheckConstraint(f"revision ~ '{_REVISION}'", name="revision_format"),
        sa.CheckConstraint(f"canonical_digest ~ '{_DIGEST}'", name="digest_format"),
        sa.CheckConstraint(
            "jsonb_typeof(canonical_document) = 'object'",
            name="document_object",
        ),
        sa.CheckConstraint(
            "(canonical_document ->> 'scenario_id') IS NOT DISTINCT FROM scenario_id",
            name="document_scenario_id",
        ),
        sa.CheckConstraint(
            "(canonical_document ->> 'revision') IS NOT DISTINCT FROM revision",
            name="document_revision",
        ),
        sa.CheckConstraint(
            "(canonical_document ->> 'schema_version') IS NOT DISTINCT FROM schema_version",
            name="document_schema_version",
        ),
        sa.PrimaryKeyConstraint("scenario_id", "revision", name="pk_scenario_revisions"),
        sa.UniqueConstraint(
            "scenario_id", "revision", "canonical_digest", name="uq_scenario_revision_digest"
        ),
        schema="public",
    )
    op.create_table(
        "agent_configuration_revisions",
        sa.Column("agent_configuration_id", sa.String(length=128), nullable=False),
        sa.Column("revision", sa.String(length=64), nullable=False),
        sa.Column("digest", sa.String(length=71), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("created_by", sa.String(length=128), nullable=False),
        sa.CheckConstraint(
            f"agent_configuration_id ~ '{_IDENTIFIER}'",
            name="agent_configuration_id_format",
        ),
        sa.CheckConstraint(f"revision ~ '{_REVISION}'", name="revision_format"),
        sa.CheckConstraint(f"digest ~ '{_DIGEST}'", name="digest_format"),
        sa.PrimaryKeyConstraint(
            "agent_configuration_id", "revision", name="pk_agent_configuration_revisions"
        ),
        sa.UniqueConstraint(
            "agent_configuration_id",
            "revision",
            "digest",
            name="uq_agent_configuration_revision_digest",
        ),
        schema="public",
    )
    op.create_table(
        "runs",
        sa.Column("run_id", sa.String(length=128), nullable=False),
        sa.Column("scenario_id", sa.String(length=128), nullable=False),
        sa.Column("scenario_revision", sa.String(length=64), nullable=False),
        sa.Column("scenario_digest", sa.String(length=71), nullable=False),
        sa.Column("agent_configuration_id", sa.String(length=128), nullable=False),
        sa.Column("agent_configuration_revision", sa.String(length=64), nullable=False),
        sa.Column("agent_configuration_digest", sa.String(length=71), nullable=False),
        sa.Column("status", sa.String(length=32), server_default="queued", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("created_by", sa.String(length=128), nullable=False),
        sa.CheckConstraint(f"run_id ~ '{_IDENTIFIER}'", name="run_id_format"),
        sa.CheckConstraint(
            "status IN ('queued', 'provisioning', 'running', 'evaluating', "
            "'completed', 'failed', 'timed_out', 'cancelled', 'infra_error')",
            name="status_value",
        ),
        sa.ForeignKeyConstraint(
            ["scenario_id", "scenario_revision", "scenario_digest"],
            [
                "public.scenario_revisions.scenario_id",
                "public.scenario_revisions.revision",
                "public.scenario_revisions.canonical_digest",
            ],
            name="fk_runs_scenario_revision",
        ),
        sa.ForeignKeyConstraint(
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
        sa.PrimaryKeyConstraint("run_id", name="pk_runs"),
        sa.UniqueConstraint(
            "run_id",
            "scenario_id",
            "scenario_revision",
            "scenario_digest",
            "agent_configuration_id",
            "agent_configuration_revision",
            "agent_configuration_digest",
            name="uq_run_frozen_references",
        ),
        schema="public",
    )
    op.create_index(
        "ix_runs_scenario_revision",
        "runs",
        ["scenario_id", "scenario_revision"],
        schema="public",
    )
    op.create_table(
        "run_events",
        sa.Column("event_id", sa.String(length=128), nullable=False),
        sa.Column("run_id", sa.String(length=128), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("schema_version", sa.String(length=128), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("document", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("payload_digest", sa.String(length=71), nullable=False),
        sa.Column(
            "inserted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(f"event_id ~ '{_IDENTIFIER}'", name="event_id_format"),
        sa.CheckConstraint("sequence BETWEEN 1 AND 9007199254740991", name="sequence_safe_integer"),
        sa.CheckConstraint("jsonb_typeof(document) = 'object'", name="document_object"),
        sa.CheckConstraint(
            "(document ->> 'event_id') IS NOT DISTINCT FROM event_id",
            name="document_event_id",
        ),
        sa.CheckConstraint(
            "(document ->> 'run_id') IS NOT DISTINCT FROM run_id", name="document_run_id"
        ),
        sa.CheckConstraint(
            "((document ->> 'sequence')::bigint) IS NOT DISTINCT FROM sequence",
            name="document_sequence",
        ),
        sa.CheckConstraint(
            "(document ->> 'schema_version') IS NOT DISTINCT FROM schema_version",
            name="document_schema_version",
        ),
        sa.CheckConstraint(
            "(document ->> 'event_type') IS NOT DISTINCT FROM event_type",
            name="document_event_type",
        ),
        sa.CheckConstraint(
            "((document ->> 'occurred_at')::timestamptz) IS NOT DISTINCT FROM occurred_at",
            name="document_occurred_at",
        ),
        sa.CheckConstraint(
            "((document ->> 'recorded_at')::timestamptz) IS NOT DISTINCT FROM recorded_at",
            name="document_recorded_at",
        ),
        sa.CheckConstraint(
            "(document ->> 'payload_digest') IS NOT DISTINCT FROM payload_digest",
            name="document_payload_digest",
        ),
        sa.CheckConstraint(f"payload_digest ~ '{_DIGEST}'", name="payload_digest_format"),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["public.runs.run_id"],
            name="fk_run_events_run_id_runs",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("event_id", name="pk_run_events"),
        sa.UniqueConstraint("run_id", "sequence", name="uq_run_events_run_sequence"),
        schema="public",
    )
    op.create_index(
        "ix_run_events_run_recorded",
        "run_events",
        ["run_id", "recorded_at"],
        schema="public",
    )
    op.create_table(
        "run_reports",
        sa.Column("report_id", sa.String(length=128), nullable=False),
        sa.Column("run_id", sa.String(length=128), nullable=False),
        sa.Column("schema_version", sa.String(length=128), nullable=False),
        sa.Column("scenario_id", sa.String(length=128), nullable=False),
        sa.Column("scenario_revision", sa.String(length=64), nullable=False),
        sa.Column("scenario_digest", sa.String(length=71), nullable=False),
        sa.Column("agent_configuration_id", sa.String(length=128), nullable=False),
        sa.Column("agent_configuration_revision", sa.String(length=64), nullable=False),
        sa.Column("agent_configuration_digest", sa.String(length=71), nullable=False),
        sa.Column("run_status", sa.String(length=32), nullable=False),
        sa.Column("classification", sa.String(length=32), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("document", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "inserted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(f"report_id ~ '{_IDENTIFIER}'", name="report_id_format"),
        sa.CheckConstraint("jsonb_typeof(document) = 'object'", name="document_object"),
        sa.CheckConstraint(
            "(document ->> 'report_id') IS NOT DISTINCT FROM report_id",
            name="document_report_id",
        ),
        sa.CheckConstraint(
            "(document ->> 'run_id') IS NOT DISTINCT FROM run_id", name="document_run_id"
        ),
        sa.CheckConstraint(
            "(document ->> 'schema_version') IS NOT DISTINCT FROM schema_version",
            name="document_schema_version",
        ),
        sa.CheckConstraint(
            "(document ->> 'run_status') IS NOT DISTINCT FROM run_status",
            name="document_run_status",
        ),
        sa.CheckConstraint(
            "(document ->> 'classification') IS NOT DISTINCT FROM classification",
            name="document_classification",
        ),
        sa.CheckConstraint(
            "run_status IN ('completed', 'failed', 'timed_out', 'cancelled', 'infra_error')",
            name="run_status_value",
        ),
        sa.CheckConstraint(
            "classification IN ('pass', 'fail', 'invalid', 'not_evaluated')",
            name="classification_value",
        ),
        sa.CheckConstraint(
            "((document ->> 'generated_at')::timestamptz) IS NOT DISTINCT FROM generated_at",
            name="document_generated_at",
        ),
        sa.CheckConstraint(
            "(document #>> '{scenario,id}') IS NOT DISTINCT FROM scenario_id",
            name="document_scenario_id",
        ),
        sa.CheckConstraint(
            "(document #>> '{scenario,revision}') IS NOT DISTINCT FROM scenario_revision",
            name="document_scenario_revision",
        ),
        sa.CheckConstraint(
            "(document #>> '{scenario,digest}') IS NOT DISTINCT FROM scenario_digest",
            name="document_scenario_digest",
        ),
        sa.CheckConstraint(
            "(document #>> '{agent_configuration,id}') IS NOT DISTINCT FROM agent_configuration_id",
            name="document_agent_configuration_id",
        ),
        sa.CheckConstraint(
            "(document #>> '{agent_configuration,revision}') "
            "IS NOT DISTINCT FROM agent_configuration_revision",
            name="document_agent_configuration_revision",
        ),
        sa.CheckConstraint(
            "(document #>> '{agent_configuration,digest}') "
            "IS NOT DISTINCT FROM agent_configuration_digest",
            name="document_agent_configuration_digest",
        ),
        sa.ForeignKeyConstraint(
            [
                "run_id",
                "scenario_id",
                "scenario_revision",
                "scenario_digest",
                "agent_configuration_id",
                "agent_configuration_revision",
                "agent_configuration_digest",
            ],
            [
                "public.runs.run_id",
                "public.runs.scenario_id",
                "public.runs.scenario_revision",
                "public.runs.scenario_digest",
                "public.runs.agent_configuration_id",
                "public.runs.agent_configuration_revision",
                "public.runs.agent_configuration_digest",
            ],
            name="fk_run_reports_frozen_run",
        ),
        sa.PrimaryKeyConstraint("report_id", name="pk_run_reports"),
        sa.UniqueConstraint("run_id", name="uq_run_reports_run_id"),
        schema="public",
    )

    op.execute(
        """
        CREATE FUNCTION public.chaosagent_reject_immutable_change()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION 'table % is append-only/immutable', TG_TABLE_NAME
                USING ERRCODE = '55000';
        END;
        $$
        """
    )
    for table in (
        "scenario_revisions",
        "agent_configuration_revisions",
        "run_events",
        "run_reports",
    ):
        op.execute(
            f"CREATE TRIGGER trg_{table}_immutable "
            f"BEFORE UPDATE OR DELETE ON public.{table} "
            "FOR EACH ROW EXECUTE FUNCTION public.chaosagent_reject_immutable_change()"
        )
        op.execute(f"REVOKE UPDATE, DELETE ON public.{table} FROM PUBLIC")


def downgrade() -> None:
    op.drop_table("run_reports", schema="public")
    op.drop_index("ix_run_events_run_recorded", table_name="run_events", schema="public")
    op.drop_table("run_events", schema="public")
    op.drop_index("ix_runs_scenario_revision", table_name="runs", schema="public")
    op.drop_table("runs", schema="public")
    op.drop_table("agent_configuration_revisions", schema="public")
    op.drop_table("scenario_revisions", schema="public")
    op.execute("DROP FUNCTION public.chaosagent_reject_immutable_change()")
