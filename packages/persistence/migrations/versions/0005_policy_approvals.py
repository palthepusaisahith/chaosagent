"""Add immutable Policy revisions and one-request approvals.

Revision ID: 0005_policy_approvals
Revises: 0004_mutation_effect_ledger
Create Date: 2026-08-26
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0005_policy_approvals"
down_revision: str | None = "0004_mutation_effect_ledger"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ID = r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"
_REVISION = r"^[A-Za-z0-9]+([._-][A-Za-z0-9]+)*$"
_DIGEST = r"^sha256:[0-9a-f]{64}$"


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_runs_approval_scenario",
        "runs",
        ["run_id", "scenario_id", "scenario_revision", "scenario_digest"],
        schema="public",
    )
    op.create_table(
        "policy_revisions",
        sa.Column("policy_id", sa.String(128), nullable=False),
        sa.Column("revision", sa.String(64), nullable=False),
        sa.Column("schema_version", sa.String(128), nullable=False),
        sa.Column("canonical_document", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("canonical_digest", sa.String(71), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("created_by", sa.String(128), nullable=False),
        sa.CheckConstraint(f"policy_id ~ '{_ID}'", name="policy_id_format"),
        sa.CheckConstraint(f"revision ~ '{_REVISION}'", name="revision_format"),
        sa.CheckConstraint(f"canonical_digest ~ '{_DIGEST}'", name="digest_format"),
        sa.CheckConstraint("jsonb_typeof(canonical_document) = 'object'", name="document_object"),
        sa.CheckConstraint(
            "(canonical_document ->> 'policy_id') IS NOT DISTINCT FROM policy_id",
            name="document_policy_id",
        ),
        sa.CheckConstraint(
            "(canonical_document ->> 'revision') IS NOT DISTINCT FROM revision",
            name="document_revision",
        ),
        sa.CheckConstraint(
            "(canonical_document ->> 'schema_version') IS NOT DISTINCT FROM schema_version",
            name="document_schema_version",
        ),
        sa.PrimaryKeyConstraint("policy_id", "revision", name="pk_policy_revisions"),
        sa.UniqueConstraint(
            "policy_id", "revision", "canonical_digest", name="uq_policy_revision_digest"
        ),
        schema="public",
    )
    op.execute(
        "CREATE TRIGGER policy_revisions_immutable BEFORE UPDATE OR DELETE "
        "ON public.policy_revisions FOR EACH ROW EXECUTE FUNCTION "
        "public.chaosagent_reject_immutable_change()"
    )

    op.create_table(
        "approval_requests",
        sa.Column("approval_id", sa.String(128), nullable=False),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("scenario_id", sa.String(128), nullable=False),
        sa.Column("scenario_revision", sa.String(64), nullable=False),
        sa.Column("scenario_digest", sa.String(71), nullable=False),
        sa.Column("policy_id", sa.String(128), nullable=False),
        sa.Column("policy_revision", sa.String(64), nullable=False),
        sa.Column("policy_digest", sa.String(71), nullable=False),
        sa.Column("tool_id", sa.String(128), nullable=False),
        sa.Column("contract_version", sa.String(256), nullable=False),
        sa.Column("request_digest", sa.String(71), nullable=False),
        sa.Column("idempotency_key_digest", sa.String(71), nullable=False),
        sa.Column("arguments_document", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("logical_call_id", sa.String(128), nullable=False),
        sa.Column("requested_attempt_id", sa.String(128), nullable=False),
        sa.Column("lease_attempt", sa.BigInteger(), nullable=False),
        sa.Column("decision_id", sa.String(128), nullable=False),
        sa.Column("decision_event_id", sa.String(128), nullable=False),
        sa.Column("request_event_id", sa.String(128), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(f"approval_id ~ '{_ID}'", name="approval_id_format"),
        sa.CheckConstraint(f"decision_id ~ '{_ID}'", name="decision_id_format"),
        sa.CheckConstraint("tool_id = 'payments.refund'", name="tool_id_value"),
        sa.CheckConstraint(
            "contract_version = 'chaosagent.tool/payments.refund/v0'",
            name="contract_version_value",
        ),
        sa.CheckConstraint(f"request_digest ~ '{_DIGEST}'", name="request_digest"),
        sa.CheckConstraint(f"idempotency_key_digest ~ '{_DIGEST}'", name="key_digest"),
        sa.CheckConstraint("jsonb_typeof(arguments_document) = 'object'", name="arguments_object"),
        sa.CheckConstraint(
            "jsonb_typeof(arguments_document -> 'amount_minor') "
            "IS NOT DISTINCT FROM 'number' AND "
            "(arguments_document ->> 'amount_minor') ~ '^[1-9][0-9]*$'",
            name="amount_integer",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(arguments_document -> 'order_id') IS NOT DISTINCT FROM 'string' AND "
            "jsonb_typeof(arguments_document -> 'payment_id') IS NOT DISTINCT FROM 'string' AND "
            "jsonb_typeof(arguments_document -> 'idempotency_key') "
            "IS NOT DISTINCT FROM 'string'",
            name="argument_identity",
        ),
        sa.CheckConstraint("lease_attempt >= 1", name="lease_attempt_positive"),
        sa.ForeignKeyConstraint(
            ["run_id", "scenario_id", "scenario_revision", "scenario_digest"],
            [
                "public.runs.run_id",
                "public.runs.scenario_id",
                "public.runs.scenario_revision",
                "public.runs.scenario_digest",
            ],
            name="fk_approval_requests_run_scenario",
        ),
        sa.ForeignKeyConstraint(
            ["scenario_id", "scenario_revision", "scenario_digest"],
            [
                "public.scenario_revisions.scenario_id",
                "public.scenario_revisions.revision",
                "public.scenario_revisions.canonical_digest",
            ],
            name="fk_approval_requests_scenario",
        ),
        sa.ForeignKeyConstraint(
            ["policy_id", "policy_revision", "policy_digest"],
            [
                "public.policy_revisions.policy_id",
                "public.policy_revisions.revision",
                "public.policy_revisions.canonical_digest",
            ],
            name="fk_approval_requests_policy",
        ),
        sa.PrimaryKeyConstraint("approval_id", name="pk_approval_requests"),
        sa.UniqueConstraint("approval_id", "run_id", name="uq_approval_request_run"),
        sa.UniqueConstraint("decision_id", name="uq_approval_requests_decision_id"),
        sa.UniqueConstraint("decision_event_id", name="uq_approval_requests_decision_event_id"),
        sa.UniqueConstraint("request_event_id", name="uq_approval_requests_request_event_id"),
        sa.UniqueConstraint(
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
        schema="public",
    )
    op.execute(
        "CREATE TRIGGER approval_requests_immutable BEFORE UPDATE OR DELETE "
        "ON public.approval_requests FOR EACH ROW EXECUTE FUNCTION "
        "public.chaosagent_reject_immutable_change()"
    )

    op.create_table(
        "approval_resolutions",
        sa.Column("approval_id", sa.String(128), nullable=False),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("result", sa.String(16), nullable=False),
        sa.Column("actor_id", sa.String(128), nullable=False),
        sa.Column("responder_type", sa.String(16), nullable=False),
        sa.Column("resolution_event_id", sa.String(128), nullable=False),
        sa.Column(
            "resolved_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint("result IN ('approved', 'denied')", name="result_value"),
        sa.CheckConstraint("responder_type IN ('human', 'simulated')", name="responder_type_value"),
        sa.CheckConstraint(f"actor_id ~ '{_ID}'", name="actor_id_format"),
        sa.ForeignKeyConstraint(
            ["approval_id", "run_id"],
            ["public.approval_requests.approval_id", "public.approval_requests.run_id"],
            name="fk_approval_resolutions_request",
        ),
        sa.PrimaryKeyConstraint("approval_id", name="pk_approval_resolutions"),
        sa.UniqueConstraint(
            "resolution_event_id", name="uq_approval_resolutions_resolution_event_id"
        ),
        schema="public",
    )
    op.execute(
        "CREATE TRIGGER approval_resolutions_immutable BEFORE UPDATE OR DELETE "
        "ON public.approval_resolutions FOR EACH ROW EXECUTE FUNCTION "
        "public.chaosagent_reject_immutable_change()"
    )
    op.execute(
        "REVOKE UPDATE, DELETE ON public.policy_revisions, public.approval_requests, "
        "public.approval_resolutions FROM PUBLIC"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER approval_resolutions_immutable ON public.approval_resolutions")
    op.drop_table("approval_resolutions", schema="public")
    op.execute("DROP TRIGGER approval_requests_immutable ON public.approval_requests")
    op.drop_table("approval_requests", schema="public")
    op.execute("DROP TRIGGER policy_revisions_immutable ON public.policy_revisions")
    op.drop_table("policy_revisions", schema="public")
    op.drop_constraint("uq_runs_approval_scenario", "runs", schema="public", type_="unique")
