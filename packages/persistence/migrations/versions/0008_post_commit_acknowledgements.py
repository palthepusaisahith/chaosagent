"""Add immutable recovery markers for ambiguous post-commit acknowledgements.

Revision ID: 0008_post_commit_ack
Revises: 0007_agent_configuration_v0
Create Date: 2026-08-29
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0008_post_commit_ack"
down_revision: str | None = "0007_agent_configuration_v0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_run_events_run_event", "run_events", ["run_id", "event_id"], schema="public"
    )
    op.create_unique_constraint(
        "uq_approval_requests_run_approval",
        "approval_requests",
        ["run_id", "approval_id"],
        schema="public",
    )
    op.create_table(
        "post_commit_acknowledgements",
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("attempt_id", sa.String(128), nullable=False),
        sa.Column("logical_call_id", sa.String(128), nullable=False),
        sa.Column("attempt_number", sa.BigInteger(), nullable=False),
        sa.Column("call_ordinal", sa.BigInteger(), nullable=False),
        sa.Column("tool_id", sa.String(128), nullable=False),
        sa.Column("contract_version", sa.String(256), nullable=False),
        sa.Column("idempotency_key_digest", sa.String(71), nullable=False),
        sa.Column("request_digest", sa.String(71), nullable=False),
        sa.Column("arguments_digest", sa.String(71), nullable=False),
        sa.Column("effect_id", sa.String(128), nullable=False),
        sa.Column("lease_attempt", sa.BigInteger(), nullable=False),
        sa.Column("request_event_id", sa.String(128), nullable=False),
        sa.Column("state_evidence_event_id", sa.String(128), nullable=False),
        sa.Column("policy_decision_event_id", sa.String(128), nullable=False),
        sa.Column("approval_id", sa.String(128), nullable=True),
        sa.Column("fault_id", sa.String(128), nullable=False),
        sa.Column("activation_id", sa.String(128), nullable=False),
        sa.Column("timeout_duration_ms", sa.BigInteger(), nullable=False),
        sa.Column("matched_event_id", sa.String(128), nullable=False),
        sa.Column("applied_event_id", sa.String(128), nullable=False),
        sa.Column("result_event_id", sa.String(128), nullable=False),
        sa.Column("observed_event_id", sa.String(128), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint("attempt_number >= 1", name="attempt_number_positive"),
        sa.CheckConstraint("call_ordinal BETWEEN 1 AND 1000", name="call_ordinal"),
        sa.CheckConstraint("lease_attempt >= 1", name="lease_attempt_positive"),
        sa.CheckConstraint("attempt_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]*$'", name="attempt_id"),
        sa.CheckConstraint(
            "logical_call_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]*$'", name="logical_call_id"
        ),
        sa.CheckConstraint("effect_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]*$'", name="effect_id"),
        sa.CheckConstraint(
            "tool_id IN ('payments.refund', 'support.update_ticket')", name="tool_id"
        ),
        sa.CheckConstraint(
            "contract_version IN ("
            "'chaosagent.tool/payments.refund/v0', "
            "'chaosagent.tool/support.update_ticket/v0')",
            name="contract_version",
        ),
        sa.CheckConstraint("idempotency_key_digest ~ '^sha256:[0-9a-f]{64}$'", name="key_digest"),
        sa.CheckConstraint("request_digest ~ '^sha256:[0-9a-f]{64}$'", name="request_digest"),
        sa.CheckConstraint("arguments_digest ~ '^sha256:[0-9a-f]{64}$'", name="arguments_digest"),
        sa.CheckConstraint("fault_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]*$'", name="fault_id"),
        sa.CheckConstraint("activation_id ~ '^activation-[0-9a-f]{64}$'", name="activation_id"),
        sa.CheckConstraint("timeout_duration_ms BETWEEN 1 AND 600000", name="timeout_duration"),
        sa.CheckConstraint(
            "matched_event_id <> applied_event_id AND "
            "matched_event_id <> result_event_id AND "
            "matched_event_id <> observed_event_id AND "
            "applied_event_id <> result_event_id AND "
            "applied_event_id <> observed_event_id AND "
            "result_event_id <> observed_event_id",
            name="planned_event_ids_distinct",
        ),
        sa.ForeignKeyConstraint(
            ["run_id", "tool_id", "contract_version", "idempotency_key_digest"],
            [
                "public.company_effects.run_id",
                "public.company_effects.tool_id",
                "public.company_effects.contract_version",
                "public.company_effects.idempotency_key_digest",
            ],
            name="fk_post_commit_ack_effect",
        ),
        sa.ForeignKeyConstraint(
            ["run_id", "effect_id"],
            ["public.company_effects.run_id", "public.company_effects.effect_id"],
            name="fk_post_commit_ack_effect_id",
        ),
        sa.ForeignKeyConstraint(
            ["run_id", "request_event_id"],
            ["public.run_events.run_id", "public.run_events.event_id"],
            name="fk_post_commit_ack_request_event",
        ),
        sa.ForeignKeyConstraint(
            ["run_id", "state_evidence_event_id"],
            ["public.run_events.run_id", "public.run_events.event_id"],
            name="fk_post_commit_ack_state_event",
        ),
        sa.ForeignKeyConstraint(
            ["run_id", "policy_decision_event_id"],
            ["public.run_events.run_id", "public.run_events.event_id"],
            name="fk_post_commit_ack_policy_event",
        ),
        sa.ForeignKeyConstraint(
            ["run_id", "approval_id"],
            ["public.approval_requests.run_id", "public.approval_requests.approval_id"],
            name="fk_post_commit_ack_approval",
        ),
        sa.PrimaryKeyConstraint("run_id", "attempt_id"),
        sa.UniqueConstraint("run_id", "effect_id", name="uq_post_commit_ack_effect"),
        sa.UniqueConstraint("matched_event_id"),
        sa.UniqueConstraint("applied_event_id"),
        sa.UniqueConstraint("result_event_id"),
        sa.UniqueConstraint("observed_event_id"),
        schema="public",
    )
    op.execute(
        "CREATE TRIGGER post_commit_acknowledgements_immutable BEFORE UPDATE OR DELETE "
        "ON public.post_commit_acknowledgements FOR EACH ROW EXECUTE FUNCTION "
        "public.chaosagent_reject_immutable_change()"
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER post_commit_acknowledgements_immutable ON public.post_commit_acknowledgements"
    )
    op.drop_table("post_commit_acknowledgements", schema="public")
    op.drop_constraint(
        "uq_approval_requests_run_approval",
        "approval_requests",
        schema="public",
        type_="unique",
    )
    op.drop_constraint("uq_run_events_run_event", "run_events", schema="public", type_="unique")
