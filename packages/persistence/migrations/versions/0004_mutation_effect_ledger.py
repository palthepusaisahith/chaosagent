"""Add idempotent synthetic-company mutation effects.

Revision ID: 0004_mutation_effect_ledger
Revises: 0003_fixture_company_state
Create Date: 2026-08-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004_mutation_effect_ledger"
down_revision: str | None = "0003_fixture_company_state"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_IDENTIFIER = r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"
_DIGEST = r"^sha256:[0-9a-f]{64}$"


def upgrade() -> None:
    op.create_table(
        "company_effects",
        sa.Column("run_id", sa.String(length=128), nullable=False),
        sa.Column("tool_id", sa.String(length=128), nullable=False),
        sa.Column("contract_version", sa.String(length=256), nullable=False),
        sa.Column("idempotency_key_digest", sa.String(length=71), nullable=False),
        sa.Column("request_digest", sa.String(length=71), nullable=False),
        sa.Column("effect_id", sa.String(length=128), nullable=False),
        sa.Column("effect_kind", sa.String(length=64), nullable=False),
        sa.Column("subject_type", sa.String(length=64), nullable=False),
        sa.Column("subject_id", sa.String(length=128), nullable=False),
        sa.Column("effect_state", sa.String(length=32), nullable=False),
        sa.Column("result_document", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("logical_call_id", sa.String(length=128), nullable=False),
        sa.Column("first_attempt_id", sa.String(length=128), nullable=False),
        sa.Column("lease_attempt", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "tool_id IN ('payments.refund', 'support.update_ticket')", name="tool_id"
        ),
        sa.CheckConstraint(
            "contract_version IN ("
            "'chaosagent.tool/payments.refund/v0', "
            "'chaosagent.tool/support.update_ticket/v0')",
            name="contract_version",
        ),
        sa.CheckConstraint(f"idempotency_key_digest ~ '{_DIGEST}'", name="key_digest"),
        sa.CheckConstraint(f"request_digest ~ '{_DIGEST}'", name="request_digest"),
        sa.CheckConstraint(f"effect_id ~ '{_IDENTIFIER}'", name="effect_id"),
        sa.CheckConstraint(
            "effect_kind IN ('refund.created', 'support_ticket.updated')",
            name="effect_kind",
        ),
        sa.CheckConstraint("subject_type IN ('refund', 'support_ticket')", name="subject_type"),
        sa.CheckConstraint(f"subject_id ~ '{_IDENTIFIER}'", name="subject_id"),
        sa.CheckConstraint("effect_state = 'applied'", name="effect_state"),
        sa.CheckConstraint("jsonb_typeof(result_document) = 'object'", name="result_object"),
        sa.CheckConstraint(
            "(result_document ->> 'effect_id') IS NOT DISTINCT FROM effect_id",
            name="result_effect_id",
        ),
        sa.CheckConstraint(
            "(result_document ->> 'application') IS NOT DISTINCT FROM 'newly_applied'",
            name="result_application",
        ),
        sa.CheckConstraint(
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
        sa.CheckConstraint(
            "(tool_id <> 'support.update_ticket') OR ("
            "jsonb_typeof(result_document -> 'ticket_id') IS NOT DISTINCT FROM 'string' AND "
            "jsonb_typeof(result_document -> 'status') IS NOT DISTINCT FROM 'string' AND "
            "jsonb_typeof(result_document -> 'note') IS NOT DISTINCT FROM 'string' AND "
            "jsonb_typeof(result_document -> 'updated_at') IS NOT DISTINCT FROM 'string')",
            name="ticket_result_shape",
        ),
        sa.CheckConstraint(
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
        sa.CheckConstraint("lease_attempt >= 1", name="lease_attempt_positive"),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["public.run_company_state.run_id"],
            name="fk_company_effects_state",
        ),
        sa.PrimaryKeyConstraint(
            "run_id",
            "tool_id",
            "contract_version",
            "idempotency_key_digest",
            name="pk_company_effects",
        ),
        sa.UniqueConstraint("run_id", "effect_id", name="uq_company_effect_run_effect_id"),
        schema="public",
    )
    op.execute(
        "CREATE TRIGGER company_effects_immutable "
        "BEFORE UPDATE OR DELETE ON public.company_effects "
        "FOR EACH ROW EXECUTE FUNCTION public.chaosagent_reject_immutable_change()"
    )
    op.execute("REVOKE UPDATE, DELETE ON public.company_effects FROM PUBLIC")

    op.add_column(
        "company_refunds",
        sa.Column("effect_id", sa.String(length=128), nullable=True),
        schema="public",
    )
    op.add_column(
        "company_refunds",
        sa.Column(
            "origin",
            sa.String(length=16),
            server_default=sa.text("'fixture'"),
            nullable=False,
        ),
        schema="public",
    )
    op.create_check_constraint(
        "origin_value",
        "company_refunds",
        "origin IN ('fixture', 'mutation')",
        schema="public",
    )
    op.create_check_constraint(
        "origin_effect",
        "company_refunds",
        "(origin = 'fixture' AND effect_id IS NULL) OR "
        "(origin = 'mutation' AND effect_id IS NOT NULL)",
        schema="public",
    )
    op.create_foreign_key(
        "fk_company_refunds_effect",
        "company_refunds",
        "company_effects",
        ["run_id", "effect_id"],
        ["run_id", "effect_id"],
        source_schema="public",
        referent_schema="public",
    )
    op.create_index(
        "ix_company_refunds_run_payment_succeeded",
        "company_refunds",
        ["run_id", "payment_id"],
        unique=False,
        schema="public",
        postgresql_where=sa.text("status = 'succeeded'"),
    )
    op.add_column(
        "company_support_tickets",
        sa.Column("last_effect_id", sa.String(length=128), nullable=True),
        schema="public",
    )
    op.create_foreign_key(
        "fk_company_support_tickets_effect",
        "company_support_tickets",
        "company_effects",
        ["run_id", "last_effect_id"],
        ["run_id", "effect_id"],
        source_schema="public",
        referent_schema="public",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_company_support_tickets_effect",
        "company_support_tickets",
        schema="public",
        type_="foreignkey",
    )
    op.drop_column("company_support_tickets", "last_effect_id", schema="public")
    op.drop_constraint(
        "fk_company_refunds_effect", "company_refunds", schema="public", type_="foreignkey"
    )
    op.drop_index(
        "ix_company_refunds_run_payment_succeeded",
        table_name="company_refunds",
        schema="public",
    )
    op.drop_constraint("origin_effect", "company_refunds", schema="public", type_="check")
    op.drop_constraint("origin_value", "company_refunds", schema="public", type_="check")
    op.drop_column("company_refunds", "origin", schema="public")
    op.drop_column("company_refunds", "effect_id", schema="public")
    op.execute("DROP TRIGGER company_effects_immutable ON public.company_effects")
    op.drop_table("company_effects", schema="public")
