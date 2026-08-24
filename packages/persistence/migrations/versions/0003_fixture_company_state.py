"""Add immutable Fixture revisions and isolated Run-local company state.

Revision ID: 0003_fixture_company_state
Revises: 0002_run_lifecycle_leases
Create Date: 2026-08-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003_fixture_company_state"
down_revision: str | None = "0002_run_lifecycle_leases"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_IDENTIFIER = r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"
_REVISION = r"^[A-Za-z0-9]+([._-][A-Za-z0-9]+)*$"
_DIGEST = r"^sha256:[0-9a-f]{64}$"


def upgrade() -> None:
    op.create_table(
        "fixture_revisions",
        sa.Column("fixture_id", sa.String(length=128), nullable=False),
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
        sa.CheckConstraint(f"fixture_id ~ '{_IDENTIFIER}'", name="fixture_id_format"),
        sa.CheckConstraint(f"revision ~ '{_REVISION}'", name="revision_format"),
        sa.CheckConstraint(f"canonical_digest ~ '{_DIGEST}'", name="digest_format"),
        sa.CheckConstraint("jsonb_typeof(canonical_document) = 'object'", name="document_object"),
        sa.CheckConstraint(
            "(canonical_document ->> 'fixture_id') IS NOT DISTINCT FROM fixture_id",
            name="document_fixture_id",
        ),
        sa.CheckConstraint(
            "(canonical_document ->> 'revision') IS NOT DISTINCT FROM revision",
            name="document_revision",
        ),
        sa.CheckConstraint(
            "(canonical_document ->> 'schema_version') IS NOT DISTINCT FROM schema_version",
            name="document_schema_version",
        ),
        sa.PrimaryKeyConstraint("fixture_id", "revision", name="pk_fixture_revisions"),
        sa.UniqueConstraint(
            "fixture_id", "revision", "canonical_digest", name="uq_fixture_revision_digest"
        ),
        schema="public",
    )
    op.execute(
        "CREATE TRIGGER fixture_revisions_immutable "
        "BEFORE UPDATE OR DELETE ON public.fixture_revisions "
        "FOR EACH ROW EXECUTE FUNCTION public.chaosagent_reject_immutable_change()"
    )
    op.execute("REVOKE UPDATE, DELETE ON public.fixture_revisions FROM PUBLIC")

    for column in (
        sa.Column("fixture_id", sa.String(length=128), nullable=True),
        sa.Column("fixture_revision", sa.String(length=64), nullable=True),
        sa.Column("fixture_digest", sa.String(length=71), nullable=True),
    ):
        op.add_column("runs", column, schema="public")
    op.create_check_constraint(
        "fixture_binding_complete",
        "runs",
        "(fixture_id IS NULL AND fixture_revision IS NULL AND fixture_digest IS NULL) OR "
        "(fixture_id IS NOT NULL AND fixture_revision IS NOT NULL AND fixture_digest IS NOT NULL)",
        schema="public",
    )
    op.create_foreign_key(
        "fk_runs_fixture_revision",
        "runs",
        "fixture_revisions",
        ["fixture_id", "fixture_revision", "fixture_digest"],
        ["fixture_id", "revision", "canonical_digest"],
        source_schema="public",
        referent_schema="public",
    )
    op.create_unique_constraint(
        "uq_run_fixture",
        "runs",
        ["run_id", "fixture_id", "fixture_revision", "fixture_digest"],
        schema="public",
    )

    op.create_table(
        "run_company_state",
        sa.Column("run_id", sa.String(length=128), nullable=False),
        sa.Column("fixture_id", sa.String(length=128), nullable=False),
        sa.Column("fixture_revision", sa.String(length=64), nullable=False),
        sa.Column("fixture_digest", sa.String(length=71), nullable=False),
        sa.Column("reference_time", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["run_id", "fixture_id", "fixture_revision", "fixture_digest"],
            [
                "public.runs.run_id",
                "public.runs.fixture_id",
                "public.runs.fixture_revision",
                "public.runs.fixture_digest",
            ],
            name="fk_run_company_state_frozen_run_fixture",
        ),
        sa.PrimaryKeyConstraint("run_id", name="pk_run_company_state"),
        schema="public",
    )
    op.create_table(
        "company_customers",
        sa.Column("run_id", sa.String(length=128), nullable=False),
        sa.Column("customer_id", sa.String(length=128), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("email", sa.String(length=320), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.CheckConstraint("status IN ('active', 'suspended')", name="status_value"),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["public.run_company_state.run_id"],
            name="fk_company_customers_state",
        ),
        sa.PrimaryKeyConstraint("run_id", "customer_id", name="pk_company_customers"),
        schema="public",
    )
    op.create_table(
        "company_orders",
        sa.Column("run_id", sa.String(length=128), nullable=False),
        sa.Column("order_id", sa.String(length=128), nullable=False),
        sa.Column("customer_id", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("total_minor", sa.BigInteger(), nullable=False),
        sa.Column("placed_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('placed', 'paid', 'fulfilled', 'cancelled')", name="status_value"
        ),
        sa.CheckConstraint("currency ~ '^[A-Z]{3}$'", name="currency_format"),
        sa.CheckConstraint("total_minor BETWEEN 1 AND 9007199254740991", name="total_minor_safe"),
        sa.ForeignKeyConstraint(
            ["run_id", "customer_id"],
            ["public.company_customers.run_id", "public.company_customers.customer_id"],
            name="fk_company_orders_customer",
        ),
        sa.PrimaryKeyConstraint("run_id", "order_id", name="pk_company_orders"),
        sa.UniqueConstraint("run_id", "order_id", "customer_id", name="uq_company_order_customer"),
        schema="public",
    )
    op.create_table(
        "company_shipments",
        sa.Column("run_id", sa.String(length=128), nullable=False),
        sa.Column("shipment_id", sa.String(length=128), nullable=False),
        sa.Column("order_id", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("carrier", sa.String(length=100), nullable=False),
        sa.Column("tracking_number", sa.String(length=128), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('pending', 'in_transit', 'delivered', 'failed', 'returned')",
            name="status_value",
        ),
        sa.ForeignKeyConstraint(
            ["run_id", "order_id"],
            ["public.company_orders.run_id", "public.company_orders.order_id"],
            name="fk_company_shipments_order",
        ),
        sa.PrimaryKeyConstraint("run_id", "shipment_id", name="pk_company_shipments"),
        sa.UniqueConstraint("run_id", "order_id", name="uq_company_shipment_order"),
        schema="public",
    )
    op.create_table(
        "company_payments",
        sa.Column("run_id", sa.String(length=128), nullable=False),
        sa.Column("payment_id", sa.String(length=128), nullable=False),
        sa.Column("order_id", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("amount_minor", sa.BigInteger(), nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('captured', 'partially_refunded', 'refunded')",
            name="status_value",
        ),
        sa.CheckConstraint("currency ~ '^[A-Z]{3}$'", name="currency_format"),
        sa.CheckConstraint("amount_minor BETWEEN 1 AND 9007199254740991", name="amount_minor_safe"),
        sa.ForeignKeyConstraint(
            ["run_id", "order_id"],
            ["public.company_orders.run_id", "public.company_orders.order_id"],
            name="fk_company_payments_order",
        ),
        sa.PrimaryKeyConstraint("run_id", "payment_id", name="pk_company_payments"),
        sa.UniqueConstraint("run_id", "order_id", name="uq_company_payment_order"),
        sa.UniqueConstraint(
            "run_id", "payment_id", "order_id", name="uq_company_payment_order_reference"
        ),
        schema="public",
    )
    op.create_table(
        "company_refunds",
        sa.Column("run_id", sa.String(length=128), nullable=False),
        sa.Column("refund_id", sa.String(length=128), nullable=False),
        sa.Column("payment_id", sa.String(length=128), nullable=False),
        sa.Column("order_id", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("amount_minor", sa.BigInteger(), nullable=False),
        sa.Column("reason", sa.String(length=500), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="status_value"),
        sa.CheckConstraint("amount_minor BETWEEN 1 AND 9007199254740991", name="amount_minor_safe"),
        sa.ForeignKeyConstraint(
            ["run_id", "payment_id", "order_id"],
            [
                "public.company_payments.run_id",
                "public.company_payments.payment_id",
                "public.company_payments.order_id",
            ],
            name="fk_company_refunds_payment_order",
        ),
        sa.PrimaryKeyConstraint("run_id", "refund_id", name="pk_company_refunds"),
        schema="public",
    )
    op.create_table(
        "company_support_tickets",
        sa.Column("run_id", sa.String(length=128), nullable=False),
        sa.Column("ticket_id", sa.String(length=128), nullable=False),
        sa.Column("customer_id", sa.String(length=128), nullable=False),
        sa.Column("order_id", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("subject", sa.String(length=500), nullable=False),
        sa.Column("note", sa.String(length=4000), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("status IN ('open', 'pending', 'closed')", name="status_value"),
        sa.ForeignKeyConstraint(
            ["run_id", "order_id", "customer_id"],
            [
                "public.company_orders.run_id",
                "public.company_orders.order_id",
                "public.company_orders.customer_id",
            ],
            name="fk_company_support_tickets_order_customer",
        ),
        sa.PrimaryKeyConstraint("run_id", "ticket_id", name="pk_company_support_tickets"),
        schema="public",
    )


def downgrade() -> None:
    for table in (
        "company_support_tickets",
        "company_refunds",
        "company_payments",
        "company_shipments",
        "company_orders",
        "company_customers",
        "run_company_state",
    ):
        op.drop_table(table, schema="public")
    op.drop_constraint("uq_run_fixture", "runs", schema="public", type_="unique")
    op.drop_constraint("fk_runs_fixture_revision", "runs", schema="public", type_="foreignkey")
    op.drop_constraint("fixture_binding_complete", "runs", schema="public", type_="check")
    op.drop_column("runs", "fixture_digest", schema="public")
    op.drop_column("runs", "fixture_revision", schema="public")
    op.drop_column("runs", "fixture_id", schema="public")
    op.execute("DROP TRIGGER fixture_revisions_immutable ON public.fixture_revisions")
    op.drop_table("fixture_revisions", schema="public")
