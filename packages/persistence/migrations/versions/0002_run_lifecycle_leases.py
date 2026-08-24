"""Add the Run lifecycle version and lease/fencing protocol.

Revision ID: 0002_run_lifecycle_leases
Revises: 0001_persistence_v0
Create Date: 2026-08-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_run_lifecycle_leases"
down_revision: str | None = "0001_persistence_v0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_IDENTIFIER = r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"


def upgrade() -> None:
    op.add_column(
        "runs",
        sa.Column("lifecycle_version", sa.BigInteger(), server_default="0", nullable=False),
        schema="public",
    )
    op.add_column(
        "runs", sa.Column("lease_owner", sa.String(length=128), nullable=True), schema="public"
    )
    op.add_column(
        "runs", sa.Column("lease_token", sa.String(length=128), nullable=True), schema="public"
    )
    op.add_column(
        "runs",
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        schema="public",
    )
    op.add_column(
        "runs",
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        schema="public",
    )
    op.add_column(
        "runs",
        sa.Column("attempt", sa.BigInteger(), server_default="0", nullable=False),
        schema="public",
    )

    # Issue #5 made a report's terminal status authoritative only inside that
    # report. Issue #6 adopts it as the Run's terminal status during upgrade.
    op.execute(
        """
        UPDATE public.runs AS run
        SET status = report.run_status
        FROM public.run_reports AS report
        WHERE report.run_id = run.run_id
          AND run.status IS DISTINCT FROM report.run_status
        """
    )
    # Every unreported Issue #5 status was structural and non-authoritative,
    # including terminal-looking values. Normalize all such rows to queued
    # rather than manufacturing a lease/evidence or freezing an accidental
    # terminal state.
    op.execute(
        """
        UPDATE public.runs AS run
        SET status = 'queued'
        WHERE status IS DISTINCT FROM 'queued'
          AND NOT EXISTS (
              SELECT 1
              FROM public.run_reports AS report
              WHERE report.run_id = run.run_id
          )
        """
    )

    op.drop_constraint(
        "fk_run_reports_frozen_run", "run_reports", schema="public", type_="foreignkey"
    )
    op.drop_constraint("uq_run_frozen_references", "runs", schema="public", type_="unique")
    op.create_unique_constraint(
        "uq_run_frozen_references",
        "runs",
        [
            "run_id",
            "scenario_id",
            "scenario_revision",
            "scenario_digest",
            "agent_configuration_id",
            "agent_configuration_revision",
            "agent_configuration_digest",
            "status",
        ],
        schema="public",
    )
    op.create_foreign_key(
        "fk_run_reports_frozen_run",
        "run_reports",
        "runs",
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
            "run_id",
            "scenario_id",
            "scenario_revision",
            "scenario_digest",
            "agent_configuration_id",
            "agent_configuration_revision",
            "agent_configuration_digest",
            "status",
        ],
        source_schema="public",
        referent_schema="public",
    )
    op.create_check_constraint(
        "lifecycle_version_nonnegative",
        "runs",
        "lifecycle_version >= 0",
        schema="public",
    )
    op.create_check_constraint("attempt_nonnegative", "runs", "attempt >= 0", schema="public")
    op.create_check_constraint(
        "lease_owner_format",
        "runs",
        f"lease_owner IS NULL OR lease_owner ~ '{_IDENTIFIER}'",
        schema="public",
    )
    op.create_check_constraint(
        "lease_token_format",
        "runs",
        f"lease_token IS NULL OR lease_token ~ '{_IDENTIFIER}'",
        schema="public",
    )
    op.create_check_constraint(
        "lease_state_consistency",
        "runs",
        "((status IN ('provisioning', 'running', 'evaluating')) "
        "AND lease_owner IS NOT NULL AND lease_token IS NOT NULL "
        "AND lease_expires_at IS NOT NULL AND heartbeat_at IS NOT NULL "
        "AND lease_expires_at > heartbeat_at) OR "
        "((status IN ('queued', 'completed', 'failed', 'timed_out', "
        "'cancelled', 'infra_error')) AND lease_owner IS NULL "
        "AND lease_token IS NULL AND lease_expires_at IS NULL "
        "AND heartbeat_at IS NULL)",
        schema="public",
    )
    op.create_index(
        "ix_runs_claim_queue",
        "runs",
        ["created_at", "run_id"],
        unique=False,
        schema="public",
        postgresql_where=sa.text("status = 'queued'"),
    )


def downgrade() -> None:
    op.drop_index("ix_runs_claim_queue", table_name="runs", schema="public")
    op.drop_constraint("lease_state_consistency", "runs", schema="public", type_="check")
    op.drop_constraint("lease_token_format", "runs", schema="public", type_="check")
    op.drop_constraint("lease_owner_format", "runs", schema="public", type_="check")
    op.drop_constraint("attempt_nonnegative", "runs", schema="public", type_="check")
    op.drop_constraint("lifecycle_version_nonnegative", "runs", schema="public", type_="check")
    op.drop_constraint(
        "fk_run_reports_frozen_run", "run_reports", schema="public", type_="foreignkey"
    )
    op.drop_constraint("uq_run_frozen_references", "runs", schema="public", type_="unique")
    op.create_unique_constraint(
        "uq_run_frozen_references",
        "runs",
        [
            "run_id",
            "scenario_id",
            "scenario_revision",
            "scenario_digest",
            "agent_configuration_id",
            "agent_configuration_revision",
            "agent_configuration_digest",
        ],
        schema="public",
    )
    op.create_foreign_key(
        "fk_run_reports_frozen_run",
        "run_reports",
        "runs",
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
            "run_id",
            "scenario_id",
            "scenario_revision",
            "scenario_digest",
            "agent_configuration_id",
            "agent_configuration_revision",
            "agent_configuration_digest",
        ],
        source_schema="public",
        referent_schema="public",
    )
    op.drop_column("runs", "attempt", schema="public")
    op.drop_column("runs", "heartbeat_at", schema="public")
    op.drop_column("runs", "lease_expires_at", schema="public")
    op.drop_column("runs", "lease_token", schema="public")
    op.drop_column("runs", "lease_owner", schema="public")
    op.drop_column("runs", "lifecycle_version", schema="public")
