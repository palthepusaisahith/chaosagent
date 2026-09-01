"""Freeze the deterministic Issue #13 fault seed on each Run.

Revision ID: 0009_run_fault_seed
Revises: 0008_post_commit_ack
Create Date: 2026-08-31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009_run_fault_seed"
down_revision: str | None = "0008_post_commit_ack"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "runs",
        sa.Column("fault_seed", sa.BigInteger(), nullable=True),
        schema="public",
    )
    op.create_check_constraint(
        "fault_seed_json_safe",
        "runs",
        "fault_seed IS NULL OR fault_seed BETWEEN 0 AND 9007199254740991",
        schema="public",
    )
    op.execute(
        """
        CREATE FUNCTION public.chaosagent_freeze_run_fault_seed()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF OLD.fault_seed IS NOT NULL
               AND NEW.fault_seed IS DISTINCT FROM OLD.fault_seed THEN
                RAISE EXCEPTION 'run fault seed is immutable'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER runs_fault_seed_immutable BEFORE UPDATE OF fault_seed "
        "ON public.runs FOR EACH ROW EXECUTE FUNCTION "
        "public.chaosagent_freeze_run_fault_seed()"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER runs_fault_seed_immutable ON public.runs")
    op.execute("DROP FUNCTION public.chaosagent_freeze_run_fault_seed()")
    op.drop_constraint(
        "fault_seed_json_safe",
        "runs",
        schema="public",
        type_="check",
    )
    op.drop_column("runs", "fault_seed", schema="public")
