"""Add immutable cross-process Campaign trial membership authority.

Revision ID: 0010_campaign_memberships
Revises: 0009_run_fault_seed
Create Date: 2026-09-03
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0010_campaign_memberships"
down_revision: str | None = "0009_run_fault_seed"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "runs", sa.Column("fault_plan_digest", sa.String(71), nullable=True), schema="public"
    )
    op.create_check_constraint(
        "fault_plan_digest_format",
        "runs",
        "fault_plan_digest IS NULL OR fault_plan_digest ~ '^sha256:[0-9a-f]{64}$'",
        schema="public",
    )
    op.execute(
        """
        CREATE FUNCTION public.chaosagent_freeze_run_fault_plan()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF OLD.fault_plan_digest IS NOT NULL
               AND NEW.fault_plan_digest IS DISTINCT FROM OLD.fault_plan_digest THEN
                RAISE EXCEPTION 'run fault plan is immutable'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER runs_fault_plan_immutable BEFORE UPDATE OF fault_plan_digest "
        "ON public.runs FOR EACH ROW EXECUTE FUNCTION "
        "public.chaosagent_freeze_run_fault_plan()"
    )
    op.create_unique_constraint(
        "uq_runs_campaign_binding",
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
    op.create_table(
        "campaign_plans",
        sa.Column("campaign_id", sa.String(128), nullable=False),
        sa.Column("arm", sa.String(16), nullable=False),
        sa.Column("planned_trials", sa.BigInteger(), nullable=False),
        sa.Column("scenario_id", sa.String(128), nullable=False),
        sa.Column("scenario_revision", sa.String(64), nullable=False),
        sa.Column("scenario_digest", sa.String(71), nullable=False),
        sa.Column("agent_configuration_id", sa.String(128), nullable=False),
        sa.Column("agent_configuration_revision", sa.String(64), nullable=False),
        sa.Column("agent_configuration_digest", sa.String(71), nullable=False),
        sa.Column("selected_fault_ids", postgresql.JSONB(), nullable=False),
        sa.Column("fault_plan_digest", sa.String(71), nullable=False),
        sa.Column("schema_version", sa.String(128), nullable=False),
        sa.Column("canonical_document", postgresql.JSONB(), nullable=False),
        sa.Column("canonical_digest", sa.String(71), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "campaign_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]*$'", name="campaign_id_format"
        ),
        sa.CheckConstraint("arm IN ('baseline', 'faulted')", name="arm_value"),
        sa.CheckConstraint(
            "planned_trials BETWEEN 1 AND 9007199254740991",
            name="planned_trials_positive",
        ),
        sa.CheckConstraint("fault_plan_digest ~ '^sha256:[0-9a-f]{64}$'", name="fault_plan_digest"),
        sa.CheckConstraint("canonical_digest ~ '^sha256:[0-9a-f]{64}$'", name="canonical_digest"),
        sa.CheckConstraint(
            "jsonb_typeof(selected_fault_ids) = 'array'", name="selected_faults_array"
        ),
        sa.CheckConstraint(
            "(arm = 'baseline' AND jsonb_array_length(selected_fault_ids) = 0) OR "
            "(arm = 'faulted' AND jsonb_array_length(selected_fault_ids) > 0)",
            name="arm_fault_assignment",
        ),
        sa.CheckConstraint("jsonb_typeof(canonical_document) = 'object'", name="document_object"),
        sa.CheckConstraint(
            "(canonical_document ->> 'schema_version') IS NOT DISTINCT FROM schema_version",
            name="document_schema_version",
        ),
        sa.CheckConstraint(
            "(canonical_document ->> 'campaign_id') IS NOT DISTINCT FROM campaign_id",
            name="document_campaign_id",
        ),
        sa.CheckConstraint(
            "(canonical_document ->> 'arm') IS NOT DISTINCT FROM arm", name="document_arm"
        ),
        sa.CheckConstraint(
            "jsonb_typeof(canonical_document -> 'planned_trials') IS NOT DISTINCT FROM 'number' "
            "AND (canonical_document ->> 'planned_trials') ~ '^[1-9][0-9]*$' "
            "AND (canonical_document ->> 'planned_trials')::bigint "
            "IS NOT DISTINCT FROM planned_trials",
            name="document_planned_trials",
        ),
        sa.CheckConstraint(
            "(canonical_document ->> 'fault_plan_digest') IS NOT DISTINCT FROM fault_plan_digest",
            name="document_fault_plan_digest",
        ),
        sa.CheckConstraint(
            "(canonical_document -> 'selected_fault_ids') IS NOT DISTINCT FROM selected_fault_ids",
            name="document_selected_fault_ids",
        ),
        sa.CheckConstraint(
            "(canonical_document #>> '{scenario,id}') IS NOT DISTINCT FROM scenario_id AND "
            "(canonical_document #>> '{scenario,revision}') "
            "IS NOT DISTINCT FROM scenario_revision AND "
            "(canonical_document #>> '{scenario,digest}') IS NOT DISTINCT FROM scenario_digest",
            name="document_scenario",
        ),
        sa.CheckConstraint(
            "(canonical_document #>> '{agent_configuration,id}') "
            "IS NOT DISTINCT FROM agent_configuration_id AND "
            "(canonical_document #>> '{agent_configuration,revision}') "
            "IS NOT DISTINCT FROM agent_configuration_revision AND "
            "(canonical_document #>> '{agent_configuration,digest}') "
            "IS NOT DISTINCT FROM agent_configuration_digest",
            name="document_agent_configuration",
        ),
        sa.ForeignKeyConstraint(
            ["scenario_id", "scenario_revision", "scenario_digest"],
            [
                "public.scenario_revisions.scenario_id",
                "public.scenario_revisions.revision",
                "public.scenario_revisions.canonical_digest",
            ],
            name="fk_campaign_plans_scenario_revision",
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
            name="fk_campaign_plans_agent_configuration_revision",
        ),
        sa.PrimaryKeyConstraint("campaign_id"),
        sa.UniqueConstraint(
            "campaign_id", "canonical_digest", name="uq_campaign_plans_identity_digest"
        ),
        schema="public",
    )
    op.create_table(
        "campaign_trial_memberships",
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("campaign_id", sa.String(128), nullable=False),
        sa.Column("campaign_plan_digest", sa.String(71), nullable=False),
        sa.Column("trial_index", sa.BigInteger(), nullable=False),
        sa.Column("scenario_id", sa.String(128), nullable=False),
        sa.Column("scenario_revision", sa.String(64), nullable=False),
        sa.Column("scenario_digest", sa.String(71), nullable=False),
        sa.Column("agent_configuration_id", sa.String(128), nullable=False),
        sa.Column("agent_configuration_revision", sa.String(64), nullable=False),
        sa.Column("agent_configuration_digest", sa.String(71), nullable=False),
        sa.Column("membership_digest", sa.String(71), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "trial_index BETWEEN 0 AND 9007199254740991", name="trial_index_nonnegative"
        ),
        sa.CheckConstraint("membership_digest ~ '^sha256:[0-9a-f]{64}$'", name="membership_digest"),
        sa.ForeignKeyConstraint(
            ["campaign_id", "campaign_plan_digest"],
            ["public.campaign_plans.campaign_id", "public.campaign_plans.canonical_digest"],
            name="fk_campaign_memberships_plan",
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
            name="fk_campaign_memberships_run_binding",
        ),
        sa.PrimaryKeyConstraint("run_id"),
        sa.UniqueConstraint(
            "campaign_id", "trial_index", name="uq_campaign_memberships_campaign_index"
        ),
        schema="public",
    )
    op.execute(
        "CREATE TRIGGER campaign_plans_immutable BEFORE UPDATE OR DELETE "
        "ON public.campaign_plans FOR EACH ROW EXECUTE FUNCTION "
        "public.chaosagent_reject_immutable_change()"
    )
    op.execute(
        "CREATE TRIGGER campaign_trial_memberships_immutable BEFORE UPDATE OR DELETE "
        "ON public.campaign_trial_memberships FOR EACH ROW EXECUTE FUNCTION "
        "public.chaosagent_reject_immutable_change()"
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER campaign_trial_memberships_immutable ON public.campaign_trial_memberships"
    )
    op.execute("DROP TRIGGER campaign_plans_immutable ON public.campaign_plans")
    op.drop_table("campaign_trial_memberships", schema="public")
    op.drop_table("campaign_plans", schema="public")
    op.drop_constraint("uq_runs_campaign_binding", "runs", schema="public", type_="unique")
    op.execute("DROP TRIGGER runs_fault_plan_immutable ON public.runs")
    op.execute("DROP FUNCTION public.chaosagent_freeze_run_fault_plan()")
    op.drop_constraint("fault_plan_digest_format", "runs", schema="public", type_="check")
    op.drop_column("runs", "fault_plan_digest", schema="public")
