"""Add CAS-protected execution checkpoints for the Issue #11 agent loop.

Revision ID: 0006_execution_checkpoints
Revises: 0005_policy_approvals
Create Date: 2026-08-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0006_execution_checkpoints"
down_revision: str | None = "0005_policy_approvals"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_DIGEST = r"^sha256:[0-9a-f]{64}$"


def upgrade() -> None:
    op.create_table(
        "execution_checkpoints",
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("schema_version", sa.String(128), nullable=False),
        sa.Column("checkpoint_version", sa.BigInteger(), nullable=False),
        sa.Column("lease_attempt", sa.BigInteger(), nullable=False),
        sa.Column("last_event_sequence", sa.BigInteger(), nullable=False),
        sa.Column("document", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("document_digest", sa.String(71), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "schema_version = 'chaosagent.execution-checkpoint/v0'",
            name="schema_version_value",
        ),
        sa.CheckConstraint(
            "checkpoint_version BETWEEN 1 AND 9007199254740991",
            name="checkpoint_version_positive",
        ),
        sa.CheckConstraint(
            "lease_attempt BETWEEN 1 AND 9007199254740991", name="lease_attempt_positive"
        ),
        sa.CheckConstraint(
            "last_event_sequence BETWEEN 1 AND 9007199254740991",
            name="last_event_sequence_positive",
        ),
        sa.CheckConstraint(f"document_digest ~ '{_DIGEST}'", name="document_digest"),
        sa.CheckConstraint("jsonb_typeof(document) = 'object'", name="document_object"),
        sa.CheckConstraint(
            "(document ->> 'schema_version') IS NOT DISTINCT FROM schema_version",
            name="document_schema_version",
        ),
        sa.CheckConstraint(
            "(document ->> 'run_id') IS NOT DISTINCT FROM run_id", name="document_run_id"
        ),
        sa.CheckConstraint(
            "jsonb_typeof(document -> 'checkpoint_version') IS NOT DISTINCT FROM 'number' "
            "AND (document ->> 'checkpoint_version') ~ '^[1-9][0-9]*$' "
            "AND (document ->> 'checkpoint_version')::bigint "
            "IS NOT DISTINCT FROM checkpoint_version",
            name="document_checkpoint_version",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(document -> 'lease_attempt') IS NOT DISTINCT FROM 'number' "
            "AND (document ->> 'lease_attempt') ~ '^[1-9][0-9]*$' "
            "AND (document ->> 'lease_attempt')::bigint IS NOT DISTINCT FROM lease_attempt",
            name="document_lease_attempt",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(document -> 'last_event_sequence') IS NOT DISTINCT FROM 'number' "
            "AND (document ->> 'last_event_sequence') ~ '^[1-9][0-9]*$' "
            "AND (document ->> 'last_event_sequence')::bigint "
            "IS NOT DISTINCT FROM last_event_sequence",
            name="document_last_event_sequence",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"], ["public.runs.run_id"], name="fk_execution_checkpoints_run"
        ),
        sa.PrimaryKeyConstraint("run_id", name="pk_execution_checkpoints"),
        schema="public",
    )


def downgrade() -> None:
    op.drop_table("execution_checkpoints", schema="public")
