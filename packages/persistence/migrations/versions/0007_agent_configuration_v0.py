"""Persist immutable hosted Agent Configuration v0 documents.

Revision ID: 0007_agent_configuration_v0
Revises: 0006_execution_checkpoints
Create Date: 2026-08-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0007_agent_configuration_v0"
down_revision: str | None = "0006_execution_checkpoints"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "agent_configuration_revisions",
        sa.Column("schema_version", sa.String(128), nullable=True),
        schema="public",
    )
    op.add_column(
        "agent_configuration_revisions",
        sa.Column(
            "canonical_document",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        schema="public",
    )
    op.create_check_constraint(
        "document_projection",
        "agent_configuration_revisions",
        "(schema_version IS NULL AND canonical_document IS NULL) OR "
        "(schema_version IS NOT NULL AND "
        "schema_version = 'chaosagent.agent-configuration/v0' AND "
        "canonical_document IS NOT NULL AND "
        "jsonb_typeof(canonical_document) IS NOT DISTINCT FROM 'object' AND "
        "(canonical_document ->> 'schema_version') IS NOT DISTINCT FROM schema_version AND "
        "(canonical_document ->> 'agent_configuration_id') IS NOT DISTINCT FROM "
        "agent_configuration_id AND "
        "(canonical_document ->> 'revision') IS NOT DISTINCT FROM revision)",
        schema="public",
    )


def downgrade() -> None:
    op.drop_constraint(
        "document_projection",
        "agent_configuration_revisions",
        type_="check",
        schema="public",
    )
    op.drop_column("agent_configuration_revisions", "canonical_document", schema="public")
    op.drop_column("agent_configuration_revisions", "schema_version", schema="public")
