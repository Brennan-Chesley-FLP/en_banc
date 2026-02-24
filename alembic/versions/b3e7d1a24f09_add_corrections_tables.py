"""add corrections tables

Create the corrections schema and all corrections tables:
- corrections.corrections: provenance table (id from CL, user_id, notes, etc.)
- Per-scraper corrections tables (ala_publicportal, conn_jud_ct_gov)
- Per-CL model corrections tables (courtlistener schema)

Revision ID: b3e7d1a24f09
Revises: 2a94a3378b88
Create Date: 2026-02-23 23:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b3e7d1a24f09"
down_revision: Union[str, Sequence[str], None] = "2a94a3378b88"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create corrections schema and all corrections tables."""
    op.execute("CREATE SCHEMA IF NOT EXISTS corrections")

    # ── corrections.corrections: provenance table ──
    op.create_table(
        "corrections",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=False),
        sa.Column("user_id", sa.Text, nullable=False),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column("metadata", sa.dialects.postgresql.JSONB, nullable=True),
        schema="corrections",
    )

    # ── Alabama scraper-level corrections ──

    op.create_table(
        "corrections_dockets",
        sa.Column("court_id", sa.Text, nullable=False),
        sa.Column("case_number", sa.Text, nullable=False),
        sa.Column(
            "correction_id",
            sa.BigInteger,
            sa.ForeignKey("corrections.corrections.id"),
            nullable=False,
        ),
        sa.Column(
            "corrections",
            sa.dialects.postgresql.JSONB,
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("court_id", "case_number", "correction_id"),
        schema="ala_publicportal",
    )

    op.create_table(
        "corrections_opinion_clusters",
        sa.Column("court_id", sa.Text, nullable=False),
        sa.Column("case_number", sa.Text, nullable=False),
        sa.Column("date_filed", sa.Date, nullable=False),
        sa.Column(
            "correction_id",
            sa.BigInteger,
            sa.ForeignKey("corrections.corrections.id"),
            nullable=False,
        ),
        sa.Column(
            "corrections",
            sa.dialects.postgresql.JSONB,
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint(
            "court_id", "case_number", "date_filed", "correction_id"
        ),
        schema="ala_publicportal",
    )

    op.create_table(
        "corrections_oral_arguments",
        sa.Column("court_id", sa.Text, nullable=False),
        sa.Column("case_number", sa.Text, nullable=False),
        sa.Column("date_argued", sa.Date, nullable=False),
        sa.Column(
            "correction_id",
            sa.BigInteger,
            sa.ForeignKey("corrections.corrections.id"),
            nullable=False,
        ),
        sa.Column(
            "corrections",
            sa.dialects.postgresql.JSONB,
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint(
            "court_id", "case_number", "date_argued", "correction_id"
        ),
        schema="ala_publicportal",
    )

    # ── Connecticut scraper-level corrections ──

    op.create_table(
        "corrections_dockets",
        sa.Column("court_id", sa.Text, nullable=False),
        sa.Column("docket_id", sa.Text, nullable=False),
        sa.Column(
            "correction_id",
            sa.BigInteger,
            sa.ForeignKey("corrections.corrections.id"),
            nullable=False,
        ),
        sa.Column(
            "corrections",
            sa.dialects.postgresql.JSONB,
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("court_id", "docket_id", "correction_id"),
        schema="conn_jud_ct_gov",
    )

    op.create_table(
        "corrections_docket_entries",
        sa.Column("court_id", sa.Text, nullable=False),
        sa.Column("docket_id", sa.Text, nullable=False),
        sa.Column("activity_type", sa.Text, nullable=False),
        sa.Column("date_filed", sa.Date, nullable=False),
        sa.Column("number", sa.Text, nullable=False),
        sa.Column(
            "correction_id",
            sa.BigInteger,
            sa.ForeignKey("corrections.corrections.id"),
            nullable=False,
        ),
        sa.Column(
            "corrections",
            sa.dialects.postgresql.JSONB,
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint(
            "court_id",
            "docket_id",
            "activity_type",
            "date_filed",
            "number",
            "correction_id",
        ),
        schema="conn_jud_ct_gov",
    )

    op.create_table(
        "corrections_opinion_clusters",
        sa.Column("court_id", sa.Text, nullable=False),
        sa.Column("docket_id", sa.Text, nullable=False),
        sa.Column("date_filed", sa.Date, nullable=False),
        sa.Column(
            "correction_id",
            sa.BigInteger,
            sa.ForeignKey("corrections.corrections.id"),
            nullable=False,
        ),
        sa.Column(
            "corrections",
            sa.dialects.postgresql.JSONB,
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint(
            "court_id", "docket_id", "date_filed", "correction_id"
        ),
        schema="conn_jud_ct_gov",
    )

    op.create_table(
        "corrections_oral_arguments",
        sa.Column("court_id", sa.Text, nullable=False),
        sa.Column("docket_number", sa.Text, nullable=False),
        sa.Column("date_argued", sa.Date, nullable=False),
        sa.Column(
            "correction_id",
            sa.BigInteger,
            sa.ForeignKey("corrections.corrections.id"),
            nullable=False,
        ),
        sa.Column(
            "corrections",
            sa.dialects.postgresql.JSONB,
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint(
            "court_id", "docket_number", "date_argued", "correction_id"
        ),
        schema="conn_jud_ct_gov",
    )

    # ── CourtListener-level corrections ──

    op.create_table(
        "corrections_dockets",
        sa.Column("court_id", sa.Text, nullable=False),
        sa.Column("docket_number", sa.Text, nullable=False),
        sa.Column(
            "correction_id",
            sa.BigInteger,
            sa.ForeignKey("corrections.corrections.id"),
            nullable=False,
        ),
        sa.Column(
            "corrections",
            sa.dialects.postgresql.JSONB,
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("court_id", "docket_number", "correction_id"),
        schema="courtlistener",
    )

    op.create_table(
        "corrections_opinion_clusters",
        sa.Column("court_id", sa.Text, nullable=False),
        sa.Column("docket_number", sa.Text, nullable=False),
        sa.Column("date_filed", sa.Date, nullable=False),
        sa.Column(
            "correction_id",
            sa.BigInteger,
            sa.ForeignKey("corrections.corrections.id"),
            nullable=False,
        ),
        sa.Column(
            "corrections",
            sa.dialects.postgresql.JSONB,
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint(
            "court_id", "docket_number", "date_filed", "correction_id"
        ),
        schema="courtlistener",
    )

    op.create_table(
        "corrections_docket_entries",
        sa.Column("court_id", sa.Text, nullable=False),
        sa.Column("docket_number", sa.Text, nullable=False),
        sa.Column("document_uuid", sa.Text, nullable=False),
        sa.Column(
            "correction_id",
            sa.BigInteger,
            sa.ForeignKey("corrections.corrections.id"),
            nullable=False,
        ),
        sa.Column(
            "corrections",
            sa.dialects.postgresql.JSONB,
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint(
            "court_id", "docket_number", "document_uuid", "correction_id"
        ),
        schema="courtlistener",
    )

    op.create_table(
        "corrections_opinions",
        sa.Column("court_id", sa.Text, nullable=False),
        sa.Column("docket_number", sa.Text, nullable=False),
        sa.Column("cluster_date_filed", sa.Date, nullable=False),
        sa.Column("type", sa.Text, nullable=False),
        sa.Column("author_str", sa.Text, nullable=False),
        sa.Column(
            "correction_id",
            sa.BigInteger,
            sa.ForeignKey("corrections.corrections.id"),
            nullable=False,
        ),
        sa.Column(
            "corrections",
            sa.dialects.postgresql.JSONB,
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint(
            "court_id",
            "docket_number",
            "cluster_date_filed",
            "type",
            "author_str",
            "correction_id",
        ),
        schema="courtlistener",
    )

    op.create_table(
        "corrections_audio",
        sa.Column("court_id", sa.Text, nullable=False),
        sa.Column("docket_number", sa.Text, nullable=False),
        sa.Column("date_argued", sa.Date, nullable=False),
        sa.Column(
            "correction_id",
            sa.BigInteger,
            sa.ForeignKey("corrections.corrections.id"),
            nullable=False,
        ),
        sa.Column(
            "corrections",
            sa.dialects.postgresql.JSONB,
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint(
            "court_id", "docket_number", "date_argued", "correction_id"
        ),
        schema="courtlistener",
    )

    op.create_table(
        "corrections_originating_court_information",
        sa.Column("court_id", sa.Text, nullable=False),
        sa.Column("docket_number", sa.Text, nullable=False),
        sa.Column(
            "correction_id",
            sa.BigInteger,
            sa.ForeignKey("corrections.corrections.id"),
            nullable=False,
        ),
        sa.Column(
            "corrections",
            sa.dialects.postgresql.JSONB,
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("court_id", "docket_number", "correction_id"),
        schema="courtlistener",
    )


def downgrade() -> None:
    """Drop all corrections tables and the corrections schema."""
    # CourtListener-level
    op.drop_table("corrections_originating_court_information", schema="courtlistener")
    op.drop_table("corrections_audio", schema="courtlistener")
    op.drop_table("corrections_opinions", schema="courtlistener")
    op.drop_table("corrections_docket_entries", schema="courtlistener")
    op.drop_table("corrections_opinion_clusters", schema="courtlistener")
    op.drop_table("corrections_dockets", schema="courtlistener")

    # Connecticut scraper-level
    op.drop_table("corrections_oral_arguments", schema="conn_jud_ct_gov")
    op.drop_table("corrections_opinion_clusters", schema="conn_jud_ct_gov")
    op.drop_table("corrections_docket_entries", schema="conn_jud_ct_gov")
    op.drop_table("corrections_dockets", schema="conn_jud_ct_gov")

    # Alabama scraper-level
    op.drop_table("corrections_oral_arguments", schema="ala_publicportal")
    op.drop_table("corrections_opinion_clusters", schema="ala_publicportal")
    op.drop_table("corrections_dockets", schema="ala_publicportal")

    # Provenance table + schema
    op.drop_table("corrections", schema="corrections")
    op.execute("DROP SCHEMA IF EXISTS corrections")
