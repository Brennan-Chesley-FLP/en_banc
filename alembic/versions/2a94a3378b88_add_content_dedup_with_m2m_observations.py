"""add content dedup with m2m observations

Drop and recreate all raw tables with the new schema:
- row_id (BIGINT autoincrement PK) replaces composite (provenance_id, record_id)
- content_hash (TEXT UNIQUE) for dedup
- paired _observations tables for M2M provenance tracking

Revision ID: 2a94a3378b88
Revises: 0f95588863c9
Create Date: 2026-02-23 15:30:09.167042

"""
from typing import Sequence, Union

from alembic import op
from sqlmodel import SQLModel

# revision identifiers, used by Alembic.
revision: str = '2a94a3378b88'
down_revision: Union[str, Sequence[str], None] = '0f95588863c9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Raw tables that existed before this migration (old schema).
_OLD_RAW_TABLES = [
    ("raw_dockets", "ala_publicportal"),
    ("raw_opinion_clusters", "ala_publicportal"),
    ("raw_oral_arguments", "ala_publicportal"),
    ("raw_historical_release_lists", "ala_publicportal"),
    ("raw_dockets", "conn_jud_ct_gov"),
    ("raw_docket_entries", "conn_jud_ct_gov"),
    ("raw_opinion_clusters", "conn_jud_ct_gov"),
    ("raw_oral_arguments", "conn_jud_ct_gov"),
    ("raw_docket_unavailable", "conn_jud_ct_gov"),
    ("raw_trial_court_dockets", "conn_jud_ct_gov"),
    ("raw_trial_court_docket_entries", "conn_jud_ct_gov"),
    ("raw_trial_case_unavailable", "conn_jud_ct_gov"),
]

_MANAGED_SCHEMAS = {"ala_publicportal", "conn_jud_ct_gov"}


def upgrade() -> None:
    """Drop old raw tables and recreate with content-addressed dedup."""
    # Importing register triggers raw_table_from_model() which also
    # creates the _observations tables on SQLModel.metadata.
    import warehouse.register  # noqa: F401

    bind = op.get_bind()

    # 1. Drop old raw tables (they have FK to provenance, no cascade needed
    #    since nothing else references them).
    for table_name, schema in _OLD_RAW_TABLES:
        op.drop_table(table_name, schema=schema)

    # 2. Create new raw tables + observations tables from current models.
    #    sorted_tables respects FK ordering (raw tables before observations).
    tables = [
        t for t in SQLModel.metadata.sorted_tables
        if t.schema in _MANAGED_SCHEMAS
    ]
    SQLModel.metadata.create_all(bind, tables=tables)


def downgrade() -> None:
    """Drop new tables. Old tables are NOT recreated (early-stage project)."""
    import warehouse.register  # noqa: F401

    bind = op.get_bind()

    # Drop new tables (observations first due to FKs, then raw).
    tables = [
        t for t in reversed(SQLModel.metadata.sorted_tables)
        if t.schema in _MANAGED_SCHEMAS
    ]
    SQLModel.metadata.drop_all(bind, tables=tables)
