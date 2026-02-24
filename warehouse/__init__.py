"""Warehouse package.

Patches ``SQLModel.metadata.create_all`` to skip schema-qualified tables
when the target engine is SQLite.  This prevents kent's ``init_database``
(which calls ``SQLModel.metadata.create_all`` on a SQLite DB) from
attempting to create PostgreSQL-only warehouse tables that use schemas,
JSONB, and other features unsupported by SQLite.
"""

from sqlmodel import SQLModel

_original_create_all = SQLModel.metadata.create_all


def _filtered_create_all(bind, tables=None, checkfirst=True):
    if tables is None and hasattr(bind, "dialect") and bind.dialect.name == "sqlite":
        tables = [t for t in SQLModel.metadata.sorted_tables if t.schema is None]
    return _original_create_all(bind, tables=tables, checkfirst=checkfirst)


SQLModel.metadata.create_all = _filtered_create_all
