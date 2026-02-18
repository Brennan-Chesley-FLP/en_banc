"""Load scraper results from SQLite into warehouse raw tables.

Juriscraper stores all results in a single ``results`` table with columns:
- id: integer PK (becomes record_id in the warehouse)
- result_type: Python class name (e.g. "AlaDocket")
- data_json: JSON-serialized model_dump(mode="json")
- is_valid: boolean

This module reads valid results, routes them by result_type to the
matching raw table class, and bulk-inserts into PostgreSQL.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections import defaultdict

from sqlalchemy import create_engine, insert
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1000


def _build_type_registry() -> dict[str, type]:
    """Map result_type class names to raw table SQLModel classes.

    Inspects all registered raw table classes from warehouse.register.
    Each raw table class inherits from the scraper output model (e.g.
    AlaDocket), so we match result_type against the parent class name.

    Returns:
        {"AlaDocket": RawAlaDocket, "AlaOpinionCluster": RawAlaOpinionCluster, ...}
    """
    import warehouse.register as reg
    import inspect

    registry: dict[str, type] = {}
    for _name, obj in inspect.getmembers(reg):
        if not (hasattr(obj, "__tablename__") and hasattr(obj, "__table_args__")):
            continue
        # The raw table class name is like "ala_publicportal__raw_dockets".
        # Its bases include the original model class (e.g. AlaDocket).
        for base in obj.__bases__:
            base_name = base.__name__
            # Skip SQLModel and other framework bases
            if base_name in ("SQLModel", "BaseModel"):
                continue
            registry[base_name] = obj
            break

    return registry


def _raw_table_columns(table_cls: type) -> set[str]:
    """Return the set of column names for a raw table."""
    return {col.name for col in table_cls.__table__.columns}


def load_sqlite_to_raw(
    db_path: str,
    provenance_id: int,
    db_url: str,
    chunk_size: int = CHUNK_SIZE,
) -> dict[str, int]:
    """Load all valid results from a SQLite DB into raw warehouse tables.

    Args:
        db_path: Path to the SQLite database file.
        provenance_id: FK to warehouse.provenance for all inserted rows.
        db_url: SQLAlchemy connection string for the analytics DB.
        chunk_size: Number of rows per bulk INSERT.

    Returns:
        Dict of {table_name: rows_loaded}.
    """
    type_registry = _build_type_registry()
    engine = create_engine(db_url)

    conn_sqlite = sqlite3.connect(db_path)
    cursor = conn_sqlite.cursor()
    cursor.execute(
        "SELECT id, result_type, data_json "
        "FROM results WHERE is_valid = 1 "
        "ORDER BY result_type, id"
    )

    # Group rows by result_type for batch insertion
    batches: dict[str, list[dict]] = defaultdict(list)
    results: dict[str, int] = {}

    for row_id, result_type, data_json in cursor:
        table_cls = type_registry.get(result_type)
        if table_cls is None:
            logger.warning(
                "No raw table registered for result_type=%r, skipping row %d",
                result_type,
                row_id,
            )
            continue

        data = json.loads(data_json)
        data["provenance_id"] = provenance_id
        data["record_id"] = row_id

        # Drop keys that don't exist as columns on the raw table
        valid_cols = _raw_table_columns(table_cls)
        data = {k: v for k, v in data.items() if k in valid_cols}

        batches[result_type].append(data)

        # Flush when chunk is full
        if len(batches[result_type]) >= chunk_size:
            _flush_batch(engine, table_cls, batches[result_type])
            results[result_type] = results.get(result_type, 0) + len(
                batches[result_type]
            )
            batches[result_type] = []

    # Flush remaining
    for result_type, batch in batches.items():
        if batch:
            table_cls = type_registry[result_type]
            _flush_batch(engine, table_cls, batch)
            results[result_type] = results.get(result_type, 0) + len(batch)

    conn_sqlite.close()

    for result_type, count in results.items():
        table_cls = type_registry[result_type]
        schema = table_cls.__table_args__[0]["schema"]
        logger.info(
            "Loaded %d rows into %s.%s",
            count,
            schema,
            table_cls.__tablename__,
        )

    return results


def _flush_batch(engine, table_cls: type, batch: list[dict]) -> None:
    """Insert a batch of rows into a raw table."""
    with Session(engine) as session:
        session.execute(insert(table_cls), batch)
        session.commit()
