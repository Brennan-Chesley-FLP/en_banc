"""Load scraper results from SQLite into warehouse raw tables.

Juriscraper stores all results in a single ``results`` table with columns:
- id: integer PK (becomes record_id in the observations table)
- result_type: Python class name (e.g. "AlaDocket")
- data_json: JSON-serialized model_dump(mode="json")
- is_valid: boolean

This module reads valid results, deduplicates on content hash, routes
them by result_type to the matching raw table class, and records
observations linking each row to its provenance entry.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from collections import defaultdict

from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1000

#: Framework metadata fields excluded from the content hash.
#: These vary between scraper runs but don't represent substantive data.
HASH_EXCLUDE_FIELDS = frozenset({"id", "date_created", "date_modified"})


def _build_type_registry() -> dict[str, type]:
    """Map result_type class names to raw table SQLModel classes.

    Inspects all registered raw table classes from warehouse.register.
    Each raw table class inherits from the scraper output model (e.g.
    AlaDocket), so we match result_type against the parent class name.

    Returns:
        {"AlaDocket": RawAlaDocket, "AlaOpinionCluster": RawAlaOpinionCluster, ...}
    """
    import inspect

    import warehouse.register as reg

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


def _compute_content_hash(data: dict) -> str:
    """Compute a deterministic SHA-256 hash of the data fields.

    Framework metadata fields (id, date_created, date_modified) are
    excluded so that re-observations of identical court data produce
    the same hash regardless of scraper-internal timestamps.
    """
    filtered = {
        k: v
        for k, v in sorted(data.items())
        if k not in HASH_EXCLUDE_FIELDS
    }
    canonical = json.dumps(filtered, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def load_sqlite_to_raw(
    db_path: str,
    provenance_id: int,
    db_url: str,
    chunk_size: int = CHUNK_SIZE,
) -> dict[str, dict[str, int]]:
    """Load all valid results from a SQLite DB into raw warehouse tables.

    Rows are deduplicated on content hash.  New unique rows are inserted
    into the raw table; all rows (new and existing) get an observation
    recorded in the paired observations table.

    Args:
        db_path: Path to the SQLite database file.
        provenance_id: FK to warehouse.provenance for all observations.
        db_url: SQLAlchemy connection string for the analytics DB.
        chunk_size: Number of rows per batch.

    Returns:
        Dict of ``{result_type: {"new": N, "observed": M}}``.
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
    results: dict[str, dict[str, int]] = {}

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
        content_hash = _compute_content_hash(data)

        # Filter to columns that exist on the raw table
        valid_cols = _raw_table_columns(table_cls)
        row_data = {k: v for k, v in data.items() if k in valid_cols}
        row_data["content_hash"] = content_hash

        batches[result_type].append({
            "row_data": row_data,
            "content_hash": content_hash,
            "record_id": row_id,
        })

        # Flush when chunk is full
        if len(batches[result_type]) >= chunk_size:
            counts = _flush_dedup_batch(
                engine, table_cls, batches[result_type], provenance_id
            )
            r = results.setdefault(result_type, {"new": 0, "observed": 0})
            r["new"] += counts["new"]
            r["observed"] += counts["observed"]
            batches[result_type] = []

    # Flush remaining
    for result_type, batch in batches.items():
        if batch:
            table_cls = type_registry[result_type]
            counts = _flush_dedup_batch(
                engine, table_cls, batch, provenance_id
            )
            r = results.setdefault(result_type, {"new": 0, "observed": 0})
            r["new"] += counts["new"]
            r["observed"] += counts["observed"]

    conn_sqlite.close()

    for result_type, counts in results.items():
        table_cls = type_registry[result_type]
        schema = table_cls.__table_args__[0]["schema"]
        logger.info(
            "Loaded into %s.%s: %d new rows, %d observations",
            schema,
            table_cls.__tablename__,
            counts["new"],
            counts["observed"],
        )

    return results


def _flush_dedup_batch(
    engine,
    table_cls: type,
    batch: list[dict],
    provenance_id: int,
) -> dict[str, int]:
    """Upsert data rows and insert observations for a batch.

    1. INSERT data rows with ON CONFLICT (content_hash) DO NOTHING
    2. SELECT row_ids for all content hashes (new + already-existing)
    3. INSERT observations with ON CONFLICT DO NOTHING (handles
       duplicate content within a single scraper run)
    """
    obs_cls = table_cls._observations_cls
    table = table_cls.__table__
    obs_table = obs_cls.__table__

    with Session(engine) as session:
        # 1. Upsert data rows
        row_datas = [item["row_data"] for item in batch]
        stmt = pg_insert(table).on_conflict_do_nothing(
            index_elements=["content_hash"]
        )
        session.execute(stmt, row_datas)

        # 2. Fetch row_ids for all content_hashes (new + already-existing)
        batch_hashes = [item["content_hash"] for item in batch]
        rows = session.execute(
            select(table.c.row_id, table.c.content_hash).where(
                table.c.content_hash.in_(batch_hashes)
            )
        ).all()
        hash_to_row_id = {r.content_hash: r.row_id for r in rows}

        # 3. Insert observations
        observations = []
        for item in batch:
            row_id = hash_to_row_id[item["content_hash"]]
            observations.append({
                "row_id": row_id,
                "provenance_id": provenance_id,
                "record_id": item["record_id"],
            })

        obs_stmt = pg_insert(obs_table).on_conflict_do_nothing()
        session.execute(obs_stmt, observations)
        session.commit()

    # new count is an upper bound (counts attempted inserts, not actual).
    # For precise counts we'd need a pre-batch SELECT, but this is fine
    # for informational logging.
    return {"new": len(row_datas), "observed": len(observations)}
