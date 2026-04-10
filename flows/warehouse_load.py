"""Warehouse loading flow: download DB from S3 -> validate -> provenance -> load -> emit event.

Triggered by a ``scrape.uploaded`` event from the kent worker. Downloads the
scraper SQLite database from S3, validates it, creates a provenance record,
bulk-loads results into raw warehouse tables, and emits ``scrape.completed``
to trigger downstream SQLMesh transforms.
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.context import get_run_context
from prefect.events import emit_event
from prefect_aws.s3 import S3Bucket

logger = logging.getLogger(__name__)

ANALYTICS_DB_URL = "postgresql://analytics:analytics@localhost:5433/analytics"
S3_BLOCK_NAME = "scrapers"


# ---------------------------------------------------------------------------
# Task: Download Database from S3
# ---------------------------------------------------------------------------


@task(log_prints=True, task_run_name="download-db")
async def download_db(s3_uri: str) -> Path:
    """Download a scraper database from S3 to a local temp file.

    Args:
        s3_uri: S3 URI like ``s3://bucket/scraper_runs/schema/run-name.db``.

    Returns:
        Path to the downloaded SQLite database.
    """
    log = get_run_logger()
    s3_bucket = await S3Bucket.aload(S3_BLOCK_NAME)

    # Parse s3://bucket/key into just the key portion
    bucket_name = s3_bucket.bucket_name
    prefix = f"s3://{bucket_name}/"
    if s3_uri.startswith(prefix):
        s3_key = s3_uri[len(prefix):]
    else:
        raise ValueError(f"S3 URI {s3_uri} does not match bucket {bucket_name}")

    tmp_dir = Path(tempfile.mkdtemp(prefix="warehouse_load_"))
    db_path = tmp_dir / Path(s3_key).name

    s3_bucket.download_object_to_path(s3_key, str(db_path))
    log.info("Downloaded %s to %s", s3_uri, db_path)
    return db_path


# ---------------------------------------------------------------------------
# Task: Validate
# ---------------------------------------------------------------------------


@task(log_prints=True, task_run_name="validate-{scraper_schema}")
def validate_run(db_path: str, scraper_schema: str) -> dict[str, int]:
    """Validate scraper output before loading."""
    from warehouse.validation import validate_scraper_output

    log = get_run_logger()
    report = validate_scraper_output(db_path)

    if not report.is_valid:
        raise ValueError(
            f"Validation failed for {scraper_schema}: "
            f"{report.valid_rows} valid / {report.total_rows} total rows"
        )

    log.info(
        "%s: %d valid rows across %d types",
        scraper_schema,
        report.valid_rows,
        len(report.counts_by_type),
    )
    return report.counts_by_type


# ---------------------------------------------------------------------------
# Task: Create Provenance
# ---------------------------------------------------------------------------


@task(log_prints=True, task_run_name="create-provenance")
def create_provenance(
    scraper_name: str,
    s3_artifact_path: str | None = None,
    description: str | None = None,
    metadata: dict | None = None,
) -> int:
    """Create a provenance record and return its ID."""
    from sqlalchemy import create_engine
    from sqlmodel import Session

    from warehouse.models import Provenance

    log = get_run_logger()

    ctx = get_run_context()
    run_id = ctx.flow_run.id if ctx and ctx.flow_run else None

    engine = create_engine(ANALYTICS_DB_URL)
    prov = Provenance(
        source_type="scraper_run",
        source_name=scraper_name,
        run_id=run_id,
        s3_artifact_path=s3_artifact_path,
        description=description or f"{scraper_name} scraper run",
        metadata_=metadata or {},
    )

    with Session(engine) as session:
        session.add(prov)
        session.commit()
        session.refresh(prov)
        provenance_id = prov.id

    log.info(
        "Created provenance %d for %s (run_id=%s)",
        provenance_id,
        scraper_name,
        run_id,
    )
    return provenance_id


# ---------------------------------------------------------------------------
# Task: Load to Warehouse
# ---------------------------------------------------------------------------


@task(log_prints=True, task_run_name="load-to-warehouse")
def load_to_warehouse(
    db_path: str,
    provenance_id: int,
) -> dict[str, int]:
    """Load SQLite results into raw warehouse tables."""
    from warehouse.loader import load_sqlite_to_raw

    return load_sqlite_to_raw(
        db_path=db_path,
        provenance_id=provenance_id,
        db_url=ANALYTICS_DB_URL,
    )


# ---------------------------------------------------------------------------
# Flow: warehouse-load
# ---------------------------------------------------------------------------


@flow(name="warehouse-load", log_prints=True)
async def warehouse_load_flow(
    s3_uri: str,
    scraper_schema: str,
) -> None:
    """Download a scraper database from S3 and load it into the warehouse.

    Validates output, creates a provenance record, bulk-loads results into
    raw tables, and emits a ``scrape.completed`` event for downstream
    SQLMesh transforms.

    Args:
        s3_uri: S3 URI of the scraper database.
        scraper_schema: PostgreSQL schema name for warehouse tables.
    """
    log = get_run_logger()

    # 1. Download database from S3
    db_path = await download_db(s3_uri)

    # 2. Validate
    validate_run(str(db_path), scraper_schema)

    # 3. Create provenance
    provenance_id = create_provenance(
        scraper_name=scraper_schema,
        s3_artifact_path=s3_uri,
    )

    # 4. Load into raw tables
    load_to_warehouse(str(db_path), provenance_id)

    # 5. Emit scrape.completed event for SQLMesh transforms
    log.info("Emitting scrape.completed event for %s", scraper_schema)
    emit_event(
        event="scrape.completed",
        resource={
            "prefect.resource.id": f"scraper.{scraper_schema}",
        },
    )
