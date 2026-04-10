"""Scraper run flow: run scraper -> doctor -> upload to S3 -> emit event.

Orchestrates scraper execution through S3 upload. After uploading, emits a
``scrape.uploaded`` event that triggers downstream warehouse loading.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import prefect.runtime
from prefect import flow, get_run_logger, task
from prefect.concurrency.asyncio import concurrency
from prefect.events import emit_event
from prefect_aws.s3 import S3Bucket

from flows.s3_archive import S3_BLOCK_NAME, make_s3_archive_callback

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Task: Run Scraper
# ---------------------------------------------------------------------------


@task(log_prints=True, task_run_name="run-scraper-{scraper_schema}")
async def run_scraper_task(
    scraper_path: str,
    seed_params: list[dict[str, dict[str, Any]]] | None,
    scraper_schema: str,
) -> Path:
    """Run a scraper using PersistentDriver with S3 archive.

    Litestream replication is handled by the worker process — the DB is
    continuously backed up to S3 for the duration of the flow run.

    Args:
        scraper_path: Import path, e.g. "module.path:ClassName".
        seed_params: Kent seed_params format.
        scraper_schema: Schema name, used for S3 paths.

    Returns:
        Path to the resulting SQLite database.
    """
    from kent.cli import import_scraper
    from kent.driver.persistent_driver import PersistentDriver

    log = get_run_logger()
    run_name = prefect.runtime.flow_run.name or "unnamed"

    # Import and instantiate scraper
    scraper_class = import_scraper(scraper_path)
    scraper = scraper_class()

    # Set up paths — DB goes directly in runs_dir so the kent web UI
    # can discover it via its flat glob("*.db") scan.
    runs_dir = Path(os.environ.get("SCRAPER_RUNS_DIR", "/tmp/scraper_runs"))
    runs_dir.mkdir(parents=True, exist_ok=True)
    db_path = runs_dir / f"{run_name}.db"

    s3_file_prefix = f"{scraper_schema}/files/"

    # Load S3 bucket for archive callback
    s3_bucket = await S3Bucket.aload(S3_BLOCK_NAME)
    archive_callback = make_s3_archive_callback(s3_bucket, s3_file_prefix)

    async with PersistentDriver.open(
        scraper=scraper,
        db_path=db_path,
        seed_params=seed_params,
        num_workers=1,
        resume=True,
    ) as driver:
        driver.on_archive = archive_callback
        log.info("Starting scraper: %s", scraper_path)
        await driver.run(setup_signal_handlers=False)
        log.info("Scraper run completed")

    return db_path


# ---------------------------------------------------------------------------
# Task: Doctor Health Check
# ---------------------------------------------------------------------------


@task(log_prints=True, task_run_name="doctor-health-check")
async def doctor_health_check(db_path: Path) -> dict[str, Any]:
    """Run pdd doctor health checks on the scraper database.

    Fails the flow if integrity issues are found or unresolved errors exist.

    Args:
        db_path: Path to the scraper SQLite database.

    Returns:
        Health check results dictionary.
    """
    from kent.driver.persistent_driver.debugger import LocalDevDriverDebugger

    log = get_run_logger()

    async with LocalDevDriverDebugger.open(db_path) as debugger:
        integrity = await debugger.check_integrity()
        ghosts = await debugger.get_ghost_requests()
        stats = await debugger.get_stats()

    log.info("Integrity: has_issues=%s", integrity["has_issues"])
    log.info(
        "Errors: total=%d, unresolved=%d",
        stats["errors"]["total"],
        stats["errors"]["unresolved"],
    )
    log.info("Ghost requests: %d", ghosts["total_count"])

    if integrity["has_issues"]:
        raise RuntimeError(
            f"Integrity check failed: "
            f"{integrity['orphaned_requests']['count']} orphaned requests, "
            f"{integrity['orphaned_responses']['count']} orphaned responses"
        )

    if stats["errors"]["unresolved"] > 0:
        raise RuntimeError(
            f"Unresolved errors: {stats['errors']['unresolved']} "
            f"(total errors: {stats['errors']['total']})"
        )

    return {
        "integrity": integrity,
        "ghosts": ghosts,
        "error_stats": stats["errors"],
    }


# ---------------------------------------------------------------------------
# Task: Upload Database to S3
# ---------------------------------------------------------------------------


@task(log_prints=True, task_run_name="upload-db")
async def upload_db(db_path: Path, scraper_schema: str) -> str:
    """Upload the scraper database to S3 for archival.

    Args:
        db_path: Path to the sqlite database file.
        scraper_schema: Schema name, used for S3 path.

    Returns:
        S3 URI of the uploaded database.
    """
    log = get_run_logger()
    s3_bucket = await S3Bucket.aload(S3_BLOCK_NAME)

    run_name = prefect.runtime.flow_run.name
    s3_key = f"scraper_runs/{scraper_schema}/{run_name}.db"

    s3_bucket.upload_from_path(str(db_path), to_path=s3_key)

    bucket_name = s3_bucket.bucket_name
    s3_uri = f"s3://{bucket_name}/{s3_key}"
    log.info("Uploaded database to %s", s3_uri)
    return s3_uri


# ---------------------------------------------------------------------------
# Flow: scraper-run
# ---------------------------------------------------------------------------


@flow(name="scraper-run", log_prints=True)
async def scraper_run_flow(
    scraper_path: str,
    seed_params: list[dict[str, dict[str, Any]]] | None = None,
    scraper_schema: str = "",
) -> None:
    """Run a scraper, verify output, and upload the database to S3.

    After uploading, emits a ``scrape.uploaded`` event for downstream
    warehouse loading.

    Args:
        scraper_path: Import path like "module.path:ClassName".
        seed_params: Kent seed_params for filtering entry points.
        scraper_schema: PostgreSQL schema name for warehouse tables.
    """
    log = get_run_logger()

    if not scraper_schema:
        raise ValueError("scraper_schema is required")

    # Acquire a per-scraper concurrency slot (limit=1 per schema).
    # A second run for the same scraper will block here until the first finishes.
    async with concurrency(f"scraper:{scraper_schema}", occupy=1):
        # 1. Run the scraper
        db_path = await run_scraper_task(
            scraper_path=scraper_path,
            seed_params=seed_params,
            scraper_schema=scraper_schema,
        )

        # 2. Doctor health check
        await doctor_health_check(db_path)

        # 3. Upload database to S3
        s3_uri = await upload_db(db_path, scraper_schema)

        # 4. Emit scrape.uploaded event for warehouse loading
        log.info("Emitting scrape.uploaded event for %s", scraper_schema)
        emit_event(
            event="scrape.uploaded",
            resource={
                "prefect.resource.id": f"scraper.{scraper_schema}",
            },
            payload={
                "s3_uri": s3_uri,
                "scraper_schema": scraper_schema,
                "scraper_path": scraper_path,
            },
        )
