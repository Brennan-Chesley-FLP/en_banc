"""Generic scraper run flow: run scraper -> integrity check -> upload -> doctor -> validate -> load -> emit event.

Orchestrates the full pipeline from scraper execution through warehouse loading.
After loading, emits a ``scrape.completed`` event that triggers downstream
SQLMesh transforms via Prefect automation.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import prefect.runtime
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact
from prefect.cache_policies import INPUTS
from prefect.concurrency.asyncio import concurrency
from prefect.events import emit_event
from prefect_aws.s3 import S3Bucket
from prefect_sqlalchemy import SqlAlchemyConnector

from flows.s3_archive import S3_BLOCK_NAME, make_s3_archive_callback

logger = logging.getLogger(__name__)

ANALYTICS_DB_BLOCK = "analytics"


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
# Task: Integrity Check and Upload
# ---------------------------------------------------------------------------


@task(
    log_prints=True,
    task_run_name="integrity-check-and-upload",
    persist_result=True,
    cache_policy=INPUTS,
)
async def integrity_check_and_upload(
    db_path: Path,
    scraper_schema: str,
) -> str:
    """Run SQLite integrity check and upload database to S3.

    Runs ``PRAGMA integrity_check`` on the database.  If the check passes,
    uploads the database to S3 at ``scraper_runs/{run_id}.db``.
    The result is cached so flow retries skip the re-upload.

    Args:
        db_path: Path to the scraper SQLite database.
        scraper_schema: Schema name (for logging).

    Returns:
        S3 URI of the uploaded database.

    Raises:
        RuntimeError: If the SQLite integrity check fails.
    """
    log = get_run_logger()

    # Run SQLite integrity check
    log.info("Running SQLite integrity check on %s", db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute("PRAGMA integrity_check;")
        results = cursor.fetchall()
    finally:
        conn.close()

    if results[0][0] != "ok":
        issues = "\n".join(row[0] for row in results)
        log.error("SQLite integrity check FAILED:\n%s", issues)
        raise RuntimeError(f"SQLite integrity check failed:\n{issues}")

    log.info("SQLite integrity check passed")

    # Upload to S3 at scraper_runs/{scraper_schema}/{run_id}.db
    run_id = prefect.runtime.flow_run.id
    s3_bucket = await S3Bucket.aload(S3_BLOCK_NAME)
    s3_key = f"scraper_runs/{scraper_schema}/{run_id}.db"

    s3_bucket.upload_from_path(str(db_path), to_path=s3_key)

    bucket_name = s3_bucket.bucket_name
    s3_uri = f"s3://{bucket_name}/{s3_key}"
    log.info("Uploaded database to %s", s3_uri)
    return s3_uri


# ---------------------------------------------------------------------------
# Task: Cleanup Litestream Replica
# ---------------------------------------------------------------------------


@task(log_prints=True, task_run_name="cleanup-litestream-replica")
def cleanup_litestream_replica(scraper_schema: str) -> None:
    """Delete litestream S3 replica objects after successful DB upload.

    Best-effort cleanup — logs a warning on failure rather than
    failing the flow.
    """
    from flows.litestream import cleanup_replica

    log = get_run_logger()
    run_name = prefect.runtime.flow_run.name or "unnamed"
    s3_bucket = os.environ.get("LITESTREAM_S3_BUCKET", "scrapers")
    s3_prefix = f"scraper_runs/{scraper_schema}/{run_name}/replica/"

    try:
        deleted = cleanup_replica(s3_bucket, s3_prefix)
        log.info("Cleaned up %d litestream replica objects", deleted)
    except Exception:
        log.warning(
            "Failed to clean up litestream replicas at s3://%s/%s",
            s3_bucket,
            s3_prefix,
            exc_info=True,
        )


# ---------------------------------------------------------------------------
# Task: Doctor Health Check
# ---------------------------------------------------------------------------


@task(log_prints=True, task_run_name="doctor-health-check")
async def doctor_health_check(db_path: Path) -> dict[str, Any]:
    """Run pdd doctor health checks on the scraper database.

    Fails the flow if integrity issues are found or unresolved errors exist.
    On failure, creates a markdown artifact with the full doctor output.

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

    has_failure = False
    failure_reasons = []

    if integrity["has_issues"]:
        has_failure = True
        failure_reasons.append(
            f"Integrity check failed: "
            f"{integrity['orphaned_requests']['count']} orphaned requests, "
            f"{integrity['orphaned_responses']['count']} orphaned responses"
        )

    if stats["errors"]["unresolved"] > 0:
        has_failure = True
        failure_reasons.append(
            f"Unresolved errors: {stats['errors']['unresolved']} "
            f"(total errors: {stats['errors']['total']})"
        )

    if has_failure:
        md_lines = [
            "# PDD Doctor Health Check — FAILED\n",
            "## Integrity",
            f"- Has issues: **{integrity['has_issues']}**",
        ]
        if integrity.get("orphaned_requests"):
            md_lines.append(
                f"- Orphaned requests: {integrity['orphaned_requests']['count']}"
            )
        if integrity.get("orphaned_responses"):
            md_lines.append(
                f"- Orphaned responses: {integrity['orphaned_responses']['count']}"
            )
        md_lines += [
            "",
            "## Error Stats",
            f"- Total errors: {stats['errors']['total']}",
            f"- Unresolved errors: **{stats['errors']['unresolved']}**",
            "",
            "## Ghost Requests",
            f"- Total: {ghosts['total_count']}",
        ]

        await create_markdown_artifact(
            key="doctor-health-check",
            markdown="\n".join(md_lines),
            description="PDD doctor health check failure report",
        )

        raise RuntimeError("; ".join(failure_reasons))

    return {
        "integrity": integrity,
        "ghosts": ghosts,
        "error_stats": stats["errors"],
    }


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
    from sqlmodel import Session

    from warehouse.models import Provenance

    log = get_run_logger()

    run_id = prefect.runtime.flow_run.id

    block = SqlAlchemyConnector.load(ANALYTICS_DB_BLOCK)
    engine = block.get_engine()
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
) -> dict[str, dict[str, int]]:
    """Load SQLite results into raw warehouse tables with dedup."""
    from warehouse.loader import load_sqlite_to_raw

    block = SqlAlchemyConnector.load(ANALYTICS_DB_BLOCK)
    return load_sqlite_to_raw(
        db_path=db_path,
        provenance_id=provenance_id,
        db_url=block.connection_info.create_url().render_as_string(hide_password=False),
    )


# ---------------------------------------------------------------------------
# Task: Create Run Summary Artifact
# ---------------------------------------------------------------------------


@task(log_prints=True, task_run_name="create-run-summary")
async def create_run_summary(
    db_path: Path,
    counts_by_type: dict[str, int],
    load_results: dict[str, dict[str, int]],
    start_time: datetime,
) -> None:
    """Create a markdown artifact with scraper run statistics.

    Includes start time, duration, total requests made, a table
    of result counts by type, and warehouse load stats (new vs observed).
    """
    log = get_run_logger()
    now = datetime.now(timezone.utc)
    duration = now - start_time

    # Query total requests from the scraper DB
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute("SELECT COUNT(*) FROM requests")
        total_requests = cursor.fetchone()[0]
    except sqlite3.OperationalError:
        total_requests = "N/A"
    finally:
        conn.close()

    # Format duration
    total_seconds = int(duration.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        duration_str = f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        duration_str = f"{minutes}m {seconds}s"
    else:
        duration_str = f"{seconds}s"

    # Build markdown
    md_lines = [
        "# Scraper Run Summary\n",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Started | {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')} |",
        f"| Duration | {duration_str} |",
        f"| Total Requests | {total_requests} |",
        "",
        "## Result Counts by Type\n",
        "| Type | Count |",
        "|------|-------|",
    ]
    for result_type, count in sorted(counts_by_type.items()):
        md_lines.append(f"| {result_type} | {count:,} |")

    # Warehouse load stats
    md_lines += [
        "",
        "## Warehouse Load\n",
        "| Type | New Rows | Observations |",
        "|------|----------|--------------|",
    ]
    for result_type, counts in sorted(load_results.items()):
        md_lines.append(
            f"| {result_type} | {counts['new']:,} | {counts['observed']:,} |"
        )

    await create_markdown_artifact(
        key="run-summary",
        markdown="\n".join(md_lines),
        description="Scraper run summary statistics",
    )
    log.info("Created run summary artifact")


# ---------------------------------------------------------------------------
# Flow: scraper-run
# ---------------------------------------------------------------------------


@flow(name="scraper-run", log_prints=True)
async def scraper_run_flow(
    scraper_path: str,
    seed_params: list[dict[str, dict[str, Any]]] | None = None,
    scraper_schema: str = "",
) -> None:
    """Generic scraper run flow.

    Runs a scraper, validates output, loads to warehouse, and emits
    a ``scrape.completed`` event for downstream processing.

    Args:
        scraper_path: Import path like "module.path:ClassName".
        seed_params: Kent seed_params for filtering entry points.
        scraper_schema: PostgreSQL schema name for warehouse tables.
    """
    log = get_run_logger()
    flow_start_time = datetime.now(timezone.utc)

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

        # 2. SQLite integrity check + upload to S3 (cached for retries)
        s3_uri = await integrity_check_and_upload(db_path, scraper_schema)

        # 3. Clean up litestream replicas (best-effort)
        cleanup_litestream_replica(scraper_schema)

        # 4. Doctor health check
        await doctor_health_check(db_path)

        # 5. Validate
        counts_by_type = validate_run(str(db_path), scraper_schema)

        # 6. Create provenance
        provenance_id = create_provenance(
            scraper_name=scraper_schema,
            s3_artifact_path=s3_uri,
        )

        # 7. Load into raw tables
        load_results = load_to_warehouse(str(db_path), provenance_id)

        # 8. Create run summary artifact
        await create_run_summary(
            db_path, counts_by_type, load_results, flow_start_time
        )

        # 9. Emit scrape.completed event
        log.info("Emitting scrape.completed event for %s", scraper_schema)
        emit_event(
            event="scrape.completed",
            resource={
                "prefect.resource.id": f"scraper.{scraper_schema}",
            },
        )
