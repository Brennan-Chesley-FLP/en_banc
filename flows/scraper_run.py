"""Generic JKent scraper run flow: run scraper -> integrity check -> upload.

Runs a JKent scraper to a resumable SQLite database. File downloads stream to
the ``files`` S3 bucket via :class:`S3AsyncStreamingArchiveHandler`; the final
database artifact is integrity-checked and uploaded to the ``scrapes`` bucket.
"""

from __future__ import annotations

import importlib
import os
import sqlite3
from pathlib import Path
from typing import Any

import prefect.runtime
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact
from prefect.cache_policies import INPUTS
from prefect.concurrency.asyncio import concurrency
from prefect_aws.s3 import S3Bucket

from flows.s3_archive import make_s3_archive_handler
from flows.scrapers import scraper_limit_name

# Name of the Prefect S3Bucket block holding scrape DB artifacts.
SCRAPES_S3_BLOCK_NAME = "scrapes"


def _import_scraper(scraper_path: str) -> type:
    """Import a scraper class from a ``module.path:ClassName`` string."""
    module_path, _, class_name = scraper_path.partition(":")
    if not class_name:
        raise ValueError(
            f"scraper_path must be 'module.path:ClassName', got {scraper_path!r}"
        )
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


@task(log_prints=True, task_run_name="run-scraper-{scraper_schema}")
async def run_scraper_task(
    scraper_path: str,
    seed_params: list[dict[str, dict[str, Any]]] | None,
    scraper_schema: str,
) -> Path:
    """Run a JKent scraper, streaming file downloads to the files bucket.

    Args:
        scraper_path: Import path, e.g. ``"module.path:ClassName"``.
        seed_params: JKent ``seed_params`` (``[{entry: kwargs}]``); ``None``
            uses the scraper's default entry points.
        scraper_schema: Schema name, used for S3 key prefixes.

    Returns:
        Path to the resulting SQLite database.
    """
    from jkent.driver.unified_driver import RunBootstrapper

    log = get_run_logger()
    run_name = prefect.runtime.flow_run.name or "unnamed"

    scraper_class = _import_scraper(scraper_path)
    scraper = scraper_class()

    runs_dir = Path(os.environ.get("SCRAPER_RUNS_DIR", "/tmp/scraper_runs"))
    runs_dir.mkdir(parents=True, exist_ok=True)
    db_path = runs_dir / f"{run_name}.db"

    archive_handler = await make_s3_archive_handler(
        prefix=f"{scraper_schema}/"
    )

    # Hold this scraper's global concurrency slot for the duration of the
    # scrape. The limit is provisioned per-scraper by Pulumi; strict=False
    # (the default) means an unprovisioned scraper simply runs unthrottled
    # rather than erroring.
    limit_name = scraper_limit_name(scraper_path)
    log.info("Starting scraper: %s (concurrency limit %r)", scraper_path, limit_name)
    async with concurrency(limit_name, occupy=1):
        async with RunBootstrapper(
            scraper,
            db_path=db_path,
            seed_params=seed_params,
            archive_handler=archive_handler,
            resume=True,
            setup_signal_handlers=False,
        ) as run:
            await run.run()
    log.info("Scraper run completed: %s", db_path)

    return db_path


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
    """Run ``PRAGMA integrity_check`` then upload the DB to the scrapes bucket.

    The result is cached (INPUTS) so flow retries skip a re-upload.

    Returns:
        S3 URI of the uploaded database.

    Raises:
        RuntimeError: If the SQLite integrity check fails.
    """
    log = get_run_logger()

    log.info("Running SQLite integrity check on %s", db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        results = conn.execute("PRAGMA integrity_check;").fetchall()
    finally:
        conn.close()

    if results[0][0] != "ok":
        issues = "\n".join(row[0] for row in results)
        log.error("SQLite integrity check FAILED:\n%s", issues)
        raise RuntimeError(f"SQLite integrity check failed:\n{issues}")
    log.info("SQLite integrity check passed")

    run_id = prefect.runtime.flow_run.id
    s3_bucket = await S3Bucket.aload(SCRAPES_S3_BLOCK_NAME)
    s3_key = f"scraper_runs/{scraper_schema}/{run_id}.db"
    s3_bucket.upload_from_path(str(db_path), to_path=s3_key)

    s3_uri = f"s3://{s3_bucket.bucket_name}/{s3_key}"
    log.info("Uploaded database to %s", s3_uri)
    return s3_uri


@flow(name="scraper-run", log_prints=True)
async def scraper_run_flow(
    scraper_path: str,
    scraper_schema: str,
    seed_params: list[dict[str, dict[str, Any]]] | None = None,
) -> str:
    """Run a JKent scrape and archive its database.

    Args:
        scraper_path: Import path, e.g. ``"module.path:ClassName"``.
        scraper_schema: Schema/source name used as the S3 key prefix and in
            artifacts (e.g. ``"ala_publicportal"``). Required and non-empty.
        seed_params: JKent ``seed_params``; ``None`` uses default entries.

    Returns:
        S3 URI of the uploaded scrape database.
    """
    if not scraper_schema:
        raise ValueError("scraper_schema is required and must be non-empty")
    db_path = await run_scraper_task(scraper_path, seed_params, scraper_schema)
    s3_uri = await integrity_check_and_upload(db_path, scraper_schema)

    await create_markdown_artifact(
        key="scrape-summary",
        markdown=(
            f"# Scrape complete\n\n"
            f"- **Scraper**: `{scraper_path}`\n"
            f"- **Schema**: `{scraper_schema or '(none)'}`\n"
            f"- **Database**: `{s3_uri}`\n"
        ),
    )
    return s3_uri
