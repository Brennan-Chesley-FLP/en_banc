"""Backfill flow for Alabama Public Portal (Appeals) scraper.

Runs the AlabamaScraper with all data types enabled via the dev driver,
serves the web UI for monitoring, uploads archived files to S3, and
uploads the resulting sqlite database to S3.
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import subprocess
import threading
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import prefect.runtime
from prefect import flow, task
from prefect_aws.s3 import S3Bucket

logger = logging.getLogger(__name__)

S3_FILE_PREFIX = "alabama/publicportal_alappeals_gov/"
S3_DB_PREFIX = "scraper_runs/alabama/publicportal_alappeals_gov/"
S3_BLOCK_NAME = "scrapers"

# Extension mapping matching juriscraper's uuid_archive_callback
TYPE_TO_EXTENSION = {
    "pdf": ".pdf",
    "audio": ".mp3",
    "mp3": ".mp3",
    "wav": ".wav",
    "image": ".jpg",
    "jpg": ".jpg",
    "jpeg": ".jpg",
    "png": ".png",
    "gif": ".gif",
    "html": ".html",
    "json": ".json",
    "xml": ".xml",
    "text": ".txt",
    "csv": ".csv",
}


def _get_extension(url: str, expected_type: str | None) -> str:
    """Determine file extension from URL or expected_type hint."""
    parsed_url = urlparse(url)
    url_path = Path(parsed_url.path)
    extension = url_path.suffix.lower() if url_path.suffix else ""

    if not extension and expected_type:
        extension = TYPE_TO_EXTENSION.get(expected_type.lower(), "")

    return extension


def _make_s3_archive_callback(s3_bucket: S3Bucket):
    """Create an on_archive callback that uploads files to S3.

    Args:
        s3_bucket: Loaded Prefect S3Bucket block.

    Returns:
        Async callback matching the on_archive signature.
    """

    async def s3_archive_callback(
        content: bytes,
        url: str,
        expected_type: str | None,
        storage_dir: Path,
    ) -> str:
        file_uuid = uuid4()
        extension = _get_extension(url, expected_type)
        filename = f"{file_uuid}{extension}"
        s3_key = f"{S3_FILE_PREFIX}{filename}"

        s3_bucket.upload_from_file_object(
            io.BytesIO(content),
            to_path=s3_key,
        )

        bucket_name = s3_bucket.bucket_name
        return f"s3://{bucket_name}/{s3_key}"

    return s3_archive_callback


@task(log_prints=True)
async def run_scraper() -> Path:
    """Run the Alabama scraper with web UI and S3 archiving.

    Starts the dev driver for AlabamaScraper with all data types,
    serves the web UI for monitoring on 0.0.0.0:8000, and uploads
    archived files directly to S3.

    Returns:
        Path to the sqlite database file for the completed run.
    """
    import uvicorn

    from juriscraper.sd.state.alabama.publicportal_alappeals_gov.scraper import (
        AlabamaScraper,
    )
    from juriscraper.scraper_driver.driver.dev_driver.web.app import (
        RunManager,
        create_app,
    )

    s3_bucket = await S3Bucket.aload(S3_BLOCK_NAME)

    # All data types enabled by default (no params filtering)
    scraper = AlabamaScraper()

    runs_dir = Path("runs")
    runs_dir.mkdir(parents=True, exist_ok=True)

    run_id = "backfill"
    manager = RunManager(runs_dir)

    run_info = await manager.create_run(
        run_id=run_id,
        scraper=scraper,
    )

    # Replace the default archive callback with S3 upload
    assert run_info.driver is not None
    run_info.driver.on_archive = _make_s3_archive_callback(s3_bucket)

    # Start the scraper run
    run_info = await manager.start_run(run_id)
    assert run_info.task is not None

    print(f"Scraper run started: {run_id}")
    print(f"Web UI available at http://0.0.0.0:8000")

    # Start the web UI in a background thread
    app = create_app(runs_dir=runs_dir)
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="warning",
    )
    server = uvicorn.Server(config)

    server_thread = threading.Thread(
        target=server.run,
        daemon=True,
    )
    server_thread.start()

    # Wait for the scraper to finish
    try:
        await run_info.task
    except asyncio.CancelledError:
        logger.info("Scraper run was cancelled")

    print("Scraper run completed")

    # Shut down uvicorn
    server.should_exit = True
    server_thread.join(timeout=10)

    return run_info.db_path


@task(log_prints=True)
async def upload_db(db_path: Path) -> str:
    """Upload the scraper run's sqlite database to S3.

    Args:
        db_path: Path to the sqlite database file.

    Returns:
        S3 URI of the uploaded database.
    """
    s3_bucket = await S3Bucket.aload(S3_BLOCK_NAME)

    run_name = prefect.runtime.flow_run.name
    s3_key = f"{S3_DB_PREFIX}{run_name}.db"

    s3_bucket.upload_from_path(str(db_path), to_path=s3_key)

    bucket_name = s3_bucket.bucket_name
    s3_uri = f"s3://{bucket_name}/{s3_key}"
    print(f"Uploaded database to {s3_uri}")
    return s3_uri


def _debug_tailscale() -> None:
    """Log diagnostic info about the Tailscale setup."""
    ts_authkey = os.environ.get("TS_AUTHKEY", "")
    print(f"TS_AUTHKEY present: {bool(ts_authkey)} (len={len(ts_authkey)})")
    print(f"TS_HOSTNAME: {os.environ.get('TS_HOSTNAME', '<not set>')}")

    sock = Path("/var/run/tailscale/tailscaled.sock")
    print(f"tailscaled socket exists: {sock.exists()}")

    state = Path("/var/lib/tailscale/tailscaled.state")
    print(f"tailscaled state file exists: {state.exists()}")

    # Check if tailscaled process is running
    try:
        result = subprocess.run(
            ["pgrep", "-a", "tailscaled"],
            capture_output=True, text=True, timeout=5,
        )
        print(f"tailscaled processes: {result.stdout.strip() or 'none'}")
    except Exception as e:
        print(f"pgrep tailscaled failed: {e}")

    # Check tailscale status
    try:
        result = subprocess.run(
            ["tailscale", "status", "--socket=/var/run/tailscale/tailscaled.sock"],
            capture_output=True, text=True, timeout=10,
        )
        print(f"tailscale status (rc={result.returncode}):\n{result.stdout}{result.stderr}")
    except Exception as e:
        print(f"tailscale status failed: {e}")

    # Check if tailscale binary exists
    try:
        result = subprocess.run(
            ["which", "tailscale"],
            capture_output=True, text=True, timeout=5,
        )
        print(f"tailscale binary: {result.stdout.strip() or 'not found'}")
    except Exception as e:
        print(f"which tailscale failed: {e}")

    # Check entrypoint — was /entrypoint.sh the PID 1 process?
    try:
        result = subprocess.run(
            ["cat", "/proc/1/cmdline"],
            capture_output=True, text=True, timeout=5,
        )
        cmdline = result.stdout.replace("\x00", " ").strip()
        print(f"PID 1 cmdline: {cmdline}")
    except Exception as e:
        print(f"PID 1 cmdline check failed: {e}")


@flow(log_prints=True)
async def alabama_publicportal_backfill() -> None:
    """Backfill flow for Alabama Public Portal scraper.

    Task 1: Runs the scraper with all data types via the dev driver,
    serves a web UI for monitoring, and uploads archived files to S3.

    Task 2: Uploads the sqlite database to S3.
    """
    run_name = prefect.runtime.flow_run.name
    print(f"Flow run name: {run_name}")

    # Set Tailscale hostname to the Prefect flow run name
    try:
        result = subprocess.run(
            ["tailscale", "set", f"--hostname={run_name}"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            print(f"Tailscale hostname set to: {run_name}")
        else:
            print(f"Failed to set Tailscale hostname: {result.stderr}")
    except Exception as e:
        print(f"Could not set Tailscale hostname: {e}")

    db_path = await run_scraper()
    await upload_db(db_path)


if __name__ == "__main__":
    asyncio.run(alabama_publicportal_backfill())
