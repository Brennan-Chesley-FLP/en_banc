"""Generic JKent scraper run flow: run scraper -> integrity check -> upload.

Runs a JKent scraper to a resumable SQLite database. File downloads stream to
the ``files`` S3 bucket via :class:`S3AsyncStreamingArchiveHandler`; the final
database artifact is integrity-checked and uploaded to the ``scrapes`` bucket.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import os
import sqlite3
from pathlib import Path
from typing import Any

import prefect.runtime
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact
from prefect.cache_policies import INPUTS
from prefect.states import Cancelled, State
from prefect_aws.s3 import S3Bucket

from flows.archive import is_local_backend, make_archive_handler, move_db_to_archive
from flows.shutdown import get_shutdown_event
from workers.telemetry import instrument_run_engine, run_baggage

# Name of the Prefect S3Bucket block holding scrape DB artifacts.
SCRAPES_S3_BLOCK_NAME = "scrapes"

# How often the background stats logger reports scrape progress to the run logs.
STATS_LOG_INTERVAL_SECONDS = 300

# Per-run continuation-worker pool cap (jkent's RunBootstrapper ``max_workers``).
# This is the number of concurrent continuation workers a *single* run may ramp
# up to — distinct from WORKER_CONCURRENCY, which is how many runs a worker
# executes at once. Dial MAX_CONTINUATION_WORKERS to test whether more workers
# inside one run improves throughput (EN_BANC_OTEL.md §5). jkent caps this to 1
# for STRICTLY_SERIAL scrapers regardless, so raising it is safe.
DEFAULT_MAX_CONTINUATION_WORKERS = 10


def resolve_max_continuation_workers() -> int:
    """Return the per-run continuation-worker cap from ``MAX_CONTINUATION_WORKERS``.

    Defaults to :data:`DEFAULT_MAX_CONTINUATION_WORKERS`. Both the scraper worker
    and the browser worker run this flow and read the same env var, so one
    setting dials the pool for whichever worker runs the scrape.

    Raises:
        ValueError: If set to a non-integer or a value below 1.
    """
    raw = os.environ.get(
        "MAX_CONTINUATION_WORKERS", str(DEFAULT_MAX_CONTINUATION_WORKERS)
    ).strip()
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(
            f"MAX_CONTINUATION_WORKERS must be an integer, got {raw!r}"
        ) from None
    if value < 1:
        raise ValueError(f"MAX_CONTINUATION_WORKERS must be >= 1, got {value}")
    return value


async def _log_stats_periodically(
    run: Any,
    log: Any,
    interval_seconds: int = STATS_LOG_INTERVAL_SECONDS,
) -> None:
    """Log a scrape progress snapshot every ``interval_seconds`` until cancelled.

    Taps JKent's :func:`get_stats` against the live run database so a long scrape
    reports progress to the flow logs instead of going silent. The queries are
    read-only (SQLite WAL lets them run alongside the scrape's writers), and the
    coroutine runs as a background task that the caller cancels once the scrape
    finishes or drains. Stats failures are logged and swallowed — surfacing
    progress must never disturb the scrape itself.

    Args:
        run: The opened :class:`ScrapeRun`; its ``_db._session_factory`` is the
            same session factory JKent's own stats queries use.
        log: The Prefect run logger.
        interval_seconds: Delay between snapshots (also the delay before the
            first one, so a just-started run isn't reported as all-zeros).
    """
    from jkent.driver.database_engine.stats import get_stats

    while True:
        await asyncio.sleep(interval_seconds)
        try:
            stats = await get_stats(run._db._session_factory)
        except Exception as exc:  # noqa: BLE001 - stats must never break the scrape
            log.warning("Could not gather scrape stats: %s", exc)
            continue
        q, r, e = stats.queue, stats.results, stats.errors
        log.info(
            "Scrape progress [%s]: requests %d/%d completed "
            "(%d pending, %d in-progress, %d failed); "
            "results=%d (valid=%d, invalid=%d); errors=%d; %.1f req/min",
            stats.run_status,
            q.completed,
            q.total,
            q.pending,
            q.in_progress,
            q.failed,
            r.total,
            r.valid,
            r.invalid,
            e.total,
            stats.throughput.requests_per_minute,
        )


def _import_scraper(scraper_path: str) -> type:
    """Import a scraper class from a ``module.path:ClassName`` string."""
    module_path, _, class_name = scraper_path.partition(":")
    if not class_name:
        raise ValueError(
            f"scraper_path must be 'module.path:ClassName', got {scraper_path!r}"
        )
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    """Return whether a table exists in the connected database."""
    return (
        conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        is not None
    )


def _read_run_summary(db_path: Path) -> dict[str, Any]:
    """Read aggregate run statistics from a scrape's SQLite database.

    JKent records per-request failures (HTTP 500s, structural/validation
    assumption failures, etc.) as ``errors`` rows rather than raising — so a
    run can "complete" while still having failed work. Alongside the errors,
    this aggregates the request queue and the harvested results so the summary
    artifact can show what the run actually did.

    Returns a dict with:
        ``requests_by_status``: ``[(continuation, status, count), ...]``
        ``errors_by_continuation``: ``[(continuation, error_type, count), ...]``
        ``results_by_type``: ``[(result_type, valid, invalid), ...]``
        ``total``: total error count (for log broadcasting)
        ``by_type``: ``{error_type: count}`` (for log broadcasting)
        ``rows``: first 50 error detail rows (for log broadcasting / detail table)
    Missing tables yield empty aggregates rather than raising.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        requests_by_status: list[tuple] = []
        if _table_exists(conn, "requests"):
            requests_by_status = conn.execute(
                "SELECT continuation, status, COUNT(*) "
                "FROM requests GROUP BY continuation, status "
                "ORDER BY continuation, status"
            ).fetchall()

        results_by_type: list[tuple] = []
        if _table_exists(conn, "results"):
            results_by_type = conn.execute(
                "SELECT result_type, "
                "SUM(is_valid), SUM(CASE WHEN is_valid THEN 0 ELSE 1 END) "
                "FROM results GROUP BY result_type ORDER BY result_type"
            ).fetchall()

        total = 0
        by_type: dict[str, int] = {}
        rows: list[tuple] = []
        errors_by_continuation: list[tuple] = []
        if _table_exists(conn, "errors"):
            total = conn.execute("SELECT COUNT(*) FROM errors").fetchone()[0]
            by_type = dict(
                conn.execute(
                    "SELECT error_type, COUNT(*) FROM errors GROUP BY error_type"
                ).fetchall()
            )
            rows = conn.execute(
                "SELECT error_type, error_class, message, request_url "
                "FROM errors ORDER BY id LIMIT 50"
            ).fetchall()
            # continuation lives on the request, not the error, so join through.
            errors_by_continuation = conn.execute(
                "SELECT r.continuation, e.error_type, COUNT(*) "
                "FROM errors e LEFT JOIN requests r ON e.request_id = r.id "
                "GROUP BY r.continuation, e.error_type "
                "ORDER BY r.continuation, e.error_type"
            ).fetchall()
    finally:
        conn.close()
    return {
        "requests_by_status": requests_by_status,
        "errors_by_continuation": errors_by_continuation,
        "results_by_type": results_by_type,
        "total": total,
        "by_type": by_type,
        "rows": rows,
    }


@task(log_prints=True, task_run_name="run-scraper-{scraper_schema}")
async def run_scraper_task(
    scraper_path: str,
    seed_params: list[dict[str, dict[str, Any]]] | None,
    scraper_schema: str,
) -> Path | None:
    """Run a JKent scraper, streaming file downloads to the files bucket.

    Per-scraper concurrency is enforced at the scheduling layer: each scraper
    has its own work queue with a concurrency limit, so the server never
    dispatches more than that many runs of a given scraper to a worker. By the
    time this task runs the slot is already held, so it just does the scrape.

    Honors cooperative shutdown: if the process-global shutdown event fires
    mid-scrape (worker received SIGTERM/SIGINT), the scrape is drained — the
    in-flight request finishes, JKent finalizes the run as ``interrupted``, and
    the DB is left consistent and resumable.

    Args:
        scraper_path: Import path, e.g. ``"module.path:ClassName"``.
        seed_params: JKent ``seed_params`` (``[{entry: kwargs}]``); ``None``
            uses the scraper's default entry points.
        scraper_schema: Schema name, used for S3 key prefixes.

    Returns:
        Path to the resulting SQLite database, or ``None`` if the scrape was
        drained for shutdown before completing (DB preserved for later resume).
    """
    from jkent.driver.unified_driver import RunBootstrapper

    log = get_run_logger()
    run_name = prefect.runtime.flow_run.name or "unnamed"

    scraper_class = _import_scraper(scraper_path)
    scraper = scraper_class()

    runs_dir = Path(os.environ.get("SCRAPER_RUNS_DIR", "/tmp/scraper_runs"))
    runs_dir.mkdir(parents=True, exist_ok=True)
    db_path = runs_dir / f"{run_name}.db"

    # The DB is named after the (unique) flow run, so an existing file means a
    # prior attempt of *this same* run already seeded and partly executed it —
    # e.g. retrying a cancelled or crashed run. Re-seeding an existing run is
    # rejected by the bootstrapper, so drop seed_params and let resume=True
    # pick up where it left off; same run name implies the same seed.
    if seed_params is not None and db_path.exists():
        log.info("Resuming existing run DB %s; ignoring seed_params", db_path)
        seed_params = None

    archive_handler = await make_archive_handler(
        prefix=f"{scraper_schema}/"
    )

    max_workers = resolve_max_continuation_workers()
    log.info(
        "Commencing scrape: %s (max_continuation_workers=%d)",
        scraper_path, max_workers,
    )
    async with RunBootstrapper(
        scraper,
        db_path=db_path,
        seed_params=seed_params,
        archive_handler=archive_handler,
        resume=True,
        max_workers=max_workers,
        setup_signal_handlers=False,
    ) as run:
        # Bind the SQLAlchemy instrumentor to this run's per-run engine so its DB
        # spans nest under jkent's request spans (EN_BANC_OTEL.md §2). No-op when
        # telemetry is disabled.
        instrument_run_engine(run)

        # Race the scrape against the shutdown signal. JKent's signal handlers
        # no-op off the main thread (the worker runs flows off-main-thread), so
        # we drive run.stop() ourselves when the process is asked to shut down.
        shutdown = get_shutdown_event()
        # Attach the flow-run identity as OTel baggage *before* scheduling the run
        # so its context snapshot carries it; jkent stamps `flow_run_id` on its
        # spans/metrics for cross-run correlation (§3). No-op when disabled.
        with run_baggage(str(prefect.runtime.flow_run.id), scraper_schema):
            scrape = asyncio.ensure_future(run.run())
            drain_signal = asyncio.ensure_future(shutdown.wait())
            # Report progress to the logs every few minutes while the scrape runs.
            # Cancelled in the finally so it never outlives the scrape.
            stats_logger = asyncio.ensure_future(_log_stats_periodically(run, log))
            try:
                await asyncio.wait(
                    {scrape, drain_signal}, return_when=asyncio.FIRST_COMPLETED
                )

                if not scrape.done():
                    # Shutdown won the race: drain cooperatively. run.stop() lets the
                    # in-flight request finish, then run.run() returns normally with
                    # the run finalized as "interrupted" and the DB left resumable.
                    log.warning("Shutdown requested; draining scrape for resume: %s", db_path)
                    run.stop()
                    await scrape
                    log.warning("Scrape drained (interrupted); DB preserved: %s", db_path)
                    return None

                drain_signal.cancel()
                # Surface any scrape error (or confirm clean completion).
                await scrape
            finally:
                stats_logger.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await stats_logger

    log.info("Scraper run completed: %s", db_path)
    return db_path


@task(
    log_prints=True,
    task_run_name="integrity-check-and-archive",
    persist_result=True,
    cache_policy=INPUTS,
)
async def integrity_check_and_archive(
    db_path: Path,
    scraper_schema: str,
) -> str:
    """Run ``PRAGMA integrity_check`` then archive the DB.

    With ``ARCHIVE_BACKEND=local`` the DB is moved to the local archive
    (``{ARCHIVE_LOCAL_ROOT}/{scraper_schema}/scrapes/{db_name}.db``); otherwise
    it is uploaded to the ``scrapes`` S3 bucket. The result is cached (INPUTS) so
    flow retries skip re-archiving.

    Returns:
        The archived DB's URI (``file://`` for local, ``s3://`` for S3).

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

    # Local backend: move the DB onto the archive drive, no S3 involved.
    if is_local_backend():
        local_size = db_path.stat().st_size
        archive_uri = await asyncio.to_thread(
            move_db_to_archive, db_path, scraper_schema
        )
        log.info(
            "Moved run DB (%d bytes) to local archive: %s",
            local_size, archive_uri,
        )
        return archive_uri

    run_id = prefect.runtime.flow_run.id
    s3_bucket = await S3Bucket.aload(SCRAPES_S3_BLOCK_NAME)
    s3_key = f"scraper_runs/{scraper_schema}/{run_id}.db"

    # NOTE: ``upload_from_path`` is ``@async_dispatch``-decorated, so inside a
    # running event loop it returns a *coroutine* instead of uploading. Calling
    # it without ``await`` silently no-ops (the upload never runs) yet raises
    # nothing — so a "success" log would be a lie. Use the explicit async
    # method and verify the object actually landed before claiming success.
    local_size = db_path.stat().st_size
    log.info("Uploading %s (%d bytes) to s3://%s/%s", db_path, local_size, s3_bucket.bucket_name, s3_key)
    await s3_bucket.aupload_from_path(str(db_path), to_path=s3_key)

    s3_uri = f"s3://{s3_bucket.bucket_name}/{s3_key}"

    # Confirm the upload by reading the object back. A 404 / size mismatch here
    # means the upload silently failed and must not be reported as complete.
    client = s3_bucket.credentials.get_client("s3")
    try:
        head = await asyncio.to_thread(
            client.head_object, Bucket=s3_bucket.bucket_name, Key=s3_key
        )
    except Exception as exc:
        log.error("Upload verification FAILED for %s: %s", s3_uri, exc)
        raise RuntimeError(
            f"Database upload to {s3_uri} could not be verified (object not found): {exc}"
        ) from exc

    uploaded_size = head["ContentLength"]
    if uploaded_size != local_size:
        log.error(
            "Upload size mismatch for %s: local=%d uploaded=%d",
            s3_uri, local_size, uploaded_size,
        )
        raise RuntimeError(
            f"Database upload to {s3_uri} is incomplete "
            f"(local={local_size} bytes, uploaded={uploaded_size} bytes)"
        )

    log.info("Verified upload of %s (%d bytes)", s3_uri, uploaded_size)
    return s3_uri


@flow(name="scraper-run", log_prints=True)
async def scraper_run_flow(
    scraper_path: str,
    scraper_schema: str,
    seed_params: list[dict[str, dict[str, Any]]] | None = None,
) -> str | State:
    """Run a JKent scrape and archive its database.

    Args:
        scraper_path: Import path, e.g. ``"module.path:ClassName"``.
        scraper_schema: Schema/source name used as the S3 key prefix and in
            artifacts (e.g. ``"ala_publicportal"``). Required and non-empty.
        seed_params: JKent ``seed_params``; ``None`` uses default entries.

    Returns:
        S3 URI of the uploaded scrape database, or a ``Cancelled`` state if the
        scrape was drained for a cooperative shutdown (the DB is preserved on
        the worker; retry this same flow run to resume it).
    """
    if not scraper_schema:
        raise ValueError("scraper_schema is required and must be non-empty")
    log = get_run_logger()
    db_path = await run_scraper_task(scraper_path, seed_params, scraper_schema)

    # The scrape was drained mid-run for a graceful shutdown. Don't upload a
    # partial DB or clean it up — leave it for resume and end the run Cancelled.
    if db_path is None:
        log.warning("Scrape drained for shutdown; retry this run to resume.")
        return Cancelled(message="Scrape drained for graceful shutdown; resume by retrying this run.")

    # Surface per-request errors recorded during the scrape. These don't fail
    # the run on their own, so broadcast them explicitly to the run logs.
    summary = await asyncio.to_thread(_read_run_summary, db_path)
    if summary["total"]:
        by_type = ", ".join(f"{t}={n}" for t, n in sorted(summary["by_type"].items()))
        log.warning(
            "Scrape recorded %d error(s) [%s]", summary["total"], by_type
        )
        for error_type, error_class, message, request_url in summary["rows"]:
            log.warning("  [%s] %s: %s (%s)", error_type, error_class, message, request_url)
    else:
        log.info("Scrape recorded no errors")

    archive_uri = await integrity_check_and_archive(db_path, scraper_schema)

    await create_markdown_artifact(
        key="scrape-summary",
        markdown=_build_summary_markdown(scraper_path, scraper_schema, archive_uri, summary),
    )

    # The DB is safely archived now (uploaded to S3, or moved to the local
    # archive — in which case the source is already gone), so reclaim the
    # worker's runs volume. This is the last step (the archive result is cached,
    # so a retry wouldn't need the local file) and best-effort: a stray unlink
    # failure must not fail an otherwise-successful run. Remove the WAL/SHM
    # sidecars too — they linger if the DB was last touched in WAL mode.
    for path in (db_path, db_path.with_suffix(".db-wal"), db_path.with_suffix(".db-shm")):
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            log.warning("Could not remove local scrape file %s: %s", path, exc)

    return archive_uri


def _pivot_table(
    triples: list[tuple],
    row_header: str,
    col_header: str,
) -> list[str]:
    """Render ``(row, col, count)`` triples as a markdown pivot table.

    Rows and columns are the sorted distinct keys; the body holds the counts
    with a trailing ``Total`` column and row. Empty cells are left blank rather
    than ``0`` so the populated cells stand out. ``None`` keys (e.g. an error
    whose request row is missing) render as ``(unknown)``.
    """

    def label(value: Any) -> str:
        return str(value) if value is not None else "(unknown)"

    rows = sorted({label(r) for r, _, _ in triples})
    cols = sorted({label(c) for _, c, _ in triples})
    counts: dict[tuple[str, str], int] = {}
    for r, c, n in triples:
        counts[(label(r), label(c))] = counts.get((label(r), label(c)), 0) + n

    lines = [
        f"| {row_header} \\ {col_header} | " + " | ".join(cols) + " | **Total** |",
        "| --- |" + "".join(" --- |" for _ in range(len(cols) + 1)),
    ]
    col_totals = dict.fromkeys(cols, 0)
    grand_total = 0
    for r in rows:
        row_total = 0
        cells = []
        for c in cols:
            n = counts.get((r, c), 0)
            cells.append(str(n) if n else "")
            col_totals[c] += n
            row_total += n
        grand_total += row_total
        lines.append(f"| {r} | " + " | ".join(cells) + f" | **{row_total}** |")
    total_row = " | ".join(f"**{col_totals[c]}**" for c in cols)
    lines.append(f"| **Total** | {total_row} | **{grand_total}** |")
    return lines


def _build_summary_markdown(
    scraper_path: str,
    scraper_schema: str,
    s3_uri: str,
    summary: dict[str, Any],
) -> str:
    """Render the scrape-summary markdown artifact from aggregate stats."""
    requests_by_status = summary["requests_by_status"]
    errors_by_continuation = summary["errors_by_continuation"]
    results_by_type = summary["results_by_type"]
    total_errors = summary["total"]

    total_requests = sum(n for _, _, n in requests_by_status)
    total_results = sum(valid + invalid for _, valid, invalid in results_by_type)
    status = "completed with errors" if total_errors else "completed"

    lines = [
        f"# Scrape {status}",
        "",
        f"- **Scraper**: `{scraper_path}`",
        f"- **Schema**: `{scraper_schema}`",
        f"- **Database**: `{s3_uri}`",
        f"- **Requests**: {total_requests}",
        f"- **Results**: {total_results}",
        f"- **Errors**: {total_errors}",
    ]

    lines += ["", "## Requests by status and continuation", ""]
    if requests_by_status:
        lines += _pivot_table(requests_by_status, "Continuation", "Status")
    else:
        lines.append("_No requests recorded._")

    lines += ["", "## Results by type", ""]
    if results_by_type:
        lines += ["| Type | Valid | Invalid | Total |", "| --- | --- | --- | --- |"]
        for result_type, valid, invalid in results_by_type:
            valid, invalid = valid or 0, invalid or 0
            lines.append(f"| {result_type} | {valid} | {invalid} | {valid + invalid} |")
        lines.append(f"| **Total** | | | **{total_results}** |")
    else:
        lines.append("_No results recorded._")

    lines += ["", "## Errors by continuation and type", ""]
    if errors_by_continuation:
        lines += _pivot_table(errors_by_continuation, "Continuation", "Error type")

        lines += ["", "## Error detail", "", "| Type | Class | Message | URL |", "| --- | --- | --- | --- |"]
        for error_type, error_class, message, request_url in summary["rows"]:
            # Escape pipes so the markdown table doesn't break on error text.
            msg = (message or "").replace("|", "\\|").replace("\n", " ")
            lines.append(f"| {error_type} | {error_class} | {msg} | {request_url} |")
        if total_errors > len(summary["rows"]):
            lines += ["", f"_…and {total_errors - len(summary['rows'])} more (showing first {len(summary['rows'])})._"]
    else:
        lines.append("_No errors recorded._")

    return "\n".join(lines) + "\n"
