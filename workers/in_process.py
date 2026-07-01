"""In-process async worker for IO-dominated scraper flows.

Runs flow functions directly in the current event loop instead of spawning
a container or subprocess per flow run. Ideal for IO-bound workloads like
web scraping, where container startup would dominate actual compute time.

The flow code is shipped *inside* this worker's image, so flows are loaded
with ``ignore_storage=True`` — updated scrapers are deployed by rebuilding
and redeploying the worker.

Concurrency isolation is selectable via the ``RUN_POOL`` env var (see
:func:`resolve_run_pool`): ``runloop`` (default — every scrape shares this
worker's event loop), ``thread`` (each scrape on its own thread + event loop),
or ``process`` (each scrape in its own subprocess). Isolation trades a little
overhead for insulating scrapes from one another's loop pressure — the OTel
signals in ``EN_BANC_OTEL.md`` tell you when that trade is worth making.

Usage
-----
Create a work pool (once)::

    prefect work-pool create scraper-pool --type in-process

Start the worker::

    python -m workers.in_process
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import sys
from typing import Optional

import anyio.abc
from prefect.client.schemas.objects import FlowRun
from prefect.exceptions import Abort, Pause
from prefect.flow_engine import run_flow_async
from prefect.flows import load_flow_from_flow_run
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseWorker,
    BaseWorkerResult,
)

from flows.shutdown import get_shutdown_event, request_shutdown, shutdown_requested
from workers.telemetry import (
    init_telemetry,
    start_loop_monitor,
    stop_loop_monitor,
)

logger = logging.getLogger(__name__)

# Which work pool this worker serves. Browser scrapers run on a separate pool
# (its worker has a browser engine installed and runs one scrape at a time)
# from the lean HTTP pool, so the pool is configurable per worker container.
DEFAULT_WORK_POOL_NAME = "scraper-pool"

# Max JKent runs this worker executes concurrently. Once reached, the worker
# stops submitting until a slot frees — start another worker for more capacity.
# The browser worker overrides this to 1 (one browser scrape at a time).
DEFAULT_CONCURRENCY = 4

# How long, on shutdown, to let active scrapes drain (finish their in-flight
# request and finalize the DB for resume) before they're force-cancelled. Keep
# this below the container's stop grace period so Docker doesn't SIGKILL first.
DEFAULT_SHUTDOWN_GRACE_SECONDS = 110.0

# How scrapes are dispatched for concurrency isolation (RUN_POOL env var).
DEFAULT_RUN_POOL = "runloop"
VALID_RUN_POOLS = ("runloop", "thread", "process")


def resolve_run_pool() -> str:
    """Return the validated ``RUN_POOL`` dispatch mode.

    - ``runloop``: run every scrape on this worker's shared event loop (the
      original, lowest-overhead behavior).
    - ``thread``: run each scrape on its own thread with its own event loop, so
      one scrape's loop pressure (sync compression, lxml) doesn't stall others.
    - ``process``: run each scrape in its own subprocess (``workers.run_one``),
      for full memory + loop isolation.

    Raises:
        ValueError: If ``RUN_POOL`` is set to an unrecognized value.
    """
    value = os.environ.get("RUN_POOL", DEFAULT_RUN_POOL).strip().lower()
    if value not in VALID_RUN_POOLS:
        raise ValueError(
            f"RUN_POOL must be one of {VALID_RUN_POOLS}, got {value!r}"
        )
    return value


def _silence_seaweedfs_header_warnings() -> None:
    """Drop urllib3's benign "Failed to parse headers" warnings.

    SeaweedFS's S3 gateway formats zero-body responses (PUT acks, empty
    objects) in a way Python's stricter header parser flags as a
    ``MissingHeaderBodySeparatorDefect``. urllib3 catches the resulting
    ``HeaderParsingError`` and logs it as a WARNING, but the request itself
    succeeds (200 OK) — so the boto3 calls in ``flows.s3_archive`` work
    correctly. We filter only this specific message rather than raising the
    logger level, so genuine connection problems still surface.
    """

    class _HeaderParseFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            return "Failed to parse headers" not in record.getMessage()

    logging.getLogger("urllib3.connection").addFilter(_HeaderParseFilter())


async def _run_flow_here(flow, flow_run: FlowRun) -> int:
    """Run a loaded flow through the async engine on the current event loop.

    Shared by ``runloop`` mode, ``thread`` mode (via a fresh loop), and the
    ``process`` mode child. ``run_flow_async`` feeds the flow through Prefect's
    ``AsyncFlowRunEngine``, which manages Running -> Completed/Failed transitions
    and task tracking against the existing flow run.

    Returns:
        ``0`` if the run completed, ``1`` otherwise.

    Raises:
        Abort, Pause: Orchestrator signals are re-raised for the caller/base
            worker to handle.
    """
    try:
        state = await run_flow_async(
            flow,
            flow_run=flow_run,
            parameters=flow_run.parameters,
            return_type="state",
        )
    except (Abort, Pause):
        # Let the base worker handle orchestrator signals.
        raise
    except Exception:
        logger.exception("Unexpected engine error in flow run %s", flow_run.id)
        return 1
    return 0 if state.is_completed() else 1


def _run_flow_in_new_loop(flow, flow_run: FlowRun) -> int:
    """Run a flow on a brand-new event loop (``thread`` mode helper).

    Invoked via ``asyncio.to_thread``, so it executes on a worker thread; the
    fresh ``asyncio.run`` loop is that thread's own. jkent's per-run asyncio
    primitives (DB lock) bind to this loop, and the loop-aware shutdown signal
    (:mod:`flows.shutdown`) reaches it. ``Abort``/``Pause`` propagate back out
    through ``to_thread``.

    The loop-lag monitor is started *here*, on this per-scrape loop, so
    ``jkent.event_loop.lag`` measures the loop the scrape actually runs on rather
    than the worker's (near-idle) submission loop.
    """

    async def _with_monitor() -> int:
        monitor = start_loop_monitor()
        try:
            return await _run_flow_here(flow, flow_run)
        finally:
            await stop_loop_monitor(monitor)

    return asyncio.run(_with_monitor())


async def _run_flow_in_subprocess(flow_run: FlowRun) -> int:
    """Run a flow in its own subprocess (``process`` mode).

    Launches ``python -m workers.run_one <flow_run_id>``, which re-loads the flow
    run, initializes its own OTel providers, and drains cooperatively on SIGTERM.
    We race the subprocess against the shutdown signal: on shutdown we SIGTERM the
    child (it finalizes the run as interrupted and preserves the DB for resume)
    and wait it out. If this task is itself cancelled (worker force-cancel past
    the grace period), the child is killed so it doesn't leak.

    Returns:
        ``0`` if the subprocess exited cleanly, ``1`` otherwise.
    """
    identifier = str(flow_run.id)
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "workers.run_one", identifier,
        env=os.environ.copy(),
    )
    logger.info("Dispatched run %s to subprocess pid=%s", identifier, proc.pid)

    shutdown = get_shutdown_event()
    wait_proc = asyncio.ensure_future(proc.wait())
    drain_signal = asyncio.ensure_future(shutdown.wait())
    try:
        await asyncio.wait(
            {wait_proc, drain_signal}, return_when=asyncio.FIRST_COMPLETED
        )
        if not wait_proc.done():
            # Shutdown won the race: ask the child to drain, then wait it out
            # (bounded by the worker's overall grace period in main()).
            logger.warning(
                "Shutdown requested; signalling subprocess pid=%s (run %s) to drain",
                proc.pid, identifier,
            )
            with contextlib.suppress(ProcessLookupError):
                proc.send_signal(signal.SIGTERM)
            await wait_proc
        return 0 if proc.returncode == 0 else 1
    finally:
        drain_signal.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await drain_signal
        # Force-cancel (or an error) left the child alive — don't leak it.
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            with contextlib.suppress(Exception):
                await proc.wait()


class InProcessWorkerResult(BaseWorkerResult):
    """Result of an in-process flow run."""


class InProcessWorker(BaseWorker):
    """Execute scraper flows in the current async event loop.

    Instead of launching a container or subprocess per flow run, this worker
    ``await``\\s the flow function directly. Multiple concurrent flow runs
    share the same event loop, which is efficient when flows are dominated by
    network I/O (HTTP requests, S3 uploads, etc.).

    The worker uses Prefect's ``AsyncFlowRunEngine`` under the hood, so all
    state transitions, task tracking, and logging behave as they would in a
    normal deployment.

    ``RUN_POOL`` selects whether each scrape shares this loop (``runloop``),
    gets its own thread (``thread``), or its own subprocess (``process``).
    """

    type = "in-process"
    job_configuration = BaseJobConfiguration

    _description = "Execute scraper flows in the current async event loop."
    _display_name = "In-Process"

    async def get_and_submit_flow_runs(self):  # type: ignore[override]
        """Stop accepting new flow runs once shutdown has been requested.

        Existing in-flight runs keep going (and drain themselves via the shared
        shutdown event); this just prevents the worker from picking up *new*
        work while we're winding down.
        """
        if shutdown_requested():
            return []
        return await super().get_and_submit_flow_runs()

    async def run(
        self,
        flow_run: FlowRun,
        configuration: BaseJobConfiguration,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ) -> InProcessWorkerResult:
        identifier = str(flow_run.id)

        if task_status is not None:
            task_status.started(identifier)

        run_pool = resolve_run_pool()

        # process mode: the subprocess loads the flow itself (ignore_storage), so
        # we don't touch the flow object here.
        if run_pool == "process":
            status_code = await _run_flow_in_subprocess(flow_run)
            return InProcessWorkerResult(
                status_code=status_code, identifier=identifier
            )

        # Load the flow from the deployment's entrypoint. ignore_storage=True
        # because the code is already local — scrapers ship in this image.
        try:
            flow = await load_flow_from_flow_run(
                flow_run, ignore_storage=True
            )
        except Exception:
            logger.exception("Failed to load flow for run %s", flow_run.id)
            return InProcessWorkerResult(
                status_code=1, identifier=identifier
            )

        # Run the flow linked to the *existing* flow run the server created.
        if run_pool == "thread":
            # Own thread + own event loop; Abort/Pause propagate out of to_thread.
            status_code = await asyncio.to_thread(
                _run_flow_in_new_loop, flow, flow_run
            )
        else:  # runloop
            status_code = await _run_flow_here(flow, flow_run)

        return InProcessWorkerResult(
            status_code=status_code, identifier=identifier
        )


if __name__ == "__main__":

    def _active_runs(worker: InProcessWorker) -> int:
        """Number of in-flight flow runs (borrowed concurrency slots)."""
        limiter = getattr(worker, "_limiter", None)
        return int(limiter.borrowed_tokens) if limiter is not None else 0

    async def main() -> None:
        # Initialize OpenTelemetry before anything does real work, so jkent's
        # spans/metrics have live providers to record against. No-op (returns
        # None) when telemetry is disabled; flush() drains the last batch on exit.
        flush = init_telemetry()
        # runloop mode runs every scrape on this loop, so the loop-lag monitor
        # started here measures the right loop. thread/process modes run scrapes
        # on other loops (a per-scrape thread loop or a subprocess), and start
        # their own monitor there — starting one here would only sample this
        # worker's near-idle submission loop and dilute the metric.
        monitor = None
        try:
            _silence_seaweedfs_header_warnings()
            pool_name = os.environ.get("WORKER_POOL_NAME", DEFAULT_WORK_POOL_NAME)
            concurrency = int(
                os.environ.get("WORKER_CONCURRENCY", DEFAULT_CONCURRENCY)
            )
            grace = float(
                os.environ.get(
                    "WORKER_SHUTDOWN_GRACE_SECONDS", DEFAULT_SHUTDOWN_GRACE_SECONDS
                )
            )
            run_pool = resolve_run_pool()  # fail fast on a bad RUN_POOL value
            if run_pool == "runloop":
                monitor = start_loop_monitor()
            logger.info(
                "Starting worker on pool=%s with concurrency limit=%d, run_pool=%s",
                pool_name, concurrency, run_pool,
            )
            worker = InProcessWorker(
                work_pool_name=pool_name, limit=concurrency
            )

            loop = asyncio.get_running_loop()
            worker_task = asyncio.ensure_future(worker.start())

            def _on_signal(signame: str) -> None:
                if shutdown_requested():
                    return  # second signal — already draining
                logger.warning(
                    "Received %s; halting intake and draining active runs...", signame
                )
                request_shutdown()

            for signame in ("SIGTERM", "SIGINT"):
                loop.add_signal_handler(getattr(signal, signame), _on_signal, signame)

            # Wait until either shutdown is requested or the worker exits on its own.
            shutdown_wait = asyncio.ensure_future(get_shutdown_event().wait())
            await asyncio.wait(
                {worker_task, shutdown_wait}, return_when=asyncio.FIRST_COMPLETED
            )

            if worker_task.done():
                # Worker stopped on its own (e.g. an error) — surface it.
                shutdown_wait.cancel()
                await worker_task
                return

            # Shutdown requested. Intake is already halted (get_and_submit_flow_runs
            # short-circuits), so just wait — bounded — for active scrapes to drain
            # themselves before tearing the worker down.
            waited = 0.0
            while _active_runs(worker) > 0 and waited < grace:
                logger.info(
                    "Draining: %d run(s) still active (%.0fs/%.0fs)...",
                    _active_runs(worker), waited, grace,
                )
                await asyncio.sleep(1.0)
                waited += 1.0

            remaining = _active_runs(worker)
            if remaining:
                logger.warning(
                    "Grace period elapsed; force-cancelling %d active run(s).", remaining
                )
            else:
                logger.info("All runs drained; shutting down cleanly.")

            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
        finally:
            await stop_loop_monitor(monitor)
            if flush is not None:
                flush()

    asyncio.run(main())
