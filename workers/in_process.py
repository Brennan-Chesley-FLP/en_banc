"""In-process async worker for IO-dominated scraper flows.

Runs flow functions directly in the current event loop instead of spawning
a container or subprocess per flow run. Ideal for IO-bound workloads like
web scraping, where container startup would dominate actual compute time.

The flow code is shipped *inside* this worker's image, so flows are loaded
with ``ignore_storage=True`` — updated scrapers are deployed by rebuilding
and redeploying the worker.

Usage
-----
Create a work pool (once)::

    prefect work-pool create scraper-pool --type in-process

Start the worker::

    python -m workers.in_process
"""

from __future__ import annotations

import logging
import os
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
        # run_flow_async feeds it through the AsyncFlowRunEngine, which manages
        # Running -> Completed/Failed transitions and task tracking.
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
            logger.exception(
                "Unexpected engine error in flow run %s", flow_run.id
            )
            return InProcessWorkerResult(
                status_code=1, identifier=identifier
            )

        status_code = 0 if state.is_completed() else 1
        return InProcessWorkerResult(
            status_code=status_code, identifier=identifier
        )


if __name__ == "__main__":
    import asyncio
    import contextlib
    import signal

    def _active_runs(worker: InProcessWorker) -> int:
        """Number of in-flight flow runs (borrowed concurrency slots)."""
        limiter = getattr(worker, "_limiter", None)
        return int(limiter.borrowed_tokens) if limiter is not None else 0

    async def main() -> None:
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
        logger.info(
            "Starting worker on pool=%s with concurrency limit=%d",
            pool_name, concurrency,
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

    asyncio.run(main())
