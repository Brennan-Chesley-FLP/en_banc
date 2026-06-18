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

logger = logging.getLogger(__name__)

WORK_POOL_NAME = "scraper-pool"

# Max JKent runs this worker executes concurrently. Once reached, the worker
# stops submitting until a slot frees — start another worker for more capacity.
DEFAULT_CONCURRENCY = 4


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

    async def main() -> None:
        concurrency = int(
            os.environ.get("WORKER_CONCURRENCY", DEFAULT_CONCURRENCY)
        )
        logger.info("Starting worker with concurrency limit=%d", concurrency)
        worker = InProcessWorker(
            work_pool_name=WORK_POOL_NAME, limit=concurrency
        )
        await worker.start()

    asyncio.run(main())
