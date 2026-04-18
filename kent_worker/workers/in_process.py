"""In-process async worker for IO-dominated scraper flows.

Runs flow functions directly in the current event loop instead of
spawning containers or subprocesses.  Ideal for IO-bound workloads
like web scraping where the overhead of container startup dominates
actual compute time.

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
from pathlib import Path
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

from flows.litestream import (
    LitestreamReplicator,
    build_litestream_config,
    check_replica_exists,
    restore_from_replica,
    write_config,
)

logger = logging.getLogger(__name__)


class InProcessWorkerResult(BaseWorkerResult):
    """Result of an in-process flow run."""


class InProcessWorker(BaseWorker):
    """Execute scraper flows in the current async event loop.

    Instead of launching a container or subprocess per flow run, this
    worker ``await``\\s the flow function directly.  Multiple concurrent
    flow runs share the same event loop, which is efficient when flows
    are dominated by network I/O (HTTP requests, S3 uploads, etc.).

    The worker uses Prefect's ``AsyncFlowRunEngine`` under the hood, so
    all state transitions, task tracking, and logging work exactly as
    they would in a normal deployment.
    """

    type = "in-process"
    job_configuration = BaseJobConfiguration

    _description = "Execute scraper flows in the current async event loop."
    _display_name = "In-Process"

    def _setup_litestream(
        self, flow_run: FlowRun
    ) -> LitestreamReplicator | None:
        """Set up litestream replication for scraper flows.

        If the flow run has a ``scraper_schema`` parameter, compute the
        DB path and S3 replica path, restore from an existing replica if
        one exists, then start continuous replication.

        Returns the running replicator (caller must stop it), or None if
        this flow run is not a scraper flow.
        """
        params = flow_run.parameters or {}
        scraper_schema = params.get("scraper_schema")
        if not scraper_schema:
            return None

        run_name = flow_run.name or "unnamed"
        runs_dir = Path(
            os.environ.get("SCRAPER_RUNS_DIR", "/tmp/scraper_runs")
        )
        runs_dir.mkdir(parents=True, exist_ok=True)
        db_path = runs_dir / f"{run_name}.db"

        s3_bucket = os.environ.get("LITESTREAM_S3_BUCKET", "scrapers")
        s3_replica_path = (
            f"scraper_runs/{scraper_schema}/{run_name}/replica/"
        )

        # Check for existing replica and restore (resume support)
        if check_replica_exists(s3_bucket, s3_replica_path):
            logger.info("Found existing S3 replica, restoring for resume")
            restored = restore_from_replica(
                db_path, s3_bucket, s3_replica_path
            )
            if restored:
                logger.info("Restored database from S3 replica")
            else:
                logger.warning("Restore failed, starting fresh")

        # Build and write litestream config
        config = build_litestream_config(
            db_path=db_path,
            s3_bucket=s3_bucket,
            s3_path=s3_replica_path,
        )
        config_file = runs_dir / f"{run_name}.litestream.yml"
        write_config(config, config_file)

        # Start replication
        replicator = LitestreamReplicator(config_file)
        replicator.start()
        return replicator

    async def run(
        self,
        flow_run: FlowRun,
        configuration: BaseJobConfiguration,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ) -> InProcessWorkerResult:
        identifier = str(flow_run.id)

        if task_status is not None:
            task_status.started(identifier)

        # Load the flow object from the deployment's entrypoint.
        # ignore_storage=True because the code is already local — we
        # ship updated scrapers by redeploying the worker itself.
        try:
            flow = await load_flow_from_flow_run(
                flow_run, ignore_storage=True
            )
        except Exception:
            logger.exception(
                "Failed to load flow for run %s", flow_run.id
            )
            return InProcessWorkerResult(
                status_code=1, identifier=identifier
            )

        # Start litestream replication for scraper flows (wraps the
        # entire flow so the DB is continuously backed up to S3).
        replicator = self._setup_litestream(flow_run)

        # Run the flow linked to the *existing* flow run that the
        # server created.  run_flow_async feeds the flow through the
        # AsyncFlowRunEngine which manages Running → Completed/Failed
        # state transitions and task tracking.
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
        finally:
            if replicator is not None:
                replicator.stop()

        status_code = 0 if state.is_completed() else 1
        return InProcessWorkerResult(
            status_code=status_code, identifier=identifier
        )


if __name__ == "__main__":
    import asyncio

    async def main() -> None:
        limit_env = os.environ.get("WORKER_CONCURRENCY_LIMIT")
        limit = int(limit_env) if limit_env else None
        worker = InProcessWorker(
            work_pool_name="scraper-pool",
            limit=limit,
        )
        await worker.start()

    asyncio.run(main())
