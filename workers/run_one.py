"""Process-mode child entrypoint: run a single flow run in its own subprocess.

Launched by :func:`workers.in_process._run_flow_in_subprocess` as
``python -m workers.run_one <flow_run_id>`` when ``RUN_POOL=process``. It mirrors
``InProcessWorker.run()`` standalone: initialize this process's own OTel providers,
install a SIGTERM/SIGINT handler that triggers the same cooperative drain as the
in-loop worker, re-load the flow run and its flow, and execute it.

Running each scrape in its own process gives full memory + event-loop isolation —
the heaviest of the ``RUN_POOL`` regimes, for when a shared loop is the ceiling
(see the provisioning decision tree in ``EN_BANC_OTEL.md``).
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from uuid import UUID

from prefect.flow_engine import load_flow_run
from prefect.flows import load_flow_from_flow_run

from flows.shutdown import request_shutdown
from workers.in_process import _run_flow_here
from workers.telemetry import (
    init_telemetry,
    start_loop_monitor,
    stop_loop_monitor,
)

logger = logging.getLogger(__name__)


async def _amain(flow_run_id: str) -> int:
    # Own process -> own providers/exporters; flush the last batch before exit.
    flush = init_telemetry()
    # This subprocess's loop is the scrape loop, so start the loop-lag monitor
    # here to get event_loop.lag per subprocess (process-mode isolation).
    monitor = start_loop_monitor()
    try:
        loop = asyncio.get_running_loop()
        # The parent SIGTERMs us to request a graceful drain; SIGINT likewise for
        # interactive use. Both route to the loop-aware shutdown signal that the
        # scrape flow watches, so run.stop() finalizes the run as interrupted and
        # leaves the DB resumable — identical to runloop-mode drain.
        for signame in ("SIGTERM", "SIGINT"):
            loop.add_signal_handler(getattr(signal, signame), request_shutdown)

        flow_run = load_flow_run(flow_run_id=UUID(flow_run_id))
        try:
            flow = await load_flow_from_flow_run(flow_run, ignore_storage=True)
        except Exception:
            logger.exception("Failed to load flow for run %s", flow_run_id)
            return 1

        try:
            return await _run_flow_here(flow, flow_run)
        except BaseException:
            # Orchestrator signals (Abort/Pause) and anything else: the child
            # can't re-raise into the parent, so map to a nonzero exit. The flow
            # engine has already recorded the run state via the API.
            logger.exception("Flow run %s ended abnormally", flow_run_id)
            return 1
    finally:
        await stop_loop_monitor(monitor)
        if flush is not None:
            flush()


def main() -> None:
    if len(sys.argv) != 2:
        print("usage: python -m workers.run_one <flow_run_id>", file=sys.stderr)
        raise SystemExit(2)
    raise SystemExit(asyncio.run(_amain(sys.argv[1])))


if __name__ == "__main__":
    main()
