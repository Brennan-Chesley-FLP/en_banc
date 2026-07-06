"""Process-local cooperative-shutdown signal for the scrape flow.

Each flow run executes in its own subprocess (``python -m prefect.engine``,
spawned by Prefect's process worker), with the flow on the process's main
thread and a single event loop. The worker container's entrypoint broadcasts
SIGUSR1 to those subprocesses on shutdown; the flow installs a handler for it
(:func:`install_shutdown_signal_handler`) and watches the event to drain the
scrape gracefully — the in-flight request finishes and jkent finalizes the run
DB as interrupted/resumable.

SIGUSR1 rather than SIGTERM because ``prefect.engine`` reserves SIGTERM for
its own termination bridge (crash-style teardown, or Prefect-driven
cancellation) — overriding it would change UI-cancel semantics.
"""

from __future__ import annotations

import asyncio
import logging
import signal

logger = logging.getLogger(__name__)

# Set once, never reset within a process lifetime. Plain bool + asyncio.Event
# suffices: the handler installed via ``loop.add_signal_handler`` runs *on* the
# flow's loop, and this process only ever runs one scrape on one loop.
_requested = False
_event: asyncio.Event | None = None


def get_shutdown_event() -> asyncio.Event:
    """Return the shutdown event for the current (only) event loop.

    Created lazily; pre-set if shutdown was requested before the first caller
    asked, so waiters can't miss the edge.
    """
    global _event
    if _event is None:
        _event = asyncio.Event()
    if _requested:
        _event.set()
    return _event


def request_shutdown() -> None:
    """Signal cooperative shutdown to everything watching."""
    global _requested
    _requested = True
    if _event is not None:
        _event.set()


def shutdown_requested() -> bool:
    """Whether cooperative shutdown has been requested."""
    return _requested


def install_shutdown_signal_handler() -> None:
    """Route SIGUSR1 to :func:`request_shutdown` on the running loop.

    Called once at flow start. The worker entrypoint sends SIGUSR1 (repeating
    until the engine subprocesses exit) when the container is stopped. Failure
    to install — e.g. the loop isn't on the main thread when the flow is run
    outside ``prefect.engine`` — is logged and tolerated: the flow still works,
    it just can't drain cooperatively.
    """
    try:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGUSR1, request_shutdown)
    except (NotImplementedError, RuntimeError, ValueError) as exc:
        logger.warning(
            "Could not install SIGUSR1 drain handler (%s); "
            "cooperative shutdown unavailable for this run.",
            exc,
        )
