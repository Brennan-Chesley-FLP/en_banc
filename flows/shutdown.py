"""Process-global cooperative-shutdown signal.

Shared by the in-process worker (which sets it on SIGTERM/SIGINT) and the
scrape flow (which watches it to drain JKent runs gracefully for resume). A
single, lazily-created :class:`asyncio.Event` is sufficient because the
in-process worker runs the worker loop *and* every flow run in one process on
one event loop, so all of them observe the same event instance.
"""

from __future__ import annotations

import asyncio

_shutdown_event: asyncio.Event | None = None


def get_shutdown_event() -> asyncio.Event:
    """Return the process-wide shutdown event, creating it on first use."""
    global _shutdown_event
    if _shutdown_event is None:
        _shutdown_event = asyncio.Event()
    return _shutdown_event


def request_shutdown() -> None:
    """Signal cooperative shutdown to everything watching the event."""
    get_shutdown_event().set()


def shutdown_requested() -> bool:
    """Whether cooperative shutdown has been requested."""
    return _shutdown_event is not None and _shutdown_event.is_set()
