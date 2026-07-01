"""Process-global cooperative-shutdown signal.

Shared by the in-process worker (which sets it on SIGTERM/SIGINT) and the scrape
flow (which watches it to drain JKent runs gracefully for resume).

The source of truth is a :class:`threading.Event` — thread-safe, so a signal
handler on any thread can set it. Because ``asyncio.Event`` is bound to the loop
it was created on (and is *not* safe to set from another loop), each event loop
that wants to *await* shutdown gets its own ``asyncio.Event``, bridged from the
flag via ``loop.call_soon_threadsafe``. This lets the signal reach scrapes no
matter which loop they run on:

- ``RUN_POOL=runloop``: one loop → one bridged event → behaves as it always has.
- ``RUN_POOL=thread``: each run's loop registers its own event; all are set.
- ``RUN_POOL=process``: each subprocess has its own module state, and its own
  SIGTERM handler calls :func:`request_shutdown` locally.
"""

from __future__ import annotations

import asyncio
import threading

# Source of truth: set once, thread-safe, never reset within a process lifetime.
_flag = threading.Event()

# Per-loop asyncio mirrors of the flag, so coroutines on any loop can `.wait()`.
_loop_events: list[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = []
_lock = threading.Lock()


def get_shutdown_event() -> asyncio.Event:
    """Return an :class:`asyncio.Event` for the *current* loop, tracking shutdown.

    Created lazily per running loop and remembered so :func:`request_shutdown` can
    set it. If shutdown was already requested before this loop asked for its event,
    the returned event is pre-set so waiters don't miss the edge.
    """
    loop = asyncio.get_running_loop()
    with _lock:
        for registered_loop, event in _loop_events:
            if registered_loop is loop:
                break
        else:
            event = asyncio.Event()
            _loop_events.append((loop, event))
        already_requested = _flag.is_set()
    if already_requested:
        event.set()
    return event


def request_shutdown() -> None:
    """Signal cooperative shutdown to everything watching, on any loop."""
    _flag.set()
    with _lock:
        pairs = list(_loop_events)
    for loop, event in pairs:
        try:
            loop.call_soon_threadsafe(event.set)
        except RuntimeError:
            # Loop already closed — its waiters are gone, nothing to wake.
            pass


def shutdown_requested() -> bool:
    """Whether cooperative shutdown has been requested."""
    return _flag.is_set()
