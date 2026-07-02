"""OpenTelemetry SDK setup for the scraper workers (the en_banc side of jkent).

jkent is instrumented *API-only*: it calls ``opentelemetry.trace.get_tracer()`` /
``opentelemetry.metrics.get_meter()`` against the global providers and never
configures an SDK. Everything that turns those no-ops into real spans and metrics
lives here — see ``EN_BANC_OTEL.md`` for the full contract.

Three entry points, all safe to call whether or not OTel is configured:

- :func:`init_telemetry` — build and install the SDK providers + instrumentors at
  worker (or subprocess) startup; returns a ``flush`` callable for shutdown, or
  ``None`` when telemetry is disabled.
- :func:`start_loop_monitor` / :func:`stop_loop_monitor` — start (and later stop)
  jkent's event-loop lag sampler on the loop that runs scrapes. jkent ships the
  monitor but never starts it; the host must, once per scrape-running loop.
- :func:`instrument_run_engine` — bind the SQLAlchemy instrumentor to a jkent run's
  per-run engine right after the run opens.
- :func:`run_baggage` — attach the ``flow_run_id`` / ``scraper.name`` baggage jkent
  reads to correlate its spans and metrics with the Prefect flow run.

Telemetry is a no-op (init returns ``None``, helpers do nothing) when
``OTEL_SDK_DISABLED`` is truthy or no ``OTEL_EXPORTER_OTLP_ENDPOINT`` is set, so
local dev and tests stay quiet without a collector.
"""

from __future__ import annotations

import logging
import os
import socket
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Optional

logger = logging.getLogger(__name__)

_TRUTHY = {"1", "true", "yes", "on"}


def telemetry_enabled() -> bool:
    """Whether the OTel SDK should be configured.

    Off when the standard ``OTEL_SDK_DISABLED`` kill-switch is truthy, or when no
    OTLP endpoint is configured (nothing to export to — so we skip setup entirely
    rather than spam connection errors at a nonexistent collector).
    """
    if os.environ.get("OTEL_SDK_DISABLED", "").strip().lower() in _TRUTHY:
        return False
    return bool(os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip())


def init_telemetry() -> Optional[Callable[[], None]]:
    """Configure the global OTel providers + instrumentors (contract §2).

    Sets a head-sampled ``TracerProvider`` and a periodic-exporting
    ``MeterProvider`` on the global OTel API, then turns on the httpx and botocore
    auto-instrumentors (SQLAlchemy is bound per-run in :func:`instrument_run_engine`).
    Idempotent-friendly: callers invoke it once at process startup.

    Returns:
        A ``flush()`` callable that shuts the providers down (flushing the last
        batch of spans/metrics) — call it in the teardown path. Returns ``None``
        when telemetry is disabled, so ``flush and flush()`` is safe.
    """
    if not telemetry_enabled():
        logger.info(
            "OpenTelemetry disabled (OTEL_SDK_DISABLED set or no "
            "OTEL_EXPORTER_OTLP_ENDPOINT); jkent spans/metrics are no-ops."
        )
        return None

    from opentelemetry import metrics, trace
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
        OTLPMetricExporter,
    )
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter,
    )
    from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

    resource = Resource.create(
        {
            "service.name": os.environ.get(
                "OTEL_SERVICE_NAME", "en-banc-worker"
            ),
            # Distinguishes the HTTP pool (concurrency 4) from the browser pool
            # (concurrency 1) — the two provisioning regimes. WORKER_POOL_NAME is
            # what this worker actually reads; fall back to the standard var.
            "worker.pool": os.environ.get("WORKER_POOL_NAME")
            or os.environ.get("PREFECT_WORK_POOL", "unknown"),
            "worker.concurrency": os.environ.get("WORKER_CONCURRENCY", ""),
            # Per-run continuation-worker cap (MAX_CONTINUATION_WORKERS). On the
            # resource so lag/throughput metrics can be grouped by the setting
            # when comparing runs dialed to different values.
            "worker.max_continuation_workers": os.environ.get(
                "MAX_CONTINUATION_WORKERS", ""
            ),
            "worker.run_pool": os.environ.get("RUN_POOL", "runloop"),
            "worker.host": socket.gethostname(),
        }
    )

    # Traces: head-sample low — per-request+phase spans are high volume at scrape
    # scale. Metrics are always aggregated, so drilling is the only thing sampling
    # costs.
    #
    # ``set_tracer_provider`` is one-shot: if something (Prefect telemetry, an
    # import side effect, a prior init) already installed an SDK provider, a second
    # ``set`` is silently refused and our exporter/sampler never attach — jkent's
    # spans then resolve to that other provider and never reach our collector, even
    # though metrics (a separate, uncontested MeterProvider) keep flowing. So attach
    # our exporter to the existing SDK provider instead of losing the race.
    span_processor = BatchSpanProcessor(OTLPSpanExporter())
    existing_tp = trace.get_tracer_provider()
    if isinstance(existing_tp, TracerProvider):
        existing_tp.add_span_processor(span_processor)
        tp = existing_tp
        owns_tp = False
        logger.warning(
            "A TracerProvider was already installed (%r); attached our OTLP span "
            "exporter to it rather than overriding (our sampler is NOT applied).",
            existing_tp,
        )
    else:
        tp = TracerProvider(
            resource=resource,
            sampler=ParentBased(TraceIdRatioBased(0.05)),
        )
        tp.add_span_processor(span_processor)
        trace.set_tracer_provider(tp)
        owns_tp = True
    logger.info("Tracer provider in effect: %r", trace.get_tracer_provider())

    mp = MeterProvider(
        resource=resource,
        metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter())],
    )
    metrics.set_meter_provider(mp)

    HTTPXClientInstrumentor().instrument()
    BotocoreInstrumentor().instrument()

    # jkent resolves its tracer/meter through ``@lru_cache``d accessors. If either
    # was called before the providers above were installed (an import-time probe,
    # an early request), the cache pins the no-op proxy for the process's life.
    # Clear both so the first real resolution binds to the providers set here.
    try:
        from jkent.observability import metrics as _jkent_metrics
        from jkent.observability import tracing as _jkent_tracing

        _jkent_tracing.tracer.cache_clear()
        _jkent_metrics.instruments.cache_clear()
    except Exception:  # noqa: BLE001 - telemetry must never break the scrape
        logger.exception("Could not clear jkent observability caches; continuing.")

    logger.info(
        "OpenTelemetry initialized: exporting to %s (service=%s pool=%s)",
        os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"),
        resource.attributes.get("service.name"),
        resource.attributes.get("worker.pool"),
    )

    def flush() -> None:
        # Flush the last batch of spans/metrics before the process exits. Always
        # drain the span processor we added; only shut the whole TracerProvider
        # down when we created it — if we merely attached to a provider someone
        # else owns, shutting it down would tear out their tracing too.
        try:
            if owns_tp:
                tp.shutdown()
            else:
                span_processor.force_flush()
        except Exception:  # noqa: BLE001 - shutdown must never mask real errors
            logger.exception("Error draining TracerProvider")
        try:
            mp.shutdown()
        except Exception:  # noqa: BLE001
            logger.exception("Error shutting down MeterProvider")

    return flush


def start_loop_monitor() -> Optional[Any]:
    """Start jkent's event-loop lag sampler on the *current* loop (contract §2).

    jkent ships :class:`~jkent.observability.LoopLagMonitor` but never starts it —
    it is off unless the host process explicitly starts it, because only the host
    knows which loop actually runs scrapes and should be measured (one monitor per
    scrape-running loop). Without this call, ``jkent.event_loop.lag`` — the
    keystone provisioning signal — is never emitted. Must be called from within a
    running event loop.

    Gated on telemetry actually being live (:func:`telemetry_enabled`), not just
    jkent's ``JKENT_OTEL_LOOP_MONITOR`` flag: with no SDK configured the lag
    instrument is a no-op, so there is no point spinning a sampling task to record
    into it.

    Returns:
        The started ``LoopLagMonitor``, or ``None`` when telemetry is disabled —
        so ``monitor and await stop_loop_monitor(monitor)`` is safe. (jkent's own
        ``JKENT_OTEL_LOOP_MONITOR=0`` still makes the returned monitor's ``start``
        an internal no-op; :func:`stop_loop_monitor` handles that case too.)
    """
    if not telemetry_enabled():
        return None
    try:
        from jkent.observability import LoopLagMonitor

        monitor = LoopLagMonitor()
        monitor.start()
        return monitor
    except Exception:  # noqa: BLE001 - telemetry must never break the scrape
        logger.exception("Could not start loop-lag monitor; continuing.")
        return None


async def stop_loop_monitor(monitor: Any) -> None:
    """Cancel and await a monitor from :func:`start_loop_monitor` (no-op on ``None``).

    Call in the teardown path of the loop the monitor was started on, before the
    loop stops. Errors are swallowed so shutdown never fails on telemetry.
    """
    if monitor is None:
        return
    try:
        await monitor.stop()
    except Exception:  # noqa: BLE001 - shutdown must never mask real errors
        logger.exception("Error stopping loop-lag monitor")


def instrument_run_engine(run: Any) -> None:
    """Bind the SQLAlchemy instrumentor to a jkent run's per-run engine (§2).

    jkent's run DB is an *async* SQLAlchemy engine over aiosqlite; the instrumentor
    binds to the sync engine underneath. jkent creates the engine per run inside
    ``RunBootstrapper``, so this is called right after the run opens, using the
    public ``SQLManager.engine`` property jkent exposes. No-op when telemetry is
    disabled.

    Args:
        run: The opened jkent ``ScrapeRun`` (``run._db.engine.sync_engine`` is the
            sync engine SQLAlchemy's instrumentor binds to).
    """
    if not telemetry_enabled():
        return
    try:
        from opentelemetry.instrumentation.sqlalchemy import (
            SQLAlchemyInstrumentor,
        )

        sync_engine = run._db.engine.sync_engine
        SQLAlchemyInstrumentor().instrument(engine=sync_engine)
    except Exception:  # noqa: BLE001 - telemetry must never break the scrape
        logger.exception("Could not instrument run DB engine; continuing.")


@contextmanager
def run_baggage(flow_run_id: str, scraper_name: str) -> Iterator[None]:
    """Attach ``flow_run_id`` / ``scraper.name`` baggage for the scrape (§3).

    jkent reads ``baggage.get_baggage("flow_run_id")`` and stamps it on its own
    spans and metrics, which is what lets the backend tell concurrent runs apart by
    flow run. Attach this *before* scheduling ``run.run()`` so the run task captures
    the baggage in its context snapshot. No-op (plain passthrough) when telemetry is
    disabled — jkent still works, the attribute is just absent.

    Args:
        flow_run_id: The Prefect flow run id (as a string).
        scraper_name: The scraper's schema/slug name.
    """
    if not telemetry_enabled():
        yield
        return
    from opentelemetry import baggage, context

    ctx = baggage.set_baggage("flow_run_id", flow_run_id)
    ctx = baggage.set_baggage("scraper.name", scraper_name, context=ctx)
    token = context.attach(ctx)
    try:
        yield
    finally:
        context.detach(token)
