# OpenTelemetry: the en_banc side of jkent instrumentation

jkent is instrumented **API-only**: it calls `opentelemetry.trace.get_tracer()`
and `opentelemetry.metrics.get_meter()` against the *global* providers and never
configures an SDK, exporter, or sampler. If no SDK is installed/configured, every
span and metric is a no-op with negligible cost — which is what keeps jkent safe
to import from juriscraper (no operational deps) and keeps exporter policy out of
the library.

**That means en_banc owns everything below.** Without it, jkent emits nothing.

This file is the contract. Do these six things in en_banc.

---

## 1. Install the SDK + instrumentors

jkent depends only on `opentelemetry-distro` (the API). en_banc adds the SDK,
an exporter, and the auto-instrumentors for the libraries jkent and en_banc use:

```
opentelemetry-sdk
opentelemetry-exporter-otlp                 # or your backend's exporter
opentelemetry-instrumentation-httpx         # jkent's HTTP transport
opentelemetry-instrumentation-sqlalchemy    # jkent's run DB (async engine)
opentelemetry-instrumentation-botocore      # en_banc's S3 uploads (boto3)
opentelemetry-instrumentation-asyncio       # optional: per-task scheduling
```

Do **not** use the `opentelemetry-instrument` CLI auto-wrapper — a long-lived
Prefect worker configures programmatically (below) so the providers are up
before the loop starts and instrumentors can bind to the specific engine.

---

## 2. Initialize the SDK at worker startup

Location: `workers/in_process.py`, top of `main()` (~line 172), **before**
`_silence_seaweedfs_header_warnings()` and before the event loop is doing work.

```python
def _init_telemetry() -> None:
    from opentelemetry import trace, metrics
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter,
    )
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
        OTLPMetricExporter,
    )
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.instrumentation.botocore import BotocoreInstrumentor

    resource = Resource.create(
        {
            "service.name": "en-banc-worker",
            # Distinguishes the HTTP pool (concurrency 4) from the browser
            # pool (concurrency 1) — the two provisioning regimes.
            "worker.pool": os.environ.get("PREFECT_WORK_POOL", "unknown"),
            "worker.concurrency": os.environ.get("WORKER_CONCURRENCY", ""),
            "worker.host": socket.gethostname(),
        }
    )

    # Traces: head-sample low — per-request+phase spans are high volume at
    # scrape scale. Metrics (below) are always aggregated, so drilling is the
    # only thing sampling costs.
    tp = TracerProvider(
        resource=resource,
        sampler=ParentBased(TraceIdRatioBased(0.05)),
    )
    tp.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(tp)

    mp = MeterProvider(
        resource=resource,
        metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter())],
    )
    metrics.set_meter_provider(mp)

    HTTPXClientInstrumentor().instrument()
    BotocoreInstrumentor().instrument()
```

Call `_init_telemetry()` first thing in `main()`. Flush on shutdown
(`tp.shutdown()` / `mp.shutdown()`) in the worker's teardown path so the last
batch of spans/metrics isn't dropped when the container stops.

### Start the loop-lag monitor (don't skip this)

jkent ships `LoopLagMonitor` (the writer of `jkent.event_loop.lag`, the keystone
signal in §5) but **never starts it** — it is off unless the host explicitly
starts it, because only the host knows which loop actually runs scrapes and
should be measured. Installing the SDK is *not* enough; and
`JKENT_OTEL_LOOP_MONITOR` only *permits* the monitor, it does not start it. So
start one monitor **on each loop that runs scrapes**, and stop it in that loop's
teardown:

```python
from workers.telemetry import start_loop_monitor, stop_loop_monitor

monitor = start_loop_monitor()   # gated on telemetry being live; None if disabled
try:
    ...                          # run scrapes on this loop
finally:
    await stop_loop_monitor(monitor)
```

Where "this loop" is depends on `RUN_POOL`:

- `runloop` — scrapes share `main()`'s loop → start it in `main()`.
- `thread` — each scrape gets its own thread + loop → start it inside
  `_run_flow_in_new_loop` (the per-scrape loop), not `main()`.
- `process` — each scrape runs in a `workers.run_one` subprocess → start it in
  that subprocess's `_amain`.

Starting it on the wrong loop (e.g. `main()`'s near-idle submission loop in
`thread`/`process` mode) samples a loop that never runs scrapes and reports a
misleadingly flat lag.

### SQLAlchemy: instrument the engine, not globally

jkent's run DB uses an **async** SQLAlchemy engine over aiosqlite. The
instrumentor binds to the *sync* engine underneath. jkent creates the engine
per run inside `RunBootstrapper`, so instrument it right after the run opens,
in `run_scraper_task` (`flows/scraper_run.py`), using the engine jkent exposes:

```python
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

async with RunBootstrapper(scraper, db_path=db_path, ...) as run:
    SQLAlchemyInstrumentor().instrument(engine=run._db.engine.sync_engine)
    scrape = asyncio.ensure_future(run.run())
```

(`run._db.engine` is the public `SQLManager.engine` property; `.sync_engine`
is the sync engine SQLAlchemy's instrumentor binds to.) DB spans will show the
effect of the single per-run
`asyncio.Lock`: statements serialize even though aiosqlite runs them off-loop
in its own thread. That serialization is the "lock getting fought over"
signal; jkent measures the lock wait/hold directly (see §5).

---

## 3. Set `flow_run_id` baggage before running the scrape  ← the key coupling

We chose **separate traces correlated by attribute**, not parent/child. So
jkent does not need propagated trace context — it needs the flow-run identity
to stamp on its own spans and metrics. Provide it via OTel **baggage**, set in
`run_scraper_task` *before* `asyncio.ensure_future(run.run())` so it's captured
in the run task's context snapshot:

```python
from opentelemetry import baggage, context
from prefect.runtime import flow_run

ctx = baggage.set_baggage("flow_run_id", str(flow_run.id))
ctx = baggage.set_baggage("scraper.name", scraper_name, context=ctx)
token = context.attach(ctx)
try:
    scrape = asyncio.ensure_future(run.run())
    ...
finally:
    context.detach(token)
```

jkent reads `baggage.get_baggage("flow_run_id")` and adds it as a span/metric
attribute. This is what lets you tell 40 concurrent CWs on one loop apart by
flow run in the backend. **If this baggage is not set, jkent still works — the
attribute is just absent and cross-run correlation is lost.**

---

## 4. What jkent emits (so you know what to dashboard)

Metrics (always on once the MeterProvider exists), attributes in parentheses:

| Metric | Type | Attrs | Answers |
|---|---|---|---|
| `jkent.event_loop.lag` | histogram (s) | *(resource only)* | **Is the shared loop blocked?** The keystone signal. |
| `jkent.request.duration` | histogram (s) | `scraper`, `step`, `phase` | Wall time per request phase. `phase` ∈ {`total`, `rate_limiter.gate`, `transport.resolve`, `continuation`}. Rate-limit wait = the `rate_limiter.gate` slice → rate-limit-bound vs throughput-bound. |
| `jkent.request.cpu_time` | histogram (s) | `scraper`, `step`, `phase` | On-loop CPU for synchronous phases (`phase=compress` today) — separates loop-hogging from I/O wait. |
| `jkent.db.lock.wait` | histogram (s) | `scraper`, `step`? | Contention on the per-run DB lock (nonzero only when contended). |
| `jkent.db.lock.hold` | histogram (s) | `scraper`, `step`? | How long the lock is held. |
| `jkent.compression.duration` | histogram (s) | `scraper`, `step`, `kind` | zstd time (currently on-loop). |
| `jkent.compression.ratio` | histogram | `scraper`, `step` | Compressed/original (lower is better). |
| `jkent.compaction.duration` | histogram (s) | `scraper`, `step`, `kind` | The one-shot train / recompress burst at the step threshold. |
| `jkent.worker.active` | gauge | `scraper`, `flow_run_id` | Live CW count per run. |
| `jkent.queue.pending` | gauge | `scraper`, `flow_run_id` | Backlog. |

(`step`? = present on lock metrics only when the lock is taken inside a
request, absent for dequeue/monitor/seed paths. `worker.pool` / `worker.host`
are **resource** attributes set in §2, so they apply to every metric and span
automatically — don't add them per-instrument.)

Spans: one per request (`jkent.request`, carrying `jkent.scraper`,
`jkent.step`, `jkent.flow_run_id`, and `jkent.outcome` — the outcome lives on
the span, not on the metrics) with phase children (`jkent.rate_limiter.gate`,
`jkent.transport.resolve`, `jkent.continuation`). httpx / SQLAlchemy spans nest
under `jkent.transport.resolve` / `jkent.continuation` automatically once their
instrumentors are on.

---

## 5. How to read it — the provisioning decision tree

Each branch is answered by one signal, so a dashboard of the above resolves
"how do I provision this":

- **High `event_loop.lag` p99 + high `compression`/`continuation` `cpu_time`,
  low httpx duration** → the loop is CPU-saturated (sync zstd/lxml). Fix is to
  **offload compression off the loop** (jkent Phase 3) and/or lower
  `WORKER_CONCURRENCY` / add worker processes ("isolate runpools"). Adding CWs
  in the same loop makes it worse.
- **Low lag, high `db.lock.wait`** → the single per-run DB lock is the ceiling.
  More flow-run concurrency or more CWs won't help a per-run lock; the fix is
  in jkent's DB layer.
- **Low lag, low lock wait, high `rate_limiter.gate.wait`** → rate-limit-bound.
  More workers/processes are pointless; you're at the courtesy ceiling.
- **All of the above low, throughput still capped** → genuinely I/O-bound →
  raise `MAX_CONTINUATION_WORKERS` (the per-run continuation-worker cap, wired
  into `RunBootstrapper.max_workers`; default 10) and/or `WORKER_CONCURRENCY`.
  Cheapest win, stays in one loop. Metrics carry the setting as the
  `worker.max_continuation_workers` resource attribute, so group by it to see
  whether dialing it up actually moved throughput/lag.

---

## 6. Env toggles

Keep telemetry disable-able without code changes. Suggested:

- `OTEL_SDK_DISABLED=true` (standard) — makes the whole SDK no-op; jkent's
  API calls stay but cost nothing. Good kill-switch.
- Standard `OTEL_EXPORTER_OTLP_ENDPOINT` etc. for backend routing.

Tuning (not telemetry, but read on the same worker):

- `MAX_CONTINUATION_WORKERS` (default 10) — per-run continuation-worker cap,
  wired into `RunBootstrapper.max_workers`. Both the scraper and browser workers
  read it. Distinct from `WORKER_CONCURRENCY` (how many *runs* a worker executes
  at once). jkent caps it to 1 for `STRICTLY_SERIAL` scrapers regardless.

jkent honors an additional `JKENT_OTEL_LOOP_MONITOR` (default on) that *permits*
the event-loop-lag sampling task — set it to `0` to hard-disable that ~200 ms
timer even where the host tries to start it. Note this flag only gates the
monitor; it does **not** start it. en_banc must explicitly start the monitor on
each scrape-running loop (see §2, "Start the loop-lag monitor") — without that,
`jkent.event_loop.lag` is never emitted no matter what this flag is set to.

---

*jkent side of this: `jkent/observability/` (API-only helpers, metric
instruments, loop-lag monitor) plus spans/metrics threaded through the worker,
compression, DB lock, and rate limiter. jkent emits nothing until en_banc does
§1–§3.*
