# Prefect CourtListener Sync Worker

How the warehouse exports data to CourtListener using a Django management command that runs a Prefect `InProcessWorker`.

## Design

The export step does not use CL's REST API, a separate Celery broker, or SQS. Instead, a Django management command on the CL side starts a Prefect worker that polls a dedicated work pool (`sync-pool`). When the en_banc pipeline finishes transforms, it creates a flow run on the `sync-warehouse` deployment. The CL-side worker picks it up and executes the sync flow in-process — meaning the flow has full access to the Django ORM and can also read from the warehouse DB directly.

All sync work is visible in the Prefect UI as normal flow and task runs.

```
en_banc Prefect flow                      CL infrastructure
────────────────────                      ──────────────────
transforms complete
  │                                       manage.py sync_worker
  ├─ create flow run for                    │
  │  "sync-warehouse" deployment ──────►  InProcessWorker(work_pool_name="sync-pool")
  │  on "sync-pool" work queue              │
  │                                         ├─ picks up flow run
  │                                         ├─ runs sync_warehouse flow IN-PROCESS
  │                                         │    (Django ORM available because
  │                                         │     it's inside a Django process)
  │                                         │
  │                                         ├─ task: sync dockets (chunked)
  │                                         ├─ task: sync opinions (chunked)
  │                                         ├─ task: write back courtlistener_id
  │                                         └─ ...all visible in Prefect UI
  │
  ├─ Prefect UI shows the sync flow
  │  with per-task status, timing, logs
```

## Why this approach

### Full Django ORM access

The sync flow calls `Docket.objects.update_or_create(...)` directly. Django validators run, signals fire (Elasticsearch indexing, alerts, webhooks), and custom save logic executes. No API serialization layer to maintain, no field-mapping drift between repos.

### Trivial confirmation

The management command has connections to both the Django DB and the warehouse DB. After `docket.save()` returns, the flow immediately writes `courtlistener_id` and `courtlistener_version` back to the warehouse. No distributed coordination, no result backends, no polling.

### Full Prefect visibility

Every sync operation shows up in the Prefect UI — same server, same dashboard as the scraper and transform flows. You can see which tables synced, how long each chunk took, which ones failed, retry history. One unified view from scraper run to CL save.

### No new infrastructure

Reuses the `InProcessWorker` pattern already built for scraper flows in `workers/in_process.py`. The only new resources are a Prefect work pool (`sync-pool`) and a deployment (`sync-warehouse`). No SQS queues, no Celery result backends, no response channels.

### Native Prefect orchestration

Triggering is a standard Prefect `run_deployment()` call. Prefect handles queuing, concurrency limits, retry policies, and automations (e.g., "alert if sync fails 3 times"). The en_banc flow does not block waiting for the sync to complete — it fires and moves on, or optionally awaits the flow run.

## Components

### Django management command (CL repo)

A thin wrapper that boots a Prefect worker inside a Django process:

```python
# cl/management/commands/sync_worker.py
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Start a Prefect worker that syncs warehouse data into CL via Django ORM"

    def handle(self, **options):
        import asyncio

        from workers.in_process import InProcessWorker

        async def main():
            worker = InProcessWorker(work_pool_name="sync-pool")
            await worker.start()

        asyncio.run(main())
```

The `InProcessWorker` with `ignore_storage=True` loads flows from the local codebase — same deployment pattern as the scraper worker.

### Sync flow (runs inside the Django process)

```python
from prefect import flow, task, get_run_logger
from django.db import connections


@task(task_run_name="sync-{table_name}-chunk-{chunk_num}")
def sync_chunk(table_name: str, chunk_num: int, provenance_id: int, chunk_size: int = 500):
    """Read a chunk from the warehouse and save via Django ORM.

    Runs inside a Django process — ORM is available.
    """
    logger = get_run_logger()
    offset = chunk_num * chunk_size

    with connections["warehouse"].cursor() as cur:
        cur.execute(
            f"SELECT * FROM courtlistener.{table_name} "
            "WHERE courtlistener_version < warehouse_version "
            "AND provenance_id = %s "
            "ORDER BY record_id "
            "LIMIT %s OFFSET %s",
            [provenance_id, chunk_size, offset],
        )
        columns = [col.name for col in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    if not rows:
        return 0

    synced = 0
    for row in rows:
        cl_id = save_to_cl(table_name, row)  # Django ORM update_or_create
        write_back_cl_id(table_name, row, cl_id)
        synced += 1

    logger.info("Synced %d rows for %s chunk %d", synced, table_name, chunk_num)
    return synced


@flow(name="sync-warehouse")
def sync_warehouse(provenance_id: int, tables: list[str] | None = None):
    """Sync pending warehouse rows to CourtListener.

    Triggered by the en_banc pipeline after transforms complete.
    """
    logger = get_run_logger()

    if tables is None:
        tables = ["dockets", "opinion_clusters", "opinions", "docket_entries", "audio"]

    for table_name in tables:
        row_count = get_pending_count(table_name, provenance_id)
        if row_count == 0:
            logger.info("No pending rows for %s", table_name)
            continue

        chunk_size = 500
        num_chunks = (row_count + chunk_size - 1) // chunk_size
        logger.info("%s: %d rows in %d chunks", table_name, row_count, num_chunks)

        for chunk_num in range(num_chunks):
            sync_chunk(table_name, chunk_num, provenance_id, chunk_size)
```

### Trigger from en_banc (after transforms)

```python
from prefect.deployments import run_deployment

@flow
def scraper_run_flow(scraper_id, params):
    artifact = run_scraper(scraper_id, params)
    validate_run(artifact.s3_path)
    provenance_id = create_provenance(scraper_id, artifact)
    load_to_warehouse(artifact.s3_path, provenance_id)
    sqlmesh_transforms(project_path="sql_processing")

    # Trigger sync on the CL-side worker — does not block
    run_deployment(
        name="sync-warehouse/sync-pool",
        parameters={"provenance_id": provenance_id},
    )
```

## Memory-conscious design

The sync flow never loads more than one chunk of rows into memory at a time.

- **Chunked reads**: Each `sync_chunk` task reads `chunk_size` rows (default 500) via `LIMIT/OFFSET`. The task completes and its locals are freed before the next chunk starts.
- **No accumulation**: Rows are processed and written back one at a time within a chunk. No list of results grows across chunks.
- **Server-side cursors**: For very large syncs, `sync_chunk` can be adapted to use a named cursor (`connections["warehouse"].cursor(name="sync")`) with `itersize` for streaming. For typical scraper runs (hundreds to low thousands of rows), `LIMIT/OFFSET` is simpler and sufficient.
- **Chunk size tuning**: 500 rows is a starting point. Each Prefect task has overhead (state transitions, API calls to the Prefect server), so very small chunks (50 rows) would be dominated by Prefect overhead. Very large chunks (10k rows) risk holding too much in memory. Tune empirically.

## Dual database configuration

The Django process needs a second database connection to the analytics/warehouse DB:

```python
# In CL's Django settings
DATABASES = {
    "default": {
        # CL's main PostgreSQL database
        ...
    },
    "warehouse": {
        "ENGINE": "django.db.backends.postgresql",
        "HOST": "analytics-db-host",
        "PORT": 5433,
        "NAME": "analytics",
        "USER": "analytics",
        "PASSWORD": "...",
    },
}
```

This requires network access from the CL deployment to the analytics DB (VPC peering or security group rules in production).

## Concurrency

The `InProcessWorker` runs flows in the current async event loop. Multiple sync flow runs can execute concurrently if multiple scraper runs finish around the same time.

To avoid overwhelming CL's database with parallel ORM saves, set a concurrency limit on the work pool:

```bash
prefect work-pool create sync-pool --type in-process --concurrency-limit 2
```

This limits to 2 concurrent sync flows. Additional flow runs queue and wait.

## Open questions

- **Where does the sync flow code live?** It needs Django imports, so it likely lives in the CL repo. But its deployment is registered against the shared Prefect server. The `InProcessWorker` with `ignore_storage=True` loads code from the local filesystem, so the CL deployment just needs the sync flow module on disk.
- **Keyset pagination vs OFFSET**: For very large tables, `OFFSET` performance degrades. Keyset pagination (`WHERE record_id > %s ORDER BY record_id LIMIT %s`) is more efficient. Worth switching to if sync batches regularly exceed ~10k rows.
- **Partial failure recovery**: If a sync flow fails mid-way (e.g., chunk 5 of 20), the rows from chunks 1-4 are already synced and have `courtlistener_version = warehouse_version`. Rerunning the flow skips them naturally because the `WHERE courtlistener_version < warehouse_version` filter excludes them. No special recovery logic needed.
