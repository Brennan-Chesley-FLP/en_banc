# Documentation Divergences

Where the current implementation differs from what is described in the docs.

## the-plan.rst

### Table naming

Docs say tables are named like `alabama_publicportal_opinions`. Actual
tables are schema-qualified: `ala_publicportal.raw_dockets`,
`ala_publicportal.raw_opinion_clusters`, etc. The abbreviated schema
names (`ala_publicportal`, `conn_jud_ct_gov`) are used throughout.

### Schema registry

Docs describe a registry that auto-creates/migrates tables and links
scrapers to a courts table. The actual implementation is
`warehouse/register.py` + `raw_table_from_model()` factory + Alembic
migrations. There is no courts table.

### Provenance linkage

Docs say every data table has a `provenance_id` FK. The current
implementation uses **M2M observations tables** instead. Data rows are
deduplicated on `content_hash` and have an autoincrement `row_id` PK.
Provenance is tracked via `{table}_observations` junction tables with PK
`(row_id, provenance_id)`.

The Provenance model stores `source_type`, `source_name`, `run_id`,
`s3_artifact_path`, `description`, and a `metadata_` JSONB column. The
docs mention "scraper version", "timestamp range", and "parameters used"
as explicit fields — these are not separate columns (parameters can go
in the JSONB metadata).

### Record identity

Docs describe composite natural keys `(court_id, docket_number,
extra_id)` as the dedup mechanism. The actual implementation uses
content-addressed dedup via SHA-256 `content_hash` on the raw tables.
Natural key resolution is deferred to the SQLMesh transform layer.

### Standard scraper flow

Docs describe a 6-step flow:

1. Scrape
2. Upload artifact to S3
3. Extract to warehouse
4. Transform
5. Emit events
6. Health check

The actual flow is 9 steps in a different order:

1. Run scraper (with litestream replication)
2. SQLite integrity check + S3 upload (cached)
3. Cleanup litestream replicas
4. PDD doctor health check (with markdown artifact on failure)
5. Validate results
6. Create provenance record
7. Load to warehouse (with content-hash dedup + M2M observations)
8. Create run summary artifact
9. Emit `scrape.completed` event

**Transform is not in the scraper flow.** It is triggered as a separate
`sqlmesh-transforms` flow via Prefect automation on the
`scrape.completed` event.

### Event-driven downstream processing

Docs describe OCR, transcription, embedding, and citation extraction
flows triggered by scraper events. None of these exist yet. Citation
extraction is partially implemented as a `courtlistener.extracted_citations`
SQLMesh model, but there are no standalone processing flows for OCR,
transcription, or embeddings.

### CourtListener integration

Docs describe bidirectional data flow, version vector synchronization,
incremental ingestion via Celery, and periodic bulk reconciliation. None
of this is implemented. The `prefect_courtlistener_worker.md` doc is a
design document only.

### Data transformation pipeline

Docs describe a 5-stage pipeline (raw → cleaning → normalization →
dedup/delta → CL export). The actual implementation uses SQLMesh models
for the transform stages and does not have a CL export step yet.

## deployment.rst

### SNS topics

Docs say `email-notices` topic is created. Actual infrastructure has
`sns_topics = []` — no SNS topics are provisioned.

### Prefect deployments

Docs list: `hello-flow`, `alabama-publicportal-backfill`,
`sqs-listener`, `follow-up`.

Actual deployments: `hello-flow`, `scraper-run` (generic, not
Alabama-specific), `sqlmesh-transforms`, `sqs-listener`, `follow-up`.

`alabama-publicportal-backfill` does not exist. `scraper-run` is a
generic flow that takes `scraper_path` and `scraper_schema` parameters.
`sqlmesh-transforms` is not documented.

### Prefect automations

Docs list 2 automations. Actual infrastructure has 5:

- `sqs-listener-trigger` (documented)
- `follow-up-trigger` (documented)
- `scrape-completed-ala-publicportal` (not documented)
- `scrape-completed-conn-jud-ct-gov` (not documented)
- `sync-prepared-trigger` (not documented)

### Missing from docs

- `analytics` SQLAlchemy connector block
- Per-scraper concurrency limits (`scraper:ala_publicportal`,
  `scraper:conn_jud_ct_gov`, limit=1)
- The scraper in-process worker (`workers/in_process.py`) and
  `scraper-pool` work pool
- Litestream replication for SQLite backup during scraper runs

## sqlmesh_prefect_integration.md

### Integration with scraper flow

The code example shows `sqlmesh_transforms()` called directly from the
scraper flow. The actual implementation uses **event-driven
triggering**: the scraper flow emits a `scrape.completed` event, and
Prefect automations trigger the `sqlmesh-transforms` deployment. The
transform flow is not called inline.

### Code examples

The example `scraper_run_flow` is a simplified sketch that does not
reflect the actual implementation (missing integrity check, doctor
health check, litestream management, summary artifacts, etc.).

## prefect_courtlistener_worker.md

This is entirely a design document. None of the described components
exist in the codebase:

- No `sync-warehouse` deployment
- No `sync-pool` work pool
- No CL-side Django management command
- No `sync_chunk` or `sync_warehouse` flows

The trigger example shows `run_deployment()`. The actual scraper flow
uses a `scrape.completed` event + automation chain, not a direct
deployment trigger.

## index.rst

The toctree only includes `the-plan` and `deployment`. The two markdown
docs (`sqlmesh_prefect_integration.md`, `prefect_courtlistener_worker.md`)
exist in the docs directory but are not linked from the index.
