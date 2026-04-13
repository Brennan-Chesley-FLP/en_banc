Scraper Flows
=============

The scraper pipeline is event-driven: each flow emits a Prefect event
on completion that triggers the next stage. The Prefect server routes
events to deployments via automations.

.. md-mermaid::

   sequenceDiagram
       participant User
       participant Prefect as Prefect Server
       participant Scraper as scraper-run<br/>(scraper-pool)
       participant Warehouse as warehouse-load<br/>(docker-pool)
       participant SQLMesh as sqlmesh-transforms<br/>(docker-pool)
       participant Sync as sync-warehouse<br/>(sync-pool)

       User->>Prefect: Trigger scraper-run
       Prefect->>Scraper: Dispatch to scraper-pool
       Scraper->>Scraper: run_scraper_task
       Scraper->>Scraper: doctor_health_check
       Scraper->>Scraper: upload_db
       Scraper->>Prefect: emit scrape.uploaded

       Prefect->>Warehouse: Dispatch to docker-pool
       Warehouse->>Warehouse: download_db
       Warehouse->>Warehouse: validate_run
       Warehouse->>Warehouse: create_provenance
       Warehouse->>Warehouse: load_to_warehouse
       Warehouse->>Prefect: emit scrape.completed

       Prefect->>SQLMesh: Dispatch to docker-pool
       SQLMesh->>SQLMesh: plan_and_apply
       SQLMesh->>Prefect: emit sync.prepared

       Prefect->>Sync: Dispatch to sync-pool
       Sync->>Sync: sync_warehouse

Flow Descriptions
-----------------

scraper-run
~~~~~~~~~~~

Runs on the **scraper-pool** (in-process on the Kent worker). Accepts a
``scraper_path`` and ``scraper_schema``. Acquires a per-scraper
concurrency slot (limit 1) to prevent parallel runs against the same
court.

**Tasks:**

- **run_scraper_task** -- Runs a Kent scraper via ``PersistentDriver``.
  The scraper produces a SQLite database with all requests, responses,
  and extracted data. Downloaded files are archived to S3 via a
  callback.
- **doctor_health_check** -- Validates the scraper database: checks for
  orphaned requests/responses and unresolved errors. Fails the flow if
  integrity issues are found.
- **upload_db** -- Uploads the SQLite database to S3 at
  ``scraper_runs/{schema}/{run_name}.db``.

Emits ``scrape.uploaded`` with the S3 URI and scraper schema.

warehouse-load
~~~~~~~~~~~~~~

Runs on the **docker-pool**. Triggered by the ``scrape.uploaded`` event.
Downloads the scraper's SQLite artifact and loads its contents into the
PostgreSQL warehouse.

**Tasks:**

- **download_db** -- Downloads the SQLite database from S3 to a
  temporary file.
- **validate_run** -- Validates scraper output before loading. Rejects
  runs with invalid rows.
- **create_provenance** -- Creates a provenance record in the warehouse
  linking data back to the scraper run, S3 artifact, and Prefect flow
  run ID.
- **load_to_warehouse** -- Bulk-loads rows from SQLite into
  scraper-specific raw warehouse tables with content-hash
  deduplication.

Emits ``scrape.completed`` for the scraper schema.

sqlmesh-transforms
~~~~~~~~~~~~~~~~~~

Runs on the **docker-pool**. Triggered by the ``scrape.completed``
event. Runs the SQLMesh transformation pipeline to clean, normalize,
and deduplicate data through the model DAG.

**Tasks:**

- **plan_and_apply** -- Runs ``sqlmesh plan`` filtering to the
  triggering scraper's schema and all downstream models. Each model
  evaluation is submitted as a separate Prefect task for visibility.

Emits ``sync.prepared`` when transforms are complete.

sync-warehouse
~~~~~~~~~~~~~~

Runs on the **sync-pool**. Triggered by the ``sync.prepared`` event.
Pushes transformed data from the ``courtlistener.staged_*`` tables to
CourtListener via its API or Celery tasks.

This flow runs in the CourtListener environment, not the en-banc
Docker image.

Event Chain
-----------

.. list-table::
   :header-rows: 1
   :widths: 25 25 25 25

   * - Event
     - Emitted By
     - Triggers
     - Payload
   * - ``scrape.uploaded``
     - scraper-run
     - warehouse-load
     - s3_uri, scraper_schema
   * - ``scrape.completed``
     - warehouse-load
     - sqlmesh-transforms
     - scraper_schema
   * - ``sync.prepared``
     - sqlmesh-transforms
     - sync-warehouse
     - scraper_schema
