Data Flow Pipeline
==================

How scraped data moves from raw ingestion through cleaning, correction, and
normalization into the ``courtlistener`` schema ready for sync.

Overview
--------

.. drawio-image:: data-flow.drawio
   :export-scale: 150

Layers
------

raw_{model} + observations
^^^^^^^^^^^^^^^^^^^^^^^^^^

Raw tables are loaded by the Python scraper pipeline. Each row is
content-addressed via a SHA-256 ``content_hash`` — duplicate content is
silently skipped. A paired ``raw_{model}_observations`` table tracks which
scraper runs (``provenance_id``) observed each row, forming a many-to-many
relationship between raw rows and scraper runs.

Columns of interest:

- ``row_id`` — auto-increment PK on the raw table
- ``content_hash`` — SHA-256 of the row content (UNIQUE)

Observations table:

- ``row_id`` — FK to the raw table
- ``provenance_id`` — FK to ``warehouse.provenance``
- ``record_id`` — position within the scraper run

latest_{model}
^^^^^^^^^^^^^^

Picks the most recent version of each record by natural key. Joins raw data
with observations and selects the row with the highest ``provenance_id`` per
natural key using ``DISTINCT ON``.

Uses a **self-referencing watermark**: on each run, queries
``COALESCE(MAX(provenance_id), 0)`` from its own table, then only processes
observations with ``provenance_id`` above that watermark. On the first run
(empty table), the watermark is 0, so everything is processed.

The ``INCREMENTAL_BY_UNIQUE_KEY`` kind handles the upsert — existing rows
are updated, new rows are inserted, untouched rows are preserved.

staged_{model}
^^^^^^^^^^^^^^

Applies scraper-specific cleaning and human corrections to ``latest_`` data.
This is the definitive scraper-level view of the data.

Two sub-patterns:

1. **Standard staged models** — read from ``latest_{model}``, LEFT JOIN the
   scraper-level ``corrections_{model}`` table, and apply the ``@correct``
   macro to each correctable column. Uses a **dual watermark**: processes
   rows where ``provenance_id > prov_watermark`` OR
   ``correction_id > corr_watermark``.

2. **JSONB-explosion models** — for data stored as JSONB arrays inside a
   parent table (e.g., Alabama's docket entries are stored as a JSON array
   inside ``raw_dockets.entries``). These use ``CROSS JOIN LATERAL
   jsonb_array_elements()`` against the parent's ``latest_`` table and
   track the parent's ``provenance_id`` as their watermark.

corrections_{model}
^^^^^^^^^^^^^^^^^^^

Human-provided corrections at both scraper and CourtListener levels. Each
row targets a specific record (by natural key) and contains a ``corrections``
JSONB column with field-level patches.

Three states per field:

- **Key absent** — no correction, use original value
- **Key present with value** — use the correction value
- **Key present with JSON null** — correct to SQL NULL (``->>`` returns NULL)

The ``@correct`` macro expands to::

    CASE WHEN c.corrections ? 'field'
         THEN (c.corrections->>'field')::type
         ELSE original_value
    END

Each corrections table has a ``correction_id`` (FK to
``corrections.corrections.id``) that forms part of the composite PK. Multiple
corrections can exist for the same natural key — the staged model always picks
the one with the highest ``correction_id`` via ``DISTINCT ON ... ORDER BY
correction_id DESC``.

corrections.corrections
^^^^^^^^^^^^^^^^^^^^^^^

Provenance table for all corrections. Tracks who made the correction and why.

.. list-table::
   :widths: 20 15 65
   :header-rows: 1

   * - Column
     - Type
     - Notes
   * - ``id``
     - BIGINT PK
     - Assigned by CourtListener, NOT auto-increment
   * - ``user_id``
     - TEXT
     - CourtListener user ID
   * - ``notes``
     - TEXT
     - Human description of the correction
   * - ``created_at``
     - TIMESTAMPTZ
     - When the correction was made
   * - ``metadata``
     - JSONB
     - Arbitrary structured data

courtlistener.raw_{model}
^^^^^^^^^^^^^^^^^^^^^^^^^

Unions and normalizes all scraper ``staged_`` models into a common
CourtListener-compatible schema. Column names are mapped from scraper-specific
names (e.g., ``case_number`` in Alabama) to CourtListener names (e.g.,
``docket_number``).

Carries through ``provenance_id``, ``record_id``, and ``correction_id`` from
the upstream scraper staged models.

courtlistener.staged_{model}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Applies CourtListener-level corrections and stamps the version clock. This is
the table that the CL sync worker reads from.

Output includes:

- All data columns (with CL corrections applied)
- ``version_provenance`` — highest ``provenance_id`` that contributed
- ``version_correction`` — highest ``correction_id`` that contributed
- ``courtlistener_id`` — written back by the sync worker after save
- ``date_created``, ``date_modified`` — execution timestamps

irr_{type}_{model}
^^^^^^^^^^^^^^^^^^

Materialized irregularity tables. These are ``FULL`` models (rebuilt from
scratch each run) that capture rows violating data quality rules.

SQLMesh audits only COUNT violations — they do not persist failing rows.
These standalone models fill that gap by materializing the full set of
violating records for inspection and debugging.

Current checks:

- ``irr_invalid_court_ids_dockets`` — ``court_id`` not in the ``court_ids`` seed
- ``irr_future_dates_opinion_clusters`` — ``date_filed`` in the future
- ``irr_null_docket_numbers_dockets`` — ``docket_number`` is NULL (CL level)

Each includes the natural key columns, the offending value, and
``checked_at`` for the run date.

Version Clock
-------------

The version clock is a two-component vector: ``(provenance_id, correction_id)``.
Both components are monotonically increasing ``BIGINT`` values.

- ``provenance_id`` increases when new scraper data flows through the pipeline
- ``correction_id`` increases when human corrections are applied

A row is considered "newer" when **either** component increases. The CL sync
worker queries::

    SELECT * FROM courtlistener.staged_dockets
    WHERE version_provenance > :last_sync_provenance
       OR version_correction > :last_sync_correction

This ensures that both new scraper observations and new corrections are picked
up by sync, without requiring a single monotonic sequence across both change
sources.

On ``courtlistener.staged_*`` tables, the columns are named
``version_provenance`` and ``version_correction``.

Incremental Strategy
--------------------

Models use SQLMesh's ``INCREMENTAL_BY_UNIQUE_KEY`` kind for the upsert
behavior, but they do **not** use SQLMesh's ``@start_date``/``@end_date``
interval system.

Instead, each model computes its own high-water mark by querying the maximum
version component from its own table::

    WITH watermark AS (
        SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
        FROM this_model
    )
    ...
    WHERE upstream.provenance_id > watermark.max_prov

On first run (empty table), ``COALESCE(MAX(...), 0)`` returns 0, so all
upstream rows are processed. On subsequent runs, only rows with version
components above the watermark are touched.

For models that track both provenance and corrections (staged models), the
watermark is dual::

    WITH prov_watermark AS (
        SELECT COALESCE(MAX(provenance_id), 0) AS max_prov FROM this_model
    ),
    corr_watermark AS (
        SELECT COALESCE(MAX(correction_id), 0) AS max_corr FROM this_model
    ),
    changed_keys AS (
        SELECT natural_key FROM upstream WHERE provenance_id > max_prov
        UNION
        SELECT natural_key FROM corrections WHERE correction_id > max_corr
    )

This means a second run with no new data produces zero upserts.

Corrections Flow
----------------

Corrections flow from CourtListener back into the warehouse:

1. A CL user makes a correction in the CourtListener web interface
2. The ``cl-sync`` process writes a row to ``corrections.corrections``
   (provenance) and the appropriate ``corrections_{model}`` table
3. On the next ``sqlmesh run``, the staged model detects the new
   ``correction_id`` above its watermark
4. The staged model re-processes the affected natural key, applying the
   JSONB patch via the ``@correct`` macro
5. The change propagates through ``courtlistener.raw_*`` and
   ``courtlistener.staged_*``
6. The sync worker picks up the row (``version_correction`` increased)
   and updates the CL database

This creates a feedback loop where corrections made in CL flow back through
the full pipeline and are re-synced, ensuring the warehouse and CL stay
consistent.
