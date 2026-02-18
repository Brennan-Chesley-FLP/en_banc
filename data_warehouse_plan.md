# Data Warehouse Plan

This document describes the data warehouse design for the scraper-to-CourtListener pipeline: raw landing tables, scraper-specific SQLMesh transforms, standardized output tables, Prefect orchestration, and export to CourtListener.

## Table of Contents

- [Design Principles](#design-principles)
- [Pipeline Overview](#pipeline-overview)
- [Provenance Table](#provenance-table)
- [Layer 1: Raw Landing Tables](#layer-1-raw-landing-tables)
- [Layer 2: Scraper-Specific Transforms (SQLMesh)](#layer-2-scraper-specific-transforms-sqlmesh)
- [Layer 3: Standardized Tables (SQLMesh)](#layer-3-standardized-tables-sqlmesh)
- [Prefect Orchestration](#prefect-orchestration)
- [Export to CourtListener](#export-to-courtlistener)
- [SQLMesh Project Layout](#sqlmesh-project-layout)
- [Migration Path](#migration-path)

---

## Design Principles

1. **Provenance everywhere** — Every row links to a `provenance` record identifying the run, source, and artifact that produced it. The provenance table is extensible to non-scraper sources (CourtListener imports, manual corrections, bulk loads).
2. **Version clocks** — Two version columns track data lineage:
   - `warehouse_version`: Incremented each time the warehouse row is updated by a transform.
   - `courtlistener_version`: Records the warehouse_version that was last exported to CL. When `courtlistener_version < warehouse_version`, the row needs re-export.
3. **No Django PKs** — The warehouse uses composite keys (`provenance_id` + `record_id`) and natural keys from the court + scraper-assigned IDs. Django PKs are recorded only after export, as a reference back to CL.
4. **Closely mirror CL models** — Standardized tables match CourtListener's Django model fields so the export step is straightforward mapping, not transformation.
5. **Scraper-specific layers are disposable** — Each scraper's raw and intermediate tables can be rebuilt from the SQLite artifacts on S3.
6. **Per-scraper schema isolation** — Each scraper gets its own PostgreSQL schema (e.g., `ala_publicportal`, `conn_jud_ct_gov`) containing both raw and staging tables. This keeps scraper data cleanly namespaced and makes it easy to drop/rebuild a single scraper's data without affecting others.

---

## Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│ SCRAPER RUN (Prefect Flow)                                              │
│                                                                         │
│  1. Run scraper → SQLite artifact on S3                                 │
│  2. If run successful → load SQLite into raw landing tables             │
│  3. Trigger SQLMesh plan/apply for affected scraper models              │
│                                                                         │
└───────────────┬─────────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ ANALYTICS DB (PostgreSQL)                                               │
│                                                                         │
│  warehouse.provenance        Provenance table (shared across all data) │
│           ──────────────     Written by Prefect, not managed by SQLMesh │
│                              Every row in every layer has provenance_id │
│                                                                         │
│  Per-scraper schemas:        Each scraper gets its own PG schema        │
│   ala_publicportal.*         containing raw_* and stg_* tables          │
│   conn_jud_ct_gov.*          Loaded by Prefect (raw), SQLMesh (stg)    │
│   <scraper_name>.*                                                      │
│                                                                         │
│  courtlistener.*             Standardized tables (SQLMesh)              │
│           ──────────────     Mirror CL Django models + version clocks   │
│                              Unions data from all scraper stg_* tables  │
│                                                                         │
└───────────────┬─────────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ EXPORT (Prefect Flow)                                                   │
│                                                                         │
│  Query rows where courtlistener_version < warehouse_version             │
│  Package into Celery tasks for CL consumption                           │
│  CL saves → Django signals fire (ES indexing, alerts, webhooks)         │
│  On success, update courtlistener_version in warehouse                  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Provenance Table

The `warehouse.provenance` table is the single source of truth for where data came from. Every row in every raw, staging, and standardized table carries a `provenance_id` FK pointing here. This table is **not** managed by SQLMesh — it is written to directly by Prefect flows (or any other process that introduces data into the warehouse).

### `warehouse.provenance`

| Column | Type | Description |
|---|---|---|
| `id` | `BIGSERIAL` | PK |
| `source_type` | `TEXT NOT NULL` | Category: `scraper_run`, `courtlistener_import`, `manual_correction`, `bulk_import` |
| `source_name` | `TEXT NOT NULL` | Identifier for the source (e.g., `ala_publicportal`, `conn_jud_ct_gov`, `courtlistener`, `harvard_caselaw`) |
| `run_id` | `UUID` | Prefect flow run ID (NULL for non-Prefect sources) |
| `s3_artifact_path` | `TEXT` | S3 path to the SQLite artifact (NULL for non-scraper sources) |
| `description` | `TEXT` | Human-readable description (e.g., "Alabama daily opinion scrape 2026-02-12") |
| `metadata` | `JSONB` | Extensible — scraper params, CL export batch ID, correction ticket, etc. |
| `created_at` | `TIMESTAMPTZ NOT NULL` | When this provenance record was created |

**Indexes**: `(source_type, source_name)`, `(run_id)`

### Usage Pattern

Before loading any data into the warehouse, the loader creates a provenance row and uses the returned `id` as the `provenance_id` on every row it inserts:

```python
# In a Prefect task
provenance_id = insert_provenance(
    source_type="scraper_run",
    source_name="ala_publicportal",
    run_id=prefect_flow_run_id,
    s3_artifact_path="s3://artifacts/ala/2026-02-12/run.sqlite",
    description="Alabama daily opinion scrape 2026-02-12",
    metadata={"params": {"court_id": ["ala"], "date_filed.gte": "2026-02-01"}},
)
# Then use provenance_id on every INSERT into ala_publicportal.raw_* tables
```

For future CourtListener imports (e.g., backfilling warehouse from existing CL data):

```python
provenance_id = insert_provenance(
    source_type="courtlistener_import",
    source_name="courtlistener",
    description="Backfill Alabama dockets from CL production DB",
    metadata={"cl_query": "Docket.objects.filter(court_id='ala')"},
)
```

---

## Layer 1: Raw Landing Tables

Each scraper's raw tables live in that scraper's own PostgreSQL schema (e.g., `ala_publicportal`, `conn_jud_ct_gov`), prefixed with `raw_`. They are **not** managed by SQLMesh — they are written to directly by the Prefect flow that loads data from the scraper's SQLite artifact.

### Automated Table Generation from Scraper Models

The scraper output models (e.g., `AlaDocket`, `ConnOpinionCluster`) are being migrated from Pydantic `BaseModel` (via `ConsumerModel`) to `SQLModel` with `table=False`. This means the models are already SQLModel-native — the factory only needs to derive a `table=True` variant with the warehouse metadata columns injected. No type introspection or Pydantic-to-SQLAlchemy mapping is needed; SQLModel handles the column generation from the existing model fields.

#### Composite Primary Key

Every raw table uses a composite primary key of `(provenance_id, record_id)`:

- **`provenance_id`** — FK to `warehouse.provenance`. Identifies the run/source.
- **`record_id`** — An integer identifying the record within that provenance context. For scraper data, this is the integer PK from the SQLite results table. For other sources (CL imports, bulk loads), this may be the source system's row ID or a sequence value.

This composite key means: "within a given provenance (run), this is record N." It naturally supports:
- Excision: delete all rows for a provenance_id.
- Deduplication: the same record_id from a later provenance supersedes an earlier one.
- Traceability: given any warehouse row, you can find the exact record in the exact SQLite artifact.

#### Factory

Since the output models are already `SQLModel(table=False)`, the factory creates a `table=True` subclass that adds the warehouse columns and places the table in the scraper's schema:

```python
from datetime import datetime

from sqlalchemy import Column, BigInteger, DateTime, ForeignKey, func
from sqlmodel import Field, SQLModel


def raw_table_from_model(
    model: type[SQLModel],
    schema_name: str,
    table_name: str,
) -> type[SQLModel]:
    """Derive a table=True SQLModel from a table=False scraper output model.

    Inherits all fields from the source model and adds the standard
    warehouse columns (provenance_id, record_id, loaded_at). The
    composite PK is (provenance_id, record_id).

    Usage:
        AlaDocketsRaw = raw_table_from_model(
            AlaDocket, "ala_publicportal", "raw_dockets"
        )
        SQLModel.metadata.create_all(engine)
    """
    # Build the table=True subclass dynamically
    namespace = {
        "__tablename__": table_name,
        "__table_args__": ({"schema": schema_name},),
        "__annotations__": {
            "provenance_id": int,
            "record_id": int,
            "loaded_at": datetime | None,
        },
        "provenance_id": Field(
            sa_column=Column(
                BigInteger,
                ForeignKey("warehouse.provenance.id"),
                primary_key=True,
            )
        ),
        "record_id": Field(
            sa_column=Column(BigInteger, primary_key=True),
        ),
        "loaded_at": Field(
            default=None,
            sa_column=Column(
                DateTime(timezone=True),
                server_default=func.now(),
            ),
        ),
    }

    table_cls = type(table_name, (model,), namespace)
    table_cls.model_config["table"] = True
    return table_cls
```

Because the source model is already SQLModel, all its field definitions (types, optionality, validators) are inherited directly. Fields that are `list[SubModel]` or `dict` are automatically handled as JSON columns by SQLModel.

#### Auto-Discovery from BaseScraper

Rather than maintaining a manual registry, the raw table registry is **auto-discovered** by inspecting all `BaseScraper` subclasses and extracting their generic type parameters:

```python
from types import UnionType
from typing import Union, get_args, get_origin

from kent.data_types import BaseScraper


def discover_output_models(scraper_class: type[BaseScraper]) -> list[type[SQLModel]]:
    """Extract all output model types from a BaseScraper subclass.

    BaseScraper is Generic[ScraperReturnType], where ScraperReturnType
    is a union of the output model types. For example:

        class AlabamaScraper(BaseScraper[
            AlaOpinionCluster | AlaOralArgument | AlaDocket | AlaHistoricalReleaseList
        ]): ...

    Returns [AlaOpinionCluster, AlaOralArgument, AlaDocket, AlaHistoricalReleaseList].
    """
    for base in getattr(scraper_class, "__orig_bases__", []):
        origin = get_origin(base)
        if origin is None:
            continue
        if not (isinstance(origin, type) and issubclass(origin, BaseScraper)):
            continue

        args = get_args(base)
        if not args:
            continue

        return_type = args[0]
        type_origin = get_origin(return_type)

        if type_origin is Union or isinstance(return_type, UnionType):
            return [t for t in get_args(return_type) if isinstance(t, type)]

        if isinstance(return_type, type):
            return [return_type]

    return []


def build_raw_table_registry() -> dict[str, list[type[SQLModel]]]:
    """Discover all scrapers and build the raw table registry.

    Walks all BaseScraper subclasses, extracts their output model types,
    and groups them by scraper schema name.

    Returns:
        {
            "ala_publicportal": [AlaDocket, AlaOpinionCluster, ...],
            "conn_jud_ct_gov": [ConnDocket, ConnOpinionCluster, ...],
        }
    """
    registry = {}
    for scraper_cls in BaseScraper.__subclasses__():
        # Schema name derived from the scraper's module path
        # e.g., juriscraper.sd.state.alabama.publicportal_alappeals_gov
        #   → "ala_publicportal" (from scraper's class attribute or module convention)
        schema_name = get_schema_name(scraper_cls)
        models = discover_output_models(scraper_cls)
        if models:
            registry[schema_name] = models
    return registry


def create_all_raw_tables(engine):
    """Auto-discover all scrapers and create their raw tables."""
    registry = build_raw_table_registry()
    for schema_name, models in registry.items():
        for model in models:
            table_name = f"raw_{model.__name__.lower()}"
            raw_table_from_model(model, schema_name, table_name)
    SQLModel.metadata.create_all(engine)
```

This means adding a new scraper automatically creates its raw tables — no registry to maintain.

#### Schema Migrations with Alembic

Since the raw tables are SQLModel classes backed by SQLAlchemy metadata, Alembic can auto-detect schema drift:

```bash
# When a scraper model adds/removes a field:
alembic revision --autogenerate -m "sync raw tables with scraper models"
alembic upgrade head
```

Adding a field to a scraper's SQLModel definition automatically flows through to the raw table schema.

### Standard Metadata Columns

Every raw table includes the composite PK plus a timestamp:

| Column | Type | Description |
|---|---|---|
| `provenance_id` | `BIGINT NOT NULL` | PK part 1. FK to `warehouse.provenance` |
| `record_id` | `BIGINT NOT NULL` | PK part 2. SQLite row PK for scraper data; source-specific ID for other sources |
| `loaded_at` | `TIMESTAMPTZ` | When this row was inserted into the warehouse |

### Docket Tables

#### `ala_publicportal.raw_dockets`

Mirrors `AlaDocket` output. One row per case scraped.

| Column | Type | Source Field |
|---|---|---|
| `case_instance_uuid` | `TEXT` | `case_instance_uuid` |
| `case_number` | `TEXT` | `case_number` |
| `court_id` | `TEXT` | `court_id` (ala, alactapp, alacrimapp) |
| `date_filed` | `DATE` | `date_filed` |
| `case_name` | `TEXT` | `case_name` |
| `case_classification` | `TEXT` | `case_classification` |
| `originating_court` | `TEXT` | `originating_court` |
| `originating_court_number` | `TEXT` | `originating_court_number` |
| `status` | `TEXT` | `status` |
| `court_guid` | `TEXT` | `court_guid` |
| `source_url` | `TEXT` | `source_url` |
| `parties` | `JSONB` | `parties` (array of party objects) |
| `entries` | `JSONB` | `entries` (array of docket entry objects) |
| `oral_arguments` | `JSONB` | `oral_arguments` (array) |

#### `conn_jud_ct_gov.raw_dockets`

Mirrors `ConnDocket` output.

| Column | Type | Source Field |
|---|---|---|
| `crn` | `INTEGER` | `crn` |
| `docket_id` | `TEXT` | `docket_id` (SC/AC number) |
| `court_id` | `TEXT` | `court_id` (conn, connappct) |
| `case_name` | `TEXT` | `case_name` |
| `status` | `TEXT` | `status` |
| `date_filed` | `DATE` | `date_filed` |
| `argued_date` | `DATE` | `argued_date` |
| `disposition_date` | `DATE` | `disposition_date` |
| `submitted_on_briefs_date` | `DATE` | `submitted_on_briefs_date` |
| `appeal_by` | `TEXT` | `appeal_by` |
| `disposition_method` | `TEXT` | `disposition_method` |
| `cite` | `TEXT` | `cite` |
| `panel` | `TEXT` | `panel` |
| `is_efiled` | `BOOLEAN` | `is_efiled` |
| `trial_court_docket_number` | `TEXT` | trial court docket number |
| `trial_court_url` | `TEXT` | trial court URL |
| `trial_court_judge` | `TEXT` | trial court judge |
| `trial_court_case_type` | `TEXT` | trial court case type |
| `parties` | `JSONB` | `parties` (array of party objects) |
| `preliminary_papers` | `JSONB` | `preliminary_papers` |
| `transcripts` | `JSONB` | `transcripts` |
| `source_url` | `TEXT` | `source_url` |

#### `conn_jud_ct_gov.raw_docket_entries`

Mirrors `ConnDocketEntry` output. Separate from dockets because Connecticut yields these independently.

| Column | Type | Source Field |
|---|---|---|
| `docket_id` | `TEXT` | `docket_id` |
| `activity_type` | `TEXT` | `activity_type` |
| `number` | `TEXT` | `number` |
| `date_filed` | `DATE` | `date_filed` |
| `description` | `TEXT` | `description` |
| `document_url` | `TEXT` | `document_url` |
| `document_local_path` | `TEXT` | `document_local_path` |

### Opinion Tables

#### `ala_publicportal.raw_opinion_clusters`

Mirrors `AlaOpinionCluster` output.

| Column | Type | Source Field |
|---|---|---|
| `case_number` | `TEXT` | `case_number` |
| `court_id` | `TEXT` | `court_id` |
| `date_filed` | `DATE` | `date_filed` |
| `case_name` | `TEXT` | `case_name` |
| `publication_number` | `TEXT` | `publication_number` |
| `authoring_judge` | `TEXT` | `authoring_judge` |
| `decision_text` | `TEXT` | `decision_text` |
| `lower_court` | `TEXT` | `lower_court` |
| `lower_court_number` | `TEXT` | `lower_court_number` |
| `per_curiam` | `BOOLEAN` | `per_curiam` |
| `on_rehearing` | `BOOLEAN` | `on_rehearing` |
| `publication_uuid` | `TEXT` | `publication_uuid` |
| `case_instance_uuid` | `TEXT` | `case_instance_uuid` |
| `source_url` | `TEXT` | `source_url` |
| `opinions` | `JSONB` | `opinions` (array of opinion objects) |

#### `conn_jud_ct_gov.raw_opinion_clusters`

Mirrors `ConnOpinionCluster` output.

| Column | Type | Source Field |
|---|---|---|
| `docket_id` | `TEXT` | `docket_id` |
| `court_id` | `TEXT` | `court_id` |
| `date_filed` | `DATE` | `date_filed` |
| `case_name` | `TEXT` | `case_name` |
| `publication_year` | `INTEGER` | `publication_year` |
| `publication_name` | `TEXT` | `publication_name` |
| `law_journal_date` | `TEXT` | `law_journal_date` |
| `source_url` | `TEXT` | `source_url` |
| `opinions` | `JSONB` | `opinions` (array of opinion objects) |

### Oral Argument Tables

#### `ala_publicportal.raw_oral_arguments`

Mirrors `AlaOralArgument` output.

| Column | Type | Source Field |
|---|---|---|
| `case_number` | `TEXT` | `case_number` |
| `court_id` | `TEXT` | `court_id` |
| `date_argued` | `DATE` | `date_argued` |
| `case_name` | `TEXT` | `case_name` |
| `source_url` | `TEXT` | `source_url` |
| `calendar_uuid` | `TEXT` | `calendar_uuid` |
| `case_instance_uuid` | `TEXT` | `case_instance_uuid` |

#### `conn_jud_ct_gov.raw_oral_arguments`

Mirrors `ConnOralArgument` output.

| Column | Type | Source Field |
|---|---|---|
| `docket_number` | `TEXT` | `docket_number` |
| `court_id` | `TEXT` | `court_id` |
| `date_argued` | `DATE` | `date_argued` |
| `case_name` | `TEXT` | `case_name` |
| `download_url` | `TEXT` | `download_url` |
| `audio_id` | `INTEGER` | `audio_id` |
| `court_year` | `TEXT` | `court_year` |
| `term` | `TEXT` | `term` |
| `case_detail_url` | `TEXT` | `case_detail_url` |

### Adding a New Scraper

To add a new scraper's raw tables:

1. Implement the scraper as a `BaseScraper[OutputModel1 | OutputModel2 | ...]` subclass with `SQLModel(table=False)` output models. Auto-discovery handles the rest.
2. Run `alembic revision --autogenerate` to generate the migration (creates the schema and tables).
3. Run `alembic upgrade head` to apply.
4. Register the schema in SQLMesh's external models config.

---

## Layer 2: Scraper-Specific Transforms (SQLMesh)

These SQLMesh models live in the same per-scraper schema as the raw tables, with a `stg_` prefix (e.g., `ala_publicportal.stg_dockets`). They read from the scraper's `raw_*` tables and produce cleaned, flattened, deduplicated data. Each scraper gets its own subdirectory of SQLMesh models.

The transforms handle:

- **JSONB explosion** — Unnest parties, entries, opinions from JSONB arrays into proper relational tables.
- **Field normalization** — Standardize date formats, trim whitespace, normalize court IDs.
- **Deduplication** — Use natural keys (court_id + case_number, court_id + docket_id, etc.) to detect and merge duplicate rows from overlapping scraper runs.
- **Scraper-specific logic** — E.g., Alabama's lower court extraction from case title parentheticals, Connecticut's CRN-based deduplication.

### SQLMesh Model Patterns

All staging models use `INCREMENTAL_BY_UNIQUE_KEY` to handle deduplication naturally — when a scraper re-scrapes a case, the new data replaces the old.

#### Example: `ala_publicportal.stg_dockets`

```sql
MODEL (
    name ala_publicportal.stg_dockets,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, case_number)
    ),
    grain (court_id, case_number),
    audits (
        assert_valid_court_ids
    )
);

SELECT
    r.case_instance_uuid,
    r.case_number,
    r.court_id,
    r.date_filed,
    TRIM(r.case_name) AS case_name,
    r.case_classification,
    r.originating_court,
    r.originating_court_number,
    r.status,
    r.source_url,
    r.provenance_id,
    r.loaded_at
FROM ala_publicportal.raw_dockets AS r
WHERE r.loaded_at >= @start_date AND r.loaded_at < @end_date;
```

#### Example: `ala_publicportal.stg_docket_parties`

Explodes the JSONB `parties` array into a relational table.

```sql
MODEL (
    name ala_publicportal.stg_docket_parties,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, case_number, party_name, party_role)
    ),
    grain (court_id, case_number, party_name, party_role)
);

SELECT
    r.court_id,
    r.case_number,
    p.value->>'name' AS party_name,
    p.value->>'type' AS party_type,
    p.value->>'role' AS party_role,
    p.value->>'status' AS party_status,
    (p.value->>'pro_se')::BOOLEAN AS pro_se,
    p.value->'attorneys' AS attorneys,
    r.provenance_id,
    r.loaded_at
FROM ala_publicportal.raw_dockets AS r
CROSS JOIN LATERAL jsonb_array_elements(r.parties) AS p(value)
WHERE r.loaded_at >= @start_date AND r.loaded_at < @end_date;
```

#### Example: `ala_publicportal.stg_docket_entries`

```sql
MODEL (
    name ala_publicportal.stg_docket_entries,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, case_number, document_uuid)
    ),
    grain (court_id, case_number, document_uuid)
);

SELECT
    r.court_id,
    r.case_number,
    e.value->>'date_filed' AS date_filed,
    e.value->>'document_type' AS document_type,
    e.value->>'document_subtype' AS document_subtype,
    e.value->>'description' AS description,
    e.value->>'document_uuid' AS document_uuid,
    r.provenance_id,
    r.loaded_at
FROM ala_publicportal.raw_dockets AS r
CROSS JOIN LATERAL jsonb_array_elements(r.entries) AS e(value)
WHERE r.loaded_at >= @start_date AND r.loaded_at < @end_date;
```

#### Example: `ala_publicportal.stg_opinions`

Explodes opinion clusters into individual opinions.

```sql
MODEL (
    name ala_publicportal.stg_opinions,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, case_number, opinion_download_url)
    ),
    grain (court_id, case_number, opinion_download_url)
);

SELECT
    r.court_id,
    r.case_number,
    r.date_filed,
    r.case_name,
    r.publication_number,
    r.per_curiam,
    r.on_rehearing,
    r.lower_court,
    r.lower_court_number,
    o.value->>'download_url' AS opinion_download_url,
    o.value->>'type' AS opinion_type,
    o.value->>'local_path' AS opinion_local_path,
    o.value->>'authoring_judge' AS authoring_judge,
    o.value->>'decision_text' AS decision_text,
    r.provenance_id,
    r.loaded_at
FROM ala_publicportal.raw_opinion_clusters AS r
CROSS JOIN LATERAL jsonb_array_elements(r.opinions) AS o(value)
WHERE r.loaded_at >= @start_date AND r.loaded_at < @end_date;
```

### Staging Models Per Scraper

| Scraper | Schema | Staging Models |
|---|---|---|
| Alabama | `ala_publicportal` | `stg_dockets`, `stg_docket_entries`, `stg_docket_parties`, `stg_opinion_clusters`, `stg_opinions`, `stg_oral_arguments`, `stg_historical_release_lists` |
| Connecticut | `conn_jud_ct_gov` | `stg_dockets`, `stg_docket_entries`, `stg_docket_parties`, `stg_opinion_clusters`, `stg_opinions`, `stg_oral_arguments` |

---

## Layer 3: Standardized Tables (SQLMesh)

These tables live in the `courtlistener` schema. They closely mirror CL's Django models, with the addition of provenance and version clock columns. Each table unions data from all per-scraper `stg_*` tables and maps fields to the CL schema.

### Common Columns (on all standardized tables)

| Column | Type | Description |
|---|---|---|
| `provenance_id` | `BIGINT NOT NULL` | Composite PK part 1. FK to `warehouse.provenance` |
| `record_id` | `BIGINT NOT NULL` | Composite PK part 2. Source record identifier |
| `warehouse_version` | `INTEGER` | Incremented on each warehouse update |
| `courtlistener_version` | `INTEGER` | Last warehouse_version exported to CL (0 = never exported) |
| `courtlistener_id` | `BIGINT` | CL Django PK (NULL until first export, updated on export) |
| `date_created` | `TIMESTAMPTZ` | When first inserted into warehouse |
| `date_modified` | `TIMESTAMPTZ` | When last modified in warehouse |

### `courtlistener.dockets`

Maps to CL's `Docket` model.

| Column | Type | CL Field | Notes |
|---|---|---|---|
| `court_id` | `TEXT` | `court_id` (FK) | CL court identifier |
| `docket_number` | `TEXT` | `docket_number` | Full docket number |
| `docket_number_core` | `TEXT` | `docket_number_core` | Distilled docket number (federal) |
| `case_name` | `TEXT` | `case_name` | Standard case name |
| `case_name_short` | `TEXT` | `case_name_short` | Abridged case name |
| `case_name_full` | `TEXT` | `case_name_full` | Full case name |
| `slug` | `TEXT` | `slug` | URL slug |
| `date_filed` | `DATE` | `date_filed` | |
| `date_terminated` | `DATE` | `date_terminated` | |
| `date_last_filing` | `DATE` | `date_last_filing` | |
| `date_argued` | `DATE` | `date_argued` | |
| `date_reargued` | `DATE` | `date_reargued` | |
| `date_reargument_denied` | `DATE` | `date_reargument_denied` | |
| `source` | `SMALLINT` | `source` | DocketSources bitmask |
| `cause` | `TEXT` | `cause` | |
| `nature_of_suit` | `TEXT` | `nature_of_suit` | |
| `assigned_to_str` | `TEXT` | `assigned_to_str` | Judge name as string |
| `referred_to_str` | `TEXT` | `referred_to_str` | |
| `panel_str` | `TEXT` | `panel_str` | |
| `appeal_from_str` | `TEXT` | `appeal_from_str` | Lower court as text |
| `filepath_local` | `TEXT` | `filepath_local` | S3 path if applicable |
| `blocked` | `BOOLEAN` | `blocked` | Default FALSE |

**Primary key**: `(court_id, docket_number)`

### `courtlistener.originating_court_information`

Maps to CL's `OriginatingCourtInformation` model. 1:1 with dockets that have appeal information.

| Column | Type | CL Field |
|---|---|---|
| `court_id` | `TEXT` | FK to `courtlistener.dockets(court_id)` |
| `docket_number` | `TEXT` | FK to `courtlistener.dockets(docket_number)` |
| `lower_court_docket_number` | `TEXT` | `docket_number` (lower court) |
| `court_reporter` | `TEXT` | `court_reporter` |
| `assigned_to_str` | `TEXT` | `assigned_to_str` |
| `ordering_judge_str` | `TEXT` | `ordering_judge_str` |
| `date_filed` | `DATE` | `date_filed` |
| `date_disposed` | `DATE` | `date_disposed` |
| `date_judgment` | `DATE` | `date_judgment` |
| `date_judgment_eod` | `DATE` | `date_judgment_eod` |
| `date_filed_noa` | `DATE` | `date_filed_noa` |
| `date_received_coa` | `DATE` | `date_received_coa` |

**Primary key**: `(court_id, docket_number)` — composite FK to dockets.

### `courtlistener.docket_entries`

Maps to CL's `DocketEntry` model.

| Column | Type | CL Field |
|---|---|---|
| `court_id` | `TEXT` | FK to `courtlistener.dockets(court_id)` |
| `docket_number` | `TEXT` | FK to `courtlistener.dockets(docket_number)` |
| `date_filed` | `DATE` | `date_filed` |
| `time_filed` | `TIME` | `time_filed` |
| `entry_number` | `BIGINT` | `entry_number` |
| `recap_sequence_number` | `TEXT` | `recap_sequence_number` |
| `description` | `TEXT` | `description` |

**Primary key**: `(court_id, docket_number, entry_number)` or `(court_id, docket_number, recap_sequence_number)` for unnumbered entries.

### `courtlistener.opinion_clusters`

Maps to CL's `OpinionCluster` model.

| Column | Type | CL Field |
|---|---|---|
| `court_id` | `TEXT` | FK to `courtlistener.dockets(court_id)` |
| `docket_number` | `TEXT` | FK to `courtlistener.dockets(docket_number)` |
| `date_filed` | `DATE` | `date_filed` |
| `date_filed_is_approximate` | `BOOLEAN` | `date_filed_is_approximate` |
| `case_name` | `TEXT` | `case_name` |
| `case_name_short` | `TEXT` | `case_name_short` |
| `case_name_full` | `TEXT` | `case_name_full` |
| `slug` | `TEXT` | `slug` |
| `judges` | `TEXT` | `judges` |
| `precedential_status` | `TEXT` | `precedential_status` |
| `nature_of_suit` | `TEXT` | `nature_of_suit` |
| `source` | `TEXT` | `source` (e.g., "C" for Court Website) |
| `syllabus` | `TEXT` | `syllabus` |
| `headnotes` | `TEXT` | `headnotes` |
| `summary` | `TEXT` | `summary` |
| `disposition` | `TEXT` | `disposition` |
| `procedural_history` | `TEXT` | `procedural_history` |
| `attorneys` | `TEXT` | `attorneys` |
| `blocked` | `BOOLEAN` | `blocked` |

**Primary key**: `(court_id, docket_number, date_filed)` — a docket may have multiple clusters if opinions are issued on different dates.

### `courtlistener.opinions`

Maps to CL's `Opinion` model.

| Column | Type | CL Field |
|---|---|---|
| `court_id` | `TEXT` | FK to `courtlistener.opinion_clusters` |
| `docket_number` | `TEXT` | FK to `courtlistener.opinion_clusters` |
| `cluster_date_filed` | `DATE` | FK to `courtlistener.opinion_clusters(date_filed)` |
| `type` | `TEXT` | `type` (010combined, 015unamimous, 020lead, etc.) |
| `author_str` | `TEXT` | `author_str` |
| `per_curiam` | `BOOLEAN` | `per_curiam` |
| `joined_by_str` | `TEXT` | `joined_by_str` |
| `sha1` | `TEXT` | `sha1` |
| `page_count` | `INTEGER` | `page_count` |
| `download_url` | `TEXT` | `download_url` |
| `local_path` | `TEXT` | `local_path` (S3 path) |
| `plain_text` | `TEXT` | `plain_text` |
| `html` | `TEXT` | `html` |
| `extracted_by_ocr` | `BOOLEAN` | `extracted_by_ocr` |

**Primary key**: `(court_id, docket_number, cluster_date_filed, type, author_str)` — within a cluster, opinions are identified by type + author.

### `courtlistener.audio`

Maps to CL's `Audio` model.

| Column | Type | CL Field |
|---|---|---|
| `court_id` | `TEXT` | FK to `courtlistener.dockets(court_id)` |
| `docket_number` | `TEXT` | FK to `courtlistener.dockets(docket_number)` |
| `case_name` | `TEXT` | `case_name` |
| `case_name_short` | `TEXT` | `case_name_short` |
| `case_name_full` | `TEXT` | `case_name_full` |
| `judges` | `TEXT` | `judges` |
| `source` | `TEXT` | `source` |
| `download_url` | `TEXT` | `download_url` |
| `local_path_mp3` | `TEXT` | `local_path_mp3` |
| `local_path_original_file` | `TEXT` | `local_path_original_file` |
| `duration` | `INTEGER` | `duration` |
| `date_argued` | `DATE` | Derived from scraper `date_argued` |
| `stt_status` | `SMALLINT` | `stt_status` |
| `blocked` | `BOOLEAN` | `blocked` |

**Primary key**: `(court_id, docket_number, date_argued)` — one oral argument per case per date.

### Standardized Transform Pattern

Each standardized model unions `stg_*` data from all scraper schemas, mapping scraper-specific field names to CL field names. Example for dockets:

```sql
MODEL (
    name courtlistener.dockets,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number)
    ),
    grain (court_id, docket_number)
);

-- Alabama dockets
SELECT
    d.court_id,
    d.case_number AS docket_number,
    NULL AS docket_number_core,
    d.case_name,
    NULL AS case_name_short,
    NULL AS case_name_full,
    NULL AS slug,
    d.date_filed,
    NULL AS date_terminated,
    NULL AS date_last_filing,
    NULL AS date_argued,
    NULL AS date_reargued,
    NULL AS date_reargument_denied,
    2 AS source,  -- SCRAPER
    d.case_classification AS cause,
    NULL AS nature_of_suit,
    NULL AS assigned_to_str,
    NULL AS referred_to_str,
    NULL AS panel_str,
    d.originating_court AS appeal_from_str,
    NULL AS filepath_local,
    FALSE AS blocked,
    d.provenance_id,
    d.record_id,
    1 AS warehouse_version,
    0 AS courtlistener_version,
    NULL::BIGINT AS courtlistener_id,
    NOW() AS date_created,
    NOW() AS date_modified
FROM ala_publicportal.stg_dockets AS d

UNION ALL

-- Connecticut dockets
SELECT
    d.court_id,
    d.docket_id AS docket_number,
    NULL AS docket_number_core,
    d.case_name,
    NULL AS case_name_short,
    NULL AS case_name_full,
    NULL AS slug,
    d.date_filed,
    NULL AS date_terminated,
    NULL AS date_last_filing,
    d.argued_date AS date_argued,
    NULL AS date_reargued,
    NULL AS date_reargument_denied,
    2 AS source,  -- SCRAPER
    NULL AS cause,
    NULL AS nature_of_suit,
    NULL AS assigned_to_str,
    NULL AS referred_to_str,
    d.panel AS panel_str,
    NULL AS appeal_from_str,
    NULL AS filepath_local,
    FALSE AS blocked,
    d.provenance_id,
    d.record_id,
    1 AS warehouse_version,
    0 AS courtlistener_version,
    NULL::BIGINT AS courtlistener_id,
    NOW() AS date_created,
    NOW() AS date_modified
FROM conn_jud_ct_gov.stg_dockets AS d;
```

### Adding a New Scraper to Standardized Tables

When a new scraper is added:

1. Create the scraper's schema with raw tables and staging SQLMesh models (Layer 1 + 2).
2. Add a new `UNION ALL` block to each relevant standardized `courtlistener.*` model, reading from the new scraper's `stg_*` tables.
3. Run `sqlmesh plan` to validate and apply.

---

## Prefect Orchestration

### Flow 1: Scraper Run Flow

One flow per scraper (or court group). Runs on a schedule or on-demand.

```
scraper_run_flow(scraper_id, params)
  │
  ├─ Task: run_scraper(scraper_id, params)
  │    → Executes scraper, produces SQLite artifact
  │    → Uploads SQLite to S3
  │    → Returns S3 path + run metadata
  │
  ├─ Task: validate_run(s3_path)
  │    → Health checks: row counts, date ranges, error rates
  │    → If failed: alert + abort (don't load bad data)
  │
  ├─ Task: create_provenance(scraper_id, run_id, s3_path)
  │    → INSERT into warehouse.provenance
  │    → Returns provenance_id
  │
  ├─ Task: load_to_warehouse(s3_path, provenance_id, scraper_schema)
  │    → Read SQLite, INSERT into <scraper_schema>.raw_* tables
  │    → Tag all rows with provenance_id
  │
  ├─ Task: run_sqlmesh_transforms(scraper_id)
  │    → sqlmesh plan --auto-apply for affected models
  │    → Staging + standardized tables updated
  │
  └─ Task: trigger_export()
       → Enqueue export flow for rows needing sync
```

### Flow 2: Export Flow

Runs after transforms complete, or on a schedule to catch any missed updates.

```
export_flow()
  │
  ├─ Task: find_pending_exports()
  │    → Query courtlistener.* WHERE courtlistener_version < warehouse_version
  │    → Batch into chunks (e.g., 100 rows per chunk)
  │
  ├─ For each chunk:
  │    │
  │    ├─ Task: package_for_celery(chunk)
  │    │    → Convert warehouse rows to Celery task payloads
  │    │    → Map warehouse fields to CL model fields
  │    │
  │    ├─ Task: send_to_celery(payloads)
  │    │    → Enqueue Celery tasks in CL
  │    │    → CL saves → Django signals fire (ES, alerts, webhooks)
  │    │
  │    └─ Task: confirm_export(chunk)
  │         → Wait for Celery task completion
  │         → UPDATE courtlistener_version = warehouse_version
  │         → UPDATE courtlistener_id = Django PK returned by Celery
  │
  └─ Task: report_export_summary()
       → Log: rows exported, failures, duration
```

### Flow 3: Processing Flows

Independent flows for derived data (citations, embeddings, transcriptions). These query the warehouse for items missing their derived data.

```
citation_extraction_flow()
  │
  ├─ Task: find_opinions_missing_citations()
  │    → Query courtlistener.opinions WHERE plain_text IS NOT NULL
  │      AND no matching row in courtlistener.citations
  │
  ├─ For each batch:
  │    ├─ Task: extract_citations(opinions_batch)
  │    └─ Task: write_citations_to_warehouse(citations)
  │
  └─ Task: trigger_export() for updated citation data
```

### Flow 4: Data Excision Flow

On-demand flow to remove data from a faulty scraper run.

```
excise_run_flow(provenance_id)
  │
  ├─ Task: identify_affected_rows(provenance_id)
  │    → Query all tables WHERE provenance_id = ?
  │    → Report: N dockets, M opinions, K entries affected
  │
  ├─ Task: mark_for_deletion(affected_rows)
  │    → Soft-delete in warehouse (set a deleted_at timestamp)
  │    → Increment warehouse_version on affected standardized rows
  │
  ├─ Task: propagate_to_cl(affected_cl_ids)
  │    → Enqueue Celery tasks to delete/update in CL
  │
  └─ Task: archive_evidence(provenance_id)
       → Record the excision in an audit table
       → Update warehouse.provenance metadata with excision details
```

---

## SQLMesh Project Layout

```
sql_processing/
├── config.yaml                          # SQLMesh config with external model declarations
├── models/
│   ├── ala_publicportal/                # Alabama scraper staging models
│   │   ├── stg_dockets.sql
│   │   ├── stg_docket_entries.sql
│   │   ├── stg_docket_parties.sql
│   │   ├── stg_opinion_clusters.sql
│   │   ├── stg_opinions.sql
│   │   ├── stg_oral_arguments.sql
│   │   └── stg_historical_release_lists.sql
│   ├── conn_jud_ct_gov/                 # Connecticut scraper staging models
│   │   ├── stg_dockets.sql
│   │   ├── stg_docket_entries.sql
│   │   ├── stg_docket_parties.sql
│   │   ├── stg_opinion_clusters.sql
│   │   ├── stg_opinions.sql
│   │   └── stg_oral_arguments.sql
│   └── courtlistener/                   # Standardized output models
│       ├── dockets.sql
│       ├── docket_entries.sql
│       ├── originating_court_information.sql
│       ├── opinion_clusters.sql
│       ├── opinions.sql
│       └── audio.sql
├── audits/
│   ├── assert_valid_court_ids.sql       # Court IDs exist in CL's court table
│   ├── assert_no_null_docket_numbers.sql
│   ├── assert_dates_not_future.sql
│   └── assert_version_monotonic.sql     # warehouse_version only increases
├── macros/
│   ├── __init__.py
│   └── version_columns.sql             # Macro for standard version/provenance columns
├── seeds/
│   └── court_ids.csv                    # Valid CL court_id values for audit validation
├── tests/
│   ├── test_ala_dockets_to_standardized.yaml
│   ├── test_conn_dockets_to_standardized.yaml
│   └── test_opinion_cluster_dedup.yaml
└── external_models/
    ├── ala_publicportal.yaml             # Declares ala_publicportal.raw_* as external
    ├── conn_jud_ct_gov.yaml             # Declares conn_jud_ct_gov.raw_* as external
    └── warehouse.yaml                   # Declares warehouse.provenance as external
```

### config.yaml Updates

```yaml
gateways:
  postgres:
    connection:
      type: postgres
      host: localhost
      port: 5433
      user: analytics
      password: analytics
      database: analytics

default_gateway: postgres

model_defaults:
  dialect: postgres
  start: "2026-02-11"
  cron: "@daily"

# Declare raw tables (per-scraper schemas) and warehouse as external
# so SQLMesh knows about them but doesn't try to manage them
external_models:
  - warehouse
  - ala_publicportal   # raw_* tables managed by Prefect loader
  - conn_jud_ct_gov    # raw_* tables managed by Prefect loader
```

---

## Migration Path

### Phase 1: Foundation

1. Create the `warehouse` and `courtlistener` schemas in the analytics DB.
2. Write DDL for `warehouse.provenance`.
3. Implement `raw_table_from_model` factory and `build_raw_table_registry` auto-discovery.
4. Run Alembic to auto-generate and apply the `ala_publicportal` schema + raw tables from the Alabama scraper's type parameters.
5. Implement staging SQLMesh models in `ala_publicportal.stg_*`.
6. Implement standardized SQLMesh models in `courtlistener.*` reading from Alabama staging only.
7. Write audits and tests.

### Phase 2: Second Scraper

1. Add Connecticut scraper; run Alembic to auto-generate `conn_jud_ct_gov` schema + raw tables. Add staging SQLMesh models.
2. Extend standardized `courtlistener.*` models with Connecticut `UNION ALL` blocks.
3. Validate that deduplication works across scrapers (if Alabama and Connecticut ever overlap — unlikely, but the pattern must work for states with overlapping courts).

### Phase 3: Prefect Integration

1. Implement the scraper run flow (run → validate → load → transform).
2. Reify SQLMesh transforms as per-model Prefect tasks using the DAG API — see [SQLMesh + Prefect Integration](docs/sqlmesh_prefect_integration.md).
3. Implement the CL sync worker and export flow — see [Prefect CourtListener Sync Worker](docs/prefect_courtlistener_worker.md).
4. Test end-to-end with Alabama scraper.

### Phase 4: Processing Flows

1. Implement citation extraction flow reading from warehouse.
2. Implement embedding generation flow.
3. Implement transcription flow for oral arguments.

### Phase 5: Scale

1. Onboard additional scrapers (follow the pattern: create schema → raw tables → staging models → union into standardized).
2. Implement the data excision flow.
3. Build data quality dashboards from warehouse data.