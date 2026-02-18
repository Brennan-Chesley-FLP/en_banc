The Plan
========

This document outlines the planned migration to Prefect-based orchestration
for all data acquisition, cleaning, verification, and monitoring across
Free Law Project's scraping infrastructure.

Today this work is split between two repositories:

- **juriscraper** -- defines scrapers for ~300 courts and data sources
- **courtlistener** -- runs scrapers as Django management commands
  (backfills) or Celery tasks (ongoing scraping), processes results, and
  serves them to users

The goal of **en-banc** is to pull orchestration out of both repositories
into a single Prefect cluster backed by a purpose-built data warehouse,
giving us centralized rate limiting, full provenance tracking, unified
observability, and the ability to trace and excise bad data.

.. contents:: On this page
   :local:
   :depth: 2


Data Warehouse
--------------

The data warehouse is a **Postgres** database that serves as the central
store for all scraped and derived data. It sits between the scrapers and
CourtListener, providing provenance, deduplication, and a clean hand-off
boundary.

Postgres is the right choice here: it's already in the stack (backing
Prefect), handles the hundreds-of-tables scale comfortably, has strong
foreign key and constraint support for the provenance model, and keeps
operational complexity low.

Table-per-Data-Type Design
~~~~~~~~~~~~~~~~~~~~~~~~~~

Each data type produced by each scraper gets its own table. For example,
the Alabama Public Portal scraper that produces opinions and dockets would
write to tables like:

- ``alabama_publicportal_opinions``
- ``alabama_publicportal_dockets``

This per-scraper-per-type granularity means:

- **Schema can match the source.** Each court's website has its own
  quirks -- column names, date formats, extra metadata. The raw table
  captures what the scraper actually produces, without forcing premature
  normalization.
- **Backfills are isolated.** Re-running a scraper writes to its own
  table without risk of colliding with other courts' data.
- **Provenance is structural.** The table name itself tells you where
  the data came from. Combined with the provenance column (see below),
  every row is traceable to a specific run.

Schema Registry
~~~~~~~~~~~~~~~

With ~300 courts and multiple data types each, the warehouse will have
hundreds of tables. A **schema registry** manages this:

- Each scraper declares its output types and columns in a registry
  entry. The extraction step uses this to auto-create or migrate tables
  as scraper outputs evolve.
- The registry links each scraper to a **court record** in a courts
  table. This court table will eventually align with (or be sourced
  from) a broader court database, giving us a unified picture of
  coverage: which courts have scrapers, which data types each scraper
  produces, when each was last run, and where gaps exist.
- Adding a new scraper means registering its court, data types, and
  column definitions. The infrastructure handles table creation,
  provenance wiring, and downstream event routing automatically.

Provenance
~~~~~~~~~~

Every data table includes a ``provenance_id`` foreign key to a shared
**provenance** table. A provenance record captures how a piece of data
arrived in the warehouse:

**Scraper runs** (the primary case):

- Prefect flow run ID
- Scraper name and version
- S3 URI of the SQLite run artifact
- Timestamp range of the run
- Parameters used (date range, court filters, etc.)

**CourtListener-originated data** (for data that enters through CL
first and must propagate to the warehouse):

- Source type (RECAP upload, recap.email, user submission, document
  purchase)
- CL user ID (if applicable)
- Timestamp
- External reference (PACER transaction ID, email message ID, etc.)

This dual-origin provenance model means the warehouse is the single
source of truth regardless of whether data arrived via a scraper or
through CourtListener.

Record Identity
~~~~~~~~~~~~~~~

Docket-level records are identified by a composite natural key:

``(court_id, docket_number, extra_id)``

- ``court_id`` -- foreign key to the courts table (see Schema Registry
  above).
- ``docket_number`` -- the docket number as assigned by the court.
- ``extra_id`` -- a disambiguator, **usually a blank string**. It exists to handle
  real-world collisions where a single court has multiple distinct
  dockets sharing the same docket number due to clerical error, court
  merger/split, or similar anomalies.

Sub-docket records carry the docket composite key plus a type-specific
discriminator:

- **Docket entries** -- ``(court_id, docket_number, extra_id,
  entry_number)``
- **Opinion clusters** -- ``(court_id, docket_number, extra_id,
  date_filed, per_curiam, cluster_seq)`` where ``cluster_seq`` handles
  the rare case of multiple clusters on the same docket and date.
- **Opinions** -- ``(cluster_key, opinion_type)`` since opinion type
  (lead, concurrence, dissent, etc.) is unique within a cluster.

The schema registry declares the natural key for each data type, with
the docket-level triple as the common prefix and type-specific columns
extending it.

Docket Identity in CourtListener (Current State)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

CL's existing unique constraint is
``(MD5(docket_number), pacer_case_id, court_id)``. When
``pacer_case_id`` is present, this maps cleanly to our
``(court_id, docket_number, extra_id='')`` triple -- the constraint
guarantees uniqueness and provides a solid basis for version-vector
synchronization.

However, CL does **not** enforce strict uniqueness in all cases. Known
duplication scenarios and how the warehouse handles them:

- **Appellate dockets without ``pacer_case_id``:** NULL values are
  treated as distinct by Postgres, so multiple dockets with the same
  court + docket_number can coexist when they both lack a PACER case
  ID. The warehouse should apply stricter matching here and surface
  likely duplicates for review.
- **State courts with empty ``docket_number_core``:**
  ``make_docket_number_core()`` only handles federal-style docket
  numbers. For most state courts it returns an empty string, causing
  lookup-by-core to fail and scrapers to create duplicates. (Florida
  DCA: ``5D2023-0888``; Ohio CTA: ``22CA15``.)
- **IDB import heuristic failures:** When the FJC Integrated Database
  import finds multiple matching dockets and case-name similarity is
  below threshold, it creates a new docket rather than picking one.
  The similarity metric may be encodable as a hash in ``extra_id`` to
  disambiguate these (see :ref:`open-questions`).
- **Multi-match fallback:** Both ``find_docket_object()`` and
  ``get_existing_docket()`` handle multiple matches by picking one
  arbitrarily (oldest or first), leaving others in the database. A goal
  of this project is to make this choice explicit -- the warehouse
  should log which CL docket it matched to and why, rather than
  silently picking one.
- **Ohio appellate districts:** The same docket number can appear across
  different Ohio appellate districts; without ``appeal_from_str`` to
  disambiguate, duplicates are created. The warehouse handles this by
  populating ``extra_id`` with the ``appeal_from_str`` value for Ohio
  appellate courts.

The warehouse is an opportunity to **improve on this** rather than
replicate it. Because the warehouse sees data from all sources through a
single extraction path, it can apply stricter matching logic than CL's
fragmented lookup chain. When the warehouse identifies what it believes
are duplicate dockets in CL, it can flag them for human review or
automated merging rather than silently picking one.

This key is the basis for:

- **Deduplication** -- matching incoming scraper data against what's
  already in the warehouse.
- **Delta detection** -- identifying what changed between the current
  warehouse state and the CourtListener database.
- **Version vector synchronization** -- the
  ``(composite_key, dw_version, cl_version)`` tuples used in the bulk
  reconciliation step (see below).
- **Cross-scraper linkage** -- when multiple scrapers cover the same
  court (e.g., one for opinions, one for dockets), the shared composite
  key is how their outputs join.
- **CL dedup surfacing** -- identifying likely-duplicate dockets in CL
  by comparing warehouse records that map to multiple CL docket IDs.

Data Transformation Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Most scrapers follow a regular transformation flow::

    Raw Data (scraper-specific table)
        │
        ▼
    Scraper-Specific Cleaning
        │  Court-specific normalization: date formats, name formats,
        │  encoding issues, known quirks for this source.
        ▼
    Normalized to Common Structure
        │  Map scraper-specific columns to the shared schema
        │  (e.g., ``case_name``, ``date_filed``, ``docket_number``).
        ▼
    Common Cleaning Tasks
        │  Cross-court normalizations: title casing, docket number
        │  formatting, citation extraction from text, etc.
        ▼
    Deduplication & Delta Detection
        │  Compare against the current version in the CourtListener DB.
        │  Identify new records, updated records, and unchanged records.
        ▼
    Export to CourtListener
        Package changes and deliver to CL via Celery tasks so Django
        signals fire normally (ES indexing, alerts, webhooks).

Each stage is a Prefect task within the scraper's processing flow,
giving us visibility into where failures occur and the ability to
retry from any stage.

Event-Driven Downstream Processing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a scraper run downloads new files, it emits Prefect events that
trigger the appropriate downstream processing flows:

- **New opinion PDFs** → OCR / text extraction flow
- **New oral argument audio** → transcription flow (OpenAI Whisper)
- **New document text** → embedding calculation flow
- **New opinion text** → citation extraction flow

These processing flows are independent and run concurrently. Each reads
from the warehouse, processes items missing their derived data, and
writes results back. This "find what's missing" pattern means flows are
idempotent and self-healing -- if a processing flow fails partway
through, the next run picks up where it left off.


CourtListener Integration
--------------------------

Bidirectional Data Flow
~~~~~~~~~~~~~~~~~~~~~~~

Data flows in both directions between the warehouse and CourtListener:

**Warehouse → CourtListener** (the primary direction):

Processed, deduplicated data is packaged and delivered to CL via Celery
tasks. This ensures Django's signal machinery fires as expected -- ES
indexing, search alerts, webhooks, and iQuery sweeps all continue to
work without modification.

**CourtListener → Warehouse** (for CL-originated data):

Some data enters through CourtListener first:

- RECAP browser extension uploads
- recap.email notifications
- User document purchases (pray-and-pay)
- Manual uploads by staff

This data is written to CL's database immediately (for low latency on
user-facing paths), then propagated to the warehouse asynchronously.

Version Vector Synchronization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each record that can be modified by either system carries a two-element
**version vector**: ``(dw_version, cl_version)``. Each component is a
monotonically increasing integer, incremented by the respective system
whenever it writes to the record.

Version vectors live at the **docket level**. A change to any part of
the docket tree (the docket itself, a docket entry, an opinion cluster,
or an opinion) bumps the docket's version. Change packages from the
warehouse to CL are organized as docket trees -- the incremental
ingestion task receives a complete docket with all its children and
applies it atomically. This keeps the protocol simple and avoids
per-record version tracking across four different model types. (See
:ref:`open-questions` for the RECAP docket tree size question.)

The version vector prevents stale overwrites in both directions:

- When CL receives a warehouse update, it compares the incoming
  ``dw_version`` against its local copy. If CL's ``cl_version`` has
  advanced since the warehouse last saw it, CL knows it has local
  changes that the warehouse hasn't acknowledged yet, and rejects the
  update (or merges field-by-field if appropriate).
- When the warehouse receives CL-originated data, it bumps
  ``cl_version`` and holds that as authoritative until the next
  warehouse write (which bumps ``dw_version``).

Consistency is maintained by two mechanisms:

**Incremental ingestion (Celery task):**

A Celery task on the CL side consumes change packages from the
warehouse. For each record, it checks version vectors before applying
the update. If the vectors indicate CL has unseen local changes, the
task skips that record (it will be reconciled in the next cycle after
the warehouse picks up CL's changes). This runs frequently (every few
minutes) and handles the steady-state flow.

**Periodic bulk reconciliation:**

On a slower schedule, CL dumps a lightweight manifest of
``(composite_key, dw_version, cl_version)`` tuples for all records in
the overlap set. The warehouse compares this against its own version
vectors and identifies any divergence -- records where the warehouse
thinks CL should be at version N but CL reports version M. Divergent
records are re-queued for export. This catches any records that slipped
through the incremental path (network errors, task failures, etc.) and
serves as a consistency safety net.

What Stays in Celery
~~~~~~~~~~~~~~~~~~~~

Latency-sensitive and Django-internal tasks remain in Celery:

- **Alerting pipeline** -- search alerts, webhooks, email notifications
- **ES indexing** -- triggered by Django ``post_save`` signals
- **User/account tasks** -- CRM sync, email retries
- **RECAP upload processing** -- user-facing, needs fast turnaround
  (with async warehouse propagation)
- **DW ingestion task** -- the incremental version-vector-aware
  consumer described above


Scraper Architecture
--------------------

Each scraper produces a SQLite database as its run artifact, capturing
every request, response, and decision. This artifact is uploaded to S3
and referenced by the provenance record. The Prefect flow then extracts
data from the SQLite artifact into the warehouse tables.

This design means:

- **Scrapers are decoupled from Django.** They don't need the CL
  codebase, ORM, or database connection to run.
- **Full replay capability.** The SQLite artifact can be re-processed
  to re-extract data without re-scraping.
- **Resume on failure.** A scraper that crashes partway through can
  resume from its SQLite checkpoint.

Standard Scraper Flow
~~~~~~~~~~~~~~~~~~~~~

Every scraper follows this Prefect flow structure:

1. **Scrape** -- run the scraper, producing the SQLite artifact
2. **Upload artifact** -- push the SQLite DB to S3
3. **Extract to warehouse** -- read from SQLite, write to
   scraper-specific warehouse tables
4. **Transform** -- clean, normalize, deduplicate
5. **Emit events** -- notify downstream processing flows of new files
6. **Health check** -- verify expected vs actual item counts, date
   coverage, content quality

Rate Limiting
~~~~~~~~~~~~~

Prefect's concurrency controls replace the current patchwork of
per-command Redis semaphores:

- **Per-court concurrency** for court websites (one scraper at a time
  per court)
- **PACER concurrency** across all PACER-touching flows (scrapers,
  iQuery, nightly updates, user-initiated fetches)
- **Per-API-provider limits** for OpenAI, Internet Archive, etc.
- **Dashboard visibility** into current utilization across all rate
  limits


Migration Strategy
------------------

The migration happens incrementally, court by court:

1. **New scrapers** (like the Alabama backfill already in this repo)
   are built directly as Prefect flows writing to the warehouse.
2. **Existing scrapers** are wrapped -- the current Juriscraper-based
   scraper runs as an opaque Prefect task, with extraction into the
   warehouse as a follow-up step.
3. **Processing flows** (citations, embeddings, transcription) migrate
   once the warehouse has enough data to justify them.
4. **CL consumption** is added per data type as the warehouse tables
   stabilize.

During the transition both systems run in parallel. CL continues to
scrape via its existing commands while en-banc takes over court by
court. The warehouse's provenance tracking makes it safe to run both --
we can always tell which system produced a given piece of data.


.. _open-questions:

Open Questions
--------------

Linking Opinions to Dockets
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Scrapers produce opinions that need to be associated with dockets, but
the opinion's natural key includes fields beyond the docket composite
key (date filed, opinion type, etc.). The question is whether the
opinion-to-docket linkage can be resolved **post-normalization** using
only the common fields (court, docket number, date), or whether it
requires **court-specific logic** to handle courts that structure their
opinion pages differently from their docket pages.

This needs data analysis: for a representative sample of courts, check
whether normalized docket numbers from opinion scrapes match normalized
docket numbers from docket scrapes. If they diverge, the schema registry
will need per-court join hints.

IDB Disambiguation via ``extra_id``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When the FJC Integrated Database import creates docket duplicates (due to
heuristic case-name matching failing), the similarity score is the only
differentiator. It may be possible to encode a disambiguating hash into
``extra_id`` -- e.g., a stable hash of normalized case name components
that the IDB importer can compute and that the warehouse can match on.

This needs investigation: examine a sample of IDB-created duplicate
dockets in CL to determine whether a computable hash would reliably
separate true duplicates from distinct cases with the same docket number.

How Big Do RECAP Docket Trees Get?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Version vectors operate at the docket level, meaning a change to any
child record (docket entry, opinion, etc.) bumps the docket's version
and causes the entire tree to be re-exported as a change package. This
is simple and correct, but could be expensive for very large RECAP
docket trees.

Some federal cases (e.g., mass torts, bankruptcy mega-cases) may have
thousands of docket entries. If a single RECAP upload adds one entry to a
5,000-entry docket, the change package for that version bump includes
the full tree. This needs measurement: query CL's database for the
distribution of docket entry counts per docket, focusing on the upper
tail. If the 99th percentile is manageable (hundreds of entries), the
docket-level approach is fine. If mega-dockets are common enough to
matter, we may need either:

- **Incremental child-level change packages** within the docket version
  (the version still bumps at the docket level, but the package only
  includes changed children).
- **A size threshold** that switches large dockets to child-level
  version vectors.

Schema Evolution Strategy
~~~~~~~~~~~~~~~~~~~~~~~~~

Court websites change their structure regularly. The per-scraper tables
isolate this, but the normalization step ("map to common structure")
needs to handle missing columns gracefully. Should the warehouse use:

- **Strict schemas with migrations** -- the schema registry defines
  exact column types, and scraper output changes require a registry
  update and table migration?
- **Flexible hybrid** -- typed columns for common fields, plus a JSONB
  column for scraper-specific extras that haven't been promoted to the
  common schema yet?

The hybrid approach is more forgiving during rapid scraper development
but makes downstream queries less predictable.

Processing Flow Scalability
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The "find what's missing" pattern (e.g., "opinions missing citations")
is great for idempotency but can be expensive at scale. What indexing
strategy should the warehouse use?

Options include a ``processing_status`` JSONB column per record (keyed
by processing stage), a separate tracking table per stage, or
materialized views. The choice depends on how many processing stages
exist and how frequently they run.
