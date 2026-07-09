# Per-scraper deployment definitions

Each TOML file in this directory is the **complete, explicit definition** of
one Prefect deployment: the Pulumi program (`infrastructure/__main__.py`)
creates one deployment (with its own concurrency limit), plus any schedules and
speculative-cursor Variables per file — and nothing else. No file, no
deployment. Nothing is inferred from the scraper class; routing, tags, and the
identity parameters are all spelled out in the file.

## Work queues are shared priority lanes

Queues are **not** per-scraper. Each pool has a fixed set of shared lanes
(`POOL_QUEUES` in `infrastructure/__main__.py`), ordered by priority (lower
number served first):

| Lane | Priority | Pools |
| --- | --- | --- |
| `high-priority` | 2 | both |
| `ongoing` | 5 | both (**default**) |
| `backfill` | 10 | both |
| `pa_ujs_portal` | 13 | scraper-pool only |
| `south_carolina_appellate` | 17 | scraper-pool only |

A deployment picks a lane with `work_queue_name` (default `ongoing`).
Concurrency is enforced **on the deployment** (`concurrency_limit`, default 1,
with an ENQUEUE collision strategy so excess runs wait rather than cancel) — not
on the lane. That's why most files set neither key: a once-a-week maintenance
scrape belongs on `ongoing` with a limit of 1.

To add a scraper, copy
[`../to_deploy/scraper_deployment_template.toml`](../to_deploy/scraper_deployment_template.toml),
name it after the scraper's **schema** slug (the same slug used for the
deployment name, work queue, and S3 prefix — e.g. `ca_app.toml`), and fill it
out. Drafts wait under `infrastructure/to_deploy/<category>/`; moving a file
into this directory deploys it on the next `pulumi up`.

The loader lives in [`flows/deployment_config.py`](../../flows/deployment_config.py)
and is pure/importable; it validates every TOML at `pulumi up` time.

## Fields

Structural fields (`flow_id`, `entrypoint`, `path`) are always
Pulumi-controlled and cannot be set here — a TOML can't repoint a deployment at
different code. `parameters.scraper_schema` must equal the filename stem, so a
copied file can't deploy under the wrong identity.

### Required

| Key | Type | Notes |
| --- | --- | --- |
| `work_pool_name` | string | `"browser-pool"` if the scraper needs a live browser (jkent's `needs_browser` predicate), else `"scraper-pool"`. Pulumi rejects anything else. |
| `tags` | array[string] | By convention `"en-banc"`, `"scraper"`, the transport (`"http"`/`"browser"`), and one `"court:<id>"` per CourtListener court covered. |
| `[parameters]` | table | Must set `scraper_path` (module:Class import path) and `scraper_schema` (= filename stem). `seed_params` (with `[key]` refs) and other default flow-run parameters go here too. |

### Optional

| Key | Type | Notes |
| --- | --- | --- |
| `work_queue_name` | string | Which shared lane to land on (see the table above). Default `ongoing`. Pulumi rejects a lane the chosen pool doesn't serve. |
| `concurrency_limit` | int | Per-**deployment** concurrency. Default 1. **Keep at 1** if you rely on the finalize step advancing speculative cursors without a monotonic guard. |
| `description` | string | Deployment description (markdown ok). |
| `version` | string | Deployment version label. |
| `enforce_parameter_schema` | bool | Default `true`. |
| `paused` | bool | Default `false`. |
| `job_variables` | table | Infrastructure overrides (JSON-encoded for the API). |
| `[[schedules]]` | array of tables | Each becomes one `DeploymentSchedule`. |
| `[variables.<key>]` | table | Initial cursor for a speculative `[key]` reference (see below). |

### `[[schedules]]`

Each entry must set **exactly one** of `cron`, `interval` (seconds), or `rrule`.
Other allowed keys: `timezone`, `active`, `slug`, `anchor_date`, `day_or`,
`catchup`, `max_active_runs`, `max_scheduled_runs`, and a `parameters` table
(per-schedule overrides, JSON-encoded).

### `[variables.<key>]` — speculative cursors

`seed_params` may reference a persisted speculative cursor with a `"[key]"`
string (see `juriscraper.state.common.params.PersistedSpeculativeRange`). At
seed time the scrape resolves each `[key]` against a Prefect **Variable** of the
same name; when the scrape finalizes, it advances that Variable to the next
starting point.

Declare the initial cursor for every referenced key in a `[variables.<key>]`
table. Pulumi creates each Variable **once** (with `ignore_changes` on its
value, so `pulumi up` never resets a cursor the runtime has advanced). The
loader **fails the `pulumi up`** if `seed_params` reference a key with no
matching table — a missing seed would otherwise be a run-time failure.

The table's contents are the JSON the range validates from: for a `CourtRange`
that means `court_id` + `min`/`soft_max`/`gap`. The `min == soft_max` convention
makes it a pure advance-window cursor.

## Example

```toml
# infrastructure/deployments/ca_app.toml
# Omits work_queue_name (-> ongoing lane) and concurrency_limit (-> 1).
description = "CA appellate — daily maintenance scrape"
work_pool_name = "browser-pool"
tags = ["en-banc", "scraper", "browser", "court:calctapp_1st", "court:calctapp_2nd"]

[parameters]
scraper_path = "juriscraper.state.california.appellatecases_courtinfo_ca_gov.scraper:CaAppScraper"
scraper_schema = "ca_app"
seed_params = [
  { dockets_by_number = { docket_number = "[ca_app_scraper__calctapp_1st]" } },
  { dockets_by_number = { docket_number = "[ca_app_scraper__calctapp_2nd]" } },
]

[[schedules]]
rrule = "FREQ=DAILY;BYHOUR=6;BYMINUTE=0;BYSECOND=0"
timezone = "America/Los_Angeles"
active = true

[variables.ca_app_scraper__calctapp_1st]
court_id = "calctapp_1st"
min = 1
soft_max = 1
gap = 100

[variables.ca_app_scraper__calctapp_2nd]
court_id = "calctapp_2nd"
min = 1
soft_max = 1
gap = 100
```
