# Per-scraper deployment customization

Each JKent scraper gets a Prefect deployment. By default the Pulumi program
(`infrastructure/__main__.py`) builds it from a computed **skeleton**: the
deployment is named after the scraper's schema, routed to the `browser-pool` or
`scraper-pool` by whether it needs a browser, tagged with its transport and
CourtListener courts, and seeded with the base `scraper_path` / `scraper_schema`
parameters.

To customize a deployment, drop a TOML file here named after the scraper's
**schema** (the same slug used for the deployment name, work queue, and S3
prefix — e.g. `ca_app.toml`). When present it is **deep-merged over the
skeleton**: scalar keys the TOML sets win, `parameters` merge key-wise, and
`tags` are unioned. If no file exists the skeleton is used unchanged.

The loader lives in [`flows/deployment_config.py`](../../flows/deployment_config.py)
and is pure/importable; it validates the TOML at `pulumi up` time.

## Fields

All fields are optional. Structural fields (`flow_id`, `entrypoint`, `path`) are
always Pulumi-controlled and cannot be set here — a TOML can't repoint a
deployment at different code. `scraper_path` / `scraper_schema` are always
re-injected into `parameters`, so a TOML can't run a different scraper than the
one it's named for.

| Key | Type | Notes |
| --- | --- | --- |
| `description` | string | Deployment description (markdown ok). |
| `version` | string | Deployment version label. |
| `work_pool_name` | string | Overrides the `needs_browser` routing default. |
| `work_queue_name` | string | Defaults to the schema; queue lives under the resolved pool. |
| `concurrency_limit` | int | Per-scraper work-queue concurrency. Defaults to the `scraperConcurrency` Pulumi config. **Keep at 1** if you rely on the finalize step advancing speculative cursors without a monotonic guard. |
| `enforce_parameter_schema` | bool | Default `true`. |
| `paused` | bool | Default `false`. |
| `job_variables` | table | Infrastructure overrides (JSON-encoded for the API). |
| `tags` | array[string] | Unioned onto the computed `en-banc` / transport / `court:*` tags. |
| `[parameters]` | table | Default flow-run parameters. This is where `seed_params` (with `[key]` refs) go. |
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
description = "CA appellate — daily maintenance scrape"
tags = ["maintenance"]
concurrency_limit = 1

[parameters]
seed_params = [
  { dockets_by_number = { docket_number = "[ca_app_scraper__calctapp_1st]" } },
  { dockets_by_number = { docket_number = "[ca_app_scraper__calctapp_2nd]" } },
]

[[schedules]]
cron = "0 6 * * *"
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
