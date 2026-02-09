# Pulumi Prefect Provider — Full Resource Reference

Provider version: 2.92.1 (Dec 2025)
Registry: https://www.pulumi.com/registry/packages/prefect/

## Provider Configuration

| Property | Type | Env Var | Description |
|---|---|---|---|
| `endpoint` | str | `PREFECT_API_URL` | API URL (default: `https://api.prefect.cloud`) |
| `apiKey` | str | `PREFECT_API_KEY` | Cloud API key |
| `accountId` | str | `PREFECT_CLOUD_ACCOUNT_ID` | Cloud account UUID |
| `workspaceId` | str | — | Default workspace UUID |
| `basicAuthKey` | str | `PREFECT_BASIC_AUTH_KEY` | Basic auth key |
| `csrfEnabled` | bool | `PREFECT_CSRF_ENABLED` | CSRF protection (default: false) |
| `customHeaders` | str | `PREFECT_CLIENT_CUSTOM_HEADERS` | Custom HTTP headers as JSON |
| `profile` | str | — | Profile name from `~/.prefect/profiles.toml` |
| `profileFile` | str | — | Custom path to profiles file |

---

## Flow

Register a flow with the Prefect server.

```python
prefect.Flow(resource_name,
    name="string",                # Flow name
    tags=["string"],              # Optional tags
    account_id="string",          # Optional account UUID
    workspace_id="string",        # Optional workspace UUID
)
```

**Outputs:** `id`, `created`, `updated`

---

## Deployment

Create a deployment for a registered flow.

```python
prefect.Deployment(resource_name,
    flow_id="string",                    # Required: flow UUID
    name="string",                       # Deployment name
    entrypoint="string",                 # e.g. "flows/main.py:my_flow"
    path="string",                       # Working directory path
    work_pool_name="string",             # Target work pool
    work_queue_name="string",            # Target work queue
    tags=["string"],                     # Tags
    version="string",                    # Version string
    description="string",               # Description
    parameters="string",                # JSON-encoded default parameters
    parameter_openapi_schema="string",   # JSON-encoded parameter schema
    enforce_parameter_schema=False,      # Enforce schema on runs
    job_variables="string",              # JSON-encoded job variable overrides
    paused=False,                        # Pause the deployment
    concurrency_limit=0,                 # Max concurrent runs (mutually exclusive with global_concurrency_limit_id)
    global_concurrency_limit_id="string",# Global limit UUID
    concurrency_options={                # Collision strategy
        "collision_strategy": "string",
    },
    pull_steps=[{                        # Steps to prepare flow code
        "type": "string",               # git_clone | set_working_directory | pull_from_s3 | pull_from_gcs | pull_from_azure_blob_storage
        "repository": "string",         # (git_clone) repo URL
        "branch": "string",             # (git_clone) branch
        "access_token": "string",       # (git_clone) auth token
        "include_submodules": False,     # (git_clone) include submodules
        "credentials": "string",        # credentials block ref
        "directory": "string",          # (set_working_directory) path
        "bucket": "string",             # (pull_from_s3/gcs) bucket name
        "folder": "string",             # (pull_from_*) folder in bucket
        "container": "string",          # (pull_from_azure) container
        "requires": "string",           # pip requirements
    }],
    storage_document_id="string",        # Storage document UUID
    account_id="string",
    workspace_id="string",
)
```

**Outputs:** `id`, `created`, `updated`

---

## DeploymentSchedule

Attach a schedule to a deployment. Supports cron, interval, and rrule.

```python
prefect.DeploymentSchedule(resource_name,
    deployment_id="string",          # Required: deployment UUID
    active=True,                     # Enable/disable
    timezone="string",               # e.g. "America/New_York"

    # One of these three:
    cron="string",                   # Cron expression (e.g. "0 8 * * *")
    interval=0,                      # Interval in seconds
    rrule="string",                  # RFC 5545 rrule

    # Cron-specific
    day_or=True,                     # croniter day/day_of_week behavior

    # Interval-specific
    anchor_date="string",            # ISO 8601 anchor date

    # Optional
    parameters="string",             # JSON-encoded run parameters
    max_scheduled_runs=0,            # Max scheduled runs
    slug="string",                   # Unique identifier
    account_id="string",
    workspace_id="string",
)
```

**Outputs:** `id`, `created`, `updated`

---

## Block

Create a typed configuration block. Schema determined by `type_slug`.

```python
prefect.Block(resource_name,
    type_slug="string",          # Required: block type (e.g. "json", "s3-bucket", "aws-credentials")
    name="string",               # Unique block name
    data="string",               # JSON-encoded payload (schema depends on type_slug)
    data_wo="string",            # Write-only data payload
    data_wo_version=0,           # Version tracker for data_wo
    account_id="string",
    workspace_id="string",
)
```

Use `prefect block type ls` to list slugs. Use `prefect block type inspect <slug>` for schema.

**Common type slugs:**
- `json` — arbitrary JSON (`{"value": {...}}`)
- `aws-credentials` — AWS auth (`{"aws_access_key_id": "...", "aws_secret_access_key": "...", "region_name": "..."}`)
- `s3-bucket` — S3 bucket ref (`{"bucket_name": "...", "credentials": {...}}`)
- `secret` — sensitive string (`{"value": "..."}`)
- `string` — plain string (`{"value": "..."}`)

**Outputs:** `id`, `created`, `updated`

---

## WorkPool

Define a work pool for running flow infrastructure.

```python
prefect.WorkPool(resource_name,
    name="string",               # Pool name
    type="string",               # e.g. "docker", "kubernetes", "process", "prefect:managed"
    description="string",        # Description
    base_job_template="string",  # JSON-encoded base job template
    concurrency_limit=0,         # Max concurrent runs
    paused=False,                # Pause the pool
    account_id="string",
    workspace_id="string",
)
```

**Outputs:** `id`, `created`, `updated`, `default_queue_id`

---

## WorkQueue

Create a queue within a work pool.

```python
prefect.WorkQueue(resource_name,
    work_pool_name="string",     # Parent work pool
    name="string",               # Queue name
    description="string",        # Description
    concurrency_limit=0,         # Max concurrent runs
    priority=0,                  # Queue priority
    is_paused=False,             # Pause the queue
    account_id="string",
    workspace_id="string",
)
```

**Outputs:** `id`, `created`, `updated`

---

## Automation

Event-driven triggers that perform actions.

```python
prefect.Automation(resource_name,
    name="string",
    enabled=True,
    trigger={                           # Trigger configuration
        "event": {                      # Event trigger
            "posture": "Reactive",      # Reactive | Proactive
            "expects": ["event.name"],  # Event names to match
            "threshold": 1,             # Number of events
            "within": 0,               # Time window in seconds
        },
    },
    actions=[{                          # Actions to perform
        "type": "string",              # Action type (see below)
        "source": "string",            # "selected" | "inferred"
        "deployment_id": "string",     # For run-deployment
        "parameters": "string",        # JSON-encoded parameters
        "job_variables": "string",     # JSON-encoded job vars
    }],
    actions_on_triggers=[...],          # Actions on trigger
    actions_on_resolves=[...],          # Actions on resolve
    description="string",
    account_id="string",
    workspace_id="string",
)
```

**Action types:** `do-nothing`, `run-deployment`, `pause-deployment`, `resume-deployment`, `cancel-flow-run`, `change-flow-run-state`, `pause-work-queue`, `resume-work-queue`, `send-notification`, `call-webhook`, `pause-automation`, `resume-automation`, `suspend-flow-run`, `resume-flow-run`, `declare-incident`, `pause-work-pool`, `resume-work-pool`

**Outputs:** `id`, `created`, `updated`

---

## Webhook

Create an inbound webhook endpoint that emits Prefect events.

```python
prefect.Webhook(resource_name,
    template="string",               # JSON template for event (use json.dumps)
    name="string",                   # Webhook name
    description="string",            # Description
    enabled=True,                    # Enable/disable
    service_account_id="string",     # Service account (Pro/Enterprise)
    account_id="string",
    workspace_id="string",
)
```

**Outputs:** `id`, `created`, `updated`, `endpoint` (the full webhook URL)

---

## Variable

Store key-value configuration accessible from flows.

```python
prefect.Variable(resource_name,
    name="string",           # Variable name
    value="any",             # Variable value
    tags=["string"],         # Tags
    account_id="string",
    workspace_id="string",
)
```

**Outputs:** `id`, `created`, `updated`

---

## GlobalConcurrencyLimit

Limit concurrent execution across deployments.

```python
prefect.GlobalConcurrencyLimit(resource_name,
    name="string",                   # Limit name
    limit=0,                         # Max concurrent
    active=True,                     # Enable/disable
    active_slots=0,                  # Current active slots
    slot_decay_per_second=0.0,       # Slot decay rate
    account_id="string",
    workspace_id="string",
)
```

**Outputs:** `id`, `created`, `updated`

---

## TaskRunConcurrencyLimit

Limit concurrent task runs by tag.

```python
prefect.TaskRunConcurrencyLimit(resource_name,
    tag="string",                # Tag to limit
    concurrency_limit=0,         # Max concurrent
    account_id="string",
    workspace_id="string",
)
```

**Outputs:** `id`, `created`, `updated`

---

## ResourceSla

Define SLAs for deployments (completion time, frequency, freshness, lateness).

```python
prefect.ResourceSla(resource_name,
    resource_id="string",        # "prefect.deployment.<uuid>"
    slas=[{
        "name": "string",       # SLA name
        "severity": "string",   # minor | low | moderate | high | critical
        "duration": 0,          # (TimeToCompletion) max seconds
        "stale_after": 0,       # (Frequency) seconds until stale
        "within": 0,            # (Freshness/Lateness) time window
        "expected_event": "",   # (Freshness) event name
        "resource_match": "",   # (Freshness) JSON resource matcher
        "enabled": True,
    }],
    account_id="string",
    workspace_id="string",
)
```

**Outputs:** `id`

---

## Cloud-Only Resources

These resources require Prefect Cloud:

| Resource | Purpose |
|---|---|
| `Account` | Manage accounts |
| `AccountMember` | Manage account members |
| `Workspace` | Create/manage workspaces |
| `WorkspaceAccess` | Workspace access control |
| `WorkspaceRole` | Custom workspace roles |
| `ServiceAccount` | Service accounts for automation |
| `UserApiKey` | API keys for users |
| `Team` | Team management |
| `TeamAccess` | Team access control |
| `BlockAccess` | Block-level access control |
| `DeploymentAccess` | Deployment-level access control |
| `WorkPoolAccess` | Work pool access control |

---

## Data Sources

Lookup existing resources:

```python
pool = prefect.get_work_pool(name="my-pool")
pools = prefect.get_work_pools()
queue = prefect.get_work_queue(name="default", work_pool_name="my-pool")
queues = prefect.get_work_queues(work_pool_name="my-pool")
block = prefect.get_block(name="my-block", type_slug="json")
deploy = prefect.get_deployment(name="my-deploy")
flow_var = prefect.get_variable(name="env")
hook = prefect.get_webhook(name="my-hook")
auto = prefect.get_automation(name="my-automation")
ws = prefect.get_workspace(handle="my-workspace")
acct = prefect.get_account()
role = prefect.get_account_role(name="Admin")
ws_role = prefect.get_workspace_role(name="Worker")
sa = prefect.get_service_account(name="ci-bot")
team = prefect.get_team(name="engineering")
teams = prefect.get_teams()
member = prefect.get_account_member(email="user@example.com")
members = prefect.get_account_members()
meta = prefect.get_worker_metadata()
limit = prefect.get_global_concurrency_limit(name="my-limit")
```
