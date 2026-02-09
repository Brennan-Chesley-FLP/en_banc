---
name: pulumi-prefect
description: This skill should be used when the user asks to "create a prefect deployment with pulumi", "manage prefect resources with pulumi", "add a prefect block", "create a work pool in pulumi", "set up prefect automation", "add a deployment schedule", "configure prefect IaC", mentions "pulumi_prefect", "pulumi prefect provider", or discusses managing Prefect infrastructure as code with Pulumi.
version: 1.0.0
---

# Pulumi Prefect Provider

Manage Prefect resources (blocks, deployments, work pools, automations, etc.) as infrastructure-as-code using the Pulumi Prefect provider.

## Installation

The provider is Terraform-based and must be generated locally:

```bash
cd infrastructure
pulumi package add terraform-provider prefecthq/prefect
```

This generates a `pulumi_prefect` Python SDK in the project. Only needed once.

## Provider Configuration

In `Pulumi.yaml`:

```yaml
config:
  # Self-hosted / OSS
  prefect:endpoint: http://localhost:4200/api

  # Prefect Cloud
  # prefect:apiKey: <PREFECT_API_KEY>
  # prefect:accountId: <ACCOUNT_UUID>
  # prefect:workspaceId: <WORKSPACE_UUID>
```

Environment variable alternatives: `PREFECT_API_URL`, `PREFECT_API_KEY`, `PREFECT_CLOUD_ACCOUNT_ID`.

## Quick Reference — Resources

| Resource | Purpose |
|---|---|
| `Flow` | Register a flow |
| `Deployment` | Create a deployment for a flow |
| `DeploymentSchedule` | Attach cron/interval/rrule schedules |
| `Block` | Create typed configuration blocks |
| `WorkPool` | Define work pools (docker, k8s, process, etc.) |
| `WorkQueue` | Create queues within a work pool |
| `Automation` | Event-driven triggers and actions |
| `Webhook` | Inbound webhook endpoints |
| `Variable` | Key-value configuration variables |
| `GlobalConcurrencyLimit` | Cross-deployment concurrency limits |
| `TaskRunConcurrencyLimit` | Tag-based task concurrency limits |
| `ResourceSla` | SLAs for deployments (completion time, frequency) |

For full property reference, see `references/RESOURCES.md`.

## Common Patterns

### Flow + Deployment + Work Pool

```python
import pulumi_prefect as prefect

pool = prefect.WorkPool("my-pool", name="docker-pool", type="docker")

flow = prefect.Flow("my-flow", name="my-flow", tags=["production"])

deployment = prefect.Deployment("my-deploy",
    name="my-deployment",
    flow_id=flow.id,
    entrypoint="flows/main.py:my_flow",
    work_pool_name=pool.name,
    work_queue_name="default",
    tags=["production"],
)
```

### Deployment with Schedule

```python
prefect.DeploymentSchedule("daily",
    deployment_id=deployment.id,
    active=True,
    cron="0 8 * * *",
    timezone="America/New_York",
)
```

### Block (JSON payload)

```python
import json

prefect.Block("my-block",
    name="my-config",
    type_slug="json",
    data=json.dumps({"value": {"key": "val"}}),
)
```

Use `prefect block type ls` to list available slugs. Use `prefect block type inspect <slug>` to see the data schema.

### Block (S3 Bucket — requires prefect-aws)

```python
prefect.Block("my-bucket",
    name="data-bucket",
    type_slug="s3-bucket",
    data=json.dumps({
        "bucket_name": "my-bucket",
        "credentials": {"$ref": {"block_document_id": "creds-block-name"}},
    }),
)
```

### Block (AWS Credentials)

```python
prefect.Block("aws-creds",
    name="aws-credentials",
    type_slug="aws-credentials",
    data=json.dumps({
        "aws_access_key_id": "...",
        "aws_secret_access_key": "...",
        "region_name": "us-east-1",
    }),
)
```

### Automation (event trigger → run deployment)

```python
prefect.Automation("on-event",
    name="trigger-on-upload",
    enabled=True,
    trigger={"event": {
        "posture": "Reactive",
        "expects": ["custom.file.uploaded"],
        "threshold": 1,
        "within": 0,
    }},
    actions=[{
        "type": "run-deployment",
        "source": "selected",
        "deployment_id": deployment.id,
        "parameters": json.dumps({}),
        "job_variables": json.dumps({}),
    }],
)
```

### Webhook (inbound events)

```python
webhook = prefect.Webhook("ingest",
    name="file-upload-hook",
    enabled=True,
    template=json.dumps({"event": "custom.file.uploaded"}),
)
pulumi.export("webhook_endpoint", webhook.endpoint)
```

### Variable

```python
prefect.Variable("env",
    name="environment",
    value="staging",
    tags=["config"],
)
```

### Concurrency Limits

```python
# Global limit across deployments
prefect.GlobalConcurrencyLimit("scraper-limit",
    name="scraper-concurrency",
    limit=5,
    active=True,
)

# Tag-based task limit
prefect.TaskRunConcurrencyLimit("api-limit",
    tag="external-api",
    concurrency_limit=3,
)
```

### Resource SLA

```python
prefect.ResourceSla("deploy-sla",
    resource_id=deployment.id.apply(lambda id: f"prefect.deployment.{id}"),
    slas=[{
        "name": "must-complete-in-5m",
        "duration": 300,
        "severity": "high",
    }],
)
```

## Combining with AWS Resources

A typical pattern is creating AWS resources and corresponding Prefect blocks in the same Pulumi program:

```python
import pulumi
import pulumi_aws as aws
import pulumi_prefect as prefect
import json

bucket = aws.s3.BucketV2("data", bucket="my-data")

prefect.Block("data-block",
    name="data-bucket",
    type_slug="s3-bucket",
    data=bucket.bucket.apply(lambda b: json.dumps({
        "bucket_name": b,
    })),
)
```

Use `.apply()` to pass Pulumi outputs (like ARNs, URLs, bucket names) into block data.

## Data Sources (Lookups)

Use `get_*` functions to reference existing resources:

```python
existing_pool = prefect.get_work_pool(name="production-pool")
existing_workspace = prefect.get_workspace(handle="my-workspace")
```

Available: `get_account`, `get_block`, `get_deployment`, `get_work_pool`, `get_work_pools`, `get_work_queue`, `get_work_queues`, `get_workspace`, `get_variable`, `get_webhook`, `get_team`, `get_teams`, `get_service_account`, `get_worker_metadata`, `get_automation`, `get_global_concurrency_limit`, `get_account_member`, `get_account_members`, `get_account_role`, `get_workspace_role`.

## Import

Existing Prefect resources can be imported by UUID:

```bash
pulumi import prefect:index/block:Block my_block <uuid>
pulumi import prefect:index/deployment:Deployment my_deploy <uuid>
pulumi import prefect:index/workPool:WorkPool my_pool <name>
```
