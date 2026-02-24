import json
import os

import pulumi
import pulumi_aws as aws
import pulumi_prefect as prefect

# ---------------------------------------------------------------------------
# AWS resources (LocalStack)
# ---------------------------------------------------------------------------

s3_buckets = ["scrapers", "emails"]
sns_topics = []
sqs_queues = ["outbox"]

buckets = {}
for name in s3_buckets:
    buckets[name] = aws.s3.BucketV2(name, bucket=name)

topics = {}
for name in sns_topics:
    topics[name] = aws.sns.Topic(name, name=name)

queues = {}
for name in sqs_queues:
    queues[name] = aws.sqs.Queue(name, name=name)

# ---------------------------------------------------------------------------
# Prefect blocks
# ---------------------------------------------------------------------------

aws_creds_block = prefect.Block(
    "aws-credentials",
    name="localstack-creds",
    type_slug="aws-credentials",
    data=json.dumps(
        {
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
            "region_name": "us-east-1",
        }
    ),
)

for name, bucket in buckets.items():
    prefect.Block(
        f"s3-{name}",
        name=name,
        type_slug="s3-bucket",
        data=pulumi.Output.all(bucket.bucket, aws_creds_block.id).apply(
            lambda args, n=name: json.dumps(
                {
                    "bucket_name": args[0],
                    "credentials": {"$ref": {"block_document_id": args[1]}},
                }
            )
        ),
    )

for name, topic in topics.items():
    prefect.Block(
        f"sns-{name}",
        name=name,
        type_slug="secret",
        data=topic.arn.apply(
            lambda arn: json.dumps(
                {"value": json.dumps({"topic_arn": arn, "endpoint": "http://mini.bopp-justice.ts.net:4566"})}
            )
        ),
    )

for name, queue in queues.items():
    prefect.Block(
        f"sqs-{name}",
        name=name,
        type_slug="secret",
        data=queue.url.apply(
            lambda url: json.dumps(
                {"value": json.dumps({"queue_url": url, "endpoint": "http://mini.bopp-justice.ts.net:4566"})}
            )
        ),
    )

analytics_db_block = prefect.Block(
    "analytics-db",
    name="analytics",
    type_slug="sqlalchemy-connector",
    data=json.dumps(
        {
            "connection_info": {
                "driver": "postgresql+psycopg2",
                "database": "analytics",
                "username": "analytics",
                "password": "analytics",
                "host": "eb-analytics",
                "port": 5432,
            }
        }
    ),
)

# ---------------------------------------------------------------------------
# Prefect deployments
# ---------------------------------------------------------------------------

hello_flow = prefect.Flow(
    "hello-flow",
    name="hello-flow",
    tags=["en-banc"],
)

ts_authkey = os.environ.get("TS_AUTHKEY", "")

docker_job_variables = json.dumps({
    "image": "localhost/en-banc:latest",
    "image_pull_policy": "Never",
    "network_mode": "host",
    "env": {
        "PREFECT_API_URL": "http://localhost:4200/api",
        "TS_AUTHKEY": ts_authkey,
    },
    "container_create_kwargs": {
        "cap_add": ["NET_ADMIN", "NET_RAW"],
        "devices": ["/dev/net/tun:/dev/net/tun"],
    },
})

prefect.Deployment(
    "hello-flow",
    name="hello-flow",
    flow_id=hello_flow.id,
    entrypoint="hello.py:hello_flow",
    path="/app",
    work_pool_name="docker-pool",
    job_variables=docker_job_variables,
    tags=["en-banc"],
)

scraper_run_flow = prefect.Flow(
    "scraper-run-flow",
    name="scraper-run",
    tags=["en-banc", "scraper"],
)

scraper_run_parameter_schema = json.dumps({
    "title": "Parameters",
    "type": "object",
    "properties": {
        "scraper_path": {
            "position": 0,
            "title": "scraper_path",
            "type": "string",
        },
        "seed_params": {
            "anyOf": [
                {
                    "items": {
                        "additionalProperties": {
                            "additionalProperties": True,
                            "type": "object",
                        },
                        "type": "object",
                    },
                    "type": "array",
                },
                {"type": "null"},
            ],
            "default": None,
            "position": 1,
            "title": "seed_params",
        },
        "scraper_schema": {
            "default": "",
            "position": 2,
            "title": "scraper_schema",
            "type": "string",
        },
    },
    "required": ["scraper_path"],
})

scraper_run_deployment = prefect.Deployment(
    "scraper-run",
    name="scraper-run",
    flow_id=scraper_run_flow.id,
    entrypoint="flows/scraper_run.py:scraper_run_flow",
    path="/app",
    work_pool_name="scraper-pool",
    parameter_openapi_schema=scraper_run_parameter_schema,
    enforce_parameter_schema=False,
    tags=["en-banc", "scraper"],
)

sqlmesh_flow = prefect.Flow(
    "sqlmesh-transforms-flow",
    name="sqlmesh-transforms",
    tags=["en-banc", "transforms"],
)

sqlmesh_deployment = prefect.Deployment(
    "sqlmesh-transforms",
    name="sqlmesh-transforms",
    flow_id=sqlmesh_flow.id,
    entrypoint="flows/sqlmesh_tasks.py:sqlmesh_transforms",
    path="/app",
    work_pool_name="docker-pool",
    job_variables=docker_job_variables,
    tags=["en-banc", "transforms"],
)

sqs_listener_flow = prefect.Flow(
    "sqs-listener-flow",
    name="sqs-listener",
    tags=["en-banc"],
)

sqs_listener_deployment = prefect.Deployment(
    "sqs-listener",
    name="sqs-listener",
    flow_id=sqs_listener_flow.id,
    entrypoint="flows/sqs_listener.py:sqs_listener",
    path="/app",
    work_pool_name="docker-pool",
    job_variables=docker_job_variables,
    tags=["en-banc"],
)

follow_up_flow = prefect.Flow(
    "follow-up-flow",
    name="follow-up",
    tags=["en-banc"],
)

follow_up_deployment = prefect.Deployment(
    "follow-up",
    name="follow-up",
    flow_id=follow_up_flow.id,
    entrypoint="flows/follow_up.py:follow_up",
    path="/app",
    work_pool_name="docker-pool",
    job_variables=docker_job_variables,
    tags=["en-banc"],
)

sync_warehouse_flow = prefect.Flow(
    "sync-warehouse-flow",
    name="sync-warehouse",
    tags=["en-banc", "sync"],
)

sync_warehouse_deployment = prefect.Deployment(
    "sync-warehouse",
    name="sync-warehouse",
    flow_id=sync_warehouse_flow.id,
    entrypoint="cl/sync/flows.py:sync_warehouse",
    path="/opt/courtlistener",
    work_pool_name="sync-pool",
    tags=["en-banc", "sync"],
)

# ---------------------------------------------------------------------------
# Prefect concurrency limits (one per scraper schema, limit=1)
# ---------------------------------------------------------------------------

for schema in ["ala_publicportal", "conn_jud_ct_gov"]:
    prefect.GlobalConcurrencyLimit(
        f"scraper-{schema}",
        name=f"scraper:{schema}",
        limit=1,
        active=True,
    )

# ---------------------------------------------------------------------------
# Prefect automations
# ---------------------------------------------------------------------------

# Automation 1: custom event → run SQS listener (4-min debounce)
prefect.Automation(
    "sqs-listener-trigger",
    name="sqs-listener-trigger",
    enabled=True,
    trigger=prefect.AutomationTriggerArgs(
        event=prefect.AutomationTriggerEventArgs(
            posture="Reactive",
            expects=["sqs-listener.trigger"],
            threshold=1,
            within=240,
        ),
    ),
    actions=[
        prefect.AutomationActionArgs(
            type="run-deployment",
            source="selected",
            deployment_id=sqs_listener_deployment.id,
        ),
    ],
)

# Automation 2: SQS listener completes → run follow-up
prefect.Automation(
    "follow-up-trigger",
    name="follow-up-trigger",
    enabled=True,
    trigger=prefect.AutomationTriggerArgs(
        event=prefect.AutomationTriggerEventArgs(
            posture="Reactive",
            match=sqs_listener_deployment.id.apply(
                lambda id: json.dumps(
                    {"prefect.resource.id": f"prefect.deployment.{id}"}
                )
            ),
            expects=["prefect.flow-run.Completed"],
            threshold=1,
            within=0,
        ),
    ),
    actions=[
        prefect.AutomationActionArgs(
            type="run-deployment",
            source="selected",
            deployment_id=follow_up_deployment.id,
        ),
    ],
)

# Automation 3: scrape.completed (Alabama) → run sqlmesh-transforms
prefect.Automation(
    "scrape-completed-ala",
    name="scrape-completed-ala-publicportal",
    enabled=True,
    trigger=prefect.AutomationTriggerArgs(
        event=prefect.AutomationTriggerEventArgs(
            posture="Reactive",
            match=json.dumps(
                {"prefect.resource.id": "scraper.ala_publicportal"}
            ),
            expects=["scrape.completed"],
            threshold=1,
            within=0,
        ),
    ),
    actions=[
        prefect.AutomationActionArgs(
            type="run-deployment",
            source="selected",
            deployment_id=sqlmesh_deployment.id,
            parameters=json.dumps({"scraper_schema": "ala_publicportal"}),
        ),
    ],
)

# Automation 4: scrape.completed (Connecticut) → run sqlmesh-transforms
prefect.Automation(
    "scrape-completed-conn",
    name="scrape-completed-conn-jud-ct-gov",
    enabled=True,
    trigger=prefect.AutomationTriggerArgs(
        event=prefect.AutomationTriggerEventArgs(
            posture="Reactive",
            match=json.dumps(
                {"prefect.resource.id": "scraper.conn_jud_ct_gov"}
            ),
            expects=["scrape.completed"],
            threshold=1,
            within=0,
        ),
    ),
    actions=[
        prefect.AutomationActionArgs(
            type="run-deployment",
            source="selected",
            deployment_id=sqlmesh_deployment.id,
            parameters=json.dumps({"scraper_schema": "conn_jud_ct_gov"}),
        ),
    ],
)

# Automation 5: sync.prepared → trigger CL sync-warehouse
prefect.Automation(
    "sync-prepared-trigger",
    name="sync-prepared-trigger",
    enabled=True,
    trigger=prefect.AutomationTriggerArgs(
        event=prefect.AutomationTriggerEventArgs(
            posture="Reactive",
            expects=["sync.prepared"],
            threshold=1,
            within=0,
        ),
    ),
    actions=[
        prefect.AutomationActionArgs(
            type="run-deployment",
            source="selected",
            deployment_id=sync_warehouse_deployment.id,
        ),
    ],
)

# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

for name, bucket in buckets.items():
    pulumi.export(f"s3_{name}", bucket.bucket)

for name, topic in topics.items():
    pulumi.export(f"sns_{name}", topic.arn)

for name, queue in queues.items():
    pulumi.export(f"sqs_{name}", queue.url)
