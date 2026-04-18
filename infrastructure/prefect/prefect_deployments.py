import json

import pulumi
import pulumi_prefect as prefect

config = pulumi.Config()

# ---------------------------------------------------------------------------
# Work pools
# ---------------------------------------------------------------------------

docker_pool = prefect.WorkPool(
    "docker-pool",
    name="docker-pool",
    type="docker",
)

scraper_pool = prefect.WorkPool(
    "scraper-pool",
    name="scraper-pool",
    type="in-process",
)

sync_pool = prefect.WorkPool(
    "sync-pool",
    name="sync-pool",
    type="process",
)

# ---------------------------------------------------------------------------
# Deployments
# ---------------------------------------------------------------------------

ts_authkey = config.get_secret("ts-authkey")

_docker_env = {
    "PREFECT_API_URL": "http://localhost:7100/api",
}
_docker_job_vars = {
    "image": "localhost/en-banc:latest",
    "image_pull_policy": "Never",
    "network_mode": "host",
    "privileged": True,
}

if ts_authkey:
    docker_job_variables = ts_authkey.apply(
        lambda k: json.dumps(
            {**_docker_job_vars, "env": {**_docker_env, "TS_AUTHKEY": k}}
        )
    )
else:
    docker_job_variables = json.dumps(
        {**_docker_job_vars, "env": _docker_env}
    )

# -- scraper-run (kent worker, in-process) --

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
    opts=pulumi.ResourceOptions(depends_on=[scraper_pool]),
)

# -- warehouse-load (docker pool) --

warehouse_load_flow = prefect.Flow(
    "warehouse-load-flow",
    name="warehouse-load",
    tags=["en-banc", "warehouse"],
)

warehouse_load_deployment = prefect.Deployment(
    "warehouse-load",
    name="warehouse-load",
    flow_id=warehouse_load_flow.id,
    entrypoint="flows/warehouse_load.py:warehouse_load_flow",
    path="/app",
    work_pool_name="docker-pool",
    job_variables=docker_job_variables,
    tags=["en-banc", "warehouse"],
    opts=pulumi.ResourceOptions(depends_on=[docker_pool]),
)

# -- sqlmesh-transforms (docker pool) --

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
    opts=pulumi.ResourceOptions(depends_on=[docker_pool]),
)

# -- follow-up (docker pool) --

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
    opts=pulumi.ResourceOptions(depends_on=[docker_pool]),
)

# -- sync-warehouse (sync pool, runs inside CL Django) --

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
    opts=pulumi.ResourceOptions(depends_on=[sync_pool]),
)

# ---------------------------------------------------------------------------
# Concurrency limits (one per scraper schema, limit=1)
# ---------------------------------------------------------------------------

for schema in ["ala_publicportal", "conn_jud_ct_gov"]:
    prefect.GlobalConcurrencyLimit(
        f"scraper-{schema}",
        name=f"scraper:{schema}",
        limit=1,
        active=True,
    )

# ---------------------------------------------------------------------------
# Automations — wire the event-driven pipeline
# ---------------------------------------------------------------------------

# scrape.uploaded → warehouse-load (with event payload as parameters)
prefect.Automation(
    "scrape-uploaded-trigger",
    name="scrape-uploaded-trigger",
    enabled=True,
    trigger=prefect.AutomationTriggerArgs(
        event=prefect.AutomationTriggerEventArgs(
            posture="Reactive",
            expects=["scrape.uploaded"],
            threshold=1,
            within=0,
        ),
    ),
    actions=[
        prefect.AutomationActionArgs(
            type="run-deployment",
            source="selected",
            deployment_id=warehouse_load_deployment.id,
            parameters=json.dumps({
                "s3_uri": "{{ event.payload.s3_uri }}",
                "scraper_schema": "{{ event.payload.scraper_schema }}",
            }),
        ),
    ],
)

# scrape.completed (Alabama) → sqlmesh-transforms
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

# scrape.completed (Connecticut) → sqlmesh-transforms
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

# sync.prepared → sync-warehouse
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
