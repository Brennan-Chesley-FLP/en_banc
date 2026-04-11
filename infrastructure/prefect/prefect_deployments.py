import os
import json

import pulumi
import pulumi_prefect as prefect

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
    type="process",
)

sync_pool = prefect.WorkPool(
    "sync-pool",
    name="sync-pool",
    type="process",
)

# ---------------------------------------------------------------------------
# Prefect deployments
# ---------------------------------------------------------------------------

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