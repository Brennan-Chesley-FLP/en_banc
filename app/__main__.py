"""App-layer resources: S3 buckets, Prefect blocks, work pools, deployments."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import json

import pulumi
import pulumi_prefect as prefect

from infrastructure.databases import DATABASES
from infrastructure.prefect.prefect_aws_resources import buckets
import infrastructure.prefect.prefect_deployments  # noqa: F401

# ---------------------------------------------------------------------------
# S3 bucket exports
# ---------------------------------------------------------------------------

for name, bucket in buckets.items():
    pulumi.export(f"s3_{name}", bucket.bucket)

# ---------------------------------------------------------------------------
# Prefect blocks
# ---------------------------------------------------------------------------

aws_creds = {
    "aws_access_key_id": "test",
    "aws_secret_access_key": "test",
    "region_name": "us-east-1",
}

# Chain all blocks sequentially to avoid Prefect server concurrency bugs
# with ENCRYPTION_KEY initialization.
prev_block = None


def _block_opts():
    if prev_block:
        return pulumi.ResourceOptions(depends_on=[prev_block])
    return None


prev_block = prefect.Block(
    "aws-credentials",
    name="localstack-creds",
    type_slug="aws-credentials",
    data=json.dumps(aws_creds),
)

for name, bucket in buckets.items():
    prev_block = prefect.Block(
        f"s3-{name}",
        name=name,
        type_slug="s3-bucket",
        data=bucket.bucket.apply(
            lambda b, n=name: json.dumps(
                {
                    "bucket_name": b,
                    "credentials": aws_creds,
                }
            )
        ),
        opts=_block_opts(),
    )

# ---------------------------------------------------------------------------
# Database blocks (SQLAlchemy connectors)
# ---------------------------------------------------------------------------

config = pulumi.Config()
pg_password = config.get("pg-password") or "postgres"

for db_name, db_config in DATABASES.items():
    aliases = db_config.get("aliases", [db_config.get("alias", db_name)])
    alias = aliases[0]
    block_name = db_name.replace("_", "-")
    prev_block = prefect.Block(
        f"db-{db_name}",
        name=block_name,
        type_slug="sqlalchemy-connector",
        data=json.dumps(
            {
                "driver": "postgresql+psycopg2",
                "database": db_name,
                "username": "postgres",
                "password": pg_password,
                "host": alias,
                "port": 5432,
            }
        ),
        opts=_block_opts(),
    )
