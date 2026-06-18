"""Pulumi program for the en-banc Prefect + SeaweedFS setup.

Provisions:

* Two S3 buckets on SeaweedFS — ``scrapes`` (scrape DB artifacts) and
  ``files`` (downloaded files).
* Prefect blocks — an ``aws-credentials`` block pointed at the SeaweedFS
  endpoint, plus an ``s3-bucket`` block per bucket.
* The ``scraper-run`` flow and its deployment on the ``scraper-pool``
  (in-process) work pool. The pool itself is created by the worker container's
  entrypoint.
"""

import json
import re
import sys
from pathlib import Path

import pulumi
import pulumi_aws as aws
import pulumi_prefect as prefect

# Make the en-banc repo root importable so we can reuse the same scraper
# discovery / limit-naming the flow uses.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from flows.scrapers import (  # noqa: E402
    discover_scraper_paths,
    scraper_limit_name,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

config = pulumi.Config()
s3_endpoint = config.get("s3Endpoint") or "http://mini.bopp-justice.ts.net:8333"
# Max simultaneous runs allowed per JKent scraper.
scraper_concurrency = config.get_int("scraperConcurrency") or 1
s3_access_key = config.get("s3AccessKey") or "en-banc"
s3_secret_key = config.get_secret("s3SecretKey") or pulumi.Output.secret(
    "en-banc-secret"
)

# SeaweedFS speaks the S3 API but isn't AWS: skip the account/credential
# probes and force path-style addressing.
s3_provider = aws.Provider(
    "seaweedfs",
    region="us-east-1",
    access_key=s3_access_key,
    secret_key=s3_secret_key,
    skip_credentials_validation=True,
    skip_requesting_account_id=True,
    skip_metadata_api_check=True,
    s3_use_path_style=True,
    endpoints=[aws.ProviderEndpointArgs(s3=s3_endpoint)],
)

# ---------------------------------------------------------------------------
# S3 buckets (SeaweedFS)
# ---------------------------------------------------------------------------

# scrapes -> scrape database artifacts; files -> downloaded files.
bucket_names = ["scrapes", "files"]
buckets = {
    name: aws.s3.BucketV2(
        name, bucket=name, opts=pulumi.ResourceOptions(provider=s3_provider)
    )
    for name in bucket_names
}

# ---------------------------------------------------------------------------
# Prefect blocks
# ---------------------------------------------------------------------------

aws_creds_block = prefect.Block(
    "aws-credentials",
    name="seaweedfs-creds",
    type_slug="aws-credentials",
    data=pulumi.Output.all(s3_access_key, s3_secret_key).apply(
        lambda args: json.dumps(
            {
                "aws_access_key_id": args[0],
                "aws_secret_access_key": args[1],
                "region_name": "us-east-1",
                "aws_client_parameters": {
                    "endpoint_url": s3_endpoint,
                    # SeaweedFS requires path-style addressing.
                    "config": {"s3": {"addressing_style": "path"}},
                },
            }
        )
    ),
)

for name, bucket in buckets.items():
    prefect.Block(
        f"s3-{name}",
        name=name,
        type_slug="s3-bucket",
        data=pulumi.Output.all(bucket.bucket, aws_creds_block.id).apply(
            lambda args: json.dumps(
                {
                    "bucket_name": args[0],
                    "credentials": {"$ref": {"block_document_id": args[1]}},
                }
            )
        ),
    )

# ---------------------------------------------------------------------------
# Prefect flow + deployment
# ---------------------------------------------------------------------------

scraper_run_flow = prefect.Flow(
    "scraper-run-flow",
    name="scraper-run",
    tags=["en-banc", "scraper"],
)

scraper_run_parameter_schema = json.dumps(
    {
        "title": "Parameters",
        "type": "object",
        "properties": {
            "scraper_path": {
                "position": 0,
                "title": "scraper_path",
                "type": "string",
                "minLength": 1,
            },
            "scraper_schema": {
                "position": 1,
                "title": "scraper_schema",
                "type": "string",
                "minLength": 1,
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
                "position": 2,
                "title": "seed_params",
            },
        },
        "required": ["scraper_path", "scraper_schema"],
    }
)

prefect.Deployment(
    "scraper-run",
    name="scraper-run",
    flow_id=scraper_run_flow.id,
    entrypoint="flows/scraper_run.py:scraper_run_flow",
    path="/app",
    work_pool_name="scraper-pool",
    parameter_openapi_schema=scraper_run_parameter_schema,
    enforce_parameter_schema=True,
    tags=["en-banc", "scraper"],
)

# ---------------------------------------------------------------------------
# Per-scraper concurrency limits
# ---------------------------------------------------------------------------

# One global concurrency limit per JKent scraper found in juriscraper. The
# scrape flow acquires the matching limit (by name) for the duration of a run,
# so each scraper runs at most `scraper_concurrency` times simultaneously.
scraper_paths = discover_scraper_paths()
pulumi.log.info(f"Discovered {len(scraper_paths)} JKent scraper(s)")

for scraper_path in scraper_paths:
    # Pulumi resource names must be URN-safe; the Prefect limit name keeps the
    # full module:Class path so it matches scraper_limit_name() in the flow.
    resource_name = "limit-" + re.sub(r"[^0-9a-zA-Z]+", "-", scraper_path).strip("-").lower()
    prefect.GlobalConcurrencyLimit(
        resource_name,
        name=scraper_limit_name(scraper_path),
        limit=scraper_concurrency,
        active=True,
    )

# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

for name, bucket in buckets.items():
    pulumi.export(f"s3_{name}", bucket.bucket)
