"""Pulumi program for the en-banc Prefect + SeaweedFS setup.

Provisions:

* Two S3 buckets on SeaweedFS — ``scrapes`` (scrape DB artifacts) and
  ``files`` (downloaded files).
* Prefect blocks — an ``aws-credentials`` block pointed at the SeaweedFS
  endpoint, plus an ``s3-bucket`` block per bucket.
* The ``scraper-run`` flow, plus one deployment and one concurrency-limited
  work queue per JKent scraper, on one of two in-process work pools:
  ``browser-pool`` for scrapers that need a live browser (FF_ALIKE /
  CHROME_ALIKE / JS_EVAL / captcha handlers — see ``scraper_needs_browser``)
  and ``scraper-pool`` for plain-HTTP scrapers. The per-queue concurrency limit
  serializes each scraper at the scheduling layer; the browser worker further
  caps itself to one scrape at a time. The pools themselves are created by the
  worker containers' entrypoints.
"""

import json
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
    scraper_court_ids,
    scraper_needs_browser,
    scraper_schema_name,
)

# Work pools served by the two worker types. Browser scrapers run on the pool
# whose worker has a browser engine installed and runs one scrape at a time;
# everything else runs on the lean HTTP pool. These names must match the
# WORKER_POOL_NAME of the matching docker-compose worker service.
BROWSER_WORK_POOL = "browser-pool"
HTTP_WORK_POOL = "scraper-pool"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

config = pulumi.Config()
s3_endpoint = config.get("s3Endpoint") or "http://mini.bopp-justice.ts.net:8333"
# Max simultaneous runs allowed per JKent scraper. Enforced at the scheduling
# layer via a per-scraper work queue concurrency limit (the server won't
# dispatch more than this many runs of a given scraper to a worker).
scraper_concurrency = config.get_int("scraperConcurrency") or 1
# Provider credentials: used by the AWS provider to create the buckets, so
# they need an admin-capable SeaweedFS identity (CreateBucket).
s3_access_key = config.get("s3AccessKey") or "en-banc"
s3_secret_key = config.get_secret("s3SecretKey") or pulumi.Output.secret(
    "en-banc-secret"
)

# Credentials baked into the Prefect aws-credentials block, i.e. what the
# worker uses for object I/O. Kept separate from the provider creds so the
# worker can run as a least-privilege identity (Read/Write/List on the
# buckets, no admin). Falls back to the provider creds if unset.
prefect_s3_access_key = config.get("prefectS3AccessKey") or s3_access_key
prefect_s3_secret_key = config.get_secret("prefectS3SecretKey") or s3_secret_key

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
    data=pulumi.Output.all(prefect_s3_access_key, prefect_s3_secret_key).apply(
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
# Prefect flow
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

# ---------------------------------------------------------------------------
# Per-scraper deployments + work queues (scheduling-level concurrency)
# ---------------------------------------------------------------------------

# Each JKent scraper gets its own work queue (concurrency-limited) and a
# deployment bound to that queue, with scraper_path/scraper_schema baked in as
# default parameters. Concurrency is enforced when a worker polls for work:
# the server only hands out runs up to a queue's open slots, so excess runs of
# a given scraper stay Scheduled instead of occupying a worker job slot — and
# never head-of-line-block other scrapers whose queues have capacity.
scraper_paths = discover_scraper_paths()
pulumi.log.info(f"Discovered {len(scraper_paths)} JKent scraper(s)")

for scraper_path in scraper_paths:
    schema = scraper_schema_name(scraper_path)

    # Route browser scrapers to the browser pool (whose worker has a browser
    # engine and runs one scrape at a time) and the rest to the HTTP pool, so a
    # browser scraper can never be dispatched to a worker that can't run it.
    needs_browser = scraper_needs_browser(scraper_path)
    work_pool = BROWSER_WORK_POOL if needs_browser else HTTP_WORK_POOL

    # A work queue lives under its pool (``/work_pools/{pool}/queues/{name}``),
    # so moving a scraper between the HTTP and browser pools is a *new* queue,
    # not an in-place edit. Force replacement on a pool change: the provider
    # otherwise PATCHes the existing queue, which both can't change pools and
    # trips its priority-defaults-to-0 update bug (the API rejects priority 0).
    queue = prefect.WorkQueue(
        f"queue-{schema}",
        name=schema,
        work_pool_name=work_pool,
        concurrency_limit=scraper_concurrency,
        opts=pulumi.ResourceOptions(replace_on_changes=["work_pool_name"]),
    )

    # Tag with the CourtListener courts this scraper covers (e.g. ``court:ark``)
    # so deployments are filterable by court in the UI. ``browser`` /
    # ``http`` tags make the transport split filterable too.
    court_tags = [f"court:{c}" for c in scraper_court_ids(scraper_path)]
    transport_tag = "browser" if needs_browser else "http"

    prefect.Deployment(
        f"deploy-{schema}",
        name=schema,
        flow_id=scraper_run_flow.id,
        entrypoint="flows/scraper_run.py:scraper_run_flow",
        path="/app",
        work_pool_name=work_pool,
        work_queue_name=queue.name,
        parameters=json.dumps(
            {"scraper_path": scraper_path, "scraper_schema": schema}
        ),
        parameter_openapi_schema=scraper_run_parameter_schema,
        enforce_parameter_schema=True,
        tags=["en-banc", "scraper", transport_tag, *court_tags],
    )

# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

for name, bucket in buckets.items():
    pulumi.export(f"s3_{name}", bucket.bucket)
