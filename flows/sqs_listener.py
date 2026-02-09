"""SQS listener flow — polls the outbox queue for 5 minutes, printing messages."""

from __future__ import annotations

import json
import time

import boto3
from prefect import flow
from prefect.blocks.system import Secret
from prefect_aws.credentials import AwsCredentials


@flow(log_prints=True)
async def sqs_listener() -> None:
    aws_creds = await AwsCredentials.aload("localstack-creds")
    raw = (await Secret.aload("outbox")).get()
    queue_config = json.loads(raw) if isinstance(raw, str) else raw

    queue_url = queue_config["queue_url"]
    endpoint = queue_config["endpoint"]

    session = aws_creds.get_boto3_session()
    sqs = session.client("sqs", endpoint_url=endpoint)

    duration = 5 * 60
    deadline = time.monotonic() + duration
    print(f"Polling SQS queue {queue_url} for {duration}s ...")

    while time.monotonic() < deadline:
        remaining = int(deadline - time.monotonic())
        wait = min(20, max(1, remaining))

        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=wait,
        )

        for msg in resp.get("Messages", []):
            print(f"[SQS] {msg['Body']}")
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=msg["ReceiptHandle"],
            )

    print("Listener finished.")


if __name__ == "__main__":
    import asyncio

    asyncio.run(sqs_listener())
