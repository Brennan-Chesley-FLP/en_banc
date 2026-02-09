"""Trigger the sqs-listener automation, then send SQS messages every 15s for 2 minutes."""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone

import boto3
import httpx

PREFECT_API = "http://localhost:4200/api"
ENDPOINT = "http://localhost:4566"
QUEUE_NAME = "outbox"
INTERVAL = 15
DURATION = 2 * 60


def emit_trigger_event() -> None:
    """Send the sqs-listener.trigger event via the Prefect REST API."""
    event_id = str(uuid.uuid4())
    payload = [
        {
            "event": "sqs-listener.trigger",
            "resource": {"prefect.resource.id": "sqs.outbox"},
            "occurred": datetime.now(timezone.utc).isoformat(),
            "id": event_id,
        }
    ]
    resp = httpx.post(f"{PREFECT_API}/events", json=payload)
    resp.raise_for_status()
    print(f"Event emitted: id={event_id}")


def main() -> None:
    sqs = boto3.client(
        "sqs",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]

    print("Emitting sqs-listener.trigger event …")
    emit_trigger_event()

    deadline = time.monotonic() + DURATION
    seq = 0
    while time.monotonic() < deadline:
        seq += 1
        body = f"test message #{seq}"
        sqs.send_message(QueueUrl=queue_url, MessageBody=body)
        print(f"Sent: {body}")
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(INTERVAL, remaining))

    print(f"Done — sent {seq} messages.")


if __name__ == "__main__":
    main()
