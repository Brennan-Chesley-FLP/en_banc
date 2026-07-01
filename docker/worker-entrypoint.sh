#!/bin/sh
set -e

RUNS_DIR="${SCRAPER_RUNS_DIR:-/app/runs}"
mkdir -p "$RUNS_DIR"

# Wait for the Prefect server to be reachable.
echo "Waiting for Prefect server at $PREFECT_API_URL ..."
until curl -sf "$PREFECT_API_URL/health" > /dev/null 2>&1; do
    sleep 2
done
echo "Prefect server is ready"

# Which pool this worker serves (browser scrapers run on their own pool).
# Must match WORKER_POOL_NAME read by workers.in_process and the pool the
# Pulumi deployments target.
POOL_NAME="${WORKER_POOL_NAME:-scraper-pool}"

# Ensure the in-process work pool exists (idempotent).
prefect work-pool create "$POOL_NAME" --type in-process 2>/dev/null || true

# Start the in-process Prefect worker.
echo "Starting in-process worker (pool=$POOL_NAME)"
exec python -m workers.in_process
