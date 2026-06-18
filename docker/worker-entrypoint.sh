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

# Ensure the in-process work pool exists (idempotent).
prefect work-pool create scraper-pool --type in-process 2>/dev/null || true

# Start the in-process Prefect worker.
echo "Starting in-process worker (pool=scraper-pool)"
exec python -m workers.in_process
