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
# Must match the pool the Pulumi deployments target.
POOL_NAME="${WORKER_POOL_NAME:-scraper-pool}"

# Max flow runs this worker executes at once (one subprocess per run). The
# browser worker pins this to 1 (one browser scrape at a time).
CONCURRENCY="${WORKER_CONCURRENCY:-4}"

# Ensure the process work pool exists (idempotent).
prefect work-pool create "$POOL_NAME" --type process 2>/dev/null || true

# Graceful stop. On SIGTERM/SIGINT (podman stop / compose down):
#
#   1. SIGTERM the worker: the prefect CLI's handler halts intake (stops
#      dequeuing new runs) and waits for its flow-run subprocesses to exit. It
#      deliberately does NOT forward the signal to them.
#   2. Broadcast SIGUSR1 to the `python -m prefect.engine` subprocesses: the
#      scrape flow installs a SIGUSR1 handler that triggers its cooperative
#      drain — the in-flight request finishes, jkent finalizes the run DB as
#      interrupted/resumable, and the process exits cleanly. (SIGTERM would
#      instead trip Prefect's own crash-style termination bridge.)
#
# The sweep repeats while engine processes remain, catching runs that were
# still starting up during an earlier pass. The whole drain is bounded by the
# container's stop_grace_period, after which the runtime SIGKILLs everything —
# run DBs are WAL-mode SQLite and resumable even then.
drain() {
    echo "Stop requested: halting intake and draining flow runs..."
    kill -TERM "$WORKER_PID" 2>/dev/null || true
    while pkill -USR1 -f 'python -m prefect\.engine' 2>/dev/null; do
        sleep 5
    done
}
trap drain TERM INT

echo "Starting process worker (pool=$POOL_NAME, limit=$CONCURRENCY)"
prefect worker start --pool "$POOL_NAME" --type process --limit "$CONCURRENCY" &
WORKER_PID=$!

# `wait` returns early when a trapped signal arrives; keep waiting until the
# worker has actually exited so PID 1 (this script) outlives the drain.
while kill -0 "$WORKER_PID" 2>/dev/null; do
    wait "$WORKER_PID" || true
done
