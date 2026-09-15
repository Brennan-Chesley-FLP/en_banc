#!/bin/sh
set -e

RUNS_DIR="${SCRAPER_RUNS_DIR:-/app/runs}"
mkdir -p "$RUNS_DIR"

# Browser image only (INSTALL_BROWSERS=1 installs Xvfb): bring up the shared X
# display. jkent's camoufox engine gives each headed browser its own private
# Xvfb (:101 and up), so this one is the fallback — CloudflareHandler's
# OS-level click aims at $DISPLAY when a browser has no private display, and a
# headed Playwright browser needs one just to launch.
if command -v Xvfb >/dev/null 2>&1; then
    DISPLAY="${DISPLAY:-:99}"
    GEOMETRY="${SCREEN_GEOMETRY:-1920x1080x24}"
    export DISPLAY
    # No X server can be running yet this early in a start, so any lock is
    # stale — left by a crash before a `restart`, which keeps the container's
    # /tmp. Xvfb refuses a locked display number.
    rm -f /tmp/.X*-lock /tmp/.X11-unix/X*
    echo "Starting Xvfb on $DISPLAY ($GEOMETRY)"
    Xvfb "$DISPLAY" -screen 0 "$GEOMETRY" -nolisten tcp >/tmp/xvfb.log 2>&1 &
    tries=0
    until xdpyinfo >/dev/null 2>&1; do
        tries=$((tries + 1))
        if [ "$tries" -ge 40 ]; then
            echo "ERROR: Xvfb never came up on $DISPLAY:" >&2
            tail -20 /tmp/xvfb.log >&2 || true
            exit 1
        fi
        sleep 0.25
    done
    echo "X display $DISPLAY ready (xdotool: $(command -v xdotool || echo MISSING))"
fi

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
