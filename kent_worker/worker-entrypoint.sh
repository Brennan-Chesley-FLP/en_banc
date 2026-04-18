#!/bin/sh
set -e

# Start Tailscale if auth key is provided
if [ -n "$TS_AUTHKEY" ]; then
    echo "Starting Tailscale..."
    tailscaled --tun=userspace-networking --state=/var/lib/tailscale/tailscaled.state &
    TSPID=$!

    # Wait for tailscaled socket
    for i in $(seq 1 30); do
        if tailscale status >/dev/null 2>&1; then
            break
        fi
        sleep 0.5
    done

    TS_ARGS="--authkey=$TS_AUTHKEY"
    [ -n "$TS_HOSTNAME" ] && TS_ARGS="$TS_ARGS --hostname=$TS_HOSTNAME"

    if tailscale up $TS_ARGS; then
        echo "Tailscale connected as ${TS_HOSTNAME:-$(tailscale status --self | awk '{print $2}')}"
    else
        echo "WARNING: Tailscale failed to connect (continuing without it)"
    fi
fi

RUNS_DIR="${SCRAPER_RUNS_DIR:-/app/runs}"
mkdir -p "$RUNS_DIR"

# Wait for Prefect server to be reachable
echo "Waiting for Prefect server at $PREFECT_API_URL ..."
until curl -sf "$PREFECT_API_URL/health" > /dev/null 2>&1; do
    sleep 2
done
echo "Prefect server is ready"

# Ensure the scraper-pool work pool exists
prefect work-pool create scraper-pool --type in-process 2>/dev/null || true

# Start the kent web UI in the background
echo "Starting kent web UI on 0.0.0.0:8081 (runs_dir=$RUNS_DIR)"
kent serve --runs-dir "$RUNS_DIR" --host 0.0.0.0 --port 8081 &

# Start the in-process Prefect worker
echo "Starting in-process worker (pool=scraper-pool)"
exec python -m workers.in_process
