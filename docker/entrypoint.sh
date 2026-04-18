#!/bin/sh
set -e

if [ -n "$TS_AUTHKEY" ]; then
    tailscaled --state=/var/lib/tailscale/tailscaled.state &

    # Wait for tailscaled to be ready
    for i in $(seq 1 30); do
        if tailscale status >/dev/null 2>&1; then
            break
        fi
        sleep 0.5
    done

    HOSTNAME_ARG=""
    if [ -n "$TS_HOSTNAME" ]; then
        HOSTNAME_ARG="--hostname=${TS_HOSTNAME}"
    fi

    TS_ARGS="--authkey=$TS_AUTHKEY --ephemeral"
    [ -n "$TS_HOSTNAME" ] && TS_ARGS="$TS_ARGS --hostname=$TS_HOSTNAME"

    if tailscale up $TS_ARGS; then
        echo "Tailscale connected as ${TS_HOSTNAME:-$(tailscale status --self | awk '{print $2}')}"
    else
        echo "WARNING: Tailscale failed to connect (continuing without it)"
    fi
fi

exec "$@"
