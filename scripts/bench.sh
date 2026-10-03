#!/usr/bin/env bash
# Builds the example app in release mode and load-tests it with wrk.
# Usage: scripts/bench.sh [connections] [duration]
set -euo pipefail
cd "$(dirname "$0")/.."

CONNS="${1:-256}"
DURATION="${2:-10s}"
PORT="${PORT:-3000}"

command -v wrk >/dev/null || { echo "wrk is required (apt install wrk / brew install wrk)"; exit 1; }

swift build -c release --product oria-example
PORT="$PORT" RATE_LIMIT=1000000000 .build/release/oria-example &
SERVER_PID=$!
trap 'kill -TERM $SERVER_PID 2>/dev/null; wait $SERVER_PID 2>/dev/null || true' EXIT
until curl -fs "http://127.0.0.1:$PORT/" >/dev/null; do sleep 0.2; done

for path in / /json /api/users/1; do
  wrk -t2 -c"$CONNS" -d3s "http://127.0.0.1:$PORT$path" >/dev/null  # warm-up
  echo "=== GET $path ($CONNS connections, $DURATION)"
  wrk -t2 -c"$CONNS" -d"$DURATION" --latency "http://127.0.0.1:$PORT$path" \
    | grep -E "Requests/sec|Latency  |99%|Socket errors|Non-2xx"
done
