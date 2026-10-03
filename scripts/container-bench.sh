#!/usr/bin/env bash
# Load-tests the example app inside a resource-limited container.
#
#   scripts/container-bench.sh            # build image, run every scenario
#   CPUS=0,1 MEMORY=256m scripts/container-bench.sh
#
# The server container is pinned to $CPUS (default 0,1 = 2 cores) with a $MEMORY cap; the load
# generators run on the remaining cores ($CLIENT_CPUS). --network host avoids docker-proxy/NAT,
# which would otherwise dominate small-request benchmarks.
# Needs: docker, wrk, curl; optional: wrk2 (WRK2=path), h2load (nghttp2-client).
set -euo pipefail
cd "$(dirname "$0")/.."

CPUS="${CPUS:-0,1}"
CLIENT_CPUS="${CLIENT_CPUS:-2,3}"
MEMORY="${MEMORY:-256m}"
PORT="${PORT:-3000}"
TLS_PORT="${TLS_PORT:-3443}"
DURATION="${DURATION:-10s}"
IMAGE="${IMAGE:-oria-example}"
WRK2="${WRK2:-$(command -v wrk2 || true)}"
URL="http://127.0.0.1:$PORT"
client() { taskset -c "$CLIENT_CPUS" "$@"; }

[ "${SKIP_BUILD:-0}" = 1 ] || docker build -t "$IMAGE" .

WORK="$(mktemp -d)"
stop() { docker rm -f oria-bench oria-bench-tls >/dev/null 2>&1 || true; }
trap 'stop; rm -rf "$WORK"' EXIT
stop

run_server() {  # name, extra args...
  local name="$1"; shift
  docker run -d --name "$name" --network host --cpuset-cpus="$CPUS" --memory="$MEMORY" --memory-swap="$MEMORY" \
    -e THREADS="$(($(echo "$CPUS" | tr ',' '\n' | wc -l)))" -e BENCH=1 -e RATE_LIMIT=1000000000 "$@" "$IMAGE" >/dev/null
}
rss() { grep -E "VmHWM" "/proc/$(docker inspect -f '{{.State.Pid}}' "$1")/status" | awk '{print $2/1024 " MB peak RSS"}'; }

mkdir -p "$WORK/data" && chmod 777 "$WORK/data"  # uploads land on a volume, as in production
run_server oria-bench -e PORT="$PORT" -v "$WORK/data:/data"
until curl -fs "$URL/" >/dev/null; do sleep 0.2; done

section() { echo; echo "=== $*"; }
wrkrun() { client wrk "$@" | grep -E "Requests/sec|Latency  | 50%| 99%|Socket errors|Non-2xx|Transfer/sec"; }

client wrk -t2 -c64 -d3s "$URL/" >/dev/null  # warm-up

section "plaintext GET /, keep-alive, 64 connections"
wrkrun -t2 -c64 -d"$DURATION" --latency "$URL/"
section "plaintext GET /, pipelined x16, 64 connections"
wrkrun -t2 -c64 -d"$DURATION" --latency -s scripts/pipeline.lua "$URL/" -- 16
section "JSON GET /json, pipelined x16, 64 connections"
wrkrun -t2 -c64 -d"$DURATION" --latency -s scripts/pipeline.lua "$URL/json" -- 16
section "routed + async actor GET /api/users/1, keep-alive, 64 connections"
wrkrun -t2 -c64 -d"$DURATION" --latency "$URL/api/users/1"

if [ -n "$WRK2" ]; then
  for rate in 10000 20000 30000; do
    section "latency at a fixed $rate req/s (wrk2, 16 connections, corrected for coordinated omission)"
    client "$WRK2" -t2 -c16 -d"$DURATION" -R"$rate" --latency "$URL/" | grep -E "^ +(50|90|99|99.9)\.000%|Requests/sec"
  done
fi

section "large bodies (memory capped at $MEMORY)"
head -c $((1 << 30)) /dev/urandom > "$WORK/big.bin"
curl -s -o /dev/null -w "1 GiB raw PUT upload:       %{http_code} in %{time_total}s\n" -T "$WORK/big.bin" "$URL/files/big.bin"
curl -s -o /dev/null -w "1 GiB multipart upload:     %{http_code} in %{time_total}s\n" -F "file=@$WORK/big.bin" "$URL/upload"
curl -s -o "$WORK/dl.bin" -w "1 GiB file download:        %{http_code} in %{time_total}s\n" "$URL/files/big.bin"
cmp -s "$WORK/big.bin" "$WORK/dl.bin" && echo "download is byte-identical"
curl -s -o "$WORK/r.bin" -w "1 MB range from the middle: %{http_code}\n" -H "Range: bytes=500000000-500999999" "$URL/files/big.bin"
cmp -s "$WORK/r.bin" <(tail -c +500000001 "$WORK/big.bin" | head -c 1000000) && echo "range is byte-identical"
curl -s -o /dev/null -w "4 GiB generated response:   %{http_code} in %{time_total}s\n" "$URL/bytes/4294967296"
section "concurrent: 32 parallel 32 MiB uploads"
head -c $((32 << 20)) /dev/urandom > "$WORK/m.bin"
start=$(date +%s%N)
codes="$WORK/codes"
for i in $(seq 32); do curl -s -o /dev/null -w "%{http_code}\n" -X POST -H "content-type: application/octet-stream" -T "$WORK/m.bin" "$URL/sink" >> "$codes" & done; wait
echo "statuses: $(sort "$codes" | uniq -c | tr -s ' \n' ' ')"
echo "1 GiB total in $(( ($(date +%s%N) - start) / 1000000 )) ms"
echo "server: $(rss oria-bench)"

if command -v h2load >/dev/null; then
  CERTS="$WORK/tls"; mkdir -p "$CERTS"
  openssl req -x509 -newkey rsa:2048 -nodes -keyout "$CERTS/key.pem" -out "$CERTS/cert.pem" -days 1 -subj /CN=localhost 2>/dev/null
  chmod 644 "$CERTS"/*.pem
  run_server oria-bench-tls -e PORT="$TLS_PORT" -e TLS_CERT=/tls/cert.pem -e TLS_KEY=/tls/key.pem -v "$CERTS:/tls:ro"
  until curl -kfs "https://127.0.0.1:$TLS_PORT/" >/dev/null; do sleep 0.2; done
  section "HTTP/2 over TLS: 64 connections x 16 streams"
  client h2load -t2 -c64 -m16 -D10 "https://127.0.0.1:$TLS_PORT/" | grep -E "finished in|requests:|time for request"
  echo "server: $(rss oria-bench-tls)"
fi
