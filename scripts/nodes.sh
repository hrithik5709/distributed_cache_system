#!/usr/bin/env bash
# Start, stop or check three local Redis nodes on ports 7001-7003.
# Persistence is off: this is a cache, and restarts should come back empty.
#
#   scripts/nodes.sh start | stop | status
set -euo pipefail

PORTS=(7001 7002 7003)
RUN_DIR="$(cd "$(dirname "$0")/.." && pwd)/run"

case "${1:-}" in
  start)
    mkdir -p "$RUN_DIR"
    for port in "${PORTS[@]}"; do
      redis-server --port "$port" --save "" --appendonly no \
        --daemonize yes --pidfile "$RUN_DIR/redis-$port.pid" \
        --logfile "$RUN_DIR/redis-$port.log" --dir "$RUN_DIR"
    done
    sleep 0.5
    "$0" status
    ;;
  stop)
    for port in "${PORTS[@]}"; do
      redis-cli -p "$port" shutdown nosave 2>/dev/null || true
    done
    ;;
  status)
    for port in "${PORTS[@]}"; do
      printf '%s: %s\n' "$port" "$(redis-cli -p "$port" ping 2>/dev/null || echo down)"
    done
    ;;
  *)
    echo "usage: $0 start|stop|status" >&2
    exit 1
    ;;
esac
