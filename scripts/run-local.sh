#!/usr/bin/env bash
# Run the full topology as three local processes with real host directories
# standing in for the three mounted volumes.  This is the no-Docker equivalent
# of `docker compose up` and exercises the same code.
#
# Usage: scripts/run-local.sh [data_dir]
# Default data dir: ./data-local
set -u -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA="${1:-$ROOT/data-local}"
mkdir -p "$DATA/index" "$DATA/blobs" "$DATA/snapshots/bundles" "$DATA/logs"

if [ -x "$ROOT/.venv/bin/python" ]; then
  PY="$ROOT/.venv/bin/python"
elif [ -x "$ROOT/tools/venv/bin/python" ]; then
  PY="$ROOT/tools/venv/bin/python"
else
  PY="${PYTHON:-python3}"
fi
echo "[run-local] using python: $PY"
echo "[run-local] data dir: $DATA"

pids=()
cleanup() {
  echo; echo "[run-local] stopping services..."
  for pid in "${pids[@]:-}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Index snapshot end
(
  cd "$ROOT"; export PYTHONPATH="$ROOT"
  VOLUME_LABEL=index SOURCE_DIR="$DATA/index" SNAPSHOT_ROOT="$DATA/snapshots" \
  AGENT_DELAY_SECONDS=0.3 \
  "$PY" -m uvicorn app.agent.snapshot_agent:app --host 127.0.0.1 --port 8001 \
    >"$DATA/logs/snap-index.log" 2>&1
) &
pids+=("$!")

# Blob snapshot end
(
  cd "$ROOT"; export PYTHONPATH="$ROOT"
  VOLUME_LABEL=blobs SOURCE_DIR="$DATA/blobs" SNAPSHOT_ROOT="$DATA/snapshots" \
  AGENT_DELAY_SECONDS=0.3 \
  "$PY" -m uvicorn app.agent.snapshot_agent:app --host 127.0.0.1 --port 8002 \
    >"$DATA/logs/snap-blob.log" 2>&1
) &
pids+=("$!")

# Web backup page / API
(
  cd "$ROOT"; export PYTHONPATH="$ROOT"
  INDEX_DIR="$DATA/index" BLOB_DIR="$DATA/blobs" \
  SNAPSHOT_DIR="$DATA/snapshots" BUNDLE_DIR="$DATA/snapshots/bundles" \
  SNAP_INDEX_URL="http://127.0.0.1:8001" SNAP_BLOB_URL="http://127.0.0.1:8002" \
  SNAPSHOT_WINDOW_SECONDS="${SNAPSHOT_WINDOW_SECONDS:-3.0}" \
  WRITE_INTERVAL_SECONDS=1.2 DRAIN_TIMEOUT_SECONDS=60 \
  "$PY" -m uvicorn app.main:app --host 127.0.0.1 --port 8000 \
    >"$DATA/logs/web.log" 2>&1
) &
pids+=("$!")

echo "[run-local] web:        http://127.0.0.1:8000"
echo "[run-local] snap-index: http://127.0.0.1:8001/health"
echo "[run-local] snap-blob:  http://127.0.0.1:8002/health"
echo "[run-local] logs:       $DATA/logs"
echo "[run-local] Ctrl-C to stop"
wait
