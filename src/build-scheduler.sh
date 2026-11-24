#!/bin/bash
set -euo pipefail

PODS_AND_NODES_MAP="${PODS_AND_NODES_MAP:-/tmp/pods_and_nodes_map.csv}"
[[ -r "$PODS_AND_NODES_MAP" ]] || { echo "[SCHEDULER] Missing mapping: $PODS_AND_NODES_MAP" >&2; exit 1; }
PODS_AND_NODES_MAP=$(realpath "$PODS_AND_NODES_MAP")
cd "$(dirname "${BASH_SOURCE[0]}")/custom-k8s-scheduler"

# Go's build cache checks source and dependency changes on every invocation.
CGO_ENABLED=0 go build -o custom-scheduler .
scheduler_pid=""
cleanup() {
    if [[ -n "$scheduler_pid" ]]; then
        kill -TERM "$scheduler_pid" 2>/dev/null || true
        # Bound cleanup even if a child ignores TERM.
        for ((attempt=0; attempt<100; attempt++)); do
            kill -0 "$scheduler_pid" 2>/dev/null || break
            sleep 0.05
        done
        kill -KILL "$scheduler_pid" 2>/dev/null || true
        wait "$scheduler_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
scheduler_args=(--mapping "$PODS_AND_NODES_MAP" --workers "${1:-1}")
# Optional overrides; the binder scales its deadline with the mapping by default.
if [[ -n "${SCHEDULER_TIMEOUT:-}" ]]; then scheduler_args+=(--timeout "$SCHEDULER_TIMEOUT"); fi
if [[ -n "${SCHEDULER_QPS:-}" ]]; then scheduler_args+=(--qps "$SCHEDULER_QPS"); fi
if [[ -n "${SCHEDULER_BURST:-}" ]]; then scheduler_args+=(--burst "$SCHEDULER_BURST"); fi
./custom-scheduler "${scheduler_args[@]}" &
scheduler_pid=$!
wait "$scheduler_pid"
scheduler_pid=""
# Stop on the first failed apply, and propagate that failure to the coordinator.
bash -e default-script.sh
