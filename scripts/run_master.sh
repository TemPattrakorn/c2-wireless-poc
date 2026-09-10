#!/bin/bash
# ==============================================================================
# run_master.sh — Launch C2 Master Ground Station daemon on PC0
#
# Usage:
#   ./scripts/run_master.sh [NODE_ID] [HTTP_PORT]
#
# Arguments:
#   NODE_ID   — Unique node identifier (default: "c2-master")
#   HTTP_PORT — HTTP REST & Web Dashboard port (default: 9000)
# ==============================================================================
set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$PROJECT_ROOT/src"

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
ensure_python_environment "$PROJECT_ROOT"

NODE_ID=${1:-"c2-master"}
HTTP_PORT=${2:-9000}

echo "=========================================================="
echo "Starting C2 Wireless Master Ground Station [${NODE_ID}]"
echo "Web UI Dashboard: http://localhost:${HTTP_PORT}"
echo "=========================================================="

"$PYTHON_BIN" "$PROJECT_ROOT/src/node.py" \
  --id "${NODE_ID}" \
  --role master \
  --port-http "${HTTP_PORT}"
