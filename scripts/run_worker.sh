#!/bin/bash
# Launch C2 Remote Worker Node (PC1 - PCn)
set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$PROJECT_ROOT/src"

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
ensure_python_environment "$PROJECT_ROOT"

NODE_ID=${1:-"node-1"}
MASTER_IP=${2:-""}
HTTP_PORT=${3:-8080}

echo "=========================================================="
echo "Starting C2 Remote Worker Node [${NODE_ID}]"
echo "HTTP/WebSocket Endpoint: http://0.0.0.0:${HTTP_PORT}"
if [ -n "$MASTER_IP" ]; then
  echo "Target Master IP (Unicast): ${MASTER_IP}"
fi
echo "=========================================================="

CMD_ARGS=("$PYTHON_BIN" "$PROJECT_ROOT/src/node.py" --id "${NODE_ID}" --role worker --port-http "${HTTP_PORT}")
if [ -n "$MASTER_IP" ]; then
  CMD_ARGS+=(--master-ip "${MASTER_IP}")
fi

"${CMD_ARGS[@]}"
