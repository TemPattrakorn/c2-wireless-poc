#!/bin/bash
# Launch C2 Remote Worker Node (PC1 - PC5)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$SCRIPT_DIR"

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

CMD="python3 \"$SCRIPT_DIR/c2_node.py\" --id \"${NODE_ID}\" --role worker --port-http ${HTTP_PORT}"

if [ -n "$MASTER_IP" ]; then
  CMD="$CMD --master-ip ${MASTER_IP}"
fi

eval $CMD
