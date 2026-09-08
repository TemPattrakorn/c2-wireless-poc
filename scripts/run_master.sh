#!/bin/bash
# Launch C2 Master Ground Station on PC0
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$SCRIPT_DIR"

NODE_ID=${1:-"c2-master"}
HTTP_PORT=${2:-9000}

echo "=========================================================="
echo "Starting C2 Wireless Master Ground Station [${NODE_ID}]"
echo "Web UI Dashboard: http://localhost:${HTTP_PORT}"
echo "=========================================================="

python3 "$SCRIPT_DIR/c2_node.py" \
  --id "${NODE_ID}" \
  --role master \
  --port-http "${HTTP_PORT}"
