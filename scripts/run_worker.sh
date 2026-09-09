#!/bin/bash
# Launch C2 Remote Worker Node (PC1 - PCn)
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
PYTHON_BIN="python3"
if [ -f "$SCRIPT_DIR/.venv/bin/python3" ]; then
  PYTHON_BIN="$SCRIPT_DIR/.venv/bin/python3"
fi

# Verify aiohttp is installed
if ! "$PYTHON_BIN" -c "import aiohttp" &>/dev/null; then
  echo "[-] 'aiohttp' is not installed for ${PYTHON_BIN}."
  # Attempt automatic venv setup if requirements.txt exists
  if [ ! -d "$SCRIPT_DIR/.venv" ] && command -v python3 &>/dev/null; then
    echo "[*] Attempting automatic virtual environment setup (.venv)..."
    if python3 -m venv "$SCRIPT_DIR/.venv" 2>/dev/null; then
      "$SCRIPT_DIR/.venv/bin/pip" install -q -r "$SCRIPT_DIR/requirements.txt" 2>/dev/null
      if [ -f "$SCRIPT_DIR/.venv/bin/python3" ] && "$SCRIPT_DIR/.venv/bin/python3" -c "import aiohttp" &>/dev/null; then
        PYTHON_BIN="$SCRIPT_DIR/.venv/bin/python3"
        echo "[+] Auto-setup complete! Using .venv"
      fi
    fi
  fi

  # If still not available, provide actionable instructions
  if ! "$PYTHON_BIN" -c "import aiohttp" &>/dev/null; then
    echo ""
    echo "=========================================================="
    echo " MISSING DEPENDENCY: aiohttp"
    echo "=========================================================="
    echo "Please install aiohttp on your Ubuntu PC using either method:"
    echo ""
    echo "Method 1 (Quickest on Ubuntu via apt):"
    echo "  sudo apt update && sudo apt install -y python3-aiohttp"
    echo ""
    echo "Method 2 (Virtual Environment):"
    echo "  sudo apt update && sudo apt install -y python3-venv python3-pip"
    echo "  python3 -m venv .venv"
    echo "  .venv/bin/pip install -r requirements.txt"
    echo "=========================================================="
    exit 1
  fi
fi

CMD="\"$PYTHON_BIN\" \"$SCRIPT_DIR/c2_node.py\" --id \"${NODE_ID}\" --role worker --port-http ${HTTP_PORT}"

if [ -n "$MASTER_IP" ]; then
  CMD="$CMD --master-ip ${MASTER_IP}"
fi

eval $CMD
