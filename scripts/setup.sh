#!/bin/bash
# Setup script for C2 Wireless Network node dependencies (Ubuntu / Debian / Linux / macOS)
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=========================================================="
echo " Installing C2 Wireless Node Dependencies"
echo "=========================================================="

if command -v apt-get &>/dev/null; then
  echo "[*] Debian/Ubuntu detected."
  echo "[*] Installing python3-aiohttp and virtual environment tools via apt..."
  sudo apt-get update && sudo apt-get install -y python3-aiohttp || {
    echo "[!] Fallback: installing via python3-venv..."
    sudo apt-get install -y python3-venv python3-pip
    python3 -m venv "$SCRIPT_DIR/.venv"
    "$SCRIPT_DIR/.venv/bin/pip" install -r "$SCRIPT_DIR/requirements.txt"
  }
else
  echo "[*] Non-apt system detected. Creating virtual environment (.venv)..."
  python3 -m venv "$SCRIPT_DIR/.venv"
  "$SCRIPT_DIR/.venv/bin/pip" install -r "$SCRIPT_DIR/requirements.txt"
fi

echo ""
echo "[+] Dependencies successfully installed!"
echo "=========================================================="
echo "You can now run:"
echo "  • On Worker node:  ./scripts/run_worker.sh node-1"
echo "  • On Master node:  ./scripts/run_master.sh c2-master"
echo "=========================================================="
