#!/bin/bash
# Setup script for C2 Wireless Network node dependencies (Ubuntu / Debian / Linux / macOS)
set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=========================================================="
echo " Installing C2 Wireless Node Dependencies"
echo "=========================================================="

SUDO_CMD=""
if [ "$(id -u)" -ne 0 ] && command -v sudo &>/dev/null; then
  SUDO_CMD="sudo"
fi

if command -v apt-get &>/dev/null; then
  echo "[*] Debian/Ubuntu detected."
  echo "[*] Installing python3-aiohttp and virtual environment tools via apt..."
  $SUDO_CMD apt-get update && $SUDO_CMD apt-get install -y python3-aiohttp || {
    echo "[!] Fallback: installing via python3-venv..."
    $SUDO_CMD apt-get install -y python3-venv python3-pip
    python3 -m venv "$PROJECT_ROOT/.venv"
    "$PROJECT_ROOT/.venv/bin/pip" install -r "$PROJECT_ROOT/requirements.txt"
  }
else
  echo "[*] Non-apt system detected. Creating virtual environment (.venv)..."
  python3 -m venv "$PROJECT_ROOT/.venv"
  "$PROJECT_ROOT/.venv/bin/pip" install -r "$PROJECT_ROOT/requirements.txt"
fi

echo "[*] Ensuring Web UI dependencies are present..."
VENDOR_DIR="$PROJECT_ROOT/src/web/vendor"
mkdir -p "$VENDOR_DIR"
ESM_FILE="$VENDOR_DIR/alpine.esm.js"

if [ ! -f "$ESM_FILE" ]; then
  echo "[*] Downloading Alpine.js ESM bundle for offline Web UI..."
  if command -v curl &>/dev/null; then
    curl -fsSL "https://cdn.jsdelivr.net/npm/alpinejs@3.14.8/dist/module.esm.js" -o "$ESM_FILE"
  elif command -v wget &>/dev/null; then
    wget -q "https://cdn.jsdelivr.net/npm/alpinejs@3.14.8/dist/module.esm.js" -O "$ESM_FILE"
  else
    echo "[!] Error: Neither curl nor wget found. Please download Alpine.js ESM bundle manually."
    exit 1
  fi
  echo "[+] Alpine.js ESM bundle downloaded successfully to $ESM_FILE"
else
  echo "[+] Alpine.js ESM bundle already present at $ESM_FILE"
fi

echo ""
echo "[+] Dependencies successfully installed!"
echo "=========================================================="
echo "You can now run:"
echo "  • On Worker node:  ./scripts/run_worker.sh node-1"
echo "  • On Master node:  ./scripts/run_master.sh c2-master"
echo "=========================================================="
