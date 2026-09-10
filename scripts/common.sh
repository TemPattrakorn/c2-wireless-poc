#!/bin/bash
# Common environment and dependency resolution routines for C2 runner scripts.

ensure_python_environment() {
  local root_dir="${1:-${PROJECT_ROOT:-$SCRIPT_DIR}}"
  if [ -z "$root_dir" ]; then
    root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  fi

  PYTHON_BIN="python3"
  if [ -f "$root_dir/.venv/bin/python3" ]; then
    PYTHON_BIN="$root_dir/.venv/bin/python3"
  fi

  # Verify aiohttp is installed
  if ! "$PYTHON_BIN" -c "import aiohttp" &>/dev/null; then
    echo "[-] 'aiohttp' is not installed for ${PYTHON_BIN}."
    # Attempt automatic venv setup if requirements.txt exists
    if [ ! -d "$root_dir/.venv" ] && command -v python3 &>/dev/null; then
      echo "[*] Attempting automatic virtual environment setup (.venv)..."
      if python3 -m venv "$root_dir/.venv" 2>/dev/null; then
        "$root_dir/.venv/bin/pip" install -q -r "$root_dir/requirements.txt" 2>/dev/null
        if [ -f "$root_dir/.venv/bin/python3" ] && "$root_dir/.venv/bin/python3" -c "import aiohttp" &>/dev/null; then
          PYTHON_BIN="$root_dir/.venv/bin/python3"
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

  export PYTHON_BIN
}
