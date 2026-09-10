"""
Command execution logic for C2 node — standard command dispatch.
"""

from __future__ import annotations

import time
from typing import Any, Dict

from protocol import C2CommandRequest


def execute_standard_command(req: C2CommandRequest, node_id: str) -> Dict[str, Any]:
    """Execute standard C2 commands (PING)."""
    cmd = req.command
    target_id = req.target_id
    if target_id not in ("all", node_id):
        return {
            "status": "IGNORED",
            "message": f"Command addressed to {target_id}, this node is {node_id}",
            "node_id": node_id,
        }
    if cmd == "PING":
        return {
            "status": "ACK",
            "response": "PONG",
            "node_id": node_id,
            "timestamp": time.time(),
        }
    return {
        "status": "ERROR",
        "error": f"Unsupported command '{cmd}'",
        "node_id": node_id,
    }
