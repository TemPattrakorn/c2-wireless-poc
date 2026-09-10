"""
Typed protocol parsing and validation for untrusted external inputs.

Covers UDP discovery beacons, TCP command frames, HTTP request payloads,
and WebSocket message payloads.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set

# Maximum payload safety boundaries
MAX_DATAGRAM_SIZE = 65507          # Max theoretical IPv4 UDP datagram payload
MAX_TCP_FRAME_SIZE = 65536         # 64 KB per line-delimited TCP command frame
MAX_COMMAND_PAYLOAD_SIZE = 65536   # 64 KB maximum HTTP/TCP command payload limit
MAX_HTTP_PAYLOAD_SIZE = 10 * 1024 * 1024  # 10 MB maximum HTTP payload limit

VALID_ROLES: Set[str] = {"master", "worker"}
VALID_COMMANDS: Set[str] = {"PING"}



class C2ParseError(ValueError):
    """Raised when an untrusted network payload fails structural or semantic validation."""

    def __init__(self, message: str, field: Optional[str] = None):
        super().__init__(message)
        self.message = message
        self.field = field


@dataclass(frozen=True)
class BeaconMessage:
    """Validated UDP discovery beacon."""

    node_id: str
    role: str
    seq: int
    timestamp: float
    start_time: float
    http_port: int
    tcp_port: int
    cpu_load: float


@dataclass(frozen=True)
class C2CommandRequest:
    """Validated C2 operational command request."""

    command: str
    target_id: str = "all"
    args: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BenchmarkRequest:
    """Validated HTTP benchmark transfer request."""

    preset: str
    client_timestamp: float
    data: str


@dataclass(frozen=True)
class WsBenchmarkRequest:
    """Validated WebSocket benchmark action request."""

    action: str
    target_id: Optional[str] = None
    preset: Optional[str] = None
    timestamp: float = 0.0
    data: Optional[str] = None


def _parse_json_dict(
    raw: str | bytes,
    max_bytes: int,
    context: str,
) -> Dict[str, Any]:
    """
    Safely decode raw bytes or text into a Python dictionary.

    Enforces size limits, strict UTF-8 decoding, valid JSON syntax, and root object type.
    """
    if isinstance(raw, bytes):
        if len(raw) > max_bytes:
            raise C2ParseError(
                f"{context} exceeds maximum permitted length of {max_bytes} bytes (received {len(raw)})"
            )
        if not raw:
            raise C2ParseError(f"{context} is empty")
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as e:
            raise C2ParseError(f"Invalid UTF-8 encoding in {context}: {e}") from e
    elif isinstance(raw, str):
        if len(raw) > max_bytes or len(raw.encode("utf-8")) > max_bytes:
            raise C2ParseError(
                f"{context} exceeds maximum permitted length of {max_bytes} bytes"
            )
        if not raw.strip():
            raise C2ParseError(f"{context} is empty")
        text = raw

    else:
        raise C2ParseError(f"Expected str or bytes for {context}, got {type(raw).__name__}")

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        raise C2ParseError(f"Malformed JSON in {context}: {e.msg} at line {e.lineno}, col {e.colno}") from e

    if not isinstance(data, dict):
        raise C2ParseError(
            f"Expected JSON object (dictionary) in {context}, got {type(data).__name__}"
        )

    return data


def _validate_port(port_val: Any, field_name: str) -> int:
    """Validate port is a non-boolean integer between 1 and 65535."""
    if isinstance(port_val, bool) or not isinstance(port_val, int):
        raise C2ParseError(
            f"Field '{field_name}' must be an integer, got {type(port_val).__name__}",
            field=field_name,
        )
    if not (1 <= port_val <= 65535):
        raise C2ParseError(
            f"Field '{field_name}' must be between 1 and 65535 (got {port_val})",
            field=field_name,
        )
    return port_val


def _validate_float(val: Any, field_name: str, min_val: Optional[float] = None) -> float:
    """Validate numeric value is a valid, finite float or int."""
    if isinstance(val, bool) or not isinstance(val, (int, float)):
        raise C2ParseError(
            f"Field '{field_name}' must be numeric, got {type(val).__name__}",
            field=field_name,
        )
    num = float(val)
    if math.isnan(num) or math.isinf(num):
        raise C2ParseError(
            f"Field '{field_name}' cannot be NaN or Infinite",
            field=field_name,
        )
    if min_val is not None and num < min_val:
        raise C2ParseError(
            f"Field '{field_name}' must be >= {min_val} (got {num})",
            field=field_name,
        )
    return num


def parse_beacon_datagram(data: bytes) -> BeaconMessage:
    """
    Parse and strictly validate an incoming UDP auto-discovery beacon.

    Raises C2ParseError on any structural, type, or constraint violation.
    """
    payload_dict = _parse_json_dict(data, MAX_DATAGRAM_SIZE, "UDP beacon datagram")

    # node_id
    node_id = payload_dict.get("node_id")
    if not isinstance(node_id, str) or not node_id.strip():
        raise C2ParseError("Field 'node_id' must be a non-empty string", field="node_id")
    if len(node_id) > 128:
        raise C2ParseError("Field 'node_id' exceeds maximum length of 128 characters", field="node_id")

    # role
    role = payload_dict.get("role")
    if not isinstance(role, str) or role.lower() not in VALID_ROLES:
        raise C2ParseError(
            f"Field 'role' must be one of {sorted(VALID_ROLES)}, got '{role}'",
            field="role",
        )
    role = role.lower()

    # seq
    seq = payload_dict.get("seq")
    if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
        raise C2ParseError("Field 'seq' must be a non-negative integer", field="seq")

    # timestamps
    timestamp = _validate_float(payload_dict.get("timestamp", 0.0), "timestamp", min_val=0.0)
    start_time = _validate_float(payload_dict.get("start_time", 0.0), "start_time", min_val=0.0)

    # ports
    http_port = _validate_port(payload_dict.get("http_port"), "http_port")
    tcp_port = _validate_port(payload_dict.get("tcp_port"), "tcp_port")

    # cpu_load
    cpu_load = _validate_float(payload_dict.get("cpu_load", 0.0), "cpu_load", min_val=0.0)

    return BeaconMessage(
        node_id=node_id.strip(),
        role=role,
        seq=seq,
        timestamp=timestamp,
        start_time=start_time,
        http_port=http_port,
        tcp_port=tcp_port,
        cpu_load=cpu_load,
    )


def _validate_command_dict(data: Dict[str, Any]) -> C2CommandRequest:
    """Validate parsed command dictionary structure and return a typed C2CommandRequest."""
    cmd = data.get("command")
    if not isinstance(cmd, str) or not cmd.strip():
        raise C2ParseError("Field 'command' must be a non-empty string", field="command")
    cmd_upper = cmd.strip().upper()
    if cmd_upper not in VALID_COMMANDS:
        raise C2ParseError(
            f"Unknown command '{cmd_upper}'. Valid commands: {sorted(VALID_COMMANDS)}",
            field="command",
        )

    target_id = data.get("target_id", "all")
    if not isinstance(target_id, str):
        raise C2ParseError("Field 'target_id' must be a string", field="target_id")
    target_id = target_id.strip() if target_id.strip() else "all"

    args = data.get("args", {})
    if not isinstance(args, dict):
        raise C2ParseError("Field 'args' must be a JSON object (dict)", field="args")

    return C2CommandRequest(
        command=cmd_upper,
        target_id=target_id,
        args=args,
    )


def parse_tcp_command_frame(line: bytes) -> C2CommandRequest:
    """
    Parse and validate a line-delimited TCP command frame.

    Raises C2ParseError on any structural, type, or constraint violation.
    """
    payload_dict = _parse_json_dict(line, MAX_TCP_FRAME_SIZE, "TCP command frame")
    return _validate_command_dict(payload_dict)


def parse_http_command_payload(body: bytes) -> C2CommandRequest:
    """
    Parse and validate an HTTP JSON body sent to POST /api/command.

    Raises C2ParseError on any structural, type, or constraint violation.
    """
    payload_dict = _parse_json_dict(body, MAX_COMMAND_PAYLOAD_SIZE, "HTTP command payload")
    return _validate_command_dict(payload_dict)



def parse_http_benchmark_payload(body: bytes) -> BenchmarkRequest:
    """
    Parse and validate an HTTP JSON body sent to POST /api/benchmark.

    Raises C2ParseError on any structural, type, or constraint violation.
    """
    payload_dict = _parse_json_dict(body, MAX_HTTP_PAYLOAD_SIZE, "HTTP benchmark payload")

    preset = payload_dict.get("preset", "Custom")
    if not isinstance(preset, str):
        raise C2ParseError("Field 'preset' must be a string", field="preset")

    client_ts_raw = payload_dict.get("client_timestamp", 0.0)
    client_ts = _validate_float(client_ts_raw, "client_timestamp", min_val=0.0)

    data = payload_dict.get("data", "")
    if not isinstance(data, str):
        raise C2ParseError("Field 'data' must be a string", field="data")

    return BenchmarkRequest(
        preset=preset,
        client_timestamp=client_ts,
        data=data,
    )


def parse_ws_message(msg_data: str | bytes) -> WsBenchmarkRequest:
    """
    Parse and validate an incoming WebSocket JSON message.

    Currently supports the 'ws_benchmark' action.
    Raises C2ParseError on unknown action or invalid schema.
    """
    payload_dict = _parse_json_dict(msg_data, MAX_HTTP_PAYLOAD_SIZE, "WebSocket message")

    action = payload_dict.get("action")
    if not isinstance(action, str) or not action.strip():
        raise C2ParseError("Field 'action' must be a non-empty string", field="action")
    action = action.strip()

    if action == "ws_benchmark":
        target_id = payload_dict.get("target_id")
        if target_id is not None and not isinstance(target_id, str):
            raise C2ParseError("Field 'target_id' must be a string or null", field="target_id")

        preset = payload_dict.get("preset")
        if preset is not None and not isinstance(preset, str):
            raise C2ParseError("Field 'preset' must be a string or null", field="preset")

        ts_raw = payload_dict.get("timestamp", 0.0)
        timestamp = _validate_float(ts_raw, "timestamp", min_val=0.0)

        data = payload_dict.get("data")
        if data is not None and not isinstance(data, str):
            raise C2ParseError("Field 'data' must be a string or null", field="data")

        return WsBenchmarkRequest(
            action=action,
            target_id=target_id,
            preset=preset,
            timestamp=timestamp,
            data=data,
        )

    raise C2ParseError(f"Unsupported WebSocket action '{action}'", field="action")
