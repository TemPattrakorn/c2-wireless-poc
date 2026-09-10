"""
Configuration constants and defaults for C2 Wireless Proof-of-Concept.
"""

import logging
import os
from dataclasses import dataclass
from typing import ClassVar, Dict

logger = logging.getLogger("C2Config")


def _get_int_env(key: str, default: int) -> int:
    """Safely parse an integer environment variable with fallback."""
    val = os.getenv(key)
    if val is None or not val.strip():
        return default
    try:
        return int(val.strip())
    except ValueError:
        logger.warning(
            f"Invalid integer value '{val}' for environment variable {key}; using default {default}"
        )
        return default


@dataclass(frozen=True)
class C2Config:
    # Network Ports
    udp_beacon_port: int = _get_int_env("C2_UDP_PORT", 9876)
    tcp_cmd_port: int = _get_int_env("C2_TCP_PORT", 9877)
    http_worker_port: int = _get_int_env("C2_HTTP_WORKER_PORT", 8080)
    http_master_port: int = _get_int_env("C2_HTTP_MASTER_PORT", 9000)

    # Timing & Failsafe Parameters
    beacon_interval_sec: float = 1.0        # High-frequency discovery & link monitoring
    failsafe_timeout_sec: float = 4.0      # Loss-of-comm timeout before pruning stale peers
    failsafe_check_interval_sec: float = 1.0 # Polling frequency for watchdog pruning loop
    ws_broadcast_interval_sec: float = 0.5   # Periodic interval for WebSocket telemetry streaming
    link_history_window: int = 30           # Number of recent samples for jitter & loss calc
    master_proxy_timeout_sec: float = 3.0   # Timeout for C2 Master HTTP command proxy calls

    # Network Discovery & Probing
    dns_probe_host: str = "8.8.8.8"          # Remote host to probe for local outbound IP resolution
    dns_probe_port: int = 80

    # Benchmarking Payload Sizes (Bytes)
    benchmark_sizes: ClassVar[Dict[str, int]] = {
        "1KB": 1024,
        "16KB": 16 * 1024,
        "64KB": 64 * 1024,
        "256KB": 256 * 1024,
    }


config = C2Config()

