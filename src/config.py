"""
Configuration constants and defaults for C2 Wireless Proof-of-Concept.
"""

import logging
import os
from dataclasses import dataclass
from typing import ClassVar, Dict

logger = logging.getLogger("C2Config")


def _get_int_env(key: str, default: int) -> int:
    """Safely parse an integer environment variable with fallback.

    Args:
        key: Environment variable name.
        default: Fallback integer value when variable is unset or invalid.

    Returns:
        Parsed integer value or default fallback.
    """
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
    """Global configuration defaults and network parameters for C2 nodes.

    Attributes:
        udp_beacon_port: UDP port used for peer discovery and link health monitoring.
        tcp_cmd_port: TCP port used for reliable line-delimited command dispatch.
        http_worker_port: Default HTTP/WebSocket port for worker nodes.
        http_master_port: Default HTTP port for the master ground station dashboard.
        beacon_interval_sec: Interval in seconds between outgoing UDP beacon transmissions.
        failsafe_timeout_sec: Silence threshold before declaring master lost and pruning peers.
        failsafe_check_interval_sec: Polling interval for the failsafe watchdog task.
        ws_broadcast_interval_sec: Interval between real-time WebSocket telemetry pushes.
        link_history_window: Number of recent samples retained for jitter and loss calculation.
        master_proxy_timeout_sec: Timeout for forwarded worker commands through master proxy.
        dns_probe_host: Remote host probed via UDP to identify local outbound network interface.
        dns_probe_port: Remote port probed to resolve local outbound IP address.
        benchmark_sizes: Map of synthetic payload labels to byte sizes for transfer testing.
    """
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

