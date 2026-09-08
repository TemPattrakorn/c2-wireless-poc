"""
Configuration constants and defaults for C2 Wireless Proof-of-Concept.
"""

import os
from dataclasses import dataclass

@dataclass
class C2Config:
    # Network Ports
    udp_beacon_port: int = int(os.getenv("C2_UDP_PORT", "9876"))
    tcp_cmd_port: int = int(os.getenv("C2_TCP_PORT", "9877"))
    http_worker_port: int = int(os.getenv("C2_HTTP_WORKER_PORT", "8080"))
    http_master_port: int = int(os.getenv("C2_HTTP_MASTER_PORT", "9000"))

    # Timing & Failsafe Parameters
    beacon_interval_sec: float = 1.0        # High-frequency discovery & link monitoring
    failsafe_timeout_sec: float = 4.0      # Loss-of-comm timeout before switching to SAFE
    link_history_window: int = 30           # Number of recent samples for jitter & loss calc

    # Benchmarking Payload Sizes (Bytes)
    benchmark_sizes = {
        "1KB": 1024,
        "16KB": 16 * 1024,
        "64KB": 64 * 1024,
        "256KB": 256 * 1024,
    }

config = C2Config()
