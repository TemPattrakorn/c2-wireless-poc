"""
Global pytest configuration and fixtures for C2 Wireless PoC test suite.
Provides dynamic ephemeral port allocation and loopback network capability checks.
Async test execution is powered by pytest-asyncio.
"""

from __future__ import annotations

import socket
from typing import Any
import pytest


def get_ephemeral_port(protocol: str = "tcp") -> int:
    """Find an available ephemeral port allocated by the operating system."""
    sock_type = socket.SOCK_STREAM if protocol == "tcp" else socket.SOCK_DGRAM
    s = socket.socket(socket.AF_INET, sock_type)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])
    finally:
        s.close()


def can_bind_and_connect_sockets() -> bool:
    """Check if the current environment allows creating, binding, and connecting local sockets."""
    server_sock = None
    client_sock = None
    try:
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(("127.0.0.1", 0))
        server_sock.listen(1)
        port = server_sock.getsockname()[1]

        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.settimeout(0.5)
        client_sock.connect(("127.0.0.1", port))
        return True
    except Exception:
        return False
    finally:
        if client_sock:
            try:
                client_sock.close()
            except Exception:
                pass
        if server_sock:
            try:
                server_sock.close()
            except Exception:
                pass


@pytest.fixture(scope="session")
def require_loopback_network() -> None:
    """Fixture ensuring the execution environment permits local loopback socket networking."""
    if not can_bind_and_connect_sockets():
        pytest.skip(
            "Local socket networking is restricted in this execution environment "
            "(e.g., sandbox or container without loopback permissions)."
        )


def create_dummy_beacon(
    node_id: str = "worker-1",
    role: str = "worker",
    seq: int = 1,
    timestamp: float | None = None,
    start_time: float | None = None,
    http_port: int = 8080,
    tcp_port: int = 9877,
    cpu_load: float = 0.25,
) -> Any:
    """Create a valid BeaconMessage instance for testing."""
    import time
    from protocol import BeaconMessage

    now = time.time()
    return BeaconMessage(
        node_id=node_id,
        role=role,
        seq=seq,
        timestamp=timestamp if timestamp is not None else now - 0.01,
        start_time=start_time if start_time is not None else now - 50.0,
        http_port=http_port,
        tcp_port=tcp_port,
        cpu_load=cpu_load,
    )


@pytest.fixture
def master_daemon() -> Any:
    """Fixture providing a configured Master C2NodeDaemon instance."""
    from node import C2NodeDaemon

    return C2NodeDaemon(
        node_id="test-master-srv",
        role="master",
        http_port=9000,
        tcp_port=9877,
        udp_port=9876,
    )


@pytest.fixture
def worker_daemon() -> Any:
    """Fixture providing a configured Worker C2NodeDaemon instance."""
    from node import C2NodeDaemon

    return C2NodeDaemon(
        node_id="test-worker-srv",
        role="worker",
        http_port=8080,
        tcp_port=9877,
        udp_port=9876,
    )
