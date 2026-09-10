"""
Universal C2 Wireless Network Node Daemon.
Runs identically on PC0 (Master Ground Station) and PC1..PCn (Remote Field Worker Nodes).

Coordinated through collaborating services:
- PeerTracker (src/tracker.py): Tracks peer registry and rolling link quality metrics.
- BeaconService (src/beacon.py): Handles UDP auto-discovery beacon broadcast & unicast.
- TcpCommandServer (src/server.py): Serves line-delimited TCP command interface.
- HttpServer (src/server.py): Serves aiohttp HTTP REST, static web dashboard, and WebSocket streaming.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
import platform
import socket
import time
from typing import Any, Dict, Optional



from beacon import BeaconService, UDPBeaconProtocol
from config import config
from protocol import C2CommandRequest
from server import HttpServer, TcpCommandServer, execute_standard_command
from tracker import NodeMetrics, PeerTracker

__all__ = [
    "C2NodeDaemon",
    "setup_logging",
    "main",
]


def setup_logging(level: int = logging.INFO) -> None:
    """Configure standard logging format and handler for C2 node runtime."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )


logger = logging.getLogger("C2Node")


class C2NodeDaemon:
    """
    Universal C2 Node Daemon coordinator.
    Orchestrates UDP beacon discovery, TCP commands, HTTP/WebSocket serving,
    and fail-safe watchdog routines across collaborating subsystems.
    """

    def __init__(
        self,
        node_id: str,
        role: str,
        http_port: int,
        tcp_port: int,
        udp_port: int,
        master_ip: Optional[str] = None,
    ):
        self.node_id = node_id
        self.role = role  # "master" or "worker"
        self.http_port = http_port
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        self.master_ip = master_ip

        self.start_time = time.time()
        self.running = False
        self.local_ip = self._determine_local_ip()

        # Web static files directory
        self.web_dir = Path(__file__).parent / "web"

        # Collaborating subsystems
        self.tracker = PeerTracker()
        self.peers: Dict[str, NodeMetrics] = self.tracker.peers

        self.beacon_service = BeaconService(
            node_id=self.node_id,
            role=self.role,
            http_port=self.http_port,
            tcp_port=self.tcp_port,
            udp_port=self.udp_port,
            tracker=self.tracker,
            master_ip=self.master_ip,
            start_time=self.start_time,
        )

        self.tcp_server = TcpCommandServer(
            node_id=self.node_id,
            tcp_port=self.tcp_port,
            command_handler=self._execute_command,
        )

        self.http_server = HttpServer(
            node_id=self.node_id,
            role=self.role,
            http_port=self.http_port,
            tcp_port=self.tcp_port,
            local_ip=self.local_ip,
            tracker=self.tracker,
            start_time=self.start_time,
            web_dir=self.web_dir,
            command_handler=self._execute_command,
        )

    def _determine_local_ip(self) -> str:
        """Determine outbound IP address by probing master or gateway."""
        probe_host = self.master_ip if self.master_ip else config.dns_probe_host
        probe_port = self.udp_port if self.master_ip else config.dns_probe_port
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect((probe_host, probe_port))
            ip = str(s.getsockname()[0])
        except OSError as e:
            logger.debug("Failed to detect local IP via probe (%s:%s): %s", probe_host, probe_port, e)
            ip = "127.0.0.1"
        finally:
            s.close()
        return ip

    def _execute_command(self, req: C2CommandRequest) -> Dict[str, Any]:
        return execute_standard_command(req, self.node_id)

    # --- Fail-Safe Watchdog (Worker Only) ---
    async def failsafe_watchdog_loop(self) -> None:
        """Monitors contact with C2 Master. Logs warning if connection drops."""
        while self.running:
            await asyncio.sleep(config.failsafe_check_interval_sec)
            if self.role == "worker" and self.master_ip:
                elapsed = time.time() - self.beacon_service.last_master_contact
                if elapsed > config.failsafe_timeout_sec:
                    logger.warning(
                        f"[FAILSAFE WATCHDOG] Lost Master contact for {elapsed:.1f}s! Silence threshold exceeded."
                    )


    # --- Lifecycle ---
    async def start(self) -> None:
        self.running = True
        self.beacon_service.running = True
        self.http_server.running = True
        loop = asyncio.get_running_loop()

        # 1. UDP Discovery Endpoint
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError as e:
                logger.debug("SO_REUSEPORT not supported or failed: %s", e)
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        udp_sock.bind(("0.0.0.0", self.udp_port))

        transport, _ = await loop.create_datagram_endpoint(
            lambda: UDPBeaconProtocol(self.beacon_service.handle_incoming_beacon),
            sock=udp_sock,
        )

        # 2. TCP Command Server
        tcp_server = await self.tcp_server.start()

        # 3. HTTP & WebSocket Server (aiohttp)
        await self.http_server.start()

        logger.info("=" * 70)
        logger.info(f" C2 WIRELESS NODE ONLINE: [{self.node_id}] (Role: {self.role.upper()})")
        logger.info(f" Local IP:       {self.local_ip}")
        logger.info(f" UDP Beacon:     0.0.0.0:{self.udp_port} (Discovery & Jitter/Loss)")
        logger.info(f" TCP Commands:   0.0.0.0:{self.tcp_port}")
        logger.info(f" HTTP / WebSock: http://0.0.0.0:{self.http_port}")
        if self.role == "master":
            logger.info(f" Web Dashboard:  http://localhost:{self.http_port}")
        logger.info("=" * 70)

        tasks = [
            asyncio.create_task(self.beacon_service.beacon_sender_loop(transport)),
            asyncio.create_task(self.failsafe_watchdog_loop()),
            asyncio.create_task(self.http_server.ws_telemetry_broadcast_loop()),
            asyncio.create_task(tcp_server.serve_forever()),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            self.running = False
            self.beacon_service.running = False
            self.http_server.running = False
            transport.close()
            await self.tcp_server.stop()
            await self.http_server.cleanup()


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------
def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser(description="Universal C2 Wireless Network Node Daemon")
    parser.add_argument(
        "--id",
        type=str,
        default=f"node-{platform.node()[:6]}",
        help="Unique identifier for this node (e.g. c2-master, node-1, node-4)",
    )
    parser.add_argument(
        "--role",
        choices=["master", "worker"],
        default="worker",
        help="Run as 'master' (C2 Ground Station) or 'worker' (Remote Field Node)",
    )
    parser.add_argument(
        "--master-ip",
        type=str,
        default=None,
        help="Optional IP address of the C2 Master (for multi-hop/routed nodes)",
    )
    parser.add_argument(
        "--port-http",
        type=int,
        default=None,
        help="HTTP/WebSocket port (default: 9000 for master, 8080 for worker)",
    )
    parser.add_argument(
        "--port-tcp",
        type=int,
        default=config.tcp_cmd_port,
        help=f"TCP command port (default: {config.tcp_cmd_port})",
    )
    parser.add_argument(
        "--port-udp",
        type=int,
        default=config.udp_beacon_port,
        help=f"UDP beacon port (default: {config.udp_beacon_port})",
    )
    args = parser.parse_args()

    http_port = args.port_http
    if http_port is None:
        http_port = config.http_master_port if args.role == "master" else config.http_worker_port

    daemon = C2NodeDaemon(
        node_id=args.id,
        role=args.role,
        http_port=http_port,
        tcp_port=args.port_tcp,
        udp_port=args.port_udp,
        master_ip=args.master_ip,
    )

    try:
        asyncio.run(daemon.start())
    except KeyboardInterrupt:
        logger.info(f"Node [{args.id}] stopped by user.")


if __name__ == "__main__":
    main()
