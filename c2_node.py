#!/usr/bin/env python3
"""
Universal C2 Wireless Network Node Daemon (Proof of Concept)
Runs on all PCs (PC0 Master and PC1-PCn Workers).
Uses aiohttp for robust HTTP REST and WebSocket streaming.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import platform
import socket
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from aiohttp import WSCloseCode, WSMsgType, web

from c2_protocol import (
    BeaconMessage,
    C2CommandRequest,
    C2ParseError,
    parse_beacon_datagram,
    parse_http_benchmark_payload,
    parse_http_command_payload,
    parse_tcp_command_frame,
    parse_ws_message,
)
from config import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("C2Node")


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------
@dataclass
class NodeMetrics:
    node_id: str
    role: str
    ip: str
    http_port: int
    tcp_port: int
    last_seen: float
    rtt_ms: float = 0.0
    jitter_ms: float = 0.0
    packets_sent: int = 0
    packets_received: int = 0
    packet_loss_pct: float = 0.0
    uptime_sec: float = 0.0
    cpu_load: float = 0.0
    last_seq: int = 0
    seq_history: List[Tuple[int, int]] = field(default_factory=list)
    rtt_history: List[float] = field(default_factory=list)


# ---------------------------------------------------------------------------
# UDP Discovery & Link Quality Monitoring Protocol
# ---------------------------------------------------------------------------
class UDPBeaconProtocol(asyncio.DatagramProtocol):
    def __init__(self, daemon: "C2NodeDaemon"):
        self.daemon = daemon
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        if isinstance(transport, asyncio.DatagramTransport):
            self.transport = transport
            sock: Optional[socket.socket] = transport.get_extra_info("socket")
            if sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        try:
            beacon = parse_beacon_datagram(data)
            self.daemon.handle_incoming_beacon(beacon, addr[0])
        except C2ParseError as e:
            logger.debug(f"Rejected untrusted beacon from {addr}: {e}")
        except Exception as e:
            logger.debug(f"Malformed beacon from {addr}: {e}")


# ---------------------------------------------------------------------------
# Core Universal C2 Node Daemon
# ---------------------------------------------------------------------------
class C2NodeDaemon:
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
        self.local_ip = self._determine_local_ip()
        self.running = False
        self.seq_num = 0

        self.last_master_contact = time.time() if self.role == "worker" else 0.0

        # Peer registry & active WebSocket subscribers
        self.peers: Dict[str, NodeMetrics] = {}
        self.ws_clients: Set[web.WebSocketResponse] = set()

        # Web static files directory
        self.web_dir = Path(__file__).parent / "web"

        # aiohttp web app and runner
        self.app: web.Application = self._create_web_app()
        self.app_runner: Optional[web.AppRunner] = None

    def display_benchmark_payload(
        self,
        preset: str,
        data: str,
        raw_bytes_len: int,
        client_ts: float,
        sender_ip: str,
        protocol: str,
    ) -> None:
        now = time.time()
        latency_ms = (now - client_ts) * 1000.0 if client_ts > 0 else 0.0
        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))

        if len(data) <= 1024:
            content_display = data
        else:
            truncated_count = len(data) - 320
            content_display = f"{data[:256]}\n... [truncated {truncated_count} characters] ...\n{data[-64:]}"

        # ANSI color codes
        CYAN = "\033[1;36m"
        GREEN = "\033[1;32m"
        YELLOW = "\033[1;33m"
        MAGENTA = "\033[1;35m"
        BOLD = "\033[1m"
        RESET = "\033[0m"

        border = "=" * 70
        banner = (
            f"\n{CYAN}{border}{RESET}\n"
            f"{BOLD}{MAGENTA}>>> C2 BENCHMARK PAYLOAD RECEIVED <<<{RESET}\n"
            f" {BOLD}Node:{RESET}       {self.node_id} ({self.role.upper()})\n"
            f" {BOLD}Protocol:{RESET}   {GREEN}{protocol}{RESET}\n"
            f" {BOLD}Sender:{RESET}     {sender_ip} at {time_str} ({now:.3f})\n"
            f" {BOLD}Preset:{RESET}     {YELLOW}{preset}{RESET} | Raw Size: {raw_bytes_len} bytes | Data Chars: {len(data)}\n"
            f" {BOLD}Client Latency:{RESET} {latency_ms:.2f} ms\n"
            f"{CYAN}{'-' * 70}{RESET}\n"
            f"{BOLD}Content:{RESET}\n{content_display}\n"
            f"{CYAN}{border}{RESET}\n"
        )
        sys.stdout.write(banner)
        sys.stdout.flush()

    def _determine_local_ip(self) -> str:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            ip = str(s.getsockname()[0])
        except Exception:
            ip = "127.0.0.1"
        finally:
            s.close()
        return ip

    def _cors_headers(self) -> Dict[str, str]:
        return {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        }

    # --- UDP Discovery & Quality Math ---
    def handle_incoming_beacon(self, beacon: BeaconMessage, sender_ip: str) -> None:
        sender_id = beacon.node_id
        if not sender_id or sender_id == self.node_id:
            return

        now = time.time()
        sender_role = beacon.role
        seq = beacon.seq
        sent_ts = beacon.timestamp
        http_p = beacon.http_port
        tcp_p = beacon.tcp_port

        # Track contact with Master for failsafe
        if sender_role == "master":
            self.last_master_contact = now
            if not self.master_ip:
                self.master_ip = sender_ip

        # Calculate one-way / RTT metric
        measured_rtt = max(0.1, (now - sent_ts) * 1000.0)

        if sender_id not in self.peers:
            logger.info(f"[+] Discovered node [{sender_id}] ({sender_role}) at {sender_ip}")
            self.peers[sender_id] = NodeMetrics(
                node_id=sender_id,
                role=sender_role,
                ip=sender_ip,
                http_port=http_p,
                tcp_port=tcp_p,
                last_seen=now,
                last_seq=seq,
            )

        peer = self.peers[sender_id]
        peer.last_seen = now
        peer.ip = sender_ip
        peer.http_port = http_p
        peer.tcp_port = tcp_p
        peer.uptime_sec = round(now - beacon.start_time, 1)
        peer.cpu_load = beacon.cpu_load
        peer.packets_received += 1

        # Calculate Jitter (RFC 3550 style) and Packet Loss
        peer.rtt_history.append(measured_rtt)
        if len(peer.rtt_history) > config.link_history_window:
            peer.rtt_history.pop(0)

        peer.rtt_ms = round(sum(peer.rtt_history) / len(peer.rtt_history), 1)

        if len(peer.rtt_history) > 1:
            diffs = [abs(peer.rtt_history[i] - peer.rtt_history[i - 1]) for i in range(1, len(peer.rtt_history))]
            peer.jitter_ms = round(sum(diffs) / len(diffs), 2)

        # Packet loss calculation from sequence delta
        if peer.last_seq > 0 and seq > peer.last_seq:
            expected = seq - peer.last_seq
            lost = max(0, expected - 1)
            peer.seq_history.append((expected, lost))
            if len(peer.seq_history) > config.link_history_window:
                peer.seq_history.pop(0)

            total_exp = sum(e for e, _ in peer.seq_history)
            total_lost = sum(l for _, l in peer.seq_history)
            peer.packet_loss_pct = round((total_lost / max(1, total_exp)) * 100.0, 1)

        peer.last_seq = seq

    async def beacon_sender_loop(self, transport: asyncio.DatagramTransport) -> None:
        """Broadcasts presence and status periodically."""
        broadcast_addr = ("<broadcast>", self.udp_port)
        while self.running:
            self.seq_num += 1
            load = os.getloadavg()[0] if hasattr(os, "getloadavg") else 0.0
            beacon = {
                "node_id": self.node_id,
                "role": self.role,
                "seq": self.seq_num,
                "timestamp": time.time(),
                "start_time": self.start_time,
                "http_port": self.http_port,
                "tcp_port": self.tcp_port,
                "cpu_load": round(load, 2),
            }
            data = json.dumps(beacon).encode("utf-8")

            # 1. Local subnet broadcast
            try:
                transport.sendto(data, broadcast_addr)
            except Exception as e:
                logger.debug(f"Broadcast send error: {e}")

            # 2. Directed unicast to Master IP (for multi-hop nodes or loopback tests)
            if self.master_ip:
                try:
                    transport.sendto(data, (self.master_ip, self.udp_port))
                except Exception as e:
                    logger.debug(f"Unicast send error to master {self.master_ip}: {e}")

            # 3. Directed unicast back to all registered peers (for routed multi-subnet workers)
            for peer in list(self.peers.values()):
                if self.master_ip and peer.ip == self.master_ip:
                    continue
                try:
                    transport.sendto(data, (peer.ip, self.udp_port))
                except Exception as e:
                    logger.debug(f"Unicast send error to peer {peer.ip}: {e}")

            # 4. Prune timed-out peers on master
            now = time.time()
            for pid, peer in list(self.peers.items()):
                if now - peer.last_seen > config.failsafe_timeout_sec:
                    logger.warning(f"[-] Node [{pid}] timed out (> {config.failsafe_timeout_sec}s silence).")
                    del self.peers[pid]

            await asyncio.sleep(config.beacon_interval_sec)

    # --- Fail-Safe Watchdog (Worker Only) ---
    async def failsafe_watchdog_loop(self) -> None:
        """Monitors contact with C2 Master. Logs warning if connection drops."""
        while self.running:
            await asyncio.sleep(1.0)
            if self.role == "worker" and self.master_ip:
                elapsed = time.time() - self.last_master_contact
                if elapsed > config.failsafe_timeout_sec:
                    logger.warning(
                        f"[FAILSAFE WATCHDOG] Lost Master contact for {elapsed:.1f}s! Silence threshold exceeded."
                    )

    # --- TCP Command Engine ---
    async def handle_tcp_command_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        client_addr = writer.get_extra_info("peername")
        res: Dict[str, Any]
        try:
            line = await reader.readline()
            if not line:
                return
            cmd_req = parse_tcp_command_frame(line)
            if cmd_req.target_id in (self.node_id, "all"):
                res = self.process_c2_action(cmd_req.command, cmd_req.args)
            else:
                res = {"status": "IGNORED", "reason": f"Target mismatch ({cmd_req.target_id})"}
        except C2ParseError as e:
            logger.warning(f"Rejected invalid TCP command frame from {client_addr}: {e}")
            res = {"status": "ERROR", "error": f"Invalid command payload: {e.message}"}
        except Exception as e:
            logger.error(f"TCP command execution error from {client_addr}: {e}")
            res = {"status": "ERROR", "error": f"Internal error: {e}"}
        finally:
            try:
                writer.write((json.dumps(res) + "\n").encode("utf-8"))
                await writer.drain()
            except Exception:
                pass
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    def process_c2_action(self, cmd: str, args: Dict[str, Any]) -> Dict[str, Any]:
        cmd = cmd.upper()
        logger.info(f"[C2 EXEC] Command '{cmd}' received with args: {args}")
        if cmd == "PING":
            return {"status": "ACK", "response": "PONG", "timestamp": time.time(), "node_id": self.node_id}
        else:
            return {"status": "NACK", "reason": f"Unknown command '{cmd}'"}

    # --- HTTP & WebSocket Server (aiohttp) ---
    def _create_web_app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/api/status", self.handle_http_status)
        app.router.add_post("/api/command", self.handle_http_command)
        app.router.add_post("/api/benchmark", self.handle_http_benchmark)
        app.router.add_get("/ws", self.handle_ws_session)
        app.router.add_route("OPTIONS", "/{tail:.*}", self.handle_http_options)

        # Serve static dashboard on Master
        app.router.add_get("/", self.handle_http_index)
        app.router.add_get("/index.html", self.handle_http_index)
        return app

    async def handle_http_options(self, request: web.Request) -> web.Response:
        return web.Response(status=204, headers=self._cors_headers())

    async def handle_http_status(self, request: web.Request) -> web.Response:
        data = {
            "node_id": self.node_id,
            "role": self.role,
            "uptime_sec": round(time.time() - self.start_time, 1),
            "ip": self.local_ip,
            "http_port": self.http_port,
            "tcp_port": self.tcp_port,
            "peers": {pid: asdict(p) for pid, p in self.peers.items()},
        }
        return web.json_response(data, headers=self._cors_headers())

    async def handle_http_command(self, request: web.Request) -> web.Response:
        try:
            body = await request.read()
            cmd_req = parse_http_command_payload(body)
            res = self.process_c2_action(cmd_req.command, cmd_req.args)
            return web.json_response(res, headers=self._cors_headers())
        except C2ParseError as e:
            return web.json_response(
                {"status": "ERROR", "error": e.message, "field": e.field},
                status=400,
                headers=self._cors_headers(),
            )
        except Exception as e:
            return web.json_response(
                {"status": "ERROR", "error": str(e)},
                status=500,
                headers=self._cors_headers(),
            )

    async def handle_http_benchmark(self, request: web.Request) -> web.Response:
        recv_time = time.time()
        try:
            body = await request.read()
            bench_req = parse_http_benchmark_payload(body)
            sender_ip = request.remote or "unknown"
            self.display_benchmark_payload(
                preset=bench_req.preset,
                data=bench_req.data,
                raw_bytes_len=len(body),
                client_ts=bench_req.client_timestamp,
                sender_ip=sender_ip,
                protocol="HTTP POST",
            )
            duration_server = time.time() - recv_time
            response_payload = {
                "status": "BENCHMARK_COMPLETE",
                "node_id": self.node_id,
                "bytes_received": len(body),
                "preset": bench_req.preset,
                "server_receive_ts": recv_time,
                "server_proc_time_ms": round(duration_server * 1000.0, 3),
                "client_latency_estimate_ms": round((recv_time - bench_req.client_timestamp) * 1000.0, 2),
            }
            return web.json_response(response_payload, headers=self._cors_headers())
        except C2ParseError as e:
            return web.json_response(
                {"status": "ERROR", "error": e.message, "field": e.field},
                status=400,
                headers=self._cors_headers(),
            )
        except Exception as e:
            return web.json_response(
                {"status": "ERROR", "error": str(e)},
                status=500,
                headers=self._cors_headers(),
            )

    async def handle_http_index(self, request: web.Request) -> web.StreamResponse:
        if self.role == "master":
            html_file = self.web_dir / "index.html"
            if html_file.exists():
                return web.FileResponse(html_file, headers=self._cors_headers())
        raise web.HTTPNotFound(headers=self._cors_headers())

    async def handle_ws_session(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.ws_clients.add(ws)
        logger.info(f"WebSocket client connected. Active connections: {len(self.ws_clients)}")

        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    try:
                        ws_req = parse_ws_message(msg.data)
                        if ws_req.action == "ws_benchmark":
                            client_ts = ws_req.timestamp if ws_req.timestamp > 0 else time.time()
                            sender_ip = request.remote or "unknown"
                            self.display_benchmark_payload(
                                preset=ws_req.preset or "Custom",
                                data=ws_req.data or "",
                                raw_bytes_len=len(msg.data),
                                client_ts=client_ts,
                                sender_ip=sender_ip,
                                protocol="WebSocket Stream",
                            )
                            resp_data = {
                                "type": "ws_benchmark_ack",
                                "node_id": self.node_id,
                                "bytes": len(msg.data),
                                "client_timestamp": client_ts,
                                "server_timestamp": time.time(),
                            }
                            await ws.send_json(resp_data)
                    except C2ParseError as e:
                        logger.debug(f"WS message parse error: {e}")
                        await ws.send_json({"type": "error", "message": e.message})
                    except Exception as e:
                        logger.debug(f"WS message unexpected error: {e}")
                elif msg.type == WSMsgType.BINARY:
                    logger.debug(f"Ignoring unexpected binary WS frame ({len(msg.data)} bytes)")
                elif msg.type == WSMsgType.ERROR:
                    logger.debug(f"WebSocket connection error: {ws.exception()}")
                    break
        finally:
            self.ws_clients.discard(ws)
            logger.info(f"WebSocket client disconnected. Remaining: {len(self.ws_clients)}")

        return ws

    async def ws_telemetry_broadcast_loop(self) -> None:
        """Streams real-time telemetry frames to all connected WebSockets (2 Hz)."""
        while self.running:
            await asyncio.sleep(0.5)
            if not self.ws_clients:
                continue

            telemetry_snapshot = {
                "type": "telemetry_update",
                "timestamp": time.time(),
                "node_id": self.node_id,
                "role": self.role,
                "local_ip": self.local_ip,
                "peers": {pid: asdict(p) for pid, p in self.peers.items()},
            }

            dead_clients: Set[web.WebSocketResponse] = set()
            for ws in list(self.ws_clients):
                if ws.closed:
                    dead_clients.add(ws)
                    continue
                try:
                    await ws.send_json(telemetry_snapshot)
                except Exception:
                    dead_clients.add(ws)

            for d in dead_clients:
                self.ws_clients.discard(d)

    # --- Start Lifecycle ---
    async def start(self) -> None:
        self.running = True
        loop = asyncio.get_running_loop()

        # 1. UDP Discovery Endpoint
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except Exception:
                pass
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        udp_sock.bind(("0.0.0.0", self.udp_port))

        transport, _ = await loop.create_datagram_endpoint(
            lambda: UDPBeaconProtocol(self),
            sock=udp_sock,
        )

        # 2. TCP Command Server
        tcp_server = await asyncio.start_server(
            self.handle_tcp_command_client, "0.0.0.0", self.tcp_port
        )

        # 3. HTTP & WebSocket Server (aiohttp)
        self.app_runner = web.AppRunner(self.app, access_log=None)
        await self.app_runner.setup()
        site = web.TCPSite(self.app_runner, "0.0.0.0", self.http_port)
        await site.start()

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
            asyncio.create_task(self.beacon_sender_loop(transport)),
            asyncio.create_task(self.failsafe_watchdog_loop()),
            asyncio.create_task(self.ws_telemetry_broadcast_loop()),
            asyncio.create_task(tcp_server.serve_forever()),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            self.running = False
            transport.close()
            tcp_server.close()
            for ws in list(self.ws_clients):
                await ws.close(code=WSCloseCode.GOING_AWAY, message=b"Server shutdown")
            if self.app_runner:
                await self.app_runner.cleanup()


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------
def main() -> None:
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
