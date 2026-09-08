#!/usr/bin/env python3
"""
Universal C2 Wireless Network Node Daemon (Proof of Concept)
Runs on all PCs (PC0 Master and PC1-PCn Workers).
Zero external pip dependencies (Pure Python 3 standard library).
"""

import argparse
import asyncio
import base64
import hashlib
import json
import logging
import os
import platform
import socket
import struct
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set

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
    state: str = "SAFE"  # SAFE, ARMED, ESTOP, FAILSAFE_ACTIVE
    uptime_sec: float = 0.0
    cpu_load: float = 0.0
    last_seq: int = 0
    seq_history: List[int] = field(default_factory=list)
    rtt_history: List[float] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Pure Python RFC 6455 WebSocket Implementation (Standard Library Only)
# ---------------------------------------------------------------------------
WS_MAGIC_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def compute_ws_accept(sec_key: str) -> str:
    sha1 = hashlib.sha1((sec_key.strip() + WS_MAGIC_GUID).encode("utf-8")).digest()
    return base64.b64encode(sha1).decode("ascii")


def encode_ws_frame(message: str, opcode: int = 0x1) -> bytes:
    """Encode an unmasked server-to-client WebSocket frame."""
    payload = message.encode("utf-8")
    length = len(payload)
    header = bytearray()
    header.append(0x80 | opcode)  # FIN + Opcode

    if length < 126:
        header.append(length)
    elif length <= 0xFFFF:
        header.append(126)
        header.extend(struct.pack("!H", length))
    else:
        header.append(127)
        header.extend(struct.pack("!Q", length))

    return bytes(header) + payload


async def read_ws_frame(reader: asyncio.StreamReader) -> Optional[tuple[int, str]]:
    """Decode a masked client-to-server WebSocket frame."""
    try:
        head = await reader.readexactly(2)
    except (asyncio.IncompleteReadError, ConnectionResetError):
        return None

    fin_and_opcode = head[0]
    opcode = fin_and_opcode & 0x0F
    mask_and_len = head[1]
    is_masked = (mask_and_len & 0x80) != 0
    payload_len = mask_and_len & 0x7F

    if payload_len == 126:
        len_bytes = await reader.readexactly(2)
        payload_len = struct.unpack("!H", len_bytes)[0]
    elif payload_len == 127:
        len_bytes = await reader.readexactly(8)
        payload_len = struct.unpack("!Q", len_bytes)[0]

    masking_key = b""
    if is_masked:
        masking_key = await reader.readexactly(4)

    payload_data = await reader.readexactly(payload_len)

    if is_masked:
        unmasked = bytearray(payload_len)
        for i in range(payload_len):
            unmasked[i] = payload_data[i] ^ masking_key[i % 4]
        payload_data = bytes(unmasked)

    if opcode == 0x8:  # Connection close
        return (0x8, "")
    elif opcode == 0x9:  # Ping
        return (0x9, "")

    try:
        return (opcode, payload_data.decode("utf-8"))
    except UnicodeDecodeError:
        return (opcode, payload_data.decode("latin1"))


# ---------------------------------------------------------------------------
# UDP Discovery & Link Quality Monitoring Protocol
# ---------------------------------------------------------------------------
class UDPBeaconProtocol(asyncio.DatagramProtocol):
    def __init__(self, daemon: "C2NodeDaemon"):
        self.daemon = daemon
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        sock = self.transport.get_extra_info("socket")
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    def datagram_received(self, data, addr):
        try:
            msg = json.loads(data.decode("utf-8"))
            self.daemon.handle_incoming_beacon(msg, addr[0])
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

        # Operational State: SAFE, ARMED, ESTOP, FAILSAFE_ACTIVE
        self.state = "SAFE"
        self.last_master_contact = time.time() if self.role == "worker" else 0.0

        # Peer registry & active WebSocket subscribers
        self.peers: Dict[str, NodeMetrics] = {}
        self.ws_clients: Set[asyncio.StreamWriter] = set()

        # Web static files directory
        self.web_dir = Path(__file__).parent / "web"

    def _determine_local_ip(self) -> str:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        except Exception:
            ip = "127.0.0.1"
        finally:
            s.close()
        return ip

    # --- UDP Discovery & Quality Math ---
    def handle_incoming_beacon(self, msg: dict, sender_ip: str):
        sender_id = msg.get("node_id")
        if not sender_id or sender_id == self.node_id:
            return

        now = time.time()
        sender_role = msg.get("role", "worker")
        seq = msg.get("seq", 0)
        sent_ts = msg.get("timestamp", now)
        http_p = msg.get("http_port", config.http_worker_port)
        tcp_p = msg.get("tcp_port", config.tcp_cmd_port)

        # Track contact with Master for failsafe
        if sender_role == "master":
            self.last_master_contact = now
            if self.state == "FAILSAFE_ACTIVE":
                logger.info(f"Master contact restored from {sender_ip}. Reverting to SAFE.")
                self.state = "SAFE"
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
        peer.state = msg.get("state", "SAFE")
        peer.uptime_sec = round(now - msg.get("start_time", now), 1)
        peer.cpu_load = msg.get("cpu_load", 0.0)
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

    async def beacon_sender_loop(self, transport):
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
                "state": self.state,
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

            # 3. Prune timed-out peers on master
            now = time.time()
            for pid, peer in list(self.peers.items()):
                if now - peer.last_seen > config.failsafe_timeout_sec:
                    logger.warning(f"[-] Node [{pid}] timed out (> {config.failsafe_timeout_sec}s silence).")
                    del self.peers[pid]

            await asyncio.sleep(config.beacon_interval_sec)

    # --- Fail-Safe Watchdog (Worker Only) ---
    async def failsafe_watchdog_loop(self):
        """Monitors contact with C2 Master. Sets state to SAFE if connection drops."""
        while self.running:
            await asyncio.sleep(1.0)
            if self.role == "worker" and self.master_ip:
                elapsed = time.time() - self.last_master_contact
                if elapsed > config.failsafe_timeout_sec and self.state != "FAILSAFE_ACTIVE":
                    logger.critical(
                        f"[FAIL-SAFE TRIGGERED] Lost Master contact for {elapsed:.1f}s! Switching to FAILSAFE_ACTIVE."
                    )
                    self.state = "FAILSAFE_ACTIVE"

    # --- TCP Command Engine ---
    async def handle_tcp_command_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        client_addr = writer.get_extra_info("peername")
        try:
            line = await reader.readline()
            if not line:
                return
            req = json.loads(line.decode("utf-8"))
            cmd = req.get("command", "")
            target = req.get("target_id", "all")
            args = req.get("args", {})

            if target in (self.node_id, "all"):
                res = self.process_c2_action(cmd, args)
            else:
                res = {"status": "IGNORED", "reason": f"Target mismatch ({target})"}

            writer.write((json.dumps(res) + "\n").encode("utf-8"))
            await writer.drain()
        except Exception as e:
            logger.error(f"TCP command execution error from {client_addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    def process_c2_action(self, cmd: str, args: dict) -> dict:
        cmd = cmd.upper()
        logger.info(f"[C2 EXEC] Command '{cmd}' received with args: {args}")
        if cmd == "PING":
            return {"status": "ACK", "response": "PONG", "timestamp": time.time(), "node_id": self.node_id}
        elif cmd == "ARM":
            if self.state == "FAILSAFE_ACTIVE":
                return {"status": "NACK", "reason": "Cannot arm while in FAILSAFE_ACTIVE", "state": self.state}
            self.state = "ARMED"
            return {"status": "ACK", "state": self.state, "node_id": self.node_id}
        elif cmd == "SAFE":
            self.state = "SAFE"
            return {"status": "ACK", "state": self.state, "node_id": self.node_id}
        elif cmd == "ESTOP":
            self.state = "ESTOP"
            logger.critical("EMERGENCY STOP (ESTOP) ACTIVE!")
            return {"status": "ACK", "state": self.state, "node_id": self.node_id}
        elif cmd == "STATUS":
            return {
                "status": "ACK",
                "node_id": self.node_id,
                "role": self.role,
                "state": self.state,
                "uptime_sec": round(time.time() - self.start_time, 1),
            }
        else:
            return {"status": "NACK", "reason": f"Unknown command '{cmd}'"}

    # --- HTTP & WebSocket Unified Server ---
    async def handle_http_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            # Read request line
            req_line = await reader.readline()
            if not req_line:
                writer.close()
                return

            req_line_str = req_line.decode("utf-8", errors="ignore").strip()
            parts = req_line_str.split(" ")
            if len(parts) < 2:
                writer.close()
                return

            method, path = parts[0].upper(), parts[1]

            # Read headers
            headers = {}
            while True:
                line = await reader.readline()
                if not line or line == b"\r\n" or line == b"\n":
                    break
                header_str = line.decode("utf-8", errors="ignore").strip()
                if ":" in header_str:
                    k, v = header_str.split(":", 1)
                    headers[k.strip().lower()] = v.strip()

            # Check for WebSocket Upgrade
            if headers.get("upgrade", "").lower() == "websocket":
                sec_key = headers.get("sec-websocket-key")
                if sec_key:
                    accept_key = compute_ws_accept(sec_key)
                    upgrade_response = (
                        "HTTP/1.1 101 Switching Protocols\r\n"
                        "Upgrade: websocket\r\n"
                        "Connection: Upgrade\r\n"
                        f"Sec-WebSocket-Accept: {accept_key}\r\n"
                        "\r\n"
                    )
                    writer.write(upgrade_response.encode("utf-8"))
                    await writer.drain()
                    await self.handle_ws_session(reader, writer)
                    return

            # Read Body if Content-Length specified
            body_bytes = b""
            content_length = int(headers.get("content-length", 0))
            if content_length > 0:
                body_bytes = await reader.readexactly(content_length)

            # Handle HTTP API and Static Content
            await self.route_http_request(method, path, headers, body_bytes, writer)

        except Exception as e:
            logger.debug(f"HTTP connection handler exception: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def route_http_request(
        self, method: str, path: str, headers: dict, body: bytes, writer: asyncio.StreamWriter
    ):
        cors_headers = (
            "Access-Control-Allow-Origin: *\r\n"
            "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
            "Access-Control-Allow-Headers: Content-Type\r\n"
        )

        if method == "OPTIONS":
            resp = f"HTTP/1.1 204 No Content\r\n{cors_headers}\r\n"
            writer.write(resp.encode("utf-8"))
            await writer.drain()
            return

        # 1. API: Node Status & Registry
        if path == "/api/status" and method == "GET":
            data = {
                "node_id": self.node_id,
                "role": self.role,
                "state": self.state,
                "uptime_sec": round(time.time() - self.start_time, 1),
                "ip": self.local_ip,
                "http_port": self.http_port,
                "tcp_port": self.tcp_port,
                "peers": {pid: asdict(p) for pid, p in self.peers.items()},
            }
            body_resp = json.dumps(data, indent=2).encode("utf-8")
            resp = (
                f"HTTP/1.1 200 OK\r\n"
                f"Content-Type: application/json\r\n"
                f"Content-Length: {len(body_resp)}\r\n"
                f"{cors_headers}\r\n"
            )
            writer.write(resp.encode("utf-8") + body_resp)
            await writer.drain()
            return

        # 2. API: C2 Command Execution via HTTP POST
        if path == "/api/command" and method == "POST":
            try:
                req_json = json.loads(body.decode("utf-8"))
                cmd = req_json.get("command", "")
                args = req_json.get("args", {})
                res = self.process_c2_action(cmd, args)
            except Exception as e:
                res = {"status": "ERROR", "error": str(e)}

            body_resp = json.dumps(res).encode("utf-8")
            resp = (
                f"HTTP/1.1 200 OK\r\n"
                f"Content-Type: application/json\r\n"
                f"Content-Length: {len(body_resp)}\r\n"
                f"{cors_headers}\r\n"
            )
            writer.write(resp.encode("utf-8") + body_resp)
            await writer.drain()
            return

        # 3. API: HTTP Data Transfer Benchmark
        if path == "/api/benchmark" and method == "POST":
            recv_time = time.time()
            size_received = len(body)
            try:
                payload_json = json.loads(body.decode("utf-8"))
                client_send_ts = payload_json.get("client_timestamp", recv_time)
                preset_label = payload_json.get("preset", "Custom")
            except Exception:
                client_send_ts = recv_time
                preset_label = "Binary/Raw"

            duration_server = time.time() - recv_time
            response_payload = {
                "status": "BENCHMARK_COMPLETE",
                "node_id": self.node_id,
                "bytes_received": size_received,
                "preset": preset_label,
                "server_receive_ts": recv_time,
                "server_proc_time_ms": round(duration_server * 1000.0, 3),
                "client_latency_estimate_ms": round((recv_time - client_send_ts) * 1000.0, 2),
            }

            body_resp = json.dumps(response_payload).encode("utf-8")
            resp = (
                f"HTTP/1.1 200 OK\r\n"
                f"Content-Type: application/json\r\n"
                f"Content-Length: {len(body_resp)}\r\n"
                f"{cors_headers}\r\n"
            )
            writer.write(resp.encode("utf-8") + body_resp)
            await writer.drain()
            return

        # 4. Static UI: Serve web/index.html on C2 Master
        if self.role == "master" and (path in ("/", "/index.html")):
            html_file = self.web_dir / "index.html"
            if html_file.exists():
                with open(html_file, "rb") as f:
                    content = f.read()
                resp = (
                    f"HTTP/1.1 200 OK\r\n"
                    f"Content-Type: text/html; charset=utf-8\r\n"
                    f"Content-Length: {len(content)}\r\n"
                    f"{cors_headers}\r\n"
                )
                writer.write(resp.encode("utf-8") + content)
                await writer.drain()
                return

        # 404 Fallback
        not_found = b"404 Not Found"
        resp = f"HTTP/1.1 404 Not Found\r\nContent-Length: {len(not_found)}\r\n{cors_headers}\r\n"
        writer.write(resp.encode("utf-8") + not_found)
        await writer.drain()

    # --- WebSocket Streaming Engine ---
    async def handle_ws_session(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Maintains live WebSocket connection for streaming telemetry and benchmark bursts."""
        self.ws_clients.add(writer)
        logger.info(f"WebSocket client connected. Active connections: {len(self.ws_clients)}")

        try:
            while self.running:
                frame = await read_ws_frame(reader)
                if not frame:
                    break
                opcode, payload_str = frame
                if opcode == 0x8:  # Close
                    break
                elif opcode == 0x9:  # Ping
                    writer.write(encode_ws_frame("", opcode=0xA))  # Pong
                    await writer.drain()
                elif opcode == 0x1:  # Text frame from client (e.g. benchmark burst request)
                    try:
                        msg = json.loads(payload_str)
                        action = msg.get("action")
                        if action == "ws_benchmark":
                            # Echo benchmark payload with server timestamp
                            client_ts = msg.get("timestamp", time.time())
                            resp_data = {
                                "type": "ws_benchmark_ack",
                                "node_id": self.node_id,
                                "bytes": len(payload_str),
                                "client_timestamp": client_ts,
                                "server_timestamp": time.time(),
                            }
                            writer.write(encode_ws_frame(json.dumps(resp_data)))
                            await writer.drain()
                    except Exception as e:
                        logger.debug(f"WS message parse error: {e}")

        except Exception as e:
            logger.debug(f"WebSocket session closed with error: {e}")
        finally:
            self.ws_clients.discard(writer)
            logger.info(f"WebSocket client disconnected. Remaining: {len(self.ws_clients)}")

    async def ws_telemetry_broadcast_loop(self):
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
                "state": self.state,
                "local_ip": self.local_ip,
                "peers": {pid: asdict(p) for pid, p in self.peers.items()},
            }
            frame = encode_ws_frame(json.dumps(telemetry_snapshot))

            dead_clients = set()
            for client in list(self.ws_clients):
                try:
                    client.write(frame)
                    await client.drain()
                except Exception:
                    dead_clients.add(client)

            for d in dead_clients:
                self.ws_clients.discard(d)

    # --- Start Lifecycle ---
    async def start(self):
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

        # 3. HTTP & WebSocket Server
        http_server = await asyncio.start_server(
            self.handle_http_connection, "0.0.0.0", self.http_port
        )

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
            asyncio.create_task(http_server.serve_forever()),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            transport.close()
            tcp_server.close()
            http_server.close()


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------
def main():
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
