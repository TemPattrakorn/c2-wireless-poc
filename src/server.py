"""
TCP command server and aiohttp HTTP/WebSocket server components for C2 node.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
import sys
import time
from typing import Any, Awaitable, Callable, Dict, Optional, Set

import aiohttp
from aiohttp import WSCloseCode, WSMsgType, web

from config import config
from presentation import (
    BANNER_MAX_FULL_DISPLAY_CHARS,
    BANNER_PREVIEW_HEAD_CHARS,
    BANNER_PREVIEW_TAIL_CHARS,
    display_benchmark_payload,
    format_benchmark_banner,
)
from protocol import (
    C2CommandRequest,
    C2ParseError,
    parse_http_benchmark_payload,
    parse_http_command_payload,
    parse_tcp_command_frame,
    parse_ws_message,
)
from tracker import PeerTracker

logger = logging.getLogger("C2Server")



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


class TcpCommandServer:
    """
    Manages line-delimited TCP command socket serving.
    """

    def __init__(
        self,
        node_id: str,
        tcp_port: int,
        command_handler: Optional[Callable[[C2CommandRequest], Dict[str, Any]]] = None,
    ):
        self.node_id = node_id
        self.tcp_port = tcp_port
        self.command_handler = (
            command_handler
            if command_handler is not None
            else (lambda req: execute_standard_command(req, self.node_id))
        )
        self.server: Optional[asyncio.Server] = None
        self.running = False

    async def handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        client_addr = writer.get_extra_info("peername")
        logger.info(f"[TCP] Command client connected from {client_addr}")
        try:
            line = await reader.readline()
            if not line:
                return
            try:
                cmd_req = parse_tcp_command_frame(line)
                response_obj = self.command_handler(cmd_req)
            except C2ParseError as e:
                response_obj = {
                    "status": "ERROR",
                    "error": e.message,
                    "field": e.field,
                    "node_id": self.node_id,
                }
            except Exception as e:
                response_obj = {
                    "status": "ERROR",
                    "error": str(e),
                    "node_id": self.node_id,
                }
            resp_bytes = (json.dumps(response_obj) + "\n").encode("utf-8")
            writer.write(resp_bytes)
            await writer.drain()
        except (ConnectionError, OSError) as e:
            logger.debug(f"[TCP] Connection error with {client_addr}: {e}")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError) as e:
                logger.debug(f"[TCP] Error closing socket for {client_addr}: {e}")

    async def start(self) -> asyncio.Server:
        self.running = True
        self.server = await asyncio.start_server(
            self.handle_client,
            "0.0.0.0",
            self.tcp_port,
        )
        return self.server

    async def stop(self) -> None:
        self.running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()


CORS_HEADERS: Dict[str, str] = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
}

NODE_ID_KEY: web.AppKey[str] = web.AppKey("node_id", str)


@web.middleware
async def cors_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    """Apply CORS headers to all responses and intercept OPTIONS preflight requests."""
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=CORS_HEADERS)
    try:
        resp = await handler(request)
    except web.HTTPException as ex:
        for k, v in CORS_HEADERS.items():
            ex.headers.setdefault(k, v)
        raise
    if not resp.prepared:
        for k, v in CORS_HEADERS.items():
            resp.headers.setdefault(k, v)
    return resp


@web.middleware
async def error_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    """Centralize exception handling: map C2ParseError to 400 and unexpected errors to 500."""
    node_id = str(request.app.get(NODE_ID_KEY, request.app.get("node_id", "unknown")))
    try:
        return await handler(request)
    except web.HTTPException:
        raise
    except asyncio.CancelledError:
        raise
    except C2ParseError as e:
        return web.json_response(
            {
                "status": "ERROR",
                "error": e.message,
                "field": e.field,
                "node_id": node_id,
            },
            status=400,
        )
    except Exception as e:
        logger.exception(f"Unhandled error handling HTTP request {request.path}: {e}")
        return web.json_response(
            {
                "status": "ERROR",
                "error": str(e),
                "node_id": node_id,
            },
            status=500,
        )


class HttpServer:
    """
    aiohttp HTTP REST endpoints, static file serving, and WebSocket telemetry manager.
    """

    def __init__(
        self,
        node_id: str,
        role: str,
        http_port: int,
        tcp_port: int,
        local_ip: str,
        tracker: PeerTracker,
        start_time: Optional[float] = None,
        web_dir: Optional[Path] = None,
        command_handler: Optional[Callable[[C2CommandRequest], Dict[str, Any]]] = None,
        display_handler: Optional[Callable[..., None]] = None,
    ):
        self.node_id = node_id
        self.role = role
        self.http_port = http_port
        self.tcp_port = tcp_port
        self.local_ip = local_ip
        self.tracker = tracker
        self.start_time = start_time if start_time is not None else time.time()
        self.web_dir = web_dir or (Path(__file__).parent / "web")
        self.command_handler = (
            command_handler
            if command_handler is not None
            else (lambda req: execute_standard_command(req, self.node_id))
        )
        self.display_handler: Callable[..., None] = (
            display_handler
            if display_handler is not None
            else self.display_benchmark_payload
        )
        self.running = False
        self.ws_clients: Set[web.WebSocketResponse] = set()

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
        display_benchmark_payload(
            node_id=self.node_id,
            role=self.role,
            preset=preset,
            data=data,
            raw_bytes_len=raw_bytes_len,
            client_ts=client_ts,
            sender_ip=sender_ip,
            protocol=protocol,
        )

    def _create_web_app(self) -> web.Application:
        app = web.Application(middlewares=[cors_middleware, error_middleware])
        app[NODE_ID_KEY] = self.node_id
        app.router.add_get("/api/status", self.handle_http_status)
        app.router.add_post("/api/command", self.handle_http_command)
        app.router.add_post("/api/benchmark", self.handle_http_benchmark)
        app.router.add_get("/ws", self.handle_ws_session)

        # Master node serves web UI dashboard and provides worker command proxy
        if self.role == "master":
            app.router.add_post("/api/proxy/{node_id}/command", self.handle_proxy_command)
            if self.web_dir.exists():
                app.router.add_get("/", self.handle_http_index)
                app.router.add_static("/static", self.web_dir)
        app.freeze()
        return app

    async def handle_proxy_command(self, request: web.Request) -> web.Response:
        """Proxy a command request from browser/client to a remote worker node."""
        target_node_id = request.match_info.get("node_id", "").strip()
        if not target_node_id:
            return web.json_response(
                {"status": "ERROR", "error": "Missing node_id in proxy path", "node_id": self.node_id},
                status=400,
            )

        body = await request.read()
        # Strictly validate command payload before proxy dispatch
        parse_http_command_payload(body)

        # Handle command locally if targeted at Master itself
        if target_node_id == self.node_id:
            cmd_req = parse_http_command_payload(body)
            return web.json_response(self.command_handler(cmd_req))

        target_peer = self.tracker.peers.get(target_node_id)
        if not target_peer:
            return web.json_response(
                {
                    "status": "ERROR",
                    "error": f"Target node [{target_node_id}] not found in peer registry",
                    "node_id": self.node_id,
                },
                status=404,
            )

        target_url = f"http://{target_peer.ip}:{target_peer.http_port}/api/command"
        timeout_sec = config.master_proxy_timeout_sec
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    target_url,
                    data=body,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=timeout_sec),
                ) as resp:
                    resp_data = await resp.json()
                    return web.json_response(resp_data, status=resp.status)
        except aiohttp.ClientError as e:
            logger.warning(f"Failed to proxy command to {target_node_id} ({target_url}): {e}")
            return web.json_response(
                {
                    "status": "ERROR",
                    "error": f"Failed to contact target node [{target_node_id}]: {e}",
                    "node_id": self.node_id,
                },
                status=502,
            )
        except asyncio.TimeoutError:
            logger.warning(f"Timeout proxying command to {target_node_id} ({target_url})")
            return web.json_response(
                {
                    "status": "ERROR",
                    "error": f"Proxy request to node [{target_node_id}] timed out (> {timeout_sec}s)",
                    "node_id": self.node_id,
                },
                status=504,
            )

    async def handle_http_status(self, request: web.Request) -> web.Response:
        data = {
            "node_id": self.node_id,
            "role": self.role,
            "uptime_sec": round(time.time() - self.start_time, 1),
            "ip": self.local_ip,
            "http_port": self.http_port,
            "tcp_port": self.tcp_port,
            "peers": self.tracker.to_dict(),
        }
        return web.json_response(data)

    async def handle_http_command(self, request: web.Request) -> web.Response:
        body = await request.read()
        cmd_req = parse_http_command_payload(body)
        res = self.command_handler(cmd_req)
        return web.json_response(res)

    async def handle_http_benchmark(self, request: web.Request) -> web.Response:
        recv_time = time.time()
        body = await request.read()
        bench_req = parse_http_benchmark_payload(body)

        client_ts = bench_req.client_timestamp if bench_req.client_timestamp > 0 else time.time()
        sender_ip = request.remote or "unknown"
        self.display_handler(
            preset=bench_req.preset,
            data=bench_req.data,
            raw_bytes_len=len(body),
            client_ts=client_ts,
            sender_ip=sender_ip,
            protocol="HTTP POST",
        )

        duration_server = time.time() - recv_time
        response_payload = {
            "status": "BENCHMARK_COMPLETE",
            "node_id": self.node_id,
            "role": self.role,
            "preset": bench_req.preset,
            "bytes_received": len(body),
            "client_timestamp": client_ts,
            "server_receive_ts": recv_time,
            "server_proc_time_ms": round(duration_server * 1000.0, 3),
            "client_latency_estimate_ms": round((recv_time - client_ts) * 1000.0, 2),
        }
        return web.json_response(response_payload)

    async def handle_http_index(self, request: web.Request) -> web.StreamResponse:
        if self.role == "master" and self.web_dir.exists():
            html_file = self.web_dir / "index.html"
            if html_file.exists():
                return web.FileResponse(html_file)
        raise web.HTTPNotFound()

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
                            self.display_handler(
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
        """Streams real-time telemetry frames concurrently to all connected WebSockets (2 Hz)."""
        while self.running:
            await asyncio.sleep(config.ws_broadcast_interval_sec)
            if not self.ws_clients:
                continue

            telemetry_snapshot = {
                "type": "telemetry_update",
                "timestamp": time.time(),
                "node_id": self.node_id,
                "role": self.role,
                "local_ip": self.local_ip,
                "peers": self.tracker.to_dict(),
            }

            clients = list(self.ws_clients)
            if not clients:
                continue

            async def _send(client: web.WebSocketResponse) -> Optional[web.WebSocketResponse]:
                if client.closed:
                    return client
                try:
                    await asyncio.wait_for(client.send_json(telemetry_snapshot), timeout=0.4)
                    return None
                except (ConnectionError, RuntimeError, OSError, asyncio.TimeoutError):
                    return client

            results = await asyncio.gather(*[_send(c) for c in clients], return_exceptions=True)
            for res in results:
                if isinstance(res, web.WebSocketResponse):
                    self.ws_clients.discard(res)


    async def start(self) -> None:
        self.running = True
        self.app_runner = web.AppRunner(self.app, access_log=None)
        await self.app_runner.setup()
        site = web.TCPSite(self.app_runner, "0.0.0.0", self.http_port)
        await site.start()

    async def cleanup(self) -> None:
        self.running = False
        for ws in list(self.ws_clients):
            if not ws.closed:
                await ws.close(code=WSCloseCode.GOING_AWAY, message=b"Server shutting down")
        if self.app_runner:
            await self.app_runner.cleanup()
