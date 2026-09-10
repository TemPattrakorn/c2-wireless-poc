"""
HTTP REST API layer — CORS/error middleware, REST endpoints, static file serving,
master command proxy, and HttpServer lifecycle.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
import time
from typing import Any, Awaitable, Callable, Dict, Optional

import aiohttp
from aiohttp import web

from config import config
from presentation import display_benchmark_payload
from protocol import (
    C2CommandRequest,
    C2ParseError,
    parse_http_benchmark_payload,
    parse_http_command_payload,
)
from tracker import PeerTracker
from server.commands import execute_standard_command
from server.ws import WebSocketManager

logger = logging.getLogger("C2Server")


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
        self.web_dir = web_dir or (Path(__file__).parent.parent / "web")
        self.command_handler = (
            command_handler
            if command_handler is not None
            else (lambda req: execute_standard_command(req, self.node_id))
        )
        self.display_handler: Callable[..., None] = (
            display_handler
            if display_handler is not None
            else self._default_display_handler
        )
        self.running = False

        self.ws_manager = WebSocketManager(
            node_id=self.node_id,
            role=self.role,
            tracker=self.tracker,
            display_handler=self.display_handler,
            broadcast_interval=config.ws_broadcast_interval_sec,
        )

        # Expose ws_clients as a pass-through for backward compatibility with tests
        # that directly manipulate master_daemon.http_server.ws_clients
        self.ws_clients = self.ws_manager.ws_clients

        self.app: web.Application = self._create_web_app()
        self.app_runner: Optional[web.AppRunner] = None

    def _default_display_handler(
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
        app.router.add_get("/ws", self.ws_manager.handle_ws_session)

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
        # Parse once for validation; reuse cmd_req for local branch, forward body bytes to remote
        cmd_req = parse_http_command_payload(body)

        # Handle command locally if targeted at Master itself
        if target_node_id == self.node_id:
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

    async def ws_telemetry_broadcast_loop(self) -> None:
        """Forward to WebSocketManager broadcast loop (delegates internal running flag)."""
        await self.ws_manager.ws_telemetry_broadcast_loop()

    async def start(self) -> None:
        self.running = True
        self.ws_manager.running = True
        self.app_runner = web.AppRunner(self.app, access_log=None)
        await self.app_runner.setup()
        site = web.TCPSite(self.app_runner, "0.0.0.0", self.http_port)
        await site.start()

    async def cleanup(self) -> None:
        self.running = False
        self.ws_manager.running = False
        await self.ws_manager.close_all()
        if self.app_runner:
            await self.app_runner.cleanup()
