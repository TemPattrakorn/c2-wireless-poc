"""
WebSocket telemetry streaming — connection management, broadcast loop, and benchmark session handling.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Dict, Optional, Set

from aiohttp import WSCloseCode, WSMsgType, web

from protocol import C2ParseError, parse_ws_message
from tracker import PeerTracker

logger = logging.getLogger("C2Server")


class WebSocketManager:
    """
    Manages WebSocket client connections, benchmark message dispatch,
    and real-time telemetry broadcasting at a configurable interval.
    """

    def __init__(
        self,
        node_id: str,
        role: str,
        tracker: PeerTracker,
        display_handler: Callable[..., None],
        broadcast_interval: float,
    ) -> None:
        self.node_id = node_id
        self.role = role
        self.tracker = tracker
        self.display_handler = display_handler
        self.broadcast_interval = broadcast_interval
        self.ws_clients: Set[web.WebSocketResponse] = set()
        self.running = False

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
                            resp_data: Dict[str, Any] = {
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
        """Streams real-time telemetry frames concurrently to all connected WebSockets."""
        while self.running:
            await asyncio.sleep(self.broadcast_interval)
            if not self.ws_clients:
                continue

            telemetry_snapshot: Dict[str, Any] = {
                "type": "telemetry_update",
                "timestamp": time.time(),
                "node_id": self.node_id,
                "role": self.role,
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

    async def close_all(self) -> None:
        """Close all active WebSocket connections gracefully."""
        for ws in list(self.ws_clients):
            if not ws.closed:
                await ws.close(code=WSCloseCode.GOING_AWAY, message=b"Server shutting down")
