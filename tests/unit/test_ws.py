"""
Unit tests for WebSocket session handling and telemetry broadcasting (server/ws.py).
Tests call WebSocketManager methods directly with patched web.WebSocketResponse objects —
no real loopback port is needed.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import WSMsgType, web

from node import C2NodeDaemon


class TestWebSocketSessions:
    async def test_ws_benchmark_flow(self, master_daemon: C2NodeDaemon) -> None:
        """ws_benchmark message is dispatched to display_handler and acknowledged."""
        req = MagicMock(spec=web.Request)
        req.remote = "unknown"

        mock_msg = MagicMock()
        mock_msg.type = WSMsgType.TEXT
        mock_msg.data = json.dumps({
            "action": "ws_benchmark",
            "preset": "16KB",
            "timestamp": 1234567.89,
            "data": "WS_TEST_PAYLOAD",
        })

        async def msg_iter() -> Any:
            yield mock_msg

        mock_ws = MagicMock()
        mock_ws.prepare = AsyncMock()
        mock_ws.send_json = AsyncMock()
        mock_ws.closed = False
        mock_ws.__aiter__ = lambda self: msg_iter()

        ws_mgr = master_daemon.http_server.ws_manager
        with patch.object(ws_mgr, "display_handler") as mock_disp:
            with patch("server.ws.web.WebSocketResponse", return_value=mock_ws):
                await ws_mgr.handle_ws_session(req)

            mock_disp.assert_called_once_with(
                preset="16KB",
                data="WS_TEST_PAYLOAD",
                raw_bytes_len=len(mock_msg.data),
                client_ts=1234567.89,
                sender_ip="unknown",
                protocol="WebSocket Stream",
            )

        assert mock_ws.prepare.called
        assert mock_ws.send_json.called
        call_args = mock_ws.send_json.call_args[0][0]
        assert call_args["type"] == "ws_benchmark_ack"
        assert call_args["node_id"] == "test-master-srv"
        assert call_args["client_timestamp"] == 1234567.89

    async def test_ws_telemetry_broadcast(self, master_daemon: C2NodeDaemon) -> None:
        """Broadcast loop sends telemetry_update frames to connected clients."""
        mock_ws = MagicMock()
        mock_ws.closed = False
        mock_ws.send_json = AsyncMock()

        ws_mgr = master_daemon.http_server.ws_manager
        ws_mgr.ws_clients.add(mock_ws)
        ws_mgr.running = True

        task = asyncio.create_task(ws_mgr.ws_telemetry_broadcast_loop())
        await asyncio.sleep(0.6)
        ws_mgr.running = False
        await task

        assert mock_ws.send_json.called
        telemetry = mock_ws.send_json.call_args[0][0]
        assert telemetry["type"] == "telemetry_update"
        assert telemetry["node_id"] == "test-master-srv"

    async def test_ws_binary_and_error_frames(self, master_daemon: C2NodeDaemon) -> None:
        """Binary frames are silently ignored; ERROR frames close the connection."""
        req = MagicMock(spec=web.Request)
        req.remote = "unknown"

        bin_msg = MagicMock()
        bin_msg.type = WSMsgType.BINARY
        bin_msg.data = b"\x00\x01\x02\x03"

        err_msg = MagicMock()
        err_msg.type = WSMsgType.ERROR
        err_msg.data = None

        async def msg_iter() -> Any:
            yield bin_msg
            yield err_msg

        mock_ws = MagicMock()
        mock_ws.prepare = AsyncMock()
        mock_ws.send_json = AsyncMock()
        mock_ws.closed = False
        mock_ws.exception.return_value = RuntimeError("Mock WS socket reset")
        mock_ws.__aiter__ = lambda self: msg_iter()

        ws_mgr = master_daemon.http_server.ws_manager
        with patch("server.ws.web.WebSocketResponse", return_value=mock_ws):
            await ws_mgr.handle_ws_session(req)

        assert mock_ws.prepare.called
        assert mock_ws not in ws_mgr.ws_clients

    async def test_ws_parse_error(self, master_daemon: C2NodeDaemon) -> None:
        """Unknown WebSocket action returns an error frame to the client."""
        req = MagicMock(spec=web.Request)
        req.remote = "unknown"

        invalid_msg = MagicMock()
        invalid_msg.type = WSMsgType.TEXT
        invalid_msg.data = json.dumps({"action": "unknown_action_xyz"})

        async def msg_iter() -> Any:
            yield invalid_msg

        mock_ws = MagicMock()
        mock_ws.prepare = AsyncMock()
        mock_ws.send_json = AsyncMock()
        mock_ws.closed = False
        mock_ws.__aiter__ = lambda self: msg_iter()

        ws_mgr = master_daemon.http_server.ws_manager
        with patch("server.ws.web.WebSocketResponse", return_value=mock_ws):
            await ws_mgr.handle_ws_session(req)

        assert mock_ws.send_json.called
        err_call = mock_ws.send_json.call_args[0][0]
        assert err_call["type"] == "error"
        assert "Unsupported WebSocket action" in err_call["message"]

    async def test_ws_broadcast_prunes_dead_clients(self, master_daemon: C2NodeDaemon) -> None:
        """Broadcast loop removes clients that raise ConnectionError on send."""
        dead_ws = MagicMock(spec=web.WebSocketResponse)
        dead_ws.closed = False
        dead_ws.send_json = AsyncMock(side_effect=ConnectionResetError("Peer closed"))

        alive_ws = MagicMock(spec=web.WebSocketResponse)
        alive_ws.closed = False
        alive_ws.send_json = AsyncMock()

        ws_mgr = master_daemon.http_server.ws_manager
        ws_mgr.ws_clients.add(dead_ws)
        ws_mgr.ws_clients.add(alive_ws)
        ws_mgr.running = True

        task = asyncio.create_task(ws_mgr.ws_telemetry_broadcast_loop())
        await asyncio.sleep(0.6)
        ws_mgr.running = False
        await task

        assert dead_ws not in ws_mgr.ws_clients
        assert alive_ws in ws_mgr.ws_clients
