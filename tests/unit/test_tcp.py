"""
Unit tests for TCP command server (server/tcp.py).
All tests call handle_client directly with mocked readers/writers — no real socket is bound.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from node import C2NodeDaemon


class TestTcpCommandServer:
    async def test_ping_response(self, master_daemon: C2NodeDaemon) -> None:
        """Valid PING command returns ACK/PONG over the TCP stream."""
        reader = AsyncMock()
        reader.readline.return_value = json.dumps({"command": "PING"}).encode("utf-8") + b"\n"

        writer = MagicMock()
        writer.get_extra_info.return_value = ("127.0.0.1", 12345)
        writer.drain = AsyncMock()
        writer.wait_closed = AsyncMock()

        written_chunks: list[bytes] = []
        writer.write = lambda data: written_chunks.append(data)

        await master_daemon.tcp_server.handle_client(reader, writer)

        assert len(written_chunks) > 0
        resp = json.loads(b"".join(written_chunks).decode("utf-8").strip())
        assert resp["status"] == "ACK"
        assert resp["response"] == "PONG"

    async def test_empty_line_no_response(self, master_daemon: C2NodeDaemon) -> None:
        """Empty line (EOF) causes handler to return without writing any response."""
        reader = AsyncMock()
        reader.readline.return_value = b""

        writer = MagicMock()
        writer.get_extra_info.return_value = ("127.0.0.1", 12345)
        writer.drain = AsyncMock()
        writer.wait_closed = AsyncMock()

        written_chunks: list[bytes] = []
        writer.write = lambda data: written_chunks.append(data)

        await master_daemon.tcp_server.handle_client(reader, writer)
        assert len(written_chunks) == 0

    async def test_handler_exception(self, master_daemon: C2NodeDaemon) -> None:
        """Unhandled exception in command_handler returns ERROR status over the stream."""
        reader = AsyncMock()
        reader.readline.return_value = json.dumps({"command": "PING"}).encode("utf-8") + b"\n"

        writer = MagicMock()
        writer.get_extra_info.return_value = ("127.0.0.1", 12345)
        writer.drain = AsyncMock()
        writer.wait_closed = AsyncMock()

        written_chunks: list[bytes] = []
        writer.write = lambda data: written_chunks.append(data)

        with patch.object(
            master_daemon.tcp_server, "command_handler", side_effect=RuntimeError("Handler crashed")
        ):
            await master_daemon.tcp_server.handle_client(reader, writer)

        assert len(written_chunks) > 0
        resp = json.loads(b"".join(written_chunks).decode("utf-8").strip())
        assert resp["status"] == "ERROR"
        assert "Handler crashed" in resp["error"]

    async def test_connection_error(self, master_daemon: C2NodeDaemon) -> None:
        """ConnectionResetError during readline is swallowed and writer.close() is still called."""
        reader = AsyncMock()
        reader.readline.side_effect = ConnectionResetError("Connection lost")

        writer = MagicMock()
        writer.get_extra_info.return_value = ("127.0.0.1", 12345)
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock(side_effect=ConnectionResetError("Socket broken"))

        await master_daemon.tcp_server.handle_client(reader, writer)
        assert writer.close.called

    async def test_start_stop(self, master_daemon: C2NodeDaemon) -> None:
        """start() returns the asyncio.Server and running flag is toggled correctly."""
        mock_srv = AsyncMock()
        mock_srv.close = MagicMock()
        with patch("server.tcp.asyncio.start_server", AsyncMock(return_value=mock_srv)):
            srv = await master_daemon.tcp_server.start()
            assert srv is mock_srv
            assert master_daemon.tcp_server.running

            await master_daemon.tcp_server.stop()
            assert not master_daemon.tcp_server.running
            assert mock_srv.close.called
