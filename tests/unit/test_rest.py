"""
Unit tests for HTTP REST endpoints and HttpServer lifecycle (server/rest.py).
Uses the aiohttp_client fixture (TestServer + TestClient) for realistic in-process
request dispatch without calling private app._handle().
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from node import C2NodeDaemon
from tracker import NodeMetrics


class TestHttpStatusAndCors:
    async def test_status_returns_200(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """GET /api/status returns HTTP 200 with correct JSON fields."""
        client = await aiohttp_client(master_daemon.http_server.app)
        resp = await client.get("/api/status")
        assert resp.status == 200
        data = await resp.json()
        assert data["node_id"] == "test-master-srv"
        assert data["role"] == "master"
        assert "uptime_sec" in data
        assert "peers" in data

    async def test_status_includes_peer_latency_ms(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """Peer dict contains latency_ms but NOT rtt_ms after terminology cleanup."""
        master_daemon.peers["worker-1"] = NodeMetrics(
            node_id="worker-1",
            role="worker",
            ip="192.168.1.10",
            http_port=8080,
            tcp_port=9877,
            last_seen=1000.0,
        )
        client = await aiohttp_client(master_daemon.http_server.app)
        resp = await client.get("/api/status")
        assert resp.status == 200
        data = await resp.json()
        assert "worker-1" in data["peers"]
        peer = data["peers"]["worker-1"]
        assert "latency_ms" in peer
        assert "rtt_ms" not in peer

    async def test_cors_headers_on_get(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """CORS header is present on all responses."""
        client = await aiohttp_client(master_daemon.http_server.app)
        resp = await client.get("/api/status")
        assert resp.headers.get("Access-Control-Allow-Origin") == "*"

    async def test_cors_options_preflight(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """OPTIONS preflight returns 204 with CORS headers."""
        client = await aiohttp_client(master_daemon.http_server.app)
        resp = await client.options("/api/command")
        assert resp.status == 204
        assert resp.headers.get("Access-Control-Allow-Origin") == "*"
        assert "POST" in resp.headers.get("Access-Control-Allow-Methods", "")


class TestHttpCommands:
    async def test_ping_ack(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """PING command returns ACK/PONG for this node's own ID."""
        client = await aiohttp_client(master_daemon.http_server.app)
        resp = await client.post(
            "/api/command",
            json={"command": "PING", "target_id": "test-master-srv"},
        )
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "ACK"
        assert data["response"] == "PONG"
        assert data["node_id"] == "test-master-srv"

    async def test_arm_safe_estop_rejected(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """ARM, SAFE, ESTOP commands are rejected with 400 (unknown command)."""
        client = await aiohttp_client(master_daemon.http_server.app)
        for cmd in ["ARM", "SAFE", "ESTOP"]:
            resp = await client.post(
                "/api/command",
                json={"command": cmd, "target_id": "test-master-srv"},
            )
            assert resp.status == 400, f"Expected 400 for command {cmd}"
            data = await resp.json()
            assert data["status"] == "ERROR"
            assert f"Unknown command '{cmd}'" in data["error"]

    async def test_invalid_json_400(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """Malformed JSON body returns 400 with descriptive error."""
        client = await aiohttp_client(master_daemon.http_server.app)
        resp = await client.post(
            "/api/command",
            data=b"{broken json",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status == 400
        data = await resp.json()
        assert data["status"] == "ERROR"
        assert "Malformed JSON" in data["error"]

    async def test_unknown_command_400(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """Unrecognized command string returns 400."""
        client = await aiohttp_client(master_daemon.http_server.app)
        resp = await client.post("/api/command", json={"command": "INVALID_CMD"})
        assert resp.status == 400
        data = await resp.json()
        assert data["status"] == "ERROR"
        assert "Unknown command" in data["error"]

    async def test_error_middleware_500(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """Unhandled exception in command_handler is caught and returned as 500."""
        client = await aiohttp_client(master_daemon.http_server.app)
        with patch.object(
            master_daemon.http_server, "command_handler",
            side_effect=RuntimeError("Unexpected DB crash"),
        ):
            resp = await client.post("/api/command", json={"command": "PING"})
        assert resp.status == 500
        data = await resp.json()
        assert data["status"] == "ERROR"
        assert "Unexpected DB crash" in data["error"]


class TestHttpBenchmark:
    async def test_benchmark_valid_response(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """POST /api/benchmark with valid payload returns BENCHMARK_COMPLETE."""
        payload = {"preset": "16KB", "client_timestamp": 1234567.8, "data": "A" * 1024}
        raw = json.dumps(payload).encode("utf-8")
        client = await aiohttp_client(master_daemon.http_server.app)

        with patch.object(master_daemon.http_server, "display_handler"):
            resp = await client.post("/api/benchmark", json=payload)

        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "BENCHMARK_COMPLETE"
        assert data["preset"] == "16KB"
        assert data["bytes_received"] == len(raw)
        assert "server_proc_time_ms" in data

    async def test_benchmark_invalid_payload_400(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """Non-numeric client_timestamp returns 400."""
        client = await aiohttp_client(master_daemon.http_server.app)
        resp = await client.post(
            "/api/benchmark", json={"client_timestamp": "not_a_number"}
        )
        assert resp.status == 400
        data = await resp.json()
        assert data["status"] == "ERROR"

    async def test_benchmark_calls_display_handler(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """display_handler is called with correct kwargs on a valid benchmark request."""
        payload = {"preset": "16KB", "client_timestamp": 1234567.8, "data": "A" * 1024}
        raw = json.dumps(payload).encode("utf-8")
        client = await aiohttp_client(master_daemon.http_server.app)

        with patch.object(master_daemon.http_server, "display_handler") as mock_disp:
            await client.post("/api/benchmark", json=payload)
            mock_disp.assert_called_once_with(
                preset="16KB",
                data="A" * 1024,
                raw_bytes_len=len(raw),
                client_ts=1234567.8,
                sender_ip="127.0.0.1",
                protocol="HTTP POST",
            )


class TestHttpDashboardStatic:
    async def test_master_serves_index_html(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """Master node GET / returns the index.html file response."""
        client = await aiohttp_client(master_daemon.http_server.app)
        resp = await client.get("/")
        assert resp.status == 200
        assert resp.headers.get("Access-Control-Allow-Origin") == "*"

    async def test_worker_index_404(
        self, worker_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """Worker node has no / route — returns 404."""
        client = await aiohttp_client(worker_daemon.http_server.app)
        resp = await client.get("/")
        assert resp.status == 404


class TestHttpMasterProxy:
    async def test_proxy_local_ping(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """Proxy request addressed to master's own node_id is handled locally."""
        client = await aiohttp_client(master_daemon.http_server.app)
        resp = await client.post(
            f"/api/proxy/{master_daemon.node_id}/command",
            json={"command": "PING", "target_id": master_daemon.node_id},
        )
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "ACK"
        assert data["response"] == "PONG"

    async def test_proxy_not_found_404(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """Proxy request for an unknown peer returns 404."""
        client = await aiohttp_client(master_daemon.http_server.app)
        resp = await client.post(
            "/api/proxy/non-existent-worker/command",
            json={"command": "PING"},
        )
        assert resp.status == 404
        data = await resp.json()
        assert data["status"] == "ERROR"
        assert "not found in peer registry" in data["error"]

    async def test_proxy_remote_success(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """Proxy successfully forwards request to a known remote worker."""
        master_daemon.peers["worker-remote-1"] = NodeMetrics(
            node_id="worker-remote-1",
            role="worker",
            ip="192.168.1.55",
            http_port=8080,
            tcp_port=9877,
            last_seen=100.0,
        )
        client = await aiohttp_client(master_daemon.http_server.app)

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(
            return_value={"status": "ACK", "response": "PONG", "node_id": "worker-remote-1"}
        )
        mock_session = MagicMock()
        mock_session.post.return_value.__aenter__.return_value = mock_resp
        mock_session.__aenter__.return_value = mock_session

        with patch("server.rest.aiohttp.ClientSession", return_value=mock_session):
            resp = await client.post(
                "/api/proxy/worker-remote-1/command",
                json={"command": "PING"},
            )

        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "ACK"
        assert data["node_id"] == "worker-remote-1"

    async def test_proxy_remote_client_error_502(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """ClientConnectionError from remote worker is mapped to 502."""
        master_daemon.peers["worker-remote-2"] = NodeMetrics(
            node_id="worker-remote-2",
            role="worker",
            ip="192.168.1.56",
            http_port=8080,
            tcp_port=9877,
            last_seen=100.0,
        )
        client = await aiohttp_client(master_daemon.http_server.app)

        mock_session = MagicMock()
        mock_session.post.side_effect = aiohttp.ClientConnectionError("Connection refused")
        mock_session.__aenter__.return_value = mock_session

        with patch("server.rest.aiohttp.ClientSession", return_value=mock_session):
            resp = await client.post(
                "/api/proxy/worker-remote-2/command",
                json={"command": "PING"},
            )

        assert resp.status == 502
        data = await resp.json()
        assert data["status"] == "ERROR"
        assert "Failed to contact target node" in data["error"]

    async def test_proxy_remote_timeout_504(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """asyncio.TimeoutError on proxy call is mapped to 504."""
        master_daemon.peers["worker-remote-3"] = NodeMetrics(
            node_id="worker-remote-3",
            role="worker",
            ip="192.168.1.57",
            http_port=8080,
            tcp_port=9877,
            last_seen=100.0,
        )
        client = await aiohttp_client(master_daemon.http_server.app)

        mock_session = MagicMock()
        mock_session.post.side_effect = asyncio.TimeoutError()
        mock_session.__aenter__.return_value = mock_session

        with patch("server.rest.aiohttp.ClientSession", return_value=mock_session):
            resp = await client.post(
                "/api/proxy/worker-remote-3/command",
                json={"command": "PING"},
            )

        assert resp.status == 504
        data = await resp.json()
        assert data["status"] == "ERROR"
        assert "timed out" in data["error"]

    async def test_proxy_missing_node_id_400(
        self, master_daemon: C2NodeDaemon, aiohttp_client: Any
    ) -> None:
        """Blank node_id in proxy path returns 400."""
        client = await aiohttp_client(master_daemon.http_server.app)
        # Route uses URL path parameter; send a single space which becomes empty after strip()
        resp = await client.post(
            "/api/proxy/%20/command",
            json={"command": "PING"},
        )
        assert resp.status == 400
        data = await resp.json()
        assert data["status"] == "ERROR"
        assert "Missing node_id" in data["error"]


class TestServerLifecycle:
    async def test_http_server_start_and_cleanup(
        self, master_daemon: C2NodeDaemon
    ) -> None:
        """start() binds the runner and site; cleanup() closes WebSockets and runner."""
        mock_runner = AsyncMock()
        mock_site = AsyncMock()

        with (
            patch("server.rest.web.AppRunner", return_value=mock_runner),
            patch("server.rest.web.TCPSite", return_value=mock_site),
        ):
            await master_daemon.http_server.start()
            assert master_daemon.http_server.running
            assert mock_runner.setup.called
            assert mock_site.start.called

            # Add a mock active WebSocket
            mock_ws = MagicMock()
            mock_ws.closed = False
            mock_ws.close = AsyncMock()
            master_daemon.http_server.ws_clients.add(mock_ws)

            await master_daemon.http_server.cleanup()
            assert not master_daemon.http_server.running
            assert mock_ws.close.called
            assert mock_runner.cleanup.called
