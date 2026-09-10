"""
Unit and integration tests for C2 node HTTP, WebSocket, TCP, and Beacon components.
Tests handler logic directly using aiohttp.test_utils and pytest fixtures
under native pytest-asyncio.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
from aiohttp import WSMsgType, web
from aiohttp.test_utils import make_mocked_request
import pytest

from node import C2NodeDaemon, NodeMetrics
from presentation import display_benchmark_payload




def make_json_request(
    app: web.Application,
    method: str,
    path: str,
    payload: Any = None,
) -> web.Request:
    """Helper to create a mocked aiohttp Request with an optional body."""
    req = make_mocked_request(method, path, app=app)
    if payload is not None:
        if isinstance(payload, bytes):
            raw_bytes = payload
        elif isinstance(payload, str):
            raw_bytes = payload.encode("utf-8")
        else:
            raw_bytes = json.dumps(payload).encode("utf-8")
        setattr(req, "read", AsyncMock(return_value=raw_bytes))
    return req


# ============================================================================
# HTTP Handlers
# ============================================================================


async def test_http_status_metrics(master_daemon: C2NodeDaemon) -> None:
    master_daemon.peers["worker-1"] = NodeMetrics(
        node_id="worker-1",
        role="worker",
        ip="192.168.1.10",
        http_port=8080,
        tcp_port=9877,
        last_seen=1000.0,
    )

    app = master_daemon.http_server.app
    req = make_mocked_request("GET", "/api/status", app=app)
    resp = await app._handle(req)

    assert resp.status == 200
    assert resp.headers.get("Access-Control-Allow-Origin") == "*"
    assert isinstance(resp, web.Response) and resp.text is not None
    data = json.loads(resp.text)
    assert data["node_id"] == "test-master-srv"
    assert data["role"] == "master"
    assert "peers" in data
    assert "worker-1" in data["peers"]
    assert "latency_ms" in data["peers"]["worker-1"]
    assert "rtt_ms" in data["peers"]["worker-1"]  # Alias maintained for compat
    assert "uptime_sec" in data


async def test_http_options_cors(master_daemon: C2NodeDaemon) -> None:
    app = master_daemon.http_server.app
    req = make_mocked_request("OPTIONS", "/api/command", app=app)
    resp = await app._handle(req)

    assert resp.status == 204
    assert resp.headers.get("Access-Control-Allow-Origin") == "*"
    assert "POST" in resp.headers.get("Access-Control-Allow-Methods", "")


async def test_http_post_command_valid(master_daemon: C2NodeDaemon) -> None:
    app = master_daemon.http_server.app
    req = make_json_request(
        app,
        "POST",
        "/api/command",
        {"command": "PING", "target_id": "test-master-srv"},
    )

    resp = await app._handle(req)
    assert resp.status == 200
    assert isinstance(resp, web.Response) and resp.text is not None
    data = json.loads(resp.text)
    assert data["status"] == "ACK"
    assert data["response"] == "PONG"
    assert data["node_id"] == "test-master-srv"


async def test_http_post_command_arm_safe_estop_rejected(
    master_daemon: C2NodeDaemon,
) -> None:
    app = master_daemon.http_server.app
    for cmd in ["ARM", "SAFE", "ESTOP"]:
        req = make_json_request(
            app,
            "POST",
            "/api/command",
            {"command": cmd, "target_id": "test-master-srv"},
        )

        resp = await app._handle(req)
        assert resp.status == 400
        assert isinstance(resp, web.Response) and resp.text is not None
        data = json.loads(resp.text)
        assert data["status"] == "ERROR"
        assert f"Unknown command '{cmd}'" in data["error"]


async def test_http_post_command_invalid_json(master_daemon: C2NodeDaemon) -> None:
    app = master_daemon.http_server.app
    req = make_json_request(app, "POST", "/api/command", b"{broken json")

    resp = await app._handle(req)
    assert resp.status == 400
    assert isinstance(resp, web.Response) and resp.text is not None
    data = json.loads(resp.text)
    assert data["status"] == "ERROR"
    assert "Malformed JSON" in data["error"]


async def test_http_post_command_unknown(master_daemon: C2NodeDaemon) -> None:
    app = master_daemon.http_server.app
    req = make_json_request(
        app,
        "POST",
        "/api/command",
        {"command": "INVALID_CMD"},
    )

    resp = await app._handle(req)
    assert resp.status == 400
    assert isinstance(resp, web.Response) and resp.text is not None
    data = json.loads(resp.text)
    assert data["status"] == "ERROR"
    assert "Unknown command" in data["error"]


async def test_error_middleware_unhandled_exception(master_daemon: C2NodeDaemon) -> None:
    app = master_daemon.http_server.app
    req = make_json_request(
        app,
        "POST",
        "/api/command",
        {"command": "PING"},
    )
    with patch.object(master_daemon.http_server, "command_handler", side_effect=RuntimeError("Unexpected DB crash")):
        resp = await app._handle(req)

    assert resp.status == 500
    assert isinstance(resp, web.Response) and resp.text is not None
    data = json.loads(resp.text)
    assert data["status"] == "ERROR"
    assert "Unexpected DB crash" in data["error"]


async def test_http_post_benchmark_valid(master_daemon: C2NodeDaemon) -> None:
    payload = {
        "preset": "16KB",
        "client_timestamp": 1234567.8,
        "data": "A" * 1024,
    }
    raw = json.dumps(payload).encode("utf-8")
    app = master_daemon.http_server.app
    req = make_json_request(app, "POST", "/api/benchmark", payload)

    with patch.object(master_daemon.http_server, "display_handler") as mock_disp:
        resp = await app._handle(req)
        mock_disp.assert_called_once_with(
            preset="16KB",
            data="A" * 1024,
            raw_bytes_len=len(raw),
            client_ts=1234567.8,
            sender_ip="unknown",
            protocol="HTTP POST",
        )

    assert resp.status == 200
    assert isinstance(resp, web.Response) and resp.text is not None
    data = json.loads(resp.text)
    assert data["status"] == "BENCHMARK_COMPLETE"
    assert data["preset"] == "16KB"
    assert data["bytes_received"] == len(raw)
    assert "server_proc_time_ms" in data


async def test_http_post_benchmark_invalid(master_daemon: C2NodeDaemon) -> None:
    app = master_daemon.http_server.app
    req = make_json_request(
        app,
        "POST",
        "/api/benchmark",
        {"client_timestamp": "not_a_number"},
    )

    resp = await app._handle(req)
    assert resp.status == 400
    assert isinstance(resp, web.Response) and resp.text is not None
    data = json.loads(resp.text)
    assert data["status"] == "ERROR"


async def test_http_master_serves_index_html(master_daemon: C2NodeDaemon) -> None:
    app = master_daemon.http_server.app
    req = make_mocked_request("GET", "/", app=app)
    resp = await app._handle(req)
    assert isinstance(resp, web.FileResponse)
    assert resp.headers.get("Access-Control-Allow-Origin") == "*"


async def test_http_worker_index_raises_404(worker_daemon: C2NodeDaemon) -> None:
    app = worker_daemon.http_server.app
    req = make_mocked_request("GET", "/", app=app)
    with pytest.raises(web.HTTPNotFound):
        await app._handle(req)


# ============================================================================
# Master Command Proxy
# ============================================================================


async def test_http_master_proxy_command_local(master_daemon: C2NodeDaemon) -> None:
    app = master_daemon.http_server.app
    req = make_json_request(
        app,
        "POST",
        "/api/proxy/test-master-srv/command",
        {"command": "PING", "target_id": "test-master-srv"},
    )
    req.match_info["node_id"] = "test-master-srv"

    resp = await app._handle(req)
    assert resp.status == 200
    assert isinstance(resp, web.Response) and resp.text is not None
    data = json.loads(resp.text)
    assert data["status"] == "ACK"
    assert data["response"] == "PONG"


async def test_http_master_proxy_command_not_found(master_daemon: C2NodeDaemon) -> None:
    app = master_daemon.http_server.app
    req = make_json_request(
        app,
        "POST",
        "/api/proxy/non-existent-worker/command",
        {"command": "PING"},
    )
    req.match_info["node_id"] = "non-existent-worker"

    resp = await app._handle(req)
    assert resp.status == 404
    assert isinstance(resp, web.Response) and resp.text is not None
    data = json.loads(resp.text)
    assert data["status"] == "ERROR"
    assert "not found in peer registry" in data["error"]


async def test_http_master_proxy_command_remote_success(master_daemon: C2NodeDaemon) -> None:
    master_daemon.peers["worker-remote-1"] = NodeMetrics(
        node_id="worker-remote-1",
        role="worker",
        ip="192.168.1.55",
        http_port=8080,
        tcp_port=9877,
        last_seen=100.0,
    )
    app = master_daemon.http_server.app
    req = make_json_request(
        app,
        "POST",
        "/api/proxy/worker-remote-1/command",
        {"command": "PING"},
    )
    req.match_info["node_id"] = "worker-remote-1"

    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value={"status": "ACK", "response": "PONG", "node_id": "worker-remote-1"})

    mock_session = MagicMock()
    mock_session.post.return_value.__aenter__.return_value = mock_resp
    mock_session.__aenter__.return_value = mock_session

    with patch("server.aiohttp.ClientSession", return_value=mock_session):
        resp = await app._handle(req)

    assert resp.status == 200
    assert isinstance(resp, web.Response) and resp.text is not None
    data = json.loads(resp.text)
    assert data["status"] == "ACK"
    assert data["node_id"] == "worker-remote-1"


async def test_http_master_proxy_command_remote_client_error(master_daemon: C2NodeDaemon) -> None:
    master_daemon.peers["worker-remote-2"] = NodeMetrics(
        node_id="worker-remote-2",
        role="worker",
        ip="192.168.1.56",
        http_port=8080,
        tcp_port=9877,
        last_seen=100.0,
    )
    app = master_daemon.http_server.app
    req = make_json_request(
        app,
        "POST",
        "/api/proxy/worker-remote-2/command",
        {"command": "PING"},
    )
    req.match_info["node_id"] = "worker-remote-2"

    mock_session = MagicMock()
    mock_session.post.side_effect = aiohttp.ClientConnectionError("Connection refused")
    mock_session.__aenter__.return_value = mock_session

    with patch("server.aiohttp.ClientSession", return_value=mock_session):
        resp = await app._handle(req)

    assert resp.status == 502
    assert isinstance(resp, web.Response) and resp.text is not None
    data = json.loads(resp.text)
    assert data["status"] == "ERROR"
    assert "Failed to contact target node" in data["error"]


async def test_http_master_proxy_command_remote_timeout(master_daemon: C2NodeDaemon) -> None:
    master_daemon.peers["worker-remote-3"] = NodeMetrics(
        node_id="worker-remote-3",
        role="worker",
        ip="192.168.1.57",
        http_port=8080,
        tcp_port=9877,
        last_seen=100.0,
    )
    app = master_daemon.http_server.app
    req = make_json_request(
        app,
        "POST",
        "/api/proxy/worker-remote-3/command",
        {"command": "PING"},
    )
    req.match_info["node_id"] = "worker-remote-3"

    mock_session = MagicMock()
    mock_session.post.side_effect = asyncio.TimeoutError()
    mock_session.__aenter__.return_value = mock_session

    with patch("server.aiohttp.ClientSession", return_value=mock_session):
        resp = await app._handle(req)

    assert resp.status == 504
    assert isinstance(resp, web.Response) and resp.text is not None
    data = json.loads(resp.text)
    assert data["status"] == "ERROR"
    assert "timed out" in data["error"]


async def test_http_master_proxy_command_missing_node_id(master_daemon: C2NodeDaemon) -> None:
    app = master_daemon.http_server.app
    req = make_json_request(
        app,
        "POST",
        "/api/proxy/ /command",
        {"command": "PING"},
    )
    req.match_info["node_id"] = "   "

    resp = await app._handle(req)
    assert resp.status == 400
    assert isinstance(resp, web.Response) and resp.text is not None
    data = json.loads(resp.text)
    assert data["status"] == "ERROR"
    assert "Missing node_id" in data["error"]


# ============================================================================
# Benchmark Payload Display
# ============================================================================


def test_display_benchmark_payload_short() -> None:
    short_data = "Hello C2 Wireless Network!"
    with patch("sys.stdout") as mock_stdout:
        display_benchmark_payload(
            node_id="test-worker-srv",
            role="worker",
            preset="1KB",
            data=short_data,
            raw_bytes_len=len(short_data),
            client_ts=1000.0,
            sender_ip="192.168.1.50",
            protocol="HTTP POST",
        )
        mock_stdout.write.assert_called_once()
        output = mock_stdout.write.call_args[0][0]
        assert "test-worker-srv (WORKER)" in output
        assert "HTTP POST" in output
        assert "192.168.1.50" in output
        assert "1KB" in output
        assert "Hello C2 Wireless Network!" in output
        assert "truncated" not in output
        mock_stdout.flush.assert_called_once()


def test_display_benchmark_payload_truncated() -> None:
    # Payload > 1024 chars
    large_data = ("A" * 256) + ("M" * 1680) + ("Z" * 64)
    assert len(large_data) == 2000
    with patch("sys.stdout") as mock_stdout:
        display_benchmark_payload(
            node_id="test-worker-srv",
            role="worker",
            preset="64KB",
            data=large_data,
            raw_bytes_len=2000,
            client_ts=0.0,
            sender_ip="10.0.0.1",
            protocol="WebSocket Stream",
        )
        mock_stdout.write.assert_called_once()
        output = mock_stdout.write.call_args[0][0]
        assert "test-worker-srv (WORKER)" in output
        assert "WebSocket Stream" in output
        assert "10.0.0.1" in output
        assert "64KB" in output
        assert "A" * 256 in output
        assert "... [truncated 1680 characters] ..." in output
        assert "Z" * 64 in output
        assert "M" * 1680 not in output
        assert "Raw Size: 2000 bytes" in output
        mock_stdout.flush.assert_called_once()


# ============================================================================
# WebSocket Handlers
# ============================================================================


async def test_ws_benchmark_flow(master_daemon: C2NodeDaemon) -> None:
    req = make_mocked_request("GET", "/ws", app=master_daemon.http_server.app)

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

    with patch.object(master_daemon.http_server, "display_handler") as mock_disp:
        with patch("server.web.WebSocketResponse", return_value=mock_ws):
            await master_daemon.http_server.handle_ws_session(req)

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


async def test_ws_telemetry_broadcast(master_daemon: C2NodeDaemon) -> None:
    mock_ws = MagicMock()
    mock_ws.closed = False
    mock_ws.send_json = AsyncMock()
    master_daemon.http_server.ws_clients.add(mock_ws)
    master_daemon.http_server.running = True

    task = asyncio.create_task(master_daemon.http_server.ws_telemetry_broadcast_loop())
    await asyncio.sleep(0.6)
    master_daemon.http_server.running = False
    await task

    assert mock_ws.send_json.called
    telemetry = mock_ws.send_json.call_args[0][0]
    assert telemetry["type"] == "telemetry_update"
    assert telemetry["node_id"] == "test-master-srv"


async def test_ws_session_binary_and_error_frames(master_daemon: C2NodeDaemon) -> None:
    req = make_mocked_request("GET", "/ws", app=master_daemon.http_server.app)

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

    with patch("server.web.WebSocketResponse", return_value=mock_ws):
        await master_daemon.http_server.handle_ws_session(req)

    assert mock_ws.prepare.called
    assert mock_ws not in master_daemon.http_server.ws_clients


async def test_ws_session_parse_error(master_daemon: C2NodeDaemon) -> None:
    req = make_mocked_request("GET", "/ws", app=master_daemon.http_server.app)

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

    with patch("server.web.WebSocketResponse", return_value=mock_ws):
        await master_daemon.http_server.handle_ws_session(req)

    assert mock_ws.send_json.called
    err_call = mock_ws.send_json.call_args[0][0]
    assert err_call["type"] == "error"
    assert "Unsupported WebSocket action" in err_call["message"]


async def test_ws_telemetry_broadcast_prunes_dead_clients(master_daemon: C2NodeDaemon) -> None:
    dead_ws = MagicMock(spec=web.WebSocketResponse)
    dead_ws.closed = False
    dead_ws.send_json = AsyncMock(side_effect=ConnectionResetError("Peer closed"))

    alive_ws = MagicMock(spec=web.WebSocketResponse)
    alive_ws.closed = False
    alive_ws.send_json = AsyncMock()

    master_daemon.http_server.ws_clients.add(dead_ws)
    master_daemon.http_server.ws_clients.add(alive_ws)
    master_daemon.http_server.running = True

    task = asyncio.create_task(master_daemon.http_server.ws_telemetry_broadcast_loop())
    await asyncio.sleep(0.6)
    master_daemon.http_server.running = False
    await task

    assert dead_ws not in master_daemon.http_server.ws_clients
    assert alive_ws in master_daemon.http_server.ws_clients


# ============================================================================
# TCP Command Server
# ============================================================================


async def test_tcp_command_client(master_daemon: C2NodeDaemon) -> None:
    reader = AsyncMock()
    reader.readline.return_value = json.dumps({"command": "PING"}).encode("utf-8") + b"\n"

    writer = MagicMock()
    writer.get_extra_info.return_value = ("127.0.0.1", 12345)
    writer.drain = AsyncMock()
    writer.wait_closed = AsyncMock()

    written_chunks = []
    writer.write = lambda data: written_chunks.append(data)

    await master_daemon.tcp_server.handle_client(reader, writer)

    assert len(written_chunks) > 0
    resp = json.loads(b"".join(written_chunks).decode("utf-8").strip())
    assert resp["status"] == "ACK"
    assert resp["response"] == "PONG"


async def test_tcp_command_client_empty_line(master_daemon: C2NodeDaemon) -> None:
    reader = AsyncMock()
    reader.readline.return_value = b""

    writer = MagicMock()
    writer.get_extra_info.return_value = ("127.0.0.1", 12345)
    writer.drain = AsyncMock()
    writer.wait_closed = AsyncMock()
    written_chunks = []
    writer.write = lambda data: written_chunks.append(data)

    await master_daemon.tcp_server.handle_client(reader, writer)
    assert len(written_chunks) == 0


async def test_tcp_command_client_exception_in_handler(master_daemon: C2NodeDaemon) -> None:
    reader = AsyncMock()
    reader.readline.return_value = json.dumps({"command": "PING"}).encode("utf-8") + b"\n"

    writer = MagicMock()
    writer.get_extra_info.return_value = ("127.0.0.1", 12345)
    writer.drain = AsyncMock()
    writer.wait_closed = AsyncMock()

    written_chunks = []
    writer.write = lambda data: written_chunks.append(data)

    with patch.object(master_daemon.tcp_server, "command_handler", side_effect=RuntimeError("Handler crashed")):
        await master_daemon.tcp_server.handle_client(reader, writer)

    assert len(written_chunks) > 0
    resp = json.loads(b"".join(written_chunks).decode("utf-8").strip())
    assert resp["status"] == "ERROR"
    assert "Handler crashed" in resp["error"]


async def test_tcp_command_client_connection_error(master_daemon: C2NodeDaemon) -> None:
    reader = AsyncMock()
    reader.readline.side_effect = ConnectionResetError("Connection lost")

    writer = MagicMock()
    writer.get_extra_info.return_value = ("127.0.0.1", 12345)
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock(side_effect=ConnectionResetError("Socket broken"))

    await master_daemon.tcp_server.handle_client(reader, writer)
    assert writer.close.called


async def test_tcp_server_start_stop(master_daemon: C2NodeDaemon) -> None:
    mock_srv = AsyncMock()
    mock_srv.close = MagicMock()
    with patch("server.asyncio.start_server", AsyncMock(return_value=mock_srv)):
        srv = await master_daemon.tcp_server.start()
        assert srv is mock_srv
        assert master_daemon.tcp_server.running

        await master_daemon.tcp_server.stop()
        assert not master_daemon.tcp_server.running
        assert mock_srv.close.called


async def test_http_server_start_and_cleanup(master_daemon: C2NodeDaemon) -> None:
    mock_runner = AsyncMock()
    mock_site = AsyncMock()

    with (
        patch("server.web.AppRunner", return_value=mock_runner),
        patch("server.web.TCPSite", return_value=mock_site),
    ):
        await master_daemon.http_server.start()
        assert master_daemon.http_server.running
        assert mock_runner.setup.called
        assert mock_site.start.called

        # Add a mock active ws
        mock_ws = MagicMock()
        mock_ws.closed = False
        mock_ws.close = AsyncMock()
        master_daemon.http_server.ws_clients.add(mock_ws)

        await master_daemon.http_server.cleanup()
        assert not master_daemon.http_server.running
        assert mock_ws.close.called
        assert mock_runner.cleanup.called


# ============================================================================
# Beacon Service
# ============================================================================


async def test_beacon_sender_unicast_to_peers(master_daemon: C2NodeDaemon) -> None:
    # Add a remote peer on a routed subnet
    master_daemon.peers["worker-remote"] = NodeMetrics(
        node_id="worker-remote",
        role="worker",
        ip="192.168.2.50",
        http_port=8080,
        tcp_port=9877,
        last_seen=time.time(),
    )

    mock_transport = MagicMock()
    master_daemon.beacon_service.running = True

    task = asyncio.create_task(master_daemon.beacon_service.beacon_sender_loop(mock_transport))
    await asyncio.sleep(0.05)
    master_daemon.beacon_service.running = False
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    destinations = [call[0][1] for call in mock_transport.sendto.call_args_list]
    assert ("<broadcast>", 9876) in destinations
    assert ("192.168.2.50", 9876) in destinations
