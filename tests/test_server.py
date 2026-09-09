"""
Unit and integration tests for C2NodeDaemon HTTP and WebSocket endpoints.
Tests handler logic directly using aiohttp.test_utils and unittest.IsolatedAsyncioTestCase
without requiring external network sockets or third-party pytest plugins.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp import WSMsgType, web
from aiohttp.test_utils import make_mocked_request

from c2_node import C2NodeDaemon


class TestHttpHandlers(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.master_daemon = C2NodeDaemon(
            node_id="test-master-srv",
            role="master",
            http_port=9000,
            tcp_port=9877,
            udp_port=9876,
        )
        self.worker_daemon = C2NodeDaemon(
            node_id="test-worker-srv",
            role="worker",
            http_port=8080,
            tcp_port=9877,
            udp_port=9876,
        )

    async def test_get_status(self) -> None:
        from c2_node import NodeMetrics

        self.master_daemon.peers["worker-1"] = NodeMetrics(
            node_id="worker-1",
            role="worker",
            ip="192.168.1.10",
            http_port=8080,
            tcp_port=9877,
            last_seen=1000.0,
        )

        req = make_mocked_request("GET", "/api/status", app=self.master_daemon.app)
        resp = await self.master_daemon.handle_http_status(req)

        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.headers.get("Access-Control-Allow-Origin"), "*")
        assert resp.text is not None
        data = json.loads(resp.text)
        self.assertEqual(data["node_id"], "test-master-srv")
        self.assertEqual(data["role"], "master")
        self.assertNotIn("state", data)
        self.assertIn("peers", data)
        self.assertNotIn("state", data["peers"]["worker-1"])
        self.assertIn("uptime_sec", data)

    async def test_options_cors(self) -> None:
        req = make_mocked_request("OPTIONS", "/api/command", app=self.master_daemon.app)
        resp = await self.master_daemon.handle_http_options(req)

        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.headers.get("Access-Control-Allow-Origin"), "*")
        self.assertIn("POST", resp.headers.get("Access-Control-Allow-Methods", ""))

    async def test_post_command_valid(self) -> None:
        req = make_mocked_request("POST", "/api/command", app=self.master_daemon.app)
        setattr(
            req,
            "read",
            AsyncMock(
                return_value=json.dumps({"command": "PING", "target_id": "test-master-srv"}).encode("utf-8")
            ),
        )

        resp = await self.master_daemon.handle_http_command(req)
        self.assertEqual(resp.status, 200)
        assert resp.text is not None
        data = json.loads(resp.text)
        self.assertEqual(data["status"], "ACK")
        self.assertEqual(data["response"], "PONG")
        self.assertEqual(data["node_id"], "test-master-srv")

    async def test_post_command_arm_safe_estop_rejected(self) -> None:
        for cmd in ["ARM", "SAFE", "ESTOP"]:
            req = make_mocked_request("POST", "/api/command", app=self.master_daemon.app)
            setattr(
                req,
                "read",
                AsyncMock(
                    return_value=json.dumps({"command": cmd, "target_id": "test-master-srv"}).encode("utf-8")
                ),
            )

            resp = await self.master_daemon.handle_http_command(req)
            self.assertEqual(resp.status, 400)
            assert resp.text is not None
            data = json.loads(resp.text)
            self.assertEqual(data["status"], "ERROR")
            self.assertIn(f"Unknown command '{cmd}'", data["error"])

    async def test_post_command_invalid_json(self) -> None:
        req = make_mocked_request("POST", "/api/command", app=self.master_daemon.app)
        setattr(req, "read", AsyncMock(return_value=b"{broken json"))

        resp = await self.master_daemon.handle_http_command(req)
        self.assertEqual(resp.status, 400)
        assert resp.text is not None
        data = json.loads(resp.text)
        self.assertEqual(data["status"], "ERROR")
        self.assertIn("Malformed JSON", data["error"])

    async def test_post_command_unknown(self) -> None:
        req = make_mocked_request("POST", "/api/command", app=self.master_daemon.app)
        setattr(req, "read", AsyncMock(return_value=json.dumps({"command": "INVALID_CMD"}).encode("utf-8")))

        resp = await self.master_daemon.handle_http_command(req)
        self.assertEqual(resp.status, 400)
        assert resp.text is not None
        data = json.loads(resp.text)
        self.assertEqual(data["status"], "ERROR")
        self.assertIn("Unknown command", data["error"])

    async def test_post_benchmark_valid(self) -> None:
        payload = {
            "preset": "16KB",
            "client_timestamp": 1234567.8,
            "data": "A" * 1024,
        }
        raw = json.dumps(payload).encode("utf-8")
        req = make_mocked_request("POST", "/api/benchmark", app=self.master_daemon.app)
        setattr(req, "read", AsyncMock(return_value=raw))

        with patch.object(self.master_daemon, "display_benchmark_payload") as mock_disp:
            resp = await self.master_daemon.handle_http_benchmark(req)
            mock_disp.assert_called_once_with(
                preset="16KB",
                data="A" * 1024,
                raw_bytes_len=len(raw),
                client_ts=1234567.8,
                sender_ip="unknown",
                protocol="HTTP POST",
            )

        self.assertEqual(resp.status, 200)
        assert resp.text is not None
        data = json.loads(resp.text)
        self.assertEqual(data["status"], "BENCHMARK_COMPLETE")
        self.assertEqual(data["preset"], "16KB")
        self.assertEqual(data["bytes_received"], len(raw))
        self.assertIn("server_proc_time_ms", data)

    async def test_post_benchmark_invalid(self) -> None:
        req = make_mocked_request("POST", "/api/benchmark", app=self.master_daemon.app)
        setattr(req, "read", AsyncMock(return_value=json.dumps({"client_timestamp": "not_a_number"}).encode("utf-8")))

        resp = await self.master_daemon.handle_http_benchmark(req)
        self.assertEqual(resp.status, 400)
        assert resp.text is not None
        data = json.loads(resp.text)
        self.assertEqual(data["status"], "ERROR")

    async def test_master_serves_index_html(self) -> None:
        req = make_mocked_request("GET", "/", app=self.master_daemon.app)
        resp = await self.master_daemon.handle_http_index(req)
        self.assertIsInstance(resp, web.FileResponse)
        self.assertEqual(resp.headers.get("Access-Control-Allow-Origin"), "*")

    async def test_master_serves_alpine_js(self) -> None:
        req = make_mocked_request("GET", "/alpine.min.js", app=self.master_daemon.app)
        resp = await self.master_daemon.handle_http_alpine(req)
        self.assertIsInstance(resp, web.FileResponse)
        self.assertEqual(resp.headers.get("Access-Control-Allow-Origin"), "*")

    async def test_worker_index_raises_404(self) -> None:
        req = make_mocked_request("GET", "/", app=self.worker_daemon.app)
        with self.assertRaises(web.HTTPNotFound):
            await self.worker_daemon.handle_http_index(req)

    async def test_worker_alpine_raises_404(self) -> None:
        req = make_mocked_request("GET", "/alpine.min.js", app=self.worker_daemon.app)
        with self.assertRaises(web.HTTPNotFound):
            await self.worker_daemon.handle_http_alpine(req)


class TestBenchmarkPayloadDisplay(unittest.TestCase):
    def setUp(self) -> None:
        self.daemon = C2NodeDaemon(
            node_id="test-node-disp",
            role="worker",
            http_port=8080,
            tcp_port=9877,
            udp_port=9876,
        )

    def test_display_benchmark_payload_short(self) -> None:
        short_data = "Hello C2 Wireless Network!"
        with patch("sys.stdout") as mock_stdout:
            self.daemon.display_benchmark_payload(
                preset="1KB",
                data=short_data,
                raw_bytes_len=len(short_data),
                client_ts=1000.0,
                sender_ip="192.168.1.50",
                protocol="HTTP POST",
            )
            mock_stdout.write.assert_called_once()
            output = mock_stdout.write.call_args[0][0]
            self.assertIn("test-node-disp (WORKER)", output)
            self.assertIn("HTTP POST", output)
            self.assertIn("192.168.1.50", output)
            self.assertIn("1KB", output)
            self.assertIn("Hello C2 Wireless Network!", output)
            self.assertNotIn("truncated", output)
            mock_stdout.flush.assert_called_once()

    def test_display_benchmark_payload_truncated(self) -> None:
        # Payload > 1024 chars
        large_data = ("A" * 256) + ("M" * 1680) + ("Z" * 64)
        self.assertEqual(len(large_data), 2000)
        with patch("sys.stdout") as mock_stdout:
            self.daemon.display_benchmark_payload(
                preset="64KB",
                data=large_data,
                raw_bytes_len=2000,
                client_ts=0.0,
                sender_ip="10.0.0.1",
                protocol="WebSocket Stream",
            )
            mock_stdout.write.assert_called_once()
            output = mock_stdout.write.call_args[0][0]
            self.assertIn("test-node-disp (WORKER)", output)
            self.assertIn("WebSocket Stream", output)
            self.assertIn("10.0.0.1", output)
            self.assertIn("64KB", output)
            self.assertIn("A" * 256, output)
            self.assertIn("... [truncated 1680 characters] ...", output)
            self.assertIn("Z" * 64, output)
            self.assertNotIn("M" * 1680, output)
            self.assertIn("Raw Size: 2000 bytes", output)
            mock_stdout.flush.assert_called_once()


class TestWebSocketHandlers(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.master_daemon = C2NodeDaemon(
            node_id="test-master-srv",
            role="master",
            http_port=9000,
            tcp_port=9877,
            udp_port=9876,
        )

    async def test_handle_ws_benchmark_flow(self) -> None:
        req = make_mocked_request("GET", "/ws", app=self.master_daemon.app)

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

        with patch.object(self.master_daemon, "display_benchmark_payload") as mock_disp:
            with patch("c2_node.web.WebSocketResponse", return_value=mock_ws):
                ws = await self.master_daemon.handle_ws_session(req)

            mock_disp.assert_called_once_with(
                preset="16KB",
                data="WS_TEST_PAYLOAD",
                raw_bytes_len=len(mock_msg.data),
                client_ts=1234567.89,
                sender_ip="unknown",
                protocol="WebSocket Stream",
            )

        self.assertTrue(mock_ws.prepare.called)
        self.assertTrue(mock_ws.send_json.called)
        call_args = mock_ws.send_json.call_args[0][0]
        self.assertEqual(call_args["type"], "ws_benchmark_ack")
        self.assertEqual(call_args["node_id"], "test-master-srv")
        self.assertEqual(call_args["client_timestamp"], 1234567.89)

    async def test_ws_telemetry_broadcast(self) -> None:
        mock_ws = MagicMock()
        mock_ws.closed = False
        mock_ws.send_json = AsyncMock()
        self.master_daemon.ws_clients.add(mock_ws)
        self.master_daemon.running = True

        task = asyncio.create_task(self.master_daemon.ws_telemetry_broadcast_loop())
        await asyncio.sleep(0.6)
        self.master_daemon.running = False
        await task

        self.assertTrue(mock_ws.send_json.called)
        telemetry = mock_ws.send_json.call_args[0][0]
        self.assertEqual(telemetry["type"], "telemetry_update")
        self.assertEqual(telemetry["node_id"], "test-master-srv")
        self.assertNotIn("state", telemetry)


class TestTcpCommands(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.master_daemon = C2NodeDaemon(
            node_id="test-master-srv",
            role="master",
            http_port=9000,
            tcp_port=9877,
            udp_port=9876,
        )

    async def test_handle_tcp_command_client(self) -> None:
        reader = AsyncMock()
        reader.readline.return_value = json.dumps({"command": "PING"}).encode("utf-8") + b"\n"

        writer = MagicMock()
        writer.get_extra_info.return_value = ("127.0.0.1", 12345)
        writer.drain = AsyncMock()
        writer.wait_closed = AsyncMock()

        written_chunks = []
        writer.write = lambda data: written_chunks.append(data)

        await self.master_daemon.handle_tcp_command_client(reader, writer)

        self.assertTrue(len(written_chunks) > 0)
        resp = json.loads(b"".join(written_chunks).decode("utf-8").strip())
        self.assertEqual(resp["status"], "ACK")
        self.assertEqual(resp["response"], "PONG")


class TestBeaconSender(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.daemon = C2NodeDaemon(
            node_id="test-master-beacon",
            role="master",
            http_port=9000,
            tcp_port=9877,
            udp_port=9876,
        )

    async def test_beacon_sender_unicast_to_peers(self) -> None:
        from c2_node import NodeMetrics
        import time

        # Add a remote peer on a routed subnet
        self.daemon.peers["worker-remote"] = NodeMetrics(
            node_id="worker-remote",
            role="worker",
            ip="192.168.2.50",
            http_port=8080,
            tcp_port=9877,
            last_seen=time.time(),
        )

        mock_transport = MagicMock()
        self.daemon.running = True

        task = asyncio.create_task(self.daemon.beacon_sender_loop(mock_transport))
        await asyncio.sleep(0.05)
        self.daemon.running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        destinations = [call[0][1] for call in mock_transport.sendto.call_args_list]
        self.assertIn(("<broadcast>", 9876), destinations)
        self.assertIn(("192.168.2.50", 9876), destinations)

