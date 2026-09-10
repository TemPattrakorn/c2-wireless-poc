"""
Hermetic in-process cluster integration tests for C2 Wireless Network PoC.
Tests multi-node discovery, command proxy routing, benchmark transfer,
and failsafe pruning without OS subprocesses or external socket network access.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
from aiohttp import WSMsgType, web
from aiohttp.test_utils import AioHTTPTestCase, TestClient, TestServer, make_mocked_request
import pytest

from config import config
from node import C2NodeDaemon, NodeMetrics
from protocol import BeaconMessage, C2CommandRequest


@pytest.mark.integration
class TestHermeticClusterFlow:
    """Hermetic integration suite exercising multi-component C2 cluster flows in-memory."""

    @pytest.fixture
    def cluster(self) -> dict[str, C2NodeDaemon]:
        """Provide paired Master and Worker daemons sharing in-process context."""
        master = C2NodeDaemon(
            node_id="cluster-master",
            role="master",
            http_port=9000,
            tcp_port=9877,
            udp_port=9876,
        )
        worker = C2NodeDaemon(
            node_id="cluster-worker-1",
            role="worker",
            http_port=8080,
            tcp_port=9877,
            udp_port=9876,
            master_ip="192.168.1.10",
        )
        return {"master": master, "worker": worker}

    async def test_beacon_discovery_bidirectional(self, cluster: dict[str, C2NodeDaemon]) -> None:
        """Verify UDP beacon discovery between worker and master updates peer registries."""
        master = cluster["master"]
        worker = cluster["worker"]

        now = time.time()
        # 1. Worker beacons to Master
        worker_beacon = BeaconMessage(
            node_id=worker.node_id,
            role=worker.role,
            seq=1,
            timestamp=now - 0.005,
            start_time=worker.start_time,
            http_port=worker.http_port,
            tcp_port=worker.tcp_port,
            cpu_load=0.20,
        )
        master.beacon_service.handle_incoming_beacon(worker_beacon, "192.168.1.50")

        assert worker.node_id in master.peers
        worker_peer = master.peers[worker.node_id]
        assert worker_peer.node_id == worker.node_id
        assert worker_peer.role == "worker"
        assert worker_peer.ip == "192.168.1.50"
        assert worker_peer.http_port == 8080
        assert worker_peer.tcp_port == 9877
        assert worker_peer.last_seq == 1
        assert worker_peer.packets_received == 1

        # 2. Master beacons back to Worker
        master_beacon = BeaconMessage(
            node_id=master.node_id,
            role=master.role,
            seq=1,
            timestamp=now - 0.003,
            start_time=master.start_time,
            http_port=master.http_port,
            tcp_port=master.tcp_port,
            cpu_load=0.15,
        )
        worker.beacon_service.handle_incoming_beacon(master_beacon, "192.168.1.10")

        assert master.node_id in worker.peers
        master_peer = worker.peers[master.node_id]
        assert master_peer.node_id == master.node_id
        assert master_peer.role == "master"
        assert worker.beacon_service.master_ip == "192.168.1.10"
        assert worker.beacon_service.last_master_contact >= now - 0.01

    async def test_proxy_command_routing_flow(self, cluster: dict[str, C2NodeDaemon]) -> None:
        """Verify Master proxy receives operator command and routes to Worker."""
        master = cluster["master"]
        worker = cluster["worker"]

        # Register worker in master's tracker
        master.peers[worker.node_id] = NodeMetrics(
            node_id=worker.node_id,
            role=worker.role,
            ip="192.168.1.50",
            http_port=8080,
            tcp_port=9877,
            last_seen=time.time(),
        )

        master_app = master.http_server.app
        worker_app = worker.http_server.app

        class FakePostCM:
            def __init__(self, target_url: str, body: bytes):
                self.target_url = target_url
                self.body = body

            async def __aenter__(self) -> Any:
                assert f"{worker.http_port}/api/command" in self.target_url
                worker_req = make_mocked_request("POST", "/api/command", app=worker_app)
                setattr(worker_req, "read", AsyncMock(return_value=self.body))
                worker_resp = await worker_app._handle(worker_req)
                assert isinstance(worker_resp, web.Response) and worker_resp.text is not None
                resp_data = json.loads(worker_resp.text)
                resp_mock = MagicMock()
                resp_mock.status = worker_resp.status
                resp_mock.json = AsyncMock(return_value=resp_data)
                return resp_mock

            async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
                pass

        # Build master proxy request
        cmd_body = json.dumps({"command": "PING", "target_id": worker.node_id}).encode("utf-8")
        req = make_mocked_request("POST", f"/api/proxy/{worker.node_id}/command", app=master_app)
        setattr(req, "read", AsyncMock(return_value=cmd_body))
        req.match_info["node_id"] = worker.node_id

        with patch("server.aiohttp.ClientSession") as mock_cls:
            mock_inst = MagicMock()
            mock_inst.post.side_effect = lambda url, **kw: FakePostCM(url, kw.get("data", b""))
            mock_inst.__aenter__.return_value = mock_inst
            mock_cls.return_value = mock_inst

            proxy_resp = await master_app._handle(req)

        assert proxy_resp.status == 200
        assert isinstance(proxy_resp, web.Response) and proxy_resp.text is not None
        data = json.loads(proxy_resp.text)
        assert data["status"] == "ACK"
        assert data["response"] == "PONG"
        assert data["node_id"] == worker.node_id

    async def test_benchmark_data_transfer_flow(self, cluster: dict[str, C2NodeDaemon]) -> None:
        """Verify data benchmark payload dispatch and presentation display."""
        worker = cluster["worker"]
        worker_app = worker.http_server.app

        payload = {
            "preset": "16KB",
            "client_timestamp": time.time(),
            "data": "HERMETIC_BENCHMARK_BURST" * 50,
        }
        raw_body = json.dumps(payload).encode("utf-8")
        req = make_mocked_request("POST", "/api/benchmark", app=worker_app)
        setattr(req, "read", AsyncMock(return_value=raw_body))

        captured_displays = []
        with patch.object(
            worker.http_server,
            "display_handler",
            side_effect=lambda **kw: captured_displays.append(kw),
        ):
            resp = await worker_app._handle(req)

        assert resp.status == 200
        assert isinstance(resp, web.Response) and resp.text is not None
        data = json.loads(resp.text)
        assert data["status"] == "BENCHMARK_COMPLETE"
        assert data["preset"] == "16KB"
        assert data["bytes_received"] == len(raw_body)
        assert len(captured_displays) == 1
        assert captured_displays[0]["preset"] == "16KB"
        assert "HERMETIC_BENCHMARK_BURST" in captured_displays[0]["data"]

    async def test_websocket_telemetry_stream(self, cluster: dict[str, C2NodeDaemon]) -> None:
        """Verify WebSocket telemetry broadcast and benchmark acknowledgment."""
        master = cluster["master"]
        worker = cluster["worker"]

        # Register worker with master
        master.peers[worker.node_id] = NodeMetrics(
            node_id=worker.node_id,
            role=worker.role,
            ip="192.168.1.50",
            http_port=8080,
            tcp_port=9877,
            last_seen=time.time(),
        )

        req = make_mocked_request("GET", "/ws", app=master.http_server.app)

        bench_msg = MagicMock()
        bench_msg.type = WSMsgType.TEXT
        bench_msg.data = json.dumps({
            "action": "ws_benchmark",
            "preset": "64KB",
            "timestamp": time.time(),
            "data": "WS_HERMETIC_DATA",
        })

        async def msg_stream() -> Any:
            yield bench_msg

        mock_ws = MagicMock(spec=web.WebSocketResponse)
        mock_ws.prepare = AsyncMock()
        mock_ws.send_json = AsyncMock()
        mock_ws.closed = False
        mock_ws.__aiter__ = lambda self: msg_stream()

        with patch("server.ws.web.WebSocketResponse", return_value=mock_ws):
            await master.http_server.ws_manager.handle_ws_session(req)

        assert mock_ws.prepare.called
        assert mock_ws.send_json.called
        ack = mock_ws.send_json.call_args[0][0]
        assert ack["type"] == "ws_benchmark_ack"
        assert ack["node_id"] == master.node_id

    async def test_carrier_loss_silence_pruning(self, cluster: dict[str, C2NodeDaemon]) -> None:
        """Verify silence timeout prunes disconnected peers without network I/O."""
        master = cluster["master"]
        worker = cluster["worker"]

        # 1. Discover worker
        beacon = BeaconMessage(
            node_id=worker.node_id,
            role=worker.role,
            seq=1,
            timestamp=time.time(),
            start_time=worker.start_time,
            http_port=worker.http_port,
            tcp_port=worker.tcp_port,
            cpu_load=0.10,
        )
        master.tracker.update_peer(beacon, "192.168.1.50")
        assert worker.node_id in master.tracker.peers

        # 2. Simulate silence by aging last_seen beyond failsafe_timeout_sec
        master.tracker.peers[worker.node_id].last_seen = time.time() - (config.failsafe_timeout_sec + 1.0)

        pruned = master.tracker.prune_stale_peers(config.failsafe_timeout_sec)
        assert worker.node_id in pruned
        assert worker.node_id not in master.tracker.peers
