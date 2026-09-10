"""
Unit tests for UDP beacon protocol and service in beacon.py.
"""

from __future__ import annotations

import asyncio
import json
import socket
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from beacon import BeaconService, UDPBeaconProtocol
from conftest import create_dummy_beacon
from protocol import BeaconMessage
from tracker import NodeMetrics, PeerTracker


class TestUDPBeaconProtocol:
    def test_connection_made_sets_broadcast_socket(self) -> None:
        """Verify connection_made configures SO_BROADCAST on datagram socket."""
        mock_sock = MagicMock()
        mock_transport = MagicMock(spec=asyncio.DatagramTransport)
        mock_transport.get_extra_info.return_value = mock_sock

        on_datagram = MagicMock()
        proto = UDPBeaconProtocol(on_datagram)
        proto.connection_made(mock_transport)

        assert proto.transport is mock_transport
        mock_sock.setsockopt.assert_called_once_with(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    def test_connection_made_without_socket(self) -> None:
        """Verify connection_made handles transports where extra info socket is None."""
        mock_transport = MagicMock(spec=asyncio.DatagramTransport)
        mock_transport.get_extra_info.return_value = None

        proto = UDPBeaconProtocol(MagicMock())
        proto.connection_made(mock_transport)
        assert proto.transport is mock_transport

    def test_datagram_received_valid(self) -> None:
        """Verify datagram_received parses valid beacon and invokes callback."""
        received_beacons = []
        proto = UDPBeaconProtocol(lambda b, ip: received_beacons.append((b, ip)))

        valid_dict = {
            "node_id": "worker-udp-1",
            "role": "worker",
            "seq": 42,
            "timestamp": time.time(),
            "start_time": time.time() - 10.0,
            "http_port": 8080,
            "tcp_port": 9877,
            "cpu_load": 0.12,
        }
        data = json.dumps(valid_dict).encode("utf-8")
        proto.datagram_received(data, ("192.168.1.100", 9876))

        assert len(received_beacons) == 1
        beacon, ip = received_beacons[0]
        assert beacon.node_id == "worker-udp-1"
        assert ip == "192.168.1.100"

    def test_datagram_received_c2_parse_error(self) -> None:
        """Verify datagram_received catches C2ParseError safely without raising."""
        callback = MagicMock()
        proto = UDPBeaconProtocol(callback)
        proto.datagram_received(b"malformed json", ("192.168.1.100", 9876))
        assert not callback.called

    def test_datagram_received_generic_exception(self) -> None:
        """Verify datagram_received catches generic ValueError safely without raising."""
        callback = MagicMock()
        proto = UDPBeaconProtocol(callback)
        with patch("beacon.parse_beacon_datagram", side_effect=ValueError("bad value")):
            proto.datagram_received(b"{}", ("192.168.1.100", 9876))
        assert not callback.called


class TestBeaconService:
    def test_service_initialization(self) -> None:
        tracker = PeerTracker()
        svc_worker = BeaconService(
            node_id="worker-init",
            role="worker",
            http_port=8080,
            tcp_port=9877,
            udp_port=9876,
            tracker=tracker,
        )
        assert svc_worker.last_master_contact > 0.0

        svc_master = BeaconService(
            node_id="master-init",
            role="master",
            http_port=9000,
            tcp_port=9877,
            udp_port=9876,
            tracker=tracker,
        )
        assert svc_master.last_master_contact == 0.0

    def test_handle_incoming_beacon_ignores_own_id(self) -> None:
        """Verify beacon service ignores datagrams matching its own node_id."""
        tracker = PeerTracker()
        svc = BeaconService(
            node_id="self-node",
            role="worker",
            http_port=8080,
            tcp_port=9877,
            udp_port=9876,
            tracker=tracker,
        )
        own_beacon = create_dummy_beacon(node_id="self-node")
        svc.handle_incoming_beacon(own_beacon, "127.0.0.1")
        assert "self-node" not in tracker.peers

    def test_handle_incoming_beacon_learns_master_ip(self) -> None:
        """Verify worker learns master_ip from incoming master beacon."""
        tracker = PeerTracker()
        svc = BeaconService(
            node_id="worker-learn",
            role="worker",
            http_port=8080,
            tcp_port=9877,
            udp_port=9876,
            tracker=tracker,
            master_ip=None,
        )
        t_before = time.time()
        master_beacon = create_dummy_beacon(node_id="c2-master", role="master")
        svc.handle_incoming_beacon(master_beacon, "192.168.1.200")

        assert svc.master_ip == "192.168.1.200"
        assert svc.last_master_contact >= t_before
        assert "c2-master" in tracker.peers

    async def test_beacon_sender_loop_resilience_to_network_errors(self) -> None:
        """Verify sender loop continues sending despite OSError on broadcast/unicast."""
        tracker = PeerTracker()
        svc = BeaconService(
            node_id="worker-sender",
            role="worker",
            http_port=8080,
            tcp_port=9877,
            udp_port=9876,
            tracker=tracker,
            master_ip="10.0.0.1",
        )
        # Register a remote peer
        peer_beacon = create_dummy_beacon(node_id="peer-remote", role="worker")
        tracker.update_peer(peer_beacon, "10.0.0.2")

        mock_transport = MagicMock()
        mock_transport.sendto.side_effect = [
            OSError("Network unreachable"),
            None,
            OSError("Host down"),
        ]

        svc.running = True
        iterations = 0

        async def fast_sleep(_sec: float) -> None:
            nonlocal iterations
            iterations += 1
            if iterations >= 1:
                svc.running = False

        with patch("asyncio.sleep", side_effect=fast_sleep):
            with patch("os.getloadavg", return_value=(0.5, 0.4, 0.3)):
                await svc.beacon_sender_loop(mock_transport)

        assert svc.seq_num >= 1
        assert mock_transport.sendto.call_count >= 2

    async def test_beacon_sender_unicast_to_peers(self, master_daemon: Any) -> None:
        """Verify beacon sender transmits both broadcast and unicast to known peers."""
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
        assert ("<broadcast>", master_daemon.udp_port) in destinations
        assert ("192.168.2.50", master_daemon.udp_port) in destinations
