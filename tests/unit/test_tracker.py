"""
Unit tests for PeerTracker and NodeMetrics in tracker.py.
Covers link quality metrics calculation, rolling latency, jitter, packet loss,
sequence resets, and silence pruning without network I/O.
"""

from __future__ import annotations

from collections import deque
import time
import pytest

from conftest import create_dummy_beacon
from tracker import NodeMetrics, PeerTracker


class TestNodeMetrics:
    def test_node_metrics_defaults(self) -> None:
        """Verify NodeMetrics default zero initialization for rolling metrics."""
        metrics = NodeMetrics(
            node_id="node-a",
            role="worker",
            ip="192.168.1.5",
            http_port=8080,
            tcp_port=9877,
            last_seen=100.0,
        )
        assert metrics.node_id == "node-a"
        assert metrics.role == "worker"
        assert metrics.latency_ms == 0.0
        assert metrics.jitter_ms == 0.0
        assert metrics.packets_received == 0
        assert metrics.packet_loss_pct == 0.0


class TestPeerTracker:
    def test_peer_tracker_initialization(self) -> None:
        """Verify PeerTracker initializes with custom history window size and empty registry."""
        tracker = PeerTracker(history_window=15)
        assert tracker.history_window == 15
        assert len(tracker.peers) == 0

    def test_update_peer_discovery(self) -> None:
        """Verify peer discovery registers a new NodeMetrics entry with initial sequence."""
        tracker = PeerTracker(history_window=10)
        beacon = create_dummy_beacon(node_id="worker-test", seq=1)
        peer = tracker.update_peer(beacon, "10.0.0.5")

        assert "worker-test" in tracker.peers
        assert peer.node_id == "worker-test"
        assert peer.ip == "10.0.0.5"
        assert peer.http_port == 8080
        assert peer.tcp_port == 9877
        assert peer.last_seq == 1
        assert peer.packets_received == 1
        assert peer.latency_ms > 0.0
        assert peer.jitter_ms == 0.0  # Only 1 sample, jitter is 0

    def test_rolling_latency_and_jitter(self) -> None:
        """Verify consecutive beacon arrivals compute moving window average latency and jitter."""
        tracker = PeerTracker(history_window=5)
        now = time.time()

        # Feed 3 beacons with known timestamps relative to 'now'
        # Sample 1: 10ms delay
        b1 = create_dummy_beacon(node_id="w1", seq=1, timestamp=now - 0.010)
        tracker.update_peer(b1, "10.0.0.1")

        # Sample 2: 20ms delay -> diff = 10ms
        b2 = create_dummy_beacon(node_id="w1", seq=2, timestamp=now - 0.020)
        tracker.update_peer(b2, "10.0.0.1")

        peer = tracker.peers["w1"]
        assert len(peer.latency_history) == 2
        assert peer.jitter_ms > 0.0
        assert peer.packets_received == 2

    def test_packet_loss_calculation_with_skips(self) -> None:
        """Verify sequence gaps correctly update rolling packet loss percentage."""
        tracker = PeerTracker(history_window=10)

        # Seq 1 received
        b1 = create_dummy_beacon(node_id="w1", seq=1)
        tracker.update_peer(b1, "10.0.0.1")
        assert tracker.peers["w1"].packet_loss_pct == 0.0

        # Seq 4 received (skipped seq 2 and 3 -> expected 3, lost 2)
        b2 = create_dummy_beacon(node_id="w1", seq=4)
        tracker.update_peer(b2, "10.0.0.1")

        peer = tracker.peers["w1"]
        assert peer.last_seq == 4
        # Expected = 3 (4 - 1), lost = 2
        # loss % = 2 / 3 * 100 = 66.7%
        assert peer.packet_loss_pct == 66.7

    def test_sequence_reset_recovery(self) -> None:
        """Verify node restart with lower sequence number resets sequence loss history."""
        tracker = PeerTracker(history_window=10)

        # Start at seq 100 with some loss
        b1 = create_dummy_beacon(node_id="w1", seq=100)
        tracker.update_peer(b1, "10.0.0.1")
        b2 = create_dummy_beacon(node_id="w1", seq=105)
        tracker.update_peer(b2, "10.0.0.1")
        assert tracker.peers["w1"].packet_loss_pct > 0.0

        # Node restarts: seq resets to 1 (seq < last_seq)
        b3 = create_dummy_beacon(node_id="w1", seq=1)
        tracker.update_peer(b3, "10.0.0.1")

        peer = tracker.peers["w1"]
        assert peer.last_seq == 1
        assert len(peer.seq_history) == 0
        assert peer.packet_loss_pct == 0.0

    def test_prune_stale_peers(self) -> None:
        """Verify peers silent longer than timeout threshold are pruned from registry."""
        tracker = PeerTracker()
        now = time.time()

        # Add active peer
        b1 = create_dummy_beacon(node_id="active-node")
        tracker.update_peer(b1, "10.0.0.1")

        # Add stale peer and manually backdate last_seen
        b2 = create_dummy_beacon(node_id="stale-node")
        tracker.update_peer(b2, "10.0.0.2")
        tracker.peers["stale-node"].last_seen = now - 10.0

        pruned = tracker.prune_stale_peers(timeout_sec=4.0)

        assert pruned == ["stale-node"]
        assert "stale-node" not in tracker.peers
        assert "active-node" in tracker.peers

    def test_to_dict_serialization(self) -> None:
        """Verify to_dict returns a valid JSON-serializable dictionary with list histories."""
        tracker = PeerTracker()
        beacon = create_dummy_beacon(node_id="worker-ser", seq=5)
        tracker.update_peer(beacon, "192.168.1.15")

        d = tracker.to_dict()
        assert "worker-ser" in d
        peer_data = d["worker-ser"]
        assert peer_data["node_id"] == "worker-ser"
        assert peer_data["ip"] == "192.168.1.15"
        assert isinstance(peer_data["seq_history"], list)
        assert isinstance(peer_data["latency_history"], list)
        assert "latency_ms" in peer_data
        assert "rtt_ms" not in peer_data

