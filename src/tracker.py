"""
Peer tracking, link quality metrics, and rolling statistics calculation for C2 nodes.
"""

from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass, field
import itertools
import logging
import time
from typing import Any, Deque, Dict, List, Optional, Tuple

from config import config
from protocol import BeaconMessage

logger = logging.getLogger("C2Tracker")


@dataclass
class NodeMetrics:
    """Rolling link quality and operational telemetry for a tracked peer node.

    Latency calculations measure one-way transit delay (OWD), assuming host clocks
    are synchronized via NTP or PTP. Jitter is computed via moving window consecutive
    difference averages, and packet loss is inferred from UDP sequence number gaps.

    Attributes:
        node_id: Unique identifier of the remote peer node.
        role: Peer node role ('master' or 'worker').
        ip: Last known IP address of the peer.
        http_port: HTTP port served by the peer.
        tcp_port: TCP command port served by the peer.
        last_seen: Epoch timestamp when the last valid beacon was received.
        latency_ms: Smoothed one-way transit delay estimate in milliseconds.
        jitter_ms: Average consecutive packet arrival variance in milliseconds.
        packets_received: Total number of valid beacons received from this peer.
        packet_loss_pct: Estimated packet loss percentage over the rolling window.
        uptime_sec: Elapsed uptime in seconds reported by the peer.
        cpu_load: Current 1-minute CPU load average reported by the peer.
        last_seq: Most recent beacon sequence number observed.
        seq_history: Rolling deque of (expected_packets, lost_packets) tuples.
        latency_history: Rolling deque of raw transit latency samples.
    """

    node_id: str
    role: str
    ip: str
    http_port: int
    tcp_port: int
    last_seen: float
    latency_ms: float = 0.0  # One-way transit delay estimate (assumes NTP-synchronized clocks)
    jitter_ms: float = 0.0
    packets_received: int = 0
    packet_loss_pct: float = 0.0
    uptime_sec: float = 0.0
    cpu_load: float = 0.0
    last_seq: int = 0
    seq_history: Deque[Tuple[int, int]] = field(default_factory=deque)
    latency_history: Deque[float] = field(default_factory=deque)



class PeerTracker:
    """Encapsulates peer registry, rolling link quality statistics, and silence pruning.

    Args:
        history_window: Maximum number of recent samples retained for moving averages.
            Defaults to config.link_history_window if None.
    """

    def __init__(self, history_window: Optional[int] = None):
        self.peers: Dict[str, NodeMetrics] = {}
        self.history_window = history_window if history_window is not None else config.link_history_window

    def update_peer(self, beacon: BeaconMessage, sender_ip: str) -> NodeMetrics:
        """Ingest an incoming beacon datagram and update link quality metrics.

        Args:
            beacon: Validated UDP beacon message received from the remote peer.
            sender_ip: Source IP address of the incoming datagram packet.

        Returns:
            Updated NodeMetrics instance for the reporting peer.
        """
        now = time.time()
        sender_id = beacon.node_id
        sender_role = beacon.role
        seq = beacon.seq
        sent_timestamp = beacon.timestamp
        http_port = beacon.http_port
        tcp_port = beacon.tcp_port

        # Calculate one-way transit delay (requires host clock synchronization)
        measured_latency = max(0.1, (now - sent_timestamp) * 1000.0)

        if sender_id not in self.peers:
            logger.info(f"[+] Discovered node [{sender_id}] ({sender_role}) at {sender_ip}")
            self.peers[sender_id] = NodeMetrics(
                node_id=sender_id,
                role=sender_role,
                ip=sender_ip,
                http_port=http_port,
                tcp_port=tcp_port,
                last_seen=now,
                last_seq=seq,
                seq_history=deque(maxlen=self.history_window),
                latency_history=deque(maxlen=self.history_window),
            )

        peer = self.peers[sender_id]
        peer.last_seen = now
        peer.ip = sender_ip
        peer.http_port = http_port
        peer.tcp_port = tcp_port
        peer.uptime_sec = round(now - beacon.start_time, 1)
        peer.cpu_load = beacon.cpu_load
        peer.packets_received += 1

        # Calculate moving window average latency and consecutive difference jitter
        peer.latency_history.append(measured_latency)
        peer.latency_ms = round(sum(peer.latency_history) / len(peer.latency_history), 1)

        if len(peer.latency_history) > 1:
            diffs = [abs(b - a) for a, b in itertools.pairwise(peer.latency_history)]
            peer.jitter_ms = round(sum(diffs) / len(diffs), 2)

        # Packet loss calculation from sequence delta
        if peer.last_seq > 0:
            if seq > peer.last_seq:
                expected = seq - peer.last_seq
                lost = max(0, expected - 1)
                peer.seq_history.append((expected, lost))
                total_exp = sum(e for e, _ in peer.seq_history)
                total_lost = sum(l for _, l in peer.seq_history)
                peer.packet_loss_pct = round((total_lost / max(1, total_exp)) * 100.0, 1)
            elif seq < peer.last_seq:
                logger.info(
                    f"[*] Sequence reset on node [{sender_id}] (previous seq={peer.last_seq}, new seq={seq}); resetting sequence history."
                )
                peer.seq_history.clear()
                peer.packet_loss_pct = 0.0

        peer.last_seq = seq
        return peer

    def prune_stale_peers(self, timeout_sec: float) -> List[str]:
        """Prune peers that have not emitted a beacon within the timeout threshold.

        Args:
            timeout_sec: Maximum elapsed silence time in seconds before removal.

        Returns:
            List of node IDs that timed out and were removed from the registry.
        """
        now = time.time()
        timed_out: List[str] = []
        for peer_id, peer in list(self.peers.items()):
            if now - peer.last_seen > timeout_sec:
                timed_out.append(peer_id)
                del self.peers[peer_id]
                logger.warning(f"[-] Node [{peer_id}] timed out (> {timeout_sec}s silence).")
        return timed_out

    def to_dict(self) -> Dict[str, Any]:
        """Convert peer registry into a JSON-serializable dictionary.

        Returns:
            Dictionary mapping peer IDs to their serialized metric dictionaries.
        """
        result: Dict[str, Any] = {}
        for peer_id, peer in self.peers.items():
            d = asdict(peer)
            d["seq_history"] = list(peer.seq_history)
            d["latency_history"] = list(peer.latency_history)
            result[peer_id] = d
        return result

