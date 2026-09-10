"""
Peer tracking, link quality metrics, and rolling statistics calculation for C2 nodes.
"""

from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass, field
import logging
import time
from typing import Any, Deque, Dict, List, Optional, Tuple

from config import config
from protocol import BeaconMessage

logger = logging.getLogger("C2Tracker")


@dataclass
class NodeMetrics:
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

    @property
    def rtt_ms(self) -> float:
        """Alias for backward compatibility with older consumers expecting rtt_ms."""
        return self.latency_ms

    @rtt_ms.setter
    def rtt_ms(self, val: float) -> None:
        self.latency_ms = val


class PeerTracker:
    """
    Encapsulates peer registry, rolling latency/jitter/packet-loss statistics,
    and silence timeout pruning.
    """

    def __init__(self, history_window: Optional[int] = None):
        self.peers: Dict[str, NodeMetrics] = {}
        self.history_window = history_window if history_window is not None else config.link_history_window

    def update_peer(self, beacon: BeaconMessage, sender_ip: str) -> NodeMetrics:
        """
        Ingest an incoming beacon datagram and update link quality metrics.
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
            diffs = [
                abs(peer.latency_history[i] - peer.latency_history[i - 1])
                for i in range(1, len(peer.latency_history))
            ]
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
        """
        Check for peers that haven't been seen within timeout_sec and remove them.
        Returns the list of pruned peer IDs.
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
        """Return serializable dictionary of all active peers."""
        result: Dict[str, Any] = {}
        for peer_id, peer in self.peers.items():
            d = asdict(peer)
            d["seq_history"] = list(peer.seq_history)
            d["latency_history"] = list(peer.latency_history)
            # Maintain backward compatibility for consumers looking for rtt_ms
            d["rtt_ms"] = peer.latency_ms
            result[peer_id] = d
        return result

