"""
UDP discovery beacon protocol and service for C2 nodes.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
from typing import Callable, Optional, Tuple

from config import config
from protocol import BeaconMessage, C2ParseError, parse_beacon_datagram
from tracker import PeerTracker

logger = logging.getLogger("C2Beacon")


class UDPBeaconProtocol(asyncio.DatagramProtocol):
    """asyncio DatagramProtocol for handling incoming UDP beacon datagrams.

    Args:
        on_datagram: Callback invoked with the validated BeaconMessage and sender IP.
    """

    def __init__(self, on_datagram: Callable[[BeaconMessage, str], None]):
        self.on_datagram = on_datagram
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Configure datagram transport and enable socket broadcast permission.

        Args:
            transport: Underlying asyncio datagram transport instance.
        """
        if isinstance(transport, asyncio.DatagramTransport):
            self.transport = transport
            sock: Optional[socket.socket] = transport.get_extra_info("socket")
            if sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        """Parse received datagram bytes and pass validated beacon to the callback.

        Args:
            data: Raw payload bytes received from the UDP socket.
            addr: Tuple of remote IP address and port number.
        """
        try:
            beacon = parse_beacon_datagram(data)
            self.on_datagram(beacon, addr[0])
        except C2ParseError as e:
            logger.debug(f"Rejected untrusted beacon from {addr}: {e}")
        except (ValueError, KeyError, TypeError) as e:
            logger.debug(f"Malformed beacon from {addr}: {e}")


class BeaconService:
    """Manages UDP broadcast and unicast beacon transmission and peer ingestion.

    Args:
        node_id: Unique identifier for this node.
        role: Operational role ('master' or 'worker').
        http_port: HTTP port served by this node.
        tcp_port: TCP command port served by this node.
        udp_port: UDP port used for peer discovery broadcasts.
        tracker: PeerTracker instance maintaining active peer metrics.
        master_ip: Optional destination IP of the master node for directed unicast.
        start_time: Boot epoch timestamp (seconds) for uptime reporting.
    """

    def __init__(
        self,
        node_id: str,
        role: str,
        http_port: int,
        tcp_port: int,
        udp_port: int,
        tracker: PeerTracker,
        master_ip: Optional[str] = None,
        start_time: Optional[float] = None,
    ):
        self.node_id = node_id
        self.role = role
        self.http_port = http_port
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        self.tracker = tracker
        self.master_ip = master_ip
        self.start_time = start_time if start_time is not None else time.time()
        self.running = False
        self.seq_num = 0
        self.last_master_contact = time.time() if self.role == "worker" else 0.0

    def handle_incoming_beacon(self, beacon: BeaconMessage, sender_ip: str) -> None:
        """Validate and ingest an incoming beacon datagram into the peer tracker.

        Args:
            beacon: Validated BeaconMessage received from peer.
            sender_ip: Remote sender IP address.
        """
        sender_id = beacon.node_id
        if sender_id == self.node_id:
            return

        now = time.time()
        sender_role = beacon.role

        # Track contact with Master for failsafe
        if sender_role == "master":
            self.last_master_contact = now
            if not self.master_ip:
                self.master_ip = sender_ip

        # Delegate link metrics update to peer tracker
        self.tracker.update_peer(beacon, sender_ip)

    async def beacon_sender_loop(self, transport: asyncio.DatagramTransport) -> None:
        """Periodically broadcast presence over subnet and unicast to known peers.

        Args:
            transport: Active asyncio DatagramTransport used for packet transmission.
        """
        broadcast_addr = ("<broadcast>", self.udp_port)
        while self.running:
            self.seq_num += 1
            load = os.getloadavg()[0] if hasattr(os, "getloadavg") else 0.0
            beacon_payload = {
                "node_id": self.node_id,
                "role": self.role,
                "seq": self.seq_num,
                "timestamp": time.time(),
                "start_time": self.start_time,
                "http_port": self.http_port,
                "tcp_port": self.tcp_port,
                "cpu_load": round(load, 2),
            }
            data = json.dumps(beacon_payload).encode("utf-8")

            # 1. Local subnet broadcast
            try:
                transport.sendto(data, broadcast_addr)
            except (OSError, RuntimeError) as e:
                logger.debug(f"Broadcast send error: {e}")

            # 2. Directed unicast to Master IP (for multi-hop nodes or loopback tests)
            if self.master_ip:
                try:
                    transport.sendto(data, (self.master_ip, self.udp_port))
                except (OSError, RuntimeError) as e:
                    logger.debug(f"Unicast send error to master {self.master_ip}: {e}")

            # 3. Directed unicast back to all registered peers (for routed multi-subnet workers)
            for peer in list(self.tracker.peers.values()):
                if self.master_ip and peer.ip == self.master_ip:
                    continue
                try:
                    transport.sendto(data, (peer.ip, self.udp_port))
                except (OSError, RuntimeError) as e:
                    logger.debug(f"Unicast send error to peer {peer.ip}: {e}")

            # 4. Prune timed-out peers
            self.tracker.prune_stale_peers(config.failsafe_timeout_sec)

            await asyncio.sleep(config.beacon_interval_sec)
