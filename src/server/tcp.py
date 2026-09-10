"""
TCP command server — line-delimited socket framing and client connection lifecycle.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable, Dict, Optional

from protocol import C2CommandRequest, C2ParseError, parse_tcp_command_frame
from server.commands import execute_standard_command

logger = logging.getLogger("C2Server")


class TcpCommandServer:
    """Manages line-delimited TCP command socket serving and client connection lifecycles.

    Args:
        node_id: Identifier of the local node.
        tcp_port: Port number to bind the TCP server socket to.
        command_handler: Optional custom handler for dispatched C2 commands.
    """

    def __init__(
        self,
        node_id: str,
        tcp_port: int,
        command_handler: Optional[Callable[[C2CommandRequest], Dict[str, Any]]] = None,
    ):
        self.node_id = node_id
        self.tcp_port = tcp_port
        self.command_handler = (
            command_handler
            if command_handler is not None
            else (lambda req: execute_standard_command(req, self.node_id))
        )
        self.server: Optional[asyncio.Server] = None
        self.running = False

    async def handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle an incoming TCP connection, read a framed command line, and respond.

        Args:
            reader: Stream reader receiving client command line frames.
            writer: Stream writer transmitting JSON command responses.
        """
        client_addr = writer.get_extra_info("peername")
        logger.info(f"[TCP] Command client connected from {client_addr}")
        try:
            line = await reader.readline()
            if not line:
                return
            try:
                cmd_req = parse_tcp_command_frame(line)
                response_obj = self.command_handler(cmd_req)
            except C2ParseError as e:
                response_obj = {
                    "status": "ERROR",
                    "error": e.message,
                    "field": e.field,
                    "node_id": self.node_id,
                }
            except Exception as e:
                response_obj = {
                    "status": "ERROR",
                    "error": str(e),
                    "node_id": self.node_id,
                }
            resp_bytes = (json.dumps(response_obj) + "\n").encode("utf-8")
            writer.write(resp_bytes)
            await writer.drain()
        except (ConnectionError, OSError) as e:
            logger.debug(f"[TCP] Connection error with {client_addr}: {e}")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError) as e:
                logger.debug(f"[TCP] Error closing socket for {client_addr}: {e}")

    async def start(self) -> asyncio.Server:
        """Start listening for incoming TCP command client connections.

        Returns:
            The running asyncio.Server instance.
        """
        self.running = True
        self.server = await asyncio.start_server(
            self.handle_client,
            "0.0.0.0",
            self.tcp_port,
        )
        return self.server

    async def stop(self) -> None:
        """Gracefully stop the TCP server and close listening sockets."""
        self.running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()

