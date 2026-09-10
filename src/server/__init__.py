"""C2 server package — public API surface for network services and HTTP/WS layers.

Provides modular server components:
- HttpServer: aiohttp REST API, static asset hosting, and proxy router.
- TcpCommandServer: Line-delimited framing and command socket server.
- WebSocketManager: Live telemetry broadcast loop and duplex benchmarking.
- execute_standard_command: Core command execution dispatch logic.
"""

from __future__ import annotations

import aiohttp
from aiohttp import web

from server.commands import execute_standard_command
from server.tcp import TcpCommandServer
from server.rest import (
    CORS_HEADERS,
    NODE_ID_KEY,
    HttpServer,
    cors_middleware,
    error_middleware,
)
from server.ws import WebSocketManager

__all__ = [
    "HttpServer",
    "TcpCommandServer",
    "WebSocketManager",
    "execute_standard_command",
    "cors_middleware",
    "error_middleware",
    "CORS_HEADERS",
    "NODE_ID_KEY",
    "aiohttp",
    "web",
]
