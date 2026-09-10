"""
server package — public API surface for the C2 server components.

Importing from `server` (e.g. `from server import HttpServer`) continues to work
after the migration from the monolithic server.py to this package.
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
