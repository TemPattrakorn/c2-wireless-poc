"""
Console presentation, ANSI formatting, and banner utilities for C2 node data transfers.
"""

from __future__ import annotations

import sys
import time
from typing import Callable, Optional

# Banner presentation sizing limits
BANNER_MAX_FULL_DISPLAY_CHARS: int = 1024
BANNER_PREVIEW_HEAD_CHARS: int = 256
BANNER_PREVIEW_TAIL_CHARS: int = 64


def format_benchmark_banner(
    node_id: str,
    role: str,
    preset: str,
    data: str,
    raw_bytes_len: int,
    client_ts: float,
    sender_ip: str,
    protocol: str,
    use_color: Optional[bool] = None,
) -> str:
    """Format a console banner summarizing a data transfer benchmark with TTY color-awareness."""
    now = time.time()
    latency_ms = (now - client_ts) * 1000.0 if client_ts > 0 else 0.0
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))

    if len(data) <= BANNER_MAX_FULL_DISPLAY_CHARS:
        content_display = data
    else:
        truncated_count = len(data) - (BANNER_PREVIEW_HEAD_CHARS + BANNER_PREVIEW_TAIL_CHARS)
        content_display = (
            f"{data[:BANNER_PREVIEW_HEAD_CHARS]}\n"
            f"... [truncated {truncated_count} characters] ...\n"
            f"{data[-BANNER_PREVIEW_TAIL_CHARS:]}"
        )

    if use_color is None:
        try:
            use_color = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()
        except Exception:
            use_color = False

    if use_color:
        CYAN = "\033[1;36m"
        GREEN = "\033[1;32m"
        YELLOW = "\033[1;33m"
        MAGENTA = "\033[1;35m"
        BOLD = "\033[1m"
        RESET = "\033[0m"
    else:
        CYAN = GREEN = YELLOW = MAGENTA = BOLD = RESET = ""

    border = "=" * 70
    return (
        f"\n{CYAN}{border}{RESET}\n"
        f"{BOLD}{MAGENTA}>>> C2 BENCHMARK PAYLOAD RECEIVED <<<{RESET}\n"
        f" {BOLD}Node:{RESET}       {node_id} ({role.upper()})\n"
        f" {BOLD}Protocol:{RESET}   {GREEN}{protocol}{RESET}\n"
        f" {BOLD}Sender:{RESET}     {sender_ip} at {time_str} ({now:.3f})\n"
        f" {BOLD}Preset:{RESET}     {YELLOW}{preset}{RESET} | Raw Size: {raw_bytes_len} bytes | Data Chars: {len(data)}\n"
        f" {BOLD}Client Latency:{RESET} {latency_ms:.2f} ms\n"
        f"{CYAN}{'-' * 70}{RESET}\n"
        f"{BOLD}Content:{RESET}\n{content_display}\n"
        f"{CYAN}{border}{RESET}\n"
    )


def display_benchmark_payload(
    node_id: str,
    role: str,
    preset: str,
    data: str,
    raw_bytes_len: int,
    client_ts: float,
    sender_ip: str,
    protocol: str,
    sink: Optional[Callable[[str], None]] = None,
    use_color: Optional[bool] = None,
) -> None:
    """Output the formatted benchmark banner to sink or stdout."""
    banner = format_benchmark_banner(
        node_id=node_id,
        role=role,
        preset=preset,
        data=data,
        raw_bytes_len=raw_bytes_len,
        client_ts=client_ts,
        sender_ip=sender_ip,
        protocol=protocol,
        use_color=use_color,
    )
    if sink is not None:
        sink(banner)
    else:
        sys.stdout.write(banner)
        sys.stdout.flush()
