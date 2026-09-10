"""
Unit tests for console presentation, ANSI coloring, and banner formatting in presentation.py.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch
import pytest

from presentation import (
    BANNER_MAX_FULL_DISPLAY_CHARS,
    display_benchmark_payload,
    format_benchmark_banner,
)


class TestPresentationFormatting:
    def test_banner_color_enabled(self) -> None:
        """Verify ANSI color codes are included when use_color=True."""
        banner = format_benchmark_banner(
            node_id="worker-color",
            role="worker",
            preset="16KB",
            data="Sample test data",
            raw_bytes_len=16,
            client_ts=1000.0,
            sender_ip="192.168.1.1",
            protocol="HTTP POST",
            use_color=True,
        )
        assert "\033[1;36m" in banner
        assert "\033[0m" in banner
        assert "worker-color" in banner
        assert "HTTP POST" in banner

    def test_banner_color_disabled(self) -> None:
        """Verify ANSI color codes are excluded when use_color=False."""
        banner = format_benchmark_banner(
            node_id="worker-nocolor",
            role="worker",
            preset="16KB",
            data="Sample test data",
            raw_bytes_len=16,
            client_ts=1000.0,
            sender_ip="192.168.1.1",
            protocol="HTTP POST",
            use_color=False,
        )
        assert "\033[" not in banner
        assert "worker-nocolor" in banner
        assert "HTTP POST" in banner

    def test_banner_color_autodetect_tty(self) -> None:
        """Verify use_color=None inspects sys.stdout.isatty()."""
        with patch("sys.stdout") as mock_stdout:
            mock_stdout.isatty.return_value = False
            banner = format_benchmark_banner(
                node_id="worker-auto",
                role="worker",
                preset="1KB",
                data="Data",
                raw_bytes_len=4,
                client_ts=0.0,
                sender_ip="10.0.0.1",
                protocol="WebSocket Stream",
                use_color=None,
            )
            assert "\033[" not in banner

    def test_display_benchmark_payload_with_custom_sink(self) -> None:
        """Verify display_benchmark_payload dispatches to a custom sink callable."""
        captured = []
        display_benchmark_payload(
            node_id="worker-sink",
            role="worker",
            preset="1KB",
            data="Custom sink test",
            raw_bytes_len=16,
            client_ts=1000.0,
            sender_ip="10.0.0.1",
            protocol="HTTP POST",
            sink=lambda text: captured.append(text),
        )
        assert len(captured) == 1
        assert "Custom sink test" in captured[0]
        assert "worker-sink" in captured[0]
