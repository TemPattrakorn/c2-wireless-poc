"""
Unit tests for node daemon lifecycle, IP resolution, failsafe watchdog,
and CLI argument parsing in node.py.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from node import C2NodeDaemon, main, setup_logging
from protocol import C2CommandRequest


class TestNodeLogging:
    def test_setup_logging(self) -> None:
        """Verify setup_logging executes without error."""
        setup_logging(logging.DEBUG)
        setup_logging(logging.INFO)


class TestNodeDaemonIPResolution:
    def test_determine_local_ip_probes_dns(self) -> None:
        """Verify outbound IP determination probes gateway/DNS when master_ip is None."""
        daemon = C2NodeDaemon(
            node_id="node-test",
            role="worker",
            http_port=8080,
            tcp_port=9877,
            udp_port=9876,
            master_ip=None,
        )
        assert isinstance(daemon.local_ip, str)
        assert len(daemon.local_ip.split(".")) == 4

    def test_determine_local_ip_with_master_ip(self) -> None:
        """Verify outbound IP determination probes master_ip when provided."""
        daemon = C2NodeDaemon(
            node_id="node-test",
            role="worker",
            http_port=8080,
            tcp_port=9877,
            udp_port=9876,
            master_ip="127.0.0.1",
        )
        assert daemon.local_ip == "127.0.0.1"

    def test_determine_local_ip_fallback_on_os_error(self) -> None:
        """Verify fallback to 127.0.0.1 when socket.connect fails with OSError."""
        with patch("socket.socket") as mock_sock_cls:
            mock_sock = MagicMock()
            mock_sock.connect.side_effect = OSError("Network unreachable")
            mock_sock_cls.return_value = mock_sock

            daemon = C2NodeDaemon(
                node_id="node-fallback",
                role="worker",
                http_port=8080,
                tcp_port=9877,
                udp_port=9876,
            )
            assert daemon.local_ip == "127.0.0.1"


class TestNodeFailsafeWatchdog:
    async def test_failsafe_watchdog_logs_warning_on_silence(self, caplog: pytest.LogCaptureFixture) -> None:
        """Verify failsafe watchdog emits warning when master communication drops."""
        daemon = C2NodeDaemon(
            node_id="worker-fs",
            role="worker",
            http_port=8080,
            tcp_port=9877,
            udp_port=9876,
            master_ip="10.0.0.1",
        )
        daemon.beacon_service.last_master_contact = 0.0
        daemon.running = True

        call_count = 0
        async def fast_sleep(sec: float) -> None:
            nonlocal call_count
            call_count += 1
            if call_count >= 1:
                daemon.running = False

        with caplog.at_level(logging.WARNING, logger="C2Node"):
            with patch("node.asyncio.sleep", side_effect=fast_sleep):
                await daemon.failsafe_watchdog_loop()

        assert any("Lost Master contact" in record.message for record in caplog.records)

    async def test_failsafe_watchdog_healthy(self, caplog: pytest.LogCaptureFixture) -> None:
        """Verify failsafe watchdog stays quiet when master contact is fresh."""
        daemon = C2NodeDaemon(
            node_id="worker-healthy",
            role="worker",
            http_port=8080,
            tcp_port=9877,
            udp_port=9876,
            master_ip="10.0.0.1",
        )
        import time
        daemon.beacon_service.last_master_contact = time.time()
        daemon.running = True

        call_count = 0
        async def fast_sleep(sec: float) -> None:
            nonlocal call_count
            call_count += 1
            if call_count >= 1:
                daemon.running = False

        with caplog.at_level(logging.WARNING, logger="C2Node"):
            with patch("node.asyncio.sleep", side_effect=fast_sleep):
                await daemon.failsafe_watchdog_loop()

        assert not any("Lost Master contact" in record.message for record in caplog.records)


class TestNodeCommandExecution:
    def test_execute_command_delegation(self) -> None:
        """Verify command handler executes standard commands correctly."""
        daemon = C2NodeDaemon(
            node_id="worker-cmd",
            role="worker",
            http_port=8080,
            tcp_port=9877,
            udp_port=9876,
        )
        req = C2CommandRequest(command="PING", target_id="worker-cmd")
        resp = daemon._execute_command(req)
        assert resp["status"] == "ACK"
        assert resp["response"] == "PONG"
        assert resp["node_id"] == "worker-cmd"


class TestNodeLifecycleAndCleanup:
    async def test_daemon_start_and_clean_shutdown(self) -> None:
        """Verify daemon startup initializes subsystems and cleanup terminates them gracefully."""
        daemon = C2NodeDaemon(
            node_id="test-lifecycle",
            role="worker",
            http_port=8080,
            tcp_port=9877,
            udp_port=9876,
        )

        mock_transport = MagicMock()
        mock_tcp_server = AsyncMock()
        mock_tcp_server.serve_forever = AsyncMock(side_effect=asyncio.CancelledError)

        with (
            patch("asyncio.get_running_loop") as mock_get_loop,
            patch.object(daemon.tcp_server, "start", AsyncMock(return_value=mock_tcp_server)),
            patch.object(daemon.tcp_server, "stop", AsyncMock()) as mock_tcp_stop,
            patch.object(daemon.http_server, "start", AsyncMock()),
            patch.object(daemon.http_server, "cleanup", AsyncMock()) as mock_http_cleanup,
            patch.object(daemon.beacon_service, "beacon_sender_loop", AsyncMock()),
            patch.object(daemon, "failsafe_watchdog_loop", AsyncMock()),
            patch.object(daemon.http_server, "ws_telemetry_broadcast_loop", AsyncMock()),
            patch("socket.socket"),
        ):
            mock_loop = MagicMock()
            mock_loop.create_datagram_endpoint = AsyncMock(return_value=(mock_transport, None))
            mock_get_loop.return_value = mock_loop

            task = asyncio.create_task(daemon.start())
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

            assert not daemon.running
            assert not daemon.beacon_service.running
            assert not daemon.http_server.running
            assert mock_transport.close.called
            assert mock_tcp_stop.called
            assert mock_http_cleanup.called


class TestNodeCliMain:
    def test_main_cli_defaults_master(self) -> None:
        """Verify CLI main entry point parses master defaults."""
        captured = []
        def fake_run(coro: Any) -> None:
            captured.append(coro.cr_frame.f_locals["self"])
            coro.close()

        with patch("sys.argv", ["node.py", "--id", "master-cli", "--role", "master"]):
            with patch("asyncio.run", side_effect=fake_run):
                main()
                assert len(captured) == 1
                daemon = captured[0]
                assert daemon.node_id == "master-cli"
                assert daemon.role == "master"
                assert daemon.http_port == 9000

    def test_main_cli_defaults_worker(self) -> None:
        """Verify CLI main entry point parses worker defaults."""
        captured = []
        def fake_run(coro: Any) -> None:
            captured.append(coro.cr_frame.f_locals["self"])
            coro.close()

        with patch("sys.argv", ["node.py", "--id", "worker-cli", "--role", "worker", "--master-ip", "10.0.0.2"]):
            with patch("asyncio.run", side_effect=fake_run):
                main()
                assert len(captured) == 1
                daemon = captured[0]
                assert daemon.node_id == "worker-cli"
                assert daemon.role == "worker"
                assert daemon.http_port == 8080
                assert daemon.master_ip == "10.0.0.2"

    def test_main_cli_custom_ports(self) -> None:
        """Verify CLI parses explicit port overrides."""
        captured = []
        def fake_run(coro: Any) -> None:
            captured.append(coro.cr_frame.f_locals["self"])
            coro.close()

        with patch("sys.argv", [
            "node.py",
            "--id", "custom-cli",
            "--role", "worker",
            "--port-http", "8085",
            "--port-tcp", "9890",
            "--port-udp", "9891",
        ]):
            with patch("asyncio.run", side_effect=fake_run):
                main()
                assert len(captured) == 1
                daemon = captured[0]
                assert daemon.http_port == 8085
                assert daemon.tcp_port == 9890
                assert daemon.udp_port == 9891
