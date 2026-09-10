"""
Unit tests for configuration loading, environment overrides, and immutability in config.py.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
import pytest

from config import C2Config, _get_int_env, config


class TestConfigParsing:
    def test_get_int_env_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify default integer fallback is returned when environment variable is unset."""
        monkeypatch.delenv("TEST_C2_INT", raising=False)
        assert _get_int_env("TEST_C2_INT", 42) == 42

    def test_get_int_env_empty_string(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify fallback is returned when environment variable is whitespace or empty."""
        monkeypatch.setenv("TEST_C2_INT", "   ")
        assert _get_int_env("TEST_C2_INT", 100) == 100

    def test_get_int_env_valid_number(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify integer is successfully parsed from valid environment variable string."""
        monkeypatch.setenv("TEST_C2_INT", " 9999 ")
        assert _get_int_env("TEST_C2_INT", 100) == 9999

    def test_get_int_env_invalid_value(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify fallback is returned and warning logged when environment variable is non-numeric."""
        monkeypatch.setenv("TEST_C2_INT", "not_an_int")
        assert _get_int_env("TEST_C2_INT", 555) == 555

    def test_config_immutability(self) -> None:
        """Verify C2Config instances are frozen and reject runtime attribute reassignment."""
        cfg = C2Config()
        with pytest.raises(FrozenInstanceError):
            cfg.tcp_cmd_port = 12345  # type: ignore

    def test_config_benchmark_sizes(self) -> None:
        """Verify benchmark payload sizes match expected byte constants."""
        assert config.benchmark_sizes["1KB"] == 1024
        assert config.benchmark_sizes["16KB"] == 16384
        assert config.benchmark_sizes["64KB"] == 65536
        assert config.benchmark_sizes["256KB"] == 262144

    def test_config_default_ports(self) -> None:
        """Verify default network port assignments for UDP, TCP, and HTTP services."""
        assert config.udp_beacon_port == 9876
        assert config.tcp_cmd_port == 9877
        assert config.http_worker_port == 8080
        assert config.http_master_port == 9000

