"""
Unit tests for configuration loading, environment overrides, and immutability in config.py.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
import pytest

from config import C2Config, _get_int_env, config


class TestConfigParsing:
    def test_get_int_env_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("TEST_C2_INT", raising=False)
        assert _get_int_env("TEST_C2_INT", 42) == 42

    def test_get_int_env_empty_string(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_C2_INT", "   ")
        assert _get_int_env("TEST_C2_INT", 100) == 100

    def test_get_int_env_valid_number(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_C2_INT", " 9999 ")
        assert _get_int_env("TEST_C2_INT", 100) == 9999

    def test_get_int_env_invalid_value(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_C2_INT", "not_an_int")
        assert _get_int_env("TEST_C2_INT", 555) == 555

    def test_config_immutability(self) -> None:
        cfg = C2Config()
        with pytest.raises(FrozenInstanceError):
            cfg.tcp_cmd_port = 12345  # type: ignore

    def test_config_benchmark_sizes(self) -> None:
        assert config.benchmark_sizes["1KB"] == 1024
        assert config.benchmark_sizes["16KB"] == 16384
        assert config.benchmark_sizes["64KB"] == 65536
        assert config.benchmark_sizes["256KB"] == 262144

    def test_config_default_ports(self) -> None:
        assert config.udp_beacon_port == 9876
        assert config.tcp_cmd_port == 9877
        assert config.http_worker_port == 8080
        assert config.http_master_port == 9000
