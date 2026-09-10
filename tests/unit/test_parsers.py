"""
Unit tests for untrusted input parsing and validation in protocol.py.
Covers UDP discovery beacons, TCP command frames, HTTP request payloads,
and WebSocket message frames.
"""

from __future__ import annotations

import json
import time
import pytest

from protocol import (
    MAX_DATAGRAM_SIZE,
    MAX_HTTP_PAYLOAD_SIZE,
    MAX_TCP_FRAME_SIZE,
    BeaconMessage,
    BenchmarkRequest,
    C2CommandRequest,
    C2ParseError,
    WsBenchmarkRequest,
    parse_beacon_datagram,
    parse_http_benchmark_payload,
    parse_http_command_payload,
    parse_tcp_command_frame,
    parse_ws_message,
)


class TestBeaconParsing:
    def _sample_beacon_dict(self) -> dict:
        return {
            "node_id": "worker-42",
            "role": "worker",
            "seq": 10,
            "timestamp": time.time(),
            "start_time": time.time() - 100.0,
            "http_port": 8080,
            "tcp_port": 9877,
            "cpu_load": 0.45,
        }

    def test_valid_beacon(self) -> None:
        raw_dict = self._sample_beacon_dict()
        data = json.dumps(raw_dict).encode("utf-8")
        beacon = parse_beacon_datagram(data)

        assert isinstance(beacon, BeaconMessage)
        assert beacon.node_id == "worker-42"
        assert beacon.role == "worker"
        assert beacon.seq == 10
        assert not hasattr(beacon, "state")
        assert beacon.http_port == 8080
        assert beacon.tcp_port == 9877
        assert beacon.cpu_load == 0.45

    def test_role_case_insensitivity(self) -> None:
        raw_dict = self._sample_beacon_dict()
        raw_dict["role"] = "MASTER"
        beacon = parse_beacon_datagram(json.dumps(raw_dict).encode("utf-8"))
        assert beacon.role == "master"

    def test_corrupted_json(self) -> None:
        with pytest.raises(C2ParseError, match="Malformed JSON"):
            parse_beacon_datagram(b'{"node_id": "test", "role": "wo')

    def test_invalid_utf8(self) -> None:
        with pytest.raises(C2ParseError, match="Invalid UTF-8"):
            parse_beacon_datagram(b"\x80\x81\x82\xff")

    def test_non_dict_json(self) -> None:
        for bad_payload in [b'["array", "of", "items"]', b'"string"', b"12345", b"null", b"true"]:
            with pytest.raises(C2ParseError, match="Expected JSON object"):
                parse_beacon_datagram(bad_payload)

    def test_empty_payload(self) -> None:
        with pytest.raises(C2ParseError, match="is empty"):
            parse_beacon_datagram(b"")

    def test_oversized_datagram(self) -> None:
        huge_bytes = b" " * (MAX_DATAGRAM_SIZE + 1)
        with pytest.raises(C2ParseError, match="exceeds maximum permitted length"):
            parse_beacon_datagram(huge_bytes)

    def test_missing_or_invalid_node_id(self) -> None:
        raw = self._sample_beacon_dict()
        del raw["node_id"]
        with pytest.raises(C2ParseError, match="Field 'node_id' must be a non-empty string"):
            parse_beacon_datagram(json.dumps(raw).encode("utf-8"))

        raw["node_id"] = "   "
        with pytest.raises(C2ParseError, match="Field 'node_id' must be a non-empty string"):
            parse_beacon_datagram(json.dumps(raw).encode("utf-8"))

        raw["node_id"] = 12345
        with pytest.raises(C2ParseError, match="Field 'node_id' must be a non-empty string"):
            parse_beacon_datagram(json.dumps(raw).encode("utf-8"))

        raw["node_id"] = "x" * 129
        with pytest.raises(C2ParseError, match="exceeds maximum length"):
            parse_beacon_datagram(json.dumps(raw).encode("utf-8"))

    def test_invalid_role(self) -> None:
        raw = self._sample_beacon_dict()
        raw["role"] = "super-admin"
        with pytest.raises(C2ParseError, match="Field 'role' must be one of"):
            parse_beacon_datagram(json.dumps(raw).encode("utf-8"))

    def test_invalid_sequence_number(self) -> None:
        raw = self._sample_beacon_dict()
        for bad_seq in [-1, "12", 3.14, True, False, None]:
            raw["seq"] = bad_seq
            with pytest.raises(C2ParseError, match="Field 'seq' must be a non-negative integer"):
                parse_beacon_datagram(json.dumps(raw).encode("utf-8"))

    def test_invalid_ports(self) -> None:
        raw = self._sample_beacon_dict()
        for bad_port in [0, -8080, 65536, 100000, "8080", True, False]:
            raw["http_port"] = bad_port
            with pytest.raises(C2ParseError, match="Field 'http_port'"):
                parse_beacon_datagram(json.dumps(raw).encode("utf-8"))

    def test_beacon_ignores_state_field(self) -> None:
        raw = self._sample_beacon_dict()
        raw["state"] = "FLYING_HIGH"
        beacon = parse_beacon_datagram(json.dumps(raw).encode("utf-8"))
        assert not hasattr(beacon, "state")

    def test_nan_or_inf_cpu_load(self) -> None:
        raw = self._sample_beacon_dict()
        # In JSON, NaN/Inf are parsed if literal or via float representation
        data = b'{"node_id":"w1","role":"worker","seq":1,"timestamp":100,"start_time":50,"http_port":8080,"tcp_port":9877,"cpu_load":NaN}'
        with pytest.raises(C2ParseError):
            parse_beacon_datagram(data)


class TestTcpCommandParsing:
    def test_valid_commands(self) -> None:
        for cmd in ["PING", "ping"]:
            data = json.dumps({"command": cmd, "target_id": "node-1", "args": {"key": "val"}}).encode("utf-8")
            req = parse_tcp_command_frame(data)
            assert req.command == "PING"
            assert req.target_id == "node-1"
            assert req.args == {"key": "val"}

    def test_rejected_commands(self) -> None:
        for cmd in ["ARM", "SAFE", "ESTOP", "STATUS", "REBOOT", "arm", "safe", "estop"]:
            data = json.dumps({"command": cmd}).encode("utf-8")
            with pytest.raises(C2ParseError, match=f"Unknown command '{cmd.upper()}'"):
                parse_tcp_command_frame(data)

    def test_default_values(self) -> None:
        data = json.dumps({"command": "PING"}).encode("utf-8")
        req = parse_tcp_command_frame(data)
        assert req.command == "PING"
        assert req.target_id == "all"
        assert req.args == {}

    def test_unknown_command(self) -> None:
        data = json.dumps({"command": "REBOOT_SYSTEM"}).encode("utf-8")
        with pytest.raises(C2ParseError, match="Unknown command 'REBOOT_SYSTEM'"):
            parse_tcp_command_frame(data)

    def test_invalid_args_type(self) -> None:
        data = json.dumps({"command": "PING", "args": ["invalid", "list"]}).encode("utf-8")
        with pytest.raises(C2ParseError, match="Field 'args' must be a JSON object"):
            parse_tcp_command_frame(data)

    def test_oversized_tcp_frame(self) -> None:
        huge = b"A" * (MAX_TCP_FRAME_SIZE + 1)
        with pytest.raises(C2ParseError, match="exceeds maximum permitted length"):
            parse_tcp_command_frame(huge)


class TestHttpPayloadParsing:
    def test_valid_command_payload(self) -> None:
        body = json.dumps({"command": "PING", "target_id": "node-2"}).encode("utf-8")
        req = parse_http_command_payload(body)
        assert isinstance(req, C2CommandRequest)
        assert req.command == "PING"
        assert req.target_id == "node-2"

    def test_http_command_rejected(self) -> None:
        for cmd in ["ARM", "SAFE", "ESTOP", "STATUS"]:
            body = json.dumps({"command": cmd}).encode("utf-8")
            with pytest.raises(C2ParseError, match=f"Unknown command '{cmd}'"):
                parse_http_command_payload(body)

    def test_http_command_malformed(self) -> None:
        with pytest.raises(C2ParseError):
            parse_http_command_payload(b"{not json")

    def test_valid_benchmark_payload(self) -> None:
        body = json.dumps({
            "preset": "64KB",
            "client_timestamp": 1700000000.5,
            "data": "X" * 1024,
        }).encode("utf-8")
        req = parse_http_benchmark_payload(body)
        assert isinstance(req, BenchmarkRequest)
        assert req.preset == "64KB"
        assert req.client_timestamp == 1700000000.5
        assert len(req.data) == 1024

    def test_benchmark_default_fields(self) -> None:
        body = b"{}"
        req = parse_http_benchmark_payload(body)
        assert req.preset == "Custom"
        assert req.client_timestamp == 0.0
        assert req.data == ""

    def test_benchmark_invalid_timestamp(self) -> None:
        body = json.dumps({"client_timestamp": "invalid_time"}).encode("utf-8")
        with pytest.raises(C2ParseError, match="Field 'client_timestamp' must be numeric"):
            parse_http_benchmark_payload(body)


class TestWebSocketMessageParsing:
    def test_valid_ws_benchmark(self) -> None:
        payload = json.dumps({
            "action": "ws_benchmark",
            "target_id": "node-3",
            "preset": "16KB",
            "timestamp": 1700000001.0,
            "data": "payload_data",
        })
        req = parse_ws_message(payload)
        assert isinstance(req, WsBenchmarkRequest)
        assert req.action == "ws_benchmark"
        assert req.target_id == "node-3"
        assert req.preset == "16KB"
        assert req.timestamp == 1700000001.0
        assert req.data == "payload_data"

    def test_ws_unknown_action(self) -> None:
        payload = json.dumps({"action": "drop_database"})
        with pytest.raises(C2ParseError, match="Unsupported WebSocket action 'drop_database'"):
            parse_ws_message(payload)

    def test_ws_missing_action(self) -> None:
        payload = json.dumps({"data": "no_action"})
        with pytest.raises(C2ParseError, match="Field 'action' must be a non-empty string"):
            parse_ws_message(payload)

    def test_ws_invalid_timestamp(self) -> None:
        payload = json.dumps({"action": "ws_benchmark", "timestamp": -5.0})
        with pytest.raises(C2ParseError, match="Field 'timestamp' must be >= 0.0"):
            parse_ws_message(payload)
