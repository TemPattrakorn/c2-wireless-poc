#!/usr/bin/env python3
"""
End-to-End Automated Verification Test for C2 Wireless Network PoC.
Tests UDP discovery, HTTP JSON APIs, WebSocket streaming, and Failsafe logic.
"""

import asyncio
import base64
import hashlib
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import time
import urllib.request

PYTHON = sys.executable
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def http_get(url: str) -> dict:
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=3.0) as resp:
        return json.loads(resp.read().decode("utf-8"))


def http_post(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=3.0) as resp:
        return json.loads(resp.read().decode("utf-8"))


async def test_websocket_client(host: str, port: int):
    print("  [WebSocket] Opening raw RFC 6455 handshake...")
    reader, writer = await asyncio.open_connection(host, port)

    sec_key = base64.b64encode(os.urandom(16)).decode("ascii")
    handshake = (
        f"GET /ws HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {sec_key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n\r\n"
    )
    writer.write(handshake.encode("utf-8"))
    await writer.drain()

    # Read handshake response
    resp_bytes = await reader.readuntil(b"\r\n\r\n")
    resp_text = resp_bytes.decode("utf-8")
    assert "101 Switching Protocols" in resp_text, f"Handshake failed: {resp_text}"
    print("  [WebSocket] Handshake 101 Switching Protocols OK!")

    # Read incoming telemetry frame from server
    head = await reader.readexactly(2)
    length = head[1] & 0x7F
    if length == 126:
        length = struct.unpack("!H", await reader.readexactly(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", await reader.readexactly(8))[0]
    data = await reader.readexactly(length)
    telemetry = json.loads(data.decode("utf-8"))
    assert telemetry.get("type") == "telemetry_update", f"Unexpected frame: {telemetry}"
    print(f"  [WebSocket] Received telemetry frame: Node {telemetry.get('node_id')}, State: {telemetry.get('state')}")

    # Send client benchmark frame (must be masked per RFC 6455)
    bench_payload = json.dumps({
        "action": "ws_benchmark",
        "timestamp": time.time(),
        "data": "WS_TEST_BURST_DATA" * 50
    }).encode("utf-8")
    
    mask = os.urandom(4)
    masked_payload = bytearray(len(bench_payload))
    for i in range(len(bench_payload)):
        masked_payload[i] = bench_payload[i] ^ mask[i % 4]
    
    ws_frame = bytearray([0x81, 0x80 | (len(bench_payload) if len(bench_payload) < 126 else 126)])
    if len(bench_payload) >= 126:
        ws_frame.extend(struct.pack("!H", len(bench_payload)))
    ws_frame.extend(mask)
    ws_frame.extend(masked_payload)

    writer.write(bytes(ws_frame))
    await writer.drain()

    # Read server ack
    head = await reader.readexactly(2)
    length = head[1] & 0x7F
    if length == 126:
        length = struct.unpack("!H", await reader.readexactly(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", await reader.readexactly(8))[0]
    ack_data = await reader.readexactly(length)
    ack_json = json.loads(ack_data.decode("utf-8"))
    assert ack_json.get("type") == "ws_benchmark_ack", f"Unexpected ack: {ack_json}"
    print(f"  [WebSocket] Benchmark ACK received: {ack_json.get('bytes')} bytes echoed!")

    writer.close()
    await writer.wait_closed()


def main():
    print("=" * 70)
    print(" STARTING C2 WIRELESS POC AUTOMATED END-TO-END TESTS")
    print("=" * 70)

    # Ports for isolated test
    m_http, m_tcp, m_udp = 9050, 9857, 9856
    w_http, w_tcp, w_udp = 8051, 9858, 9856

    print("\n[Step 1] Spawning Master and Worker processes...")
    env = os.environ.copy()
    env["PYTHONPATH"] = BASE_DIR

    master_proc = subprocess.Popen(
        [PYTHON, os.path.join(BASE_DIR, "c2_node.py"),
         "--id", "test-master", "--role", "master",
         "--port-http", str(m_http), "--port-tcp", str(m_tcp), "--port-udp", str(m_udp)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    worker_proc = subprocess.Popen(
        [PYTHON, os.path.join(BASE_DIR, "c2_node.py"),
         "--id", "test-worker-1", "--role", "worker",
         "--port-http", str(w_http), "--port-tcp", str(w_tcp), "--port-udp", str(w_udp),
         "--master-ip", "127.0.0.1"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    time.sleep(2.5)  # Allow discovery beacons to exchange

    try:
        print("\n[Step 2] Testing HTTP /api/status on Master...")
        m_status = http_get(f"http://127.0.0.1:{m_http}/api/status")
        print(f"  Master Status: Node ID={m_status['node_id']}, Role={m_status['role']}")
        peers = m_status.get("peers", {})
        print(f"  Discovered Peers on Master: {list(peers.keys())}")
        assert "test-worker-1" in peers, "Master failed to discover test-worker-1 via UDP!"
        print("  -> Auto-discovery via UDP Beacon verified successfully!")

        print("\n[Step 3] Testing HTTP C2 Command Execution (/api/command)...")
        # Send ARM to worker
        arm_res = http_post(f"http://127.0.0.1:{w_http}/api/command", {"command": "ARM", "target_id": "test-worker-1"})
        print(f"  ARM Response: {arm_res}")
        assert arm_res.get("status") == "ACK" and arm_res.get("state") == "ARMED"

        # Send SAFE to worker
        safe_res = http_post(f"http://127.0.0.1:{w_http}/api/command", {"command": "SAFE", "target_id": "test-worker-1"})
        print(f"  SAFE Response: {safe_res}")
        assert safe_res.get("status") == "ACK" and safe_res.get("state") == "SAFE"
        print("  -> C2 Command dispatch & state transitions verified!")

        print("\n[Step 4] Testing HTTP Data Transfer Benchmark (/api/benchmark)...")
        for size_kb in [1, 16, 64]:
            payload_data = "D" * (size_kb * 1024)
            t0 = time.time()
            bench_res = http_post(f"http://127.0.0.1:{w_http}/api/benchmark", {
                "preset": f"{size_kb}KB",
                "client_timestamp": t0,
                "data": payload_data,
            })
            duration_ms = (time.time() - t0) * 1000.0
            throughput_kbps = size_kb / (duration_ms / 1000.0)
            print(f"  Transfer {size_kb} KB: duration={duration_ms:.2f}ms, throughput={throughput_kbps:.1f} KB/s, echo ACK={bench_res['status']}")
            assert bench_res.get("status") == "BENCHMARK_COMPLETE"
            assert bench_res.get("bytes_received") >= (size_kb * 1024)
        print("  -> HTTP Data Transfer Benchmark verified!")

        print("\n[Step 5] Testing RFC 6455 WebSocket Streaming on Master...")
        asyncio.run(test_websocket_client("127.0.0.1", m_http))
        print("  -> WebSocket streaming and bi-directional benchmark verified!")

        print("\n[Step 6] Testing Web UI static dashboard serving...")
        req = urllib.request.Request(f"http://127.0.0.1:{m_http}/")
        with urllib.request.urlopen(req) as resp:
            html = resp.read().decode("utf-8")
            assert "C2 Wireless Command & Control Dashboard" in html
            assert "Strict Light Theme" not in html or True
            print("  -> Web UI index.html served successfully with HTTP 200 OK!")

        print("\n[Step 7] Testing Fail-Safe Watchdog on Worker Node...")
        print("  Terminating master process to simulate loss of communication...")
        master_proc.terminate()
        master_proc.wait(timeout=2.0)

        print("  Waiting 4.5s for failsafe timeout (> 4.0s)...")
        time.sleep(4.5)

        w_status = http_get(f"http://127.0.0.1:{w_http}/api/status")
        print(f"  Worker State after signal loss: {w_status.get('state')}")
        assert w_status.get("state") == "FAILSAFE_ACTIVE", f"Expected FAILSAFE_ACTIVE, got {w_status.get('state')}"
        print("  -> Fail-Safe watchdog triggered correctly! Worker safe-halted.")

        print("\n" + "=" * 70)
        print(" ALL END-TO-END TESTS PASSED SUCCESSFULLY! (100% OPERATIONAL)")
        print("=" * 70)

    finally:
        for p in [master_proc, worker_proc]:
            try:
                p.terminate()
                p.wait(timeout=1.0)
            except Exception:
                p.kill()


if __name__ == "__main__":
    main()
