#!/usr/bin/env python3
"""
End-to-End Automated Verification Test for C2 Wireless Network PoC.
Tests UDP discovery, HTTP JSON APIs, WebSocket streaming (via aiohttp), and Failsafe logic.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time

import aiohttp

PYTHON = sys.executable
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


async def run_e2e_tests() -> None:
    print("=" * 70)
    print(" STARTING C2 WIRELESS POC AUTOMATED END-TO-END TESTS (AIOHTTP)")
    print("=" * 70)

    # Ports for isolated test
    m_http, m_tcp, m_udp = 9050, 9857, 9856
    w_http, w_tcp, w_udp = 8051, 9858, 9856

    print("\n[Step 1] Spawning Master and Worker processes...")
    env = os.environ.copy()
    env["PYTHONPATH"] = BASE_DIR

    master_proc = subprocess.Popen(
        [
            PYTHON,
            os.path.join(BASE_DIR, "c2_node.py"),
            "--id", "test-master",
            "--role", "master",
            "--port-http", str(m_http),
            "--port-tcp", str(m_tcp),
            "--port-udp", str(m_udp),
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    worker_proc = subprocess.Popen(
        [
            PYTHON,
            os.path.join(BASE_DIR, "c2_node.py"),
            "--id", "test-worker-1",
            "--role", "worker",
            "--port-http", str(w_http),
            "--port-tcp", str(w_tcp),
            "--port-udp", str(w_udp),
            "--master-ip", "127.0.0.1",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    time.sleep(2.5)  # Allow processes to start and exchange discovery beacons

    try:
        async with aiohttp.ClientSession() as session:
            print("\n[Step 2] Testing HTTP /api/status on Master...")
            async with session.get(f"http://127.0.0.1:{m_http}/api/status") as resp:
                assert resp.status == 200, f"Expected status 200, got {resp.status}"
                m_status = await resp.json()

            print(f"  Master Status: Node ID={m_status['node_id']}, Role={m_status['role']}")
            peers = m_status.get("peers", {})
            print(f"  Discovered Peers on Master: {list(peers.keys())}")
            assert "test-worker-1" in peers, "Master failed to discover test-worker-1 via UDP!"
            print("  -> Auto-discovery via UDP Beacon verified successfully!")

            print("\n[Step 3] Testing HTTP C2 Command Execution (/api/command)...")
            # Send PING to worker
            async with session.post(
                f"http://127.0.0.1:{w_http}/api/command",
                json={"command": "PING", "target_id": "test-worker-1"},
            ) as resp:
                assert resp.status == 200, f"Expected 200, got {resp.status}"
                ping_res = await resp.json()
            print(f"  PING Response: {ping_res}")
            assert ping_res.get("status") == "ACK", f"Expected ACK, got {ping_res.get('status')}"
            assert ping_res.get("response") == "PONG", f"Expected PONG, got {ping_res.get('response')}"
            assert ping_res.get("node_id") == "test-worker-1"

            # Verify invalid commands (ARM, SAFE, ESTOP) are rejected with 400
            for invalid_cmd in ["ARM", "SAFE", "ESTOP"]:
                async with session.post(
                    f"http://127.0.0.1:{w_http}/api/command",
                    json={"command": invalid_cmd, "target_id": "test-worker-1"},
                ) as resp:
                    assert resp.status == 400, f"Expected 400 for {invalid_cmd}, got {resp.status}"
                    err_res = await resp.json()
                    assert err_res.get("status") == "ERROR"
            print("  -> PING command execution and rejection of invalid commands verified!")

            print("\n[Step 4] Testing HTTP Data Transfer Benchmark (/api/benchmark)...")
            for size_kb in [1, 16, 64]:
                payload_data = "D" * (size_kb * 1024)
                t0 = time.time()
                async with session.post(
                    f"http://127.0.0.1:{w_http}/api/benchmark",
                    json={
                        "preset": f"{size_kb}KB",
                        "client_timestamp": t0,
                        "data": payload_data,
                    },
                ) as resp:
                    assert resp.status == 200
                    bench_res = await resp.json()

                duration_ms = (time.time() - t0) * 1000.0
                throughput_kbps = size_kb / (duration_ms / 1000.0)
                print(
                    f"  Transfer {size_kb} KB: duration={duration_ms:.2f}ms, "
                    f"throughput={throughput_kbps:.1f} KB/s, echo ACK={bench_res['status']}"
                )
                assert bench_res.get("status") == "BENCHMARK_COMPLETE"
                assert bench_res.get("bytes_received") >= (size_kb * 1024)
            print("  -> HTTP Data Transfer Benchmark verified!")

            print("\n[Step 5] Testing aiohttp WebSocket Streaming on Master...")
            async with session.ws_connect(f"http://127.0.0.1:{m_http}/ws") as ws:
                # Read incoming telemetry frame
                msg = await ws.receive()
                assert msg.type == aiohttp.WSMsgType.TEXT, f"Expected text frame, got {msg.type}"
                telemetry = msg.json()
                assert telemetry.get("type") == "telemetry_update", f"Unexpected frame: {telemetry}"
                assert "state" not in telemetry, "Telemetry should not contain 'state'!"
                print(
                    f"  [WebSocket] Received telemetry frame: Node {telemetry.get('node_id')}"
                )

                # Send client benchmark frame
                bench_payload = {
                    "action": "ws_benchmark",
                    "preset": "16KB",
                    "timestamp": time.time(),
                    "data": "WS_TEST_BURST_DATA" * 50,
                }
                await ws.send_json(bench_payload)

                # Read server ack
                ack_msg = await ws.receive()
                assert ack_msg.type == aiohttp.WSMsgType.TEXT
                ack_json = ack_msg.json()
                assert ack_json.get("type") == "ws_benchmark_ack", f"Unexpected ack: {ack_json}"
                print(f"  [WebSocket] Benchmark ACK received: {ack_json.get('bytes')} bytes echoed!")

                await ws.close()
            print("  -> WebSocket streaming and bi-directional benchmark verified!")

            print("\n[Step 6] Testing Web UI static dashboard serving...")
            async with session.get(f"http://127.0.0.1:{m_http}/") as resp:
                assert resp.status == 200
                html = await resp.text()
                assert "C2 Wireless Command & Control Dashboard" in html
                print("  -> Web UI index.html served successfully with HTTP 200 OK!")

            print("\n[Step 7] Testing Peer Silence Timeout on Worker Node...")
            print("  Terminating master process to simulate loss of communication...")
            master_proc.terminate()
            master_proc.wait(timeout=2.0)

            print("  Waiting 4.5s for silence timeout (> 4.0s)...")
            await asyncio.sleep(4.5)

            async with session.get(f"http://127.0.0.1:{w_http}/api/status") as resp:
                assert resp.status == 200
                w_status = await resp.json()
            assert "state" not in w_status
            assert "test-master" not in w_status.get("peers", {}), "Master peer should be pruned after silence threshold!"
            print("  -> Silence threshold exceeded: Master timed out and was pruned from peers.")

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


def main() -> None:
    asyncio.run(run_e2e_tests())


if __name__ == "__main__":
    main()
