#!/usr/bin/env python3
"""
End-to-End Automated Verification Test for C2 Wireless Network PoC.
Tests UDP discovery, HTTP JSON APIs, Master proxy routing, WebSocket streaming (via aiohttp),
static asset serving, and failsafe silence pruning under native async test runners.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any, Awaitable, Callable, Generator

import aiohttp
import pytest

PYTHON = sys.executable
BASE_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = BASE_DIR / "src"


async def wait_until(
    predicate: Callable[[], Any] | Callable[[], Awaitable[Any]],
    timeout: float = 6.0,
    interval: float = 0.1,
    error_msg: str = "Condition not met before timeout",
) -> Any:
    """Dynamically poll predicate until it returns a truthy value or timeout expires."""
    start = time.time()
    while time.time() - start < timeout:
        res = predicate()
        if asyncio.iscoroutine(res):
            res = await res
        if res:
            return res
        await asyncio.sleep(interval)
    raise TimeoutError(f"{error_msg} (timed out after {timeout}s)")


@dataclass
class C2Cluster:
    master_proc: subprocess.Popen[bytes]
    worker_proc: subprocess.Popen[bytes]
    m_http: int
    m_tcp: int
    m_udp: int
    w_http: int
    w_tcp: int
    w_udp: int


@pytest.fixture(scope="module")
def c2_cluster() -> Generator[C2Cluster, None, None]:
    """Module-scoped fixture managing C2 master and worker subprocess lifecycle with guaranteed cleanup."""
    m_http, m_tcp, m_udp = 9050, 9857, 9856
    w_http, w_tcp, w_udp = 8051, 9858, 9856

    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC_DIR)

    master_proc = subprocess.Popen(
        [
            PYTHON,
            str(SRC_DIR / "node.py"),
            "--id",
            "test-master",
            "--role",
            "master",
            "--port-http",
            str(m_http),
            "--port-tcp",
            str(m_tcp),
            "--port-udp",
            str(m_udp),
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    worker_proc = subprocess.Popen(
        [
            PYTHON,
            str(SRC_DIR / "node.py"),
            "--id",
            "test-worker-1",
            "--role",
            "worker",
            "--port-http",
            str(w_http),
            "--port-tcp",
            str(w_tcp),
            "--port-udp",
            str(w_udp),
            "--master-ip",
            "127.0.0.1",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    cluster = C2Cluster(
        master_proc=master_proc,
        worker_proc=worker_proc,
        m_http=m_http,
        m_tcp=m_tcp,
        m_udp=m_udp,
        w_http=w_http,
        w_tcp=w_tcp,
        w_udp=w_udp,
    )

    try:
        yield cluster
    finally:
        for p in [master_proc, worker_proc]:
            try:
                p.terminate()
                p.wait(timeout=1.5)
            except Exception:
                p.kill()


# ---------------------------------------------------------------------------
# Discrete E2E Test Cases
# ---------------------------------------------------------------------------


@pytest.mark.e2e
async def test_e2e_udp_discovery(c2_cluster: C2Cluster) -> None:
    """Stage 1: Verify UDP auto-discovery beacons connect worker to master."""
    async with aiohttp.ClientSession() as session:
        async def _check_discovery() -> bool:
            try:
                async with session.get(
                    f"http://127.0.0.1:{c2_cluster.m_http}/api/status",
                    timeout=aiohttp.ClientTimeout(total=1.0),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return "test-worker-1" in data.get("peers", {})
            except Exception:
                return False
            return False

        await wait_until(
            _check_discovery,
            timeout=8.0,
            error_msg="Master failed to auto-discover test-worker-1 via UDP beacon",
        )

        # Assert status format
        async with session.get(f"http://127.0.0.1:{c2_cluster.m_http}/api/status") as resp:
            assert resp.status == 200
            m_status = await resp.json()
            assert m_status["node_id"] == "test-master"
            assert m_status["role"] == "master"
            assert "test-worker-1" in m_status["peers"]
            worker_peer = m_status["peers"]["test-worker-1"]
            assert "latency_ms" in worker_peer or "rtt_ms" in worker_peer


@pytest.mark.e2e
async def test_e2e_http_command(c2_cluster: C2Cluster) -> None:
    """Stage 2: Verify direct HTTP command dispatch and Master proxy routing."""
    async with aiohttp.ClientSession() as session:
        # 1. Direct PING command to worker
        async with session.post(
            f"http://127.0.0.1:{c2_cluster.w_http}/api/command",
            json={"command": "PING", "target_id": "test-worker-1"},
        ) as resp:
            assert resp.status == 200
            res = await resp.json()
            assert res.get("status") == "ACK"
            assert res.get("response") == "PONG"
            assert res.get("node_id") == "test-worker-1"

        # 2. Master proxy routing to worker
        async with session.post(
            f"http://127.0.0.1:{c2_cluster.m_http}/api/proxy/test-worker-1/command",
            json={"command": "PING", "target_id": "test-worker-1"},
        ) as resp:
            assert resp.status == 200
            proxy_res = await resp.json()
            assert proxy_res.get("status") == "ACK"
            assert proxy_res.get("response") == "PONG"
            assert proxy_res.get("node_id") == "test-worker-1"

        # 3. Rejection of invalid commands
        for invalid_cmd in ["ARM", "SAFE", "ESTOP"]:
            async with session.post(
                f"http://127.0.0.1:{c2_cluster.w_http}/api/command",
                json={"command": invalid_cmd, "target_id": "test-worker-1"},
            ) as resp:
                assert resp.status == 400
                err = await resp.json()
                assert err.get("status") == "ERROR"


@pytest.mark.e2e
async def test_e2e_benchmark_transfer(c2_cluster: C2Cluster) -> None:
    """Stage 3: Verify HTTP payload benchmark transfer across multiple sizes."""
    async with aiohttp.ClientSession() as session:
        for size_kb in [1, 16, 64]:
            payload_data = "D" * (size_kb * 1024)
            t0 = time.time()
            async with session.post(
                f"http://127.0.0.1:{c2_cluster.w_http}/api/benchmark",
                json={
                    "preset": f"{size_kb}KB",
                    "client_timestamp": t0,
                    "data": payload_data,
                },
            ) as resp:
                assert resp.status == 200
                bench_res = await resp.json()
                assert bench_res.get("status") == "BENCHMARK_COMPLETE"
                assert bench_res.get("bytes_received") >= (size_kb * 1024)


@pytest.mark.e2e
async def test_e2e_websocket_streaming(c2_cluster: C2Cluster) -> None:
    """Stage 4: Verify WebSocket telemetry stream and bi-directional benchmark echoing."""
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(f"http://127.0.0.1:{c2_cluster.m_http}/ws") as ws:
            # 1. Receive live telemetry frame
            msg = await ws.receive()
            assert msg.type == aiohttp.WSMsgType.TEXT
            telemetry = msg.json()
            assert telemetry.get("type") == "telemetry_update"
            assert "state" not in telemetry

            # 2. Send WebSocket benchmark payload
            bench_payload = {
                "action": "ws_benchmark",
                "preset": "16KB",
                "timestamp": time.time(),
                "data": "WS_TEST_BURST_DATA" * 50,
            }
            await ws.send_json(bench_payload)

            # 3. Receive benchmark ACK
            ack_msg = await ws.receive()
            assert ack_msg.type == aiohttp.WSMsgType.TEXT
            ack_json = ack_msg.json()
            assert ack_json.get("type") == "ws_benchmark_ack"
            assert ack_json.get("bytes") is not None

            await ws.close()


@pytest.mark.e2e
async def test_e2e_static_dashboard(c2_cluster: C2Cluster) -> None:
    """Stage 5: Verify Web UI static assets (HTML, CSS, JS) served on Master."""
    async with aiohttp.ClientSession() as session:
        # 1. HTML index
        async with session.get(f"http://127.0.0.1:{c2_cluster.m_http}/") as resp:
            assert resp.status == 200
            html = await resp.text()
            assert "C2 Wireless Command & Control Dashboard" in html
            assert '<link rel="stylesheet" href="/static/styles.css">' in html
            assert '<script type="importmap">' in html
            assert '"alpinejs": "/static/vendor/alpine.esm.js"' in html
            assert '<script type="module" src="/static/app.js"></script>' in html

        # 2. Modular CSS
        async with session.get(f"http://127.0.0.1:{c2_cluster.m_http}/static/styles.css") as resp:
            assert resp.status == 200
            css = await resp.text()
            assert "--primary:" in css

        # 3. Modular JS
        async with session.get(f"http://127.0.0.1:{c2_cluster.m_http}/static/app.js") as resp:
            assert resp.status == 200
            js = await resp.text()
            assert "import Alpine from 'alpinejs'" in js
            assert "export function c2Dashboard()" in js

        # 4. Vendored Alpine ESM module
        async with session.get(f"http://127.0.0.1:{c2_cluster.m_http}/static/vendor/alpine.esm.js") as resp:
            assert resp.status == 200
            esm_js = await resp.text()
            assert "export" in esm_js
            assert "Alpine" in esm_js


@pytest.mark.e2e
async def test_e2e_silence_pruning(c2_cluster: C2Cluster) -> None:
    """Stage 6: Verify carrier loss silence pruning on worker after master termination."""
    async with aiohttp.ClientSession() as session:
        # Terminate master process to simulate communication drop
        c2_cluster.master_proc.terminate()
        c2_cluster.master_proc.wait(timeout=2.0)

        async def _check_master_pruned() -> bool:
            try:
                async with session.get(
                    f"http://127.0.0.1:{c2_cluster.w_http}/api/status",
                    timeout=aiohttp.ClientTimeout(total=1.0),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return "test-master" not in data.get("peers", {})
            except Exception:
                return False
            return False

        # Failsafe timeout is 4.0s; polling with wait_until allows immediate detection
        await wait_until(
            _check_master_pruned,
            timeout=7.0,
            error_msg="Master peer was not pruned from worker registry within silence timeout",
        )


# ---------------------------------------------------------------------------
# Standalone CLI Entry Point
# ---------------------------------------------------------------------------
def main() -> None:
    """Allows running all E2E verification stages directly via `python3 tests/test_e2e.py`."""
    print("=" * 70)
    print(" RUNNING C2 WIRELESS POC MODULAR E2E INTEGRATION SUITE")
    print("=" * 70)
    sys.exit(pytest.main(["-v", "-m", "e2e", __file__]))


if __name__ == "__main__":
    main()
