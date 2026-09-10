# Testing Architecture & Verification Guide

This document details the testing architecture, execution workflows, and test authoring standards for the **C2 Wireless Network Proof-of-Concept**.

---

## 1. Testing Architecture Overview

The test suite implements a **3-tier testing pyramid** designed to balance verification speed, environmental hermeticity, and physical hardware realism:

| Tier | Directory | Marker | Execution Environment | Key Responsibilities |
| :--- | :--- | :--- | :--- | :--- |
| **Unit** | [`tests/unit/`](../tests/unit/) | `-m unit` | In-memory / hermetic | Input parsing boundaries, mathematical link quality calculations, single-handler HTTP/WS endpoints, daemon state machines. |
| **Hermetic Integration** | [`tests/integration/`](../tests/integration/) | `-m integration` | In-memory / hermetic | Multi-node cluster coordination without OS subprocesses: bi-directional UDP discovery, Master command proxy routing, data benchmarking, and silence watchdog pruning. |
| **Multi-Process E2E** | [`tests/e2e/`](../tests/e2e/) | `-m e2e` | Live OS subprocesses | Multi-process system verification over loopback network interfaces: actual TCP line protocols, UDP datagram broadcast, and static web dashboard serving. |

---

## 2. Directory Layout & Test Matrix

```tree
tests/
├── conftest.py                   # Global fixtures, factories (dummy beacons, daemon instances), port allocation
├── unit/
│   ├── test_config.py            # Configuration parsing, environment overrides, and immutability
│   ├── test_parsers.py           # Untrusted input boundaries (UDP beacons, TCP frames, HTTP JSON, WebSockets)
│   ├── test_node.py              # Daemon lifecycle, IP detection, failsafe loop, and CLI argument parsing
│   ├── test_beacon.py            # UDPBeaconProtocol socket handling and BeaconService transmission
│   ├── test_server.py            # HTTP REST, CORS, Master command proxy, WebSocket streaming, TCP commands
│   ├── test_presentation.py      # Console ANSI color formatting, TTY detection, and payload display sinks
│   ├── test_tracker.py           # Rolling latency, jitter, sequence resets, and carrier loss pruning
│   └── test_web_assets.py        # Static asset serving, ES module importmaps, and Alpine.js bundling
├── integration/
│   └── test_cluster_flow.py      # In-process Master <-> Worker cluster flows without OS sockets
└── e2e/
    └── test_e2e.py               # Subprocess cluster verification over OS loopback sockets
```

---

## 3. Running Tests

All test runs are managed through `pytest` and configured via [`pytest.ini`](../pytest.ini).

### 3.1 Default Test Run (Hermetic Unit + Integration)

By default, running `pytest` executes all hermetic unit and integration tests:

```bash
.venv/bin/pytest
```

### 3.2 Running Specific Test Tiers

#### Fast Unit Tests Only

```bash
.venv/bin/pytest tests/unit -v
```

#### Hermetic In-Process Integration Tests

```bash
.venv/bin/pytest tests/integration -v
# Or via pytest marker:
.venv/bin/pytest -m integration -v
```

#### Full Multi-Process End-to-End System Tests

> **Note:** E2E tests require local loopback socket binding permissions (`require_loopback_network`).

```bash
.venv/bin/pytest -m e2e -v
# Or run as a standalone script:
.venv/bin/python3 tests/e2e/test_e2e.py
```

#### Running a Single Test File or Test Case

```bash
# Specific test file:
.venv/bin/pytest tests/unit/test_node.py -v

# Specific test function:
.venv/bin/pytest tests/unit/test_node.py -k "test_determine_local_ip" -v
```

---

## 4. Static Type Checking (mypy)

Strict static typing is enforced with `mypy` across all source modules and test files:

```bash
.venv/bin/mypy src tests
```

Configuration is defined in [`mypy.ini`](../mypy.ini) with `disallow_untyped_defs = True` and strict optional checking.

---

## 5. Test Authoring Guidelines

When adding new features or fixing bugs, follow these conventions:

### 5.1 Use Shared Fixtures from `conftest.py`

Avoid instantiating daemons or dummy beacons manually. Use the pre-configured fixtures:

```python
import pytest
from node import C2NodeDaemon
from conftest import create_dummy_beacon

async def test_my_feature(master_daemon: C2NodeDaemon, worker_daemon: C2NodeDaemon) -> None:
    beacon = create_dummy_beacon(node_id="worker-custom", cpu_load=0.15)
    master_daemon.beacon_service.handle_incoming_beacon(beacon, "192.168.1.50")
    assert "worker-custom" in master_daemon.peers
```

### 5.2 In-Process Mocking for Integration Tests (`FakePostCM`)

For multi-node HTTP proxying, intercept outbound HTTP client sessions in-process without binding TCP ports:

```python
from unittest.mock import AsyncMock, MagicMock, patch
from aiohttp.test_utils import make_mocked_request

class FakePostCM:
    def __init__(self, target_url: str, body: bytes):
        self.target_url = target_url
        self.body = body

    async def __aenter__(self) -> Any:
        worker_req = make_mocked_request("POST", "/api/command", app=worker_app)
        setattr(worker_req, "read", AsyncMock(return_value=self.body))
        worker_resp = await worker_app._handle(worker_req)
        resp_mock = MagicMock()
        resp_mock.status = worker_resp.status
        resp_mock.json = AsyncMock(return_value=json.loads(worker_resp.text))
        return resp_mock

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        pass
```
