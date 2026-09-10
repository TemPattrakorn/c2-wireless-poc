# C2 Wireless Network Proof-of-Concept Testbed

A lightweight software suite to validate and benchmark wireless communications for a Command and Control (C2) system.

---

## 1. Network Architecture Overview

```
                      ┌────────────────────────────────────────┐
                      │            C2 Master (PC0)             │
                      │  • Web Dashboard: http://localhost:9000│
                      │  • Live Node Grid, Latency & Controls  │
                      │  • Interactive Data Transfer Lab       │
                      └──────────────────┬─────────────────────┘
                                         │ Gigabit LAN
                 ┌───────────────────────┴───────────────────────┐
                 │               Wireless Network                │
                 │   • UDP 9876: Auto-discovery, Latency & Jitter│
                 │   • TCP 9877: Guaranteed C2 Commands (PING)   │
                 │   • HTTP 9000/api/proxy: Master Command Proxy │
                 │   • HTTP 8080: JSON REST API & Benchmarks     │
                 │   • WS 8080/ws: Real-time Telemetry Stream    │
                 └───────┬─────────────────┬─────────────────┬───┘
                         │ Wireless / LAN  │ Wireless / LAN  │ Wireless / LAN
              ┌──────────┴──────┐ ┌────────┴──────┐ ┌────────┴──────┐
              │   Node 1 (PC1)  │ │  Node 2 (PC2) │ │  Node 5 (PC3) │
              │      40L70      │ │       1G      │ │       2G      │
              └─────────────────┘ └───────────────┘ └───────────────┘
```

* **Lightweight & Production-Ready:** Powered by Python 3 and `aiohttp` for asynchronous HTTP REST endpoints and live WebSocket streaming.
* **Strictly Typed & Validated (`src/protocol.py`):** Comprehensive schema validation and input boundary enforcement against untrusted network inputs (UDP beacons, TCP command frames, HTTP payloads, WebSocket messages) with `mypy` strict mode.
* **Broad Hardware Compatibility:** Runs out-of-the-box on Linux, macOS, and Windows across commodity PCs, laptops, mini-PCs (Intel NUC), single-board computers (Raspberry Pi, NVIDIA Jetson, Orange Pi), and robotics/drone companion computers.
* **Network & Router Agnostic:** Works across any standard TCP/IP network: commercial Wi-Fi routers (Wi-Fi 5/6/6E/7), enterprise access points (APs), ad-hoc mesh networks, cellular routers, or wired Ethernet.
* **Auto-Discovery & Link Health:** High-frequency UDP beacons (1 Hz) continuously monitor one-way transit latency (OWD with NTP synchronization), wireless jitter, and sequence-based packet loss.
* **Master Command Proxy:** Centralized `/api/proxy/{node_id}/command` endpoint on the Master node enables operators to dispatch commands to any field worker node through the Master, eliminating CORS, NAT, and private subnet barriers.
* **Dual-Channel Data Transfer:**
  * **HTTP POST (JSON):** On-demand request/response for state inspection, command dispatch, and throughput benchmarking.
  * **WebSocket Telemetry Stream:** Live 2 Hz telemetry streaming and duplex data transfer testing.
* **Automated Fail-Safe:** If communication with a node is lost for > 4.0 seconds, the watchdog prunes the timed-out peer from the active registry and logs a critical silence warning.

---

## 2. Deployment on Physical Hardware

### Step A: Configure the Wireless Router / Access Point

The testbed works with any standard Wi-Fi router, enterprise wireless access point (AP), or wireless bridge. Configure the following general settings in your router/AP's administration interface:

1. **AP / Client Isolation (CRITICAL):**
   * Ensure **AP Isolation** (also called *Client Isolation*, *Station Isolation*, or *Guest Network Isolation*) is **DISABLED**.
   * *Why:* When enabled, client isolation prevents wireless devices from communicating directly with each other or with wired LAN stations. Disabling it allows UDP auto-discovery beacons and direct TCP/HTTP C2 communication across nodes.

2. **Wi-Fi Band & SSID Separation (Recommended):**
   * Separate 2.4 GHz and 5 GHz (or 6 GHz) into distinct SSIDs (e.g., `C2-NET-5G` and `C2-NET-2.4G`), or disable Smart Connect / band steering.
   * Connect C2 nodes to the 5 GHz or 6 GHz band for higher throughput, lower channel congestion, and minimized latency.

3. **Traffic & Latency Optimizations (Optional):**
   * **Airtime Fairness:** Disable if available. Airtime fairness algorithms can delay or buffer small, frequent UDP packets, introducing latency jitter into real-time telemetry.
   * **OFDMA / MU-MIMO:** Enable (on Wi-Fi 6 / 802.11ax or newer routers) to reduce multi-node transmission queue delays.
   * **WMM / Power Saving:** Disable aggressive power-saving or sleep states on client wireless adapters if experiencing periodic latency spikes.

4. **IP Subnet & Addressing:**
   * Ensure all devices are assigned IP addresses within the same subnet (e.g., `192.168.1.0/24` or `10.0.0.0/24`) for zero-configuration broadcast discovery.
   * *(Optional)* Set DHCP IP reservations for the Master and critical worker nodes to maintain consistent addressing.

---

### Step B: Launch the C2 Master on PC0 (Ground Station)

Connect PC0 to the router or network switch via Gigabit Ethernet cable (recommended for minimal jitter) or Wi-Fi:

```bash
cd c2-wireless-poc
chmod +x scripts/*.sh
./scripts/setup.sh   # Installs dependencies (or: sudo apt install -y python3-aiohttp)
./scripts/run_master.sh c2-master 9000
```

* Open your browser to: **`http://localhost:9000`**
* You will see the **Strict Clean Light Theme** C2 dashboard.

---

### Step C: Launch Remote Worker Nodes on PC1 – PCn

Connect each remote device (PC, laptop, SBC, or companion computer) to the wireless network (e.g., `C2-NET-5G`):

```bash
# On Node 1 (Run ./scripts/setup.sh first on a fresh machine):
./scripts/setup.sh
./scripts/run_worker.sh node-1

# On Node n
./scripts/run_worker.sh node-n

# Remote Node across a routed subnet, VLAN, or multi-hop link:
# Pass the Master IP explicitly for directed unicast registration:
./scripts/run_worker.sh node-n <MASTER_IP>
```

Within 1 second, every active node will automatically appear on the C2 Master's dashboard grid.

---

## 3. Web Dashboard Features

1. **Active Node Cards:**
   * Displays real-time one-way transit latency (ms) (requires NTP host clock synchronization), wireless jitter, packet loss percentage, CPU load, and uptime.
   * Direct `Ping` button for any specific node.
2. **Global Controls:**
   * `PING ALL`: Dispatches a concurrent ping to all discovered nodes.
3. **Data Transfer Lab (HTTP & WebSocket Benchmarking):**
   * Select any target node from the dropdown.
   * Choose between **HTTP POST (JSON)** and **WebSocket Stream**.
   * Pick payload sizes (**1 KB**, **16 KB**, **64 KB**, **256 KB**) or type custom payload messages in the textarea with live byte counting.
   * Click **Run Benchmark Transfer** to measure real-time transfer latency (ms), throughput (KB/s and MB/s), and view the returned JSON echo.
   * The receiving worker node prints a formatted ANSI banner to its terminal CLI showing sender IP, timestamp, latency, and payload content.
4. **Live Activity Log:**
   * Displays timestamped network events, command acknowledgments, and connection alerts.

---

## 4. Manual / Direct CLI Testing (curl, netcat & WebSockets)

You can interact directly with any node or the Master proxy via standard terminal tools:

### 4.1 Reliable Line-Delimited TCP Command (Port 9877)
```bash
# Send direct PING command to a worker node via netcat:
echo '{"command": "PING", "target_id": "node-1"}' | nc <NODE_IP> 9877
```

### 4.2 HTTP REST API via curl
```bash
# Query node health, uptime, and active peer registry:
curl -s http://<NODE_IP>:8080/api/status | jq .

# Send a direct PING command to worker node:
curl -X POST http://<NODE_IP>:8080/api/command \
  -H "Content-Type: application/json" \
  -d '{"command": "PING", "target_id": "node-1"}'

# Route command to a worker through the Master command proxy:
curl -X POST http://<MASTER_IP>:9000/api/proxy/node-1/command \
  -H "Content-Type: application/json" \
  -d '{"command": "PING", "target_id": "node-1"}'

# Execute an HTTP data transfer benchmark with custom payload:
curl -X POST http://<NODE_IP>:8080/api/benchmark \
  -H "Content-Type: application/json" \
  -d '{"preset": "Custom", "data": "Sample benchmark content..."}'
```

### 4.3 Real-Time WebSocket Telemetry Stream (Port 9000 / 8080)
```bash
# Stream live telemetry updates using Python:
python3 -c '
import asyncio, aiohttp

async def listen():
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect("http://<MASTER_IP>:9000/ws") as ws:
            print("Connected to telemetry stream. Waiting for updates...")
            async for msg in ws:
                data = msg.json()
                print(f"[{data.get(\"type\")}] Node: {data.get(\"node_id\")} Peers: {list(data.get(\"peers\", {}).keys())}")

asyncio.run(listen())
'

# Or using websocat CLI:
websocat ws://<MASTER_IP>:9000/ws
```

---

## 5. Repository Layout & Codebase Structure

```
c2-wireless-poc/
├── README.md                 # Primary system overview, hardware setup, and quickstart guide
├── requirements.txt          # Runtime dependencies (aiohttp)
├── requirements-dev.txt      # Development & verification tooling (mypy, pytest, pytest-asyncio)
├── pytest.ini                # Pytest configuration, markers, and asyncio execution mode
├── mypy.ini                  # Strict static typing rules and boundary checks
│
├── docs/                     # Detailed technical specifications
│   ├── architecture.md       # Subsystem deep-dives, protocol flows, and Mermaid/ASCII diagrams
│   ├── api.md                # Wire formats, JSON schemas, safety boundaries, and CLI recipes
│   └── testing.md            # 3-tier testing architecture, fixtures, and authoring guidelines
│
├── scripts/                  # Operational launch & setup utilities
│   ├── setup.sh              # Automatic environment & dependency bootstrap
│   ├── run_master.sh         # Master ground station launcher with port configuration
│   ├── run_worker.sh         # Worker node launcher with automatic/explicit master IP
│   └── run_tests.sh          # Automated test suite and static type check executor
│
├── src/                      # Application source code
│   ├── config.py             # Global constants, network defaults, and environment overrides
│   ├── protocol.py           # Typed input parsers, boundaries, and validation dataclasses
│   ├── tracker.py            # PeerTracker, NodeMetrics, and link quality mathematics
│   ├── beacon.py             # UDPBeaconProtocol and BeaconService discovery loops
│   ├── presentation.py       # Console ANSI color formatting and benchmark payload sinks
│   ├── node.py               # C2NodeDaemon entry point and async lifecycle coordinator
│   │
│   ├── server/               # Network service layer
│   │   ├── __init__.py       # Server module exports
│   │   ├── commands.py       # Command dispatch & execution handlers (PING)
│   │   ├── tcp.py            # Line-delimited TCP command socket server
│   │   ├── ws.py             # WebSocketManager: live telemetry streaming & benchmarking
│   │   └── rest.py           # HttpServer: aiohttp REST routing, CORS, and Master proxy
│   │
│   └── web/                  # Standalone offline web dashboard
│       ├── index.html        # HTML structure & import map definitions
│       ├── styles.css        # Strict clean light design system
│       ├── app.js            # Reactive Alpine.js frontend logic
│       └── vendor/           # Vendored offline libraries (Alpine.js)
│
└── tests/                    # 3-tier test suite
    ├── conftest.py           # Shared fixtures, factories, and port allocators
    ├── unit/                 # Hermetic unit tests (parsers, trackers, nodes, servers)
    ├── integration/          # Hermetic in-process multi-node cluster flow tests
    └── e2e/                  # Multi-process end-to-end integration tests over loopback
```

---

## 6. Documentation Index

For in-depth technical references and specifications, consult the dedicated documentation guides:

* **[System Architecture Specification](docs/architecture.md):** Detailed subsystem breakdowns (`C2NodeDaemon`, `BeaconService`, `PeerTracker`, `TcpCommandServer`, `HttpServer`, `WebSocketManager`), moving window link quality mathematics (OWD, jitter, sequence gap packet loss), Mermaid sequence flows, and watchdog state machines.
* **[Network Protocol & API Specification](docs/api.md):** Complete wire framing specifications, JSON schemas, payload safety limits, REST route descriptions, WebSocket action formats, and CLI testing recipes.
* **[Testing Architecture & Verification Guide](docs/testing.md):** 3-tier testing pyramid overview (Unit, Hermetic Integration, E2E), fixture authoring standards, and mock socket patterns.

---

## 7. Development, Type Checking & Testing

### Installation

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt       # Runtime dependencies (aiohttp)
pip install -r requirements-dev.txt   # Development dependencies (mypy, pytest, pytest-asyncio)
```

### Static Type Checking (mypy)

Strict static typing is enforced across all 24 source and test files:

```bash
.venv/bin/mypy src tests
```

### Automated Testing (pytest)

The testbed features a **3-tier testing architecture** (Hermetic Unit, Hermetic In-Process Integration, and Multi-Process E2E):

```bash
# 1. Run full hermetic test suite (Unit + Integration):
.venv/bin/pytest

# 2. Run fast unit tests only:
.venv/bin/pytest tests/unit -v

# 3. Run hermetic in-process cluster integration tests:
.venv/bin/pytest tests/integration -v

# 4. Run multi-process end-to-end integration tests (requires loopback socket permissions):
.venv/bin/pytest -m e2e -v
# Or run directly via standalone script:
.venv/bin/python3 tests/e2e/test_e2e.py
```
