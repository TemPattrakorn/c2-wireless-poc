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
   * Displays real-time RTT (ms), wireless jitter, packet loss percentage, CPU load, and uptime.
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

## 4. Manual / Direct CLI Testing (curl & APIs)

You can also interact directly with any worker node via standard HTTP commands:

```bash
# Query node health and status:
curl http://<NODE_IP>:8080/api/status

# Send a PING command to node-1:
curl -X POST http://<NODE_IP>:8080/api/command \
  -H "Content-Type: application/json" \
  -d '{"command": "PING", "target_id": "node-1"}'

# Execute an HTTP data transfer test with custom message:
curl -X POST http://<NODE_IP>:8080/api/benchmark \
  -H "Content-Type: application/json" \
  -d '{"preset": "Custom", "data": "Sample benchmark content..."}'
```

*(Replace `<NODE_IP>` with the actual IP address of the target worker node, e.g., `192.168.1.20`)*

---

## 5. Development, Type Checking & Testing

### Installation

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt       # Runtime dependencies (aiohttp)
pip install -r requirements-dev.txt   # Development dependencies (mypy, pytest, pytest-asyncio)
```

### Static Type Checking (mypy)

Strict static typing is enforced across all core modules and tests:

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

> 📖 For detailed testing architecture, testbed requirements, and fixture authoring guides, see the **[Testing Architecture & Verification Guide](docs/testing.md)**.
