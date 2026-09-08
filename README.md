# C2 Wireless Network Proof-of-Concept Testbed

A lightweight, zero-dependency software suite to validate and benchmark wireless communications for a Command and Control (C2) system.

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
                 │   • UDP 9876: Auto-discovery, RTT & Jitter    │
                 │   • TCP 9877: Guaranteed C2 Commands (ARM)    │
                 │   • HTTP 8080: JSON REST API & Benchmarks     │
                 │   • WS 8080/ws: Real-time Telemetry Stream    │
                 └───────┬─────────────────┬─────────────────┬───┘
                         │ Wireless / LAN  │ Wireless / LAN  │ Wireless / LAN
              ┌──────────┴──────┐ ┌────────┴──────┐ ┌────────┴──────┐
              │   Node 1 (PC1)  │ │  Node 2 (PC2) │ │  Node 5 (PC3) │
              │      40L70      │ │       1G      │ │       2G      │
              └─────────────────┘ └───────────────┘ └───────────────┘
```

* **Zero External Dependencies:** Built 100% using Python 3's standard library (`asyncio`, `socket`, `hashlib`, `struct`, `json`). No `pip install` required on any machine.
* **Broad Hardware Compatibility:** Runs out-of-the-box on Linux, macOS, and Windows across commodity PCs, laptops, mini-PCs (Intel NUC), single-board computers (Raspberry Pi, NVIDIA Jetson, Orange Pi), and robotics/drone companion computers.
* **Network & Router Agnostic:** Works across any standard TCP/IP network: commercial Wi-Fi routers (Wi-Fi 5/6/6E/7), enterprise access points (APs), ad-hoc mesh networks, cellular routers, or wired Ethernet.
* **Auto-Discovery & Link Health:** High-frequency UDP beacons (1 Hz) continuously monitor Round-Trip Time (RTT), wireless jitter, and sequence-based packet loss.
* **Dual-Channel Data Transfer:**
  * **HTTP POST (JSON):** On-demand request/response for state inspection, command dispatch, and throughput benchmarking.
  * **Native WebSocket (RFC 6455):** Live 2 Hz telemetry streaming and duplex data transfer testing.
* **Automated Fail-Safe:** If a worker node loses communication with the C2 Master for > 4.0 seconds, it autonomously switches its operational state to `FAILSAFE_ACTIVE` / `SAFE`.

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
./scripts/run_master.sh c2-master 9000
```

* Open your browser to: **`http://localhost:9000`**
* You will see the **Strict Clean Light Theme** C2 dashboard.

---

### Step C: Launch Remote Worker Nodes on PC1 – PCn

Connect each remote device (PC, laptop, SBC, or companion computer) to the wireless network (e.g., `C2-NET-5G`):

```bash
# On Node 1
./scripts/run_worker.sh node-1

# On Node 2
./scripts/run_worker.sh node-2

# On Node 3
./scripts/run_worker.sh node-3

# On Node 4
./scripts/run_worker.sh node-4

# On Node 5 (Remote Node across a routed subnet, VLAN, or multi-hop link):
# Pass the Master IP explicitly for directed unicast registration:
./scripts/run_worker.sh node-5 <MASTER_IP>
```

Within 1 second, every active node will automatically appear on the C2 Master's dashboard grid!

---

## 3. Web Dashboard Features

1. **Active Node Cards:**
   * Displays real-time RTT (ms), wireless jitter, packet loss percentage, CPU load, and system state (`SAFE`, `ARMED`, `ESTOP`).
   * Individual buttons to `Ping`, `Arm`, `Safe`, or `E-Stop` any specific node.
2. **Global Controls:**
   * `ARM ALL`: Transitions all connected nodes to `ARMED`.
   * `SAFE ALL`: Returns all nodes to `SAFE`.
   * `EMERGENCY STOP`: Broadcasts immediate emergency halt.
3. **Data Transfer Lab (HTTP & WebSocket Benchmarking):**
   * Select any target node from the dropdown.
   * Choose between **HTTP POST (JSON)** and **WebSocket Stream**.
   * Pick payload sizes: **1 KB**, **16 KB**, **64 KB**, or **256 KB**.
   * Click **Run Benchmark Transfer** to measure real-time transfer latency (ms), throughput (KB/s and MB/s), and view the returned JSON echo.
4. **Live Activity Log:**
   * Displays timestamped network events, command acknowledgments, and connection alerts.

---

## 4. Manual / Direct CLI Testing (curl & APIs)

You can also interact directly with any worker node via standard HTTP commands:

```bash
# Query node health and status:
curl http://<NODE_IP>:8080/api/status

# Send an ARM command to node-1:
curl -X POST http://<NODE_IP>:8080/api/command \
  -H "Content-Type: application/json" \
  -d '{"command": "ARM", "target_id": "node-1"}'

# Execute an HTTP data transfer test:
curl -X POST http://<NODE_IP>:8080/api/benchmark \
  -H "Content-Type: application/json" \
  -d '{"preset": "TestPayload", "data": "Sample benchmark content..."}'
```

*(Replace `<NODE_IP>` with the actual IP address of the target worker node, e.g., `192.168.1.20`)*
