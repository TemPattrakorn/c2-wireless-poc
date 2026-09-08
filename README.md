# C2 Wireless Network Proof-of-Concept Testbed

A lightweight, zero-dependency software suite running on all PCs to validate and benchmark wireless communications for a Command and Control (C2) system across the **ASUS RT-AX1800HP** wireless network.

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
                 │       Wireless Network (ASUS RT-AX1800HP)     │
                 │   • UDP 9876: Auto-discovery, RTT & Jitter    │
                 │   • TCP 9877: Guaranteed C2 Commands (ARM)    │
                 │   • HTTP 8080: JSON REST API & Benchmarks     │
                 │   • WS 8080/ws: Real-time Telemetry Stream    │
                 └───────┬─────────────────┬─────────────────┬───┘
                         │ 5 GHz Wi-Fi     │ 5 GHz Wi-Fi     │ Relay / Multi-hop
              ┌──────────┴──────┐ ┌────────┴──────┐ ┌────────┴──────┐
              │ Node 1 (PC1)    │ │ Node 2 (PC2)  │ │ Node 5 (PC5)  │
              │ Payload/Sensors │ │ Actuators/Tur │ │ Remote Node   │
              └─────────────────┘ └───────────────┘ └───────────────┘
```

* **Zero External Dependencies:** Built 100% using Python 3's standard library (`asyncio`, `socket`, `hashlib`, `struct`, `json`). No `pip install` required on any machine.
* **Auto-Discovery & Link Health:** High-frequency UDP beacons (1 Hz) continuously monitor Round-Trip Time (RTT), wireless jitter, and sequence-based packet loss.
* **Dual-Channel Data Transfer:**
  * **HTTP POST (JSON):** On-demand request/response for state inspection, command dispatch, and throughput benchmarking.
  * **Native WebSocket (RFC 6455):** Live 2 Hz telemetry streaming and duplex data transfer testing.
* **Automated Fail-Safe:** If a worker node loses communication with the C2 Master for > 4.0 seconds, it autonomously switches its operational state to `FAILSAFE_ACTIVE` / `SAFE`.

---

## 2. Deployment on Physical Hardware

### Step A: Configure the ASUS RT-AX1800HP Router
1. Log into ASUSWRT (`http://192.168.50.1`).
2. Go to **Wireless** > **General**:
   * Separate SSIDs (e.g. `C2-NET-5G` and `C2-NET-2.4G`). Disable Smart Connect.
3. Go to **Wireless** > **Professional**:
   * Set **Set AP Isolation:** `No` (Critical: allows inter-PC wireless communication).
   * Disable **Airtime Fairness** (minimizes jitter for small UDP telemetry packets).
   * Enable **DL/UL OFDMA** (Wi-Fi 6 latency reduction).

---

### Step B: Launch the C2 Master on PC0 (Ground Station)
Plug PC0 into **LAN Port 1** of the ASUS router via Gigabit Ethernet cable:

```bash
cd c2-wireless-poc
chmod +x scripts/*.sh
./scripts/run_master.sh c2-master 9000
```
* Open your browser to: **`http://localhost:9000`**
* You will see the **Strict Clean Light Theme** C2 dashboard.

---

### Step C: Launch Remote Worker Nodes on PC1 – PC5
Connect each remote PC to the `C2-NET-5G` Wi-Fi:

```bash
# On PC1:
./scripts/run_worker.sh node-1

# On PC2:
./scripts/run_worker.sh node-2

# On PC3:
./scripts/run_worker.sh node-3

# On PC4 (Relay Node):
./scripts/run_worker.sh node-4

# On PC5 (Remote Node behind PC4 if on a routed/multi-hop subnet):
# Pass the Master IP explicitly for directed unicast registration:
./scripts/run_worker.sh node-5 192.168.50.10
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
curl http://192.168.50.20:8080/api/status

# Send an ARM command to node-1:
curl -X POST http://192.168.50.20:8080/api/command \
  -H "Content-Type: application/json" \
  -d '{"command": "ARM", "target_id": "node-1"}'

# Execute an HTTP data transfer test:
curl -X POST http://192.168.50.20:8080/api/benchmark \
  -H "Content-Type: application/json" \
  -d '{"preset": "TestPayload", "data": "Sample benchmark content..."}'
```
