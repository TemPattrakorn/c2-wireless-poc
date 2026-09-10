# C2 Wireless Network Proof-of-Concept — Network Protocol & API Specification

This document provides the authoritative wire specification, payload schemas, safety boundaries, and CLI testing recipes for all network interfaces in the **C2 Wireless Network Proof-of-Concept**.

---

## 1. Protocol Safety Boundaries

To ensure resilience against untrusted network inputs, packet corruption, and denial-of-service attempts, strict input size thresholds and schema boundaries are enforced across all protocols in [`src/protocol.py`](../src/protocol.py):

| Protocol Boundary | Constant | Value | Enforcement Mechanism | Failure Response |
| :--- | :--- | :--- | :--- | :--- |
| **Max UDP Datagram** | `MAX_DATAGRAM_SIZE` | 65,507 bytes | Evaluated before JSON decoding | Datagram dropped; logged at `DEBUG` |
| **Max TCP Command Frame** | `MAX_TCP_FRAME_SIZE` | 65,536 bytes (64 KB) | Checked per line chunk before JSON parsing | Client socket returns `ERROR` JSON and closes |
| **Max HTTP Command Body** | `MAX_COMMAND_PAYLOAD_SIZE` | 65,536 bytes (64 KB) | Checked prior to JSON parsing | HTTP `400 Bad Request` |
| **Max HTTP Benchmark Body** | `MAX_HTTP_PAYLOAD_SIZE` | 10,485,760 bytes (10 MB) | Checked prior to JSON parsing | HTTP `400 Bad Request` |
| **Max WebSocket Frame** | `MAX_HTTP_PAYLOAD_SIZE` | 10,485,760 bytes (10 MB) | Checked on incoming text frame | Returns `{"type": "error", ...}` |
| **Node ID String Length** | Inline Check | 1..128 characters | Non-empty string constraint | Raises `C2ParseError(field="node_id")` |

---

## 2. Port Allocation & Protocol Matrix

| Port | Transport | Direction | Target Role | Service / Endpoint | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **9876** | **UDP** | Bidirectional | All Nodes | `BeaconService` | Auto-discovery, transit latency (OWD), jitter, and packet loss estimation. |
| **9877** | **TCP** | Inbound | All Nodes | `TcpCommandServer` | Reliable line-delimited C2 command socket (PING/PONG). |
| **9000** | **HTTP** | Inbound | Master Only | `HttpServer` | Web Dashboard SPA, `/api/status`, `/api/proxy`, `/api/benchmark`. |
| **9000** | **WS** | Inbound | Master Only | `WebSocketManager` | Real-time telemetry broadcast stream and benchmark echo over `/ws`. |
| **8080** | **HTTP** | Inbound | Worker Nodes | `HttpServer` | Local `/api/status`, `/api/command`, `/api/benchmark`. |
| **8080** | **WS** | Inbound | Worker Nodes | `WebSocketManager` | Worker telemetry stream and local duplex benchmark echo over `/ws`. |

---

## 3. UDP Discovery Beacon Protocol (Port 9876)

Nodes periodically emit UDP beacons (default `1.0s` interval) via subnet broadcast (`255.255.255.255:9876`), directed unicast to Master (`master_ip:9876`), and unicast back to all active peers.

### 3.1 Datagram Payload Schema

```json
{
  "node_id": "node-1",
  "role": "worker",
  "seq": 42,
  "timestamp": 1710000000.123,
  "start_time": 1709999000.0,
  "http_port": 8080,
  "tcp_port": 9877,
  "cpu_load": 0.25
}
```

### 3.2 Field Definitions & Validation Rules

| Field | Type | Required | Constraints / Validation | Description |
| :--- | :--- | :--- | :--- | :--- |
| `node_id` | `string` | Yes | 1 to 128 chars, non-whitespace | Unique identifier of emitting node. |
| `role` | `string` | Yes | Must be `"master"` or `"worker"` | Operational cluster role. |
| `seq` | `integer` | Yes | $\ge 0$ (monotonic) | Sequence counter for sequence gap packet loss estimation. |
| `timestamp` | `float` | Yes | Finite positive float ($\ge 0.0$) | Epoch timestamp at moment of transmission. |
| `start_time` | `float` | Yes | Finite positive float ($\ge 0.0$) | Process start epoch timestamp for uptime calculation. |
| `http_port` | `integer` | Yes | 1 to 65535 | HTTP REST and WebSocket listening port. |
| `tcp_port` | `integer` | Yes | 1 to 65535 | Line-delimited TCP command listening port. |
| `cpu_load` | `float` | Yes | Finite float ($\ge 0.0$) | Normalized 1-minute system load average. |

---

## 4. TCP Command Protocol (Port 9877)

A lightweight stream-oriented command protocol designed for low-overhead operational execution over long-range or lossy wireless links.

### 4.1 Wire Framing
- **Framing:** Strict single newline character (`\n`) delimiter.
- **Encoding:** UTF-8.
- **Lifecycle:** The client establishes a connection, transmits a single line-delimited JSON frame, awaits the single-line JSON response, and the connection is closed.

### 4.2 Request Frame Schema

```json
{"command": "PING", "target_id": "node-1", "args": {}}
```

| Field | Type | Default | Constraints | Description |
| :--- | :--- | :--- | :--- | :--- |
| `command` | `string` | (Required) | Must be `"PING"` (case-insensitive) | Operational action verb. |
| `target_id` | `string` | `"all"` | String or `"all"` | Target node identifier. If not `"all"` or local node ID, command is ignored. |
| `args` | `object` | `{}` | JSON Object | Optional dictionary for command parameters. |

### 4.3 Response Frame Schemas

#### A. Successful Execution (ACK)
```json
{
  "status": "ACK",
  "response": "PONG",
  "node_id": "node-1",
  "timestamp": 1710000000.150
}
```

#### B. Target Mismatch (IGNORED)
```json
{
  "status": "IGNORED",
  "message": "Command addressed to node-2, this node is node-1",
  "node_id": "node-1"
}
```

#### C. Validation or Parse Error (ERROR)
```json
{
  "status": "ERROR",
  "error": "Field 'command' must be a non-empty string",
  "field": "command",
  "node_id": "node-1"
}
```

---

## 5. HTTP REST API (Port 9000 / 8080)

All HTTP endpoints support cross-origin requests via permissive CORS headers (`Access-Control-Allow-Origin: *`). Requests that fail validation return HTTP `400 Bad Request` with an explanatory error payload.

### 5.1 `GET /api/status`

Returns node identity, operational role, uptime, and the full dictionary of all actively tracked peers with their computed link quality metrics.

#### Response Headers:
- `Content-Type: application/json; charset=utf-8`

#### Response Payload Schema (HTTP 200 OK):
```json
{
  "node_id": "c2-master",
  "role": "master",
  "uptime_sec": 142.5,
  "ip": "192.168.1.10",
  "http_port": 9000,
  "tcp_port": 9877,
  "peers": {
    "node-1": {
      "node_id": "node-1",
      "role": "worker",
      "ip": "192.168.1.50",
      "http_port": 8080,
      "tcp_port": 9877,
      "last_seen": 1710000010.5,
      "latency_ms": 3.4,
      "jitter_ms": 0.8,
      "packets_received": 142,
      "packet_loss_pct": 0.0,
      "uptime_sec": 142.0,
      "cpu_load": 0.15,
      "last_seq": 142,
      "seq_history": [[1, 0], [1, 0]],
      "latency_history": [3.2, 3.6, 3.4]
    }
  }
}
```

---

### 5.2 `POST /api/command`

Executes a validated C2 operational command locally on the receiving node.

#### Request Headers:
- `Content-Type: application/json`

#### Request Body Schema:
```json
{
  "command": "PING",
  "target_id": "node-1",
  "args": {}
}
```

#### Response (HTTP 200 OK):
```json
{
  "status": "ACK",
  "response": "PONG",
  "node_id": "node-1",
  "timestamp": 1710000015.654
}
```

#### Error Response (HTTP 400 Bad Request):
```json
{
  "status": "ERROR",
  "error": "Unknown command 'REBOOT'. Valid commands: ['PING']",
  "field": "command",
  "node_id": "node-1"
}
```

---

### 5.3 `POST /api/proxy/{node_id}/command` *(Master Node Only)*

Routes a command from an operator client (or web browser dashboard) to a remote worker node through the Master node. Eliminates browser CORS issues, private IP routing hurdles, and NAT barriers.

#### Path Parameters:
- `node_id` (string, required): Identifier of destination node (e.g., `node-1`).

#### Proxy Execution Logic:
1. If `node_id == local_node_id`, the command is executed locally.
2. The Master searches its local `PeerTracker` for the destination `node_id`.
3. If not found, returns HTTP `404 Not Found`.
4. If found, forwards the raw body to `http://{peer.ip}:{peer.http_port}/api/command` with a `3.0s` timeout.

#### Status Code Mappings:
| Status Code | Condition | Description |
| :--- | :--- | :--- |
| **200 OK** | Success | Forwarded JSON response from remote worker node. |
| **400 Bad Request** | Invalid Input | Missing path parameter or malformed JSON command frame. |
| **404 Not Found** | Peer Missing | Target node ID is not present in Master's active peer registry. |
| **502 Bad Gateway** | Connection Error | Target worker cannot be reached (socket refused, connection reset). |
| **504 Gateway Timeout** | Proxy Timeout | Worker failed to respond within `master_proxy_timeout_sec` (3.0s). |

#### Example Error Response (HTTP 404 Not Found):
```json
{
  "status": "ERROR",
  "error": "Target node [node-99] not found in peer registry",
  "node_id": "c2-master"
}
```

---

### 5.4 `POST /api/benchmark`

Executes a synchronous data transfer benchmark test over HTTP. On the receiving node, incoming payload content is rendered to stdout via an ANSI console banner, and transfer timing metadata is returned.

#### Request Headers:
- `Content-Type: application/json`

#### Request Body Schema:
```json
{
  "preset": "16KB",
  "client_timestamp": 1710000020.100,
  "data": "Synthetic benchmark payload content..."
}
```

#### Response Payload Schema (HTTP 200 OK):
```json
{
  "status": "BENCHMARK_COMPLETE",
  "node_id": "node-1",
  "role": "worker",
  "preset": "16KB",
  "bytes_received": 16420,
  "client_timestamp": 1710000020.100,
  "server_receive_ts": 1710000020.108,
  "server_proc_time_ms": 0.452,
  "client_latency_estimate_ms": 8.00
}
```

---

### 5.5 Static Web Routes *(Master Node Only)*

- `GET /`: Serves `src/web/index.html` (Master Web Dashboard).
- `GET /static/*`: Serves static assets (`styles.css`, `app.js`, `vendor/alpine.esm.js`).

---

## 6. WebSocket Protocol (`/ws`)

The WebSocket endpoint provides real-time streaming telemetry and bidirectional benchmarking without HTTP polling overhead.

### 6.1 Handshake
- **URL:** `ws://<HOST>:<PORT>/ws`
- Standard HTTP 101 Switching Protocols upgrade.

### 6.2 Server-to-Client Broadcast: Telemetry Update
Pushed periodically every `0.5s` (`ws_broadcast_interval_sec`) to all connected WebSocket clients.

```json
{
  "type": "telemetry_update",
  "timestamp": 1710000030.500,
  "node_id": "c2-master",
  "role": "master",
  "peers": {
    "node-1": {
      "node_id": "node-1",
      "role": "worker",
      "ip": "192.168.1.50",
      "http_port": 8080,
      "tcp_port": 9877,
      "last_seen": 1710000030.480,
      "latency_ms": 4.1,
      "jitter_ms": 0.6,
      "packets_received": 180,
      "packet_loss_pct": 0.0,
      "uptime_sec": 180.0,
      "cpu_load": 0.12,
      "last_seq": 180,
      "seq_history": [[1, 0]],
      "latency_history": [4.0, 4.2, 4.1]
    }
  }
}
```

### 6.3 Client-to-Server Benchmark Request: `ws_benchmark`

```json
{
  "action": "ws_benchmark",
  "target_id": "node-1",
  "preset": "64KB",
  "timestamp": 1710000035.120,
  "data": "Repeated synthetic buffer..."
}
```

### 6.4 Server-to-Client Benchmark Acknowledgment: `ws_benchmark_ack`

```json
{
  "type": "ws_benchmark_ack",
  "node_id": "c2-master",
  "bytes": 65580,
  "client_timestamp": 1710000035.120,
  "server_timestamp": 1710000035.132
}
```

### 6.5 Error Frame Schema
If an invalid JSON frame or unrecognized action is transmitted, the server responds with:

```json
{
  "type": "error",
  "message": "Unsupported WebSocket action 'unknown_verb'"
}
```

---

## 7. Direct CLI Testing Recipes

The following recipes demonstrate how to interact with and verify all C2 network interfaces directly from the terminal.

### 7.1 Line-Delimited TCP Command (Port 9877)

Send a direct TCP command to any node using `netcat` (`nc`):

```bash
# Direct PING command to worker node:
echo '{"command": "PING", "target_id": "node-1"}' | nc 192.168.1.50 9877

# Expected Output:
# {"status": "ACK", "response": "PONG", "node_id": "node-1", "timestamp": 1710000040.12}

# Send command addressed to all nodes:
echo '{"command": "PING", "target_id": "all"}' | nc 192.168.1.50 9877
```

### 7.2 HTTP REST API via `curl`

#### Query Node Status & Peer Registry:
```bash
curl -s http://192.168.1.10:9000/api/status | jq .
```

#### Direct Command Execution on Worker:
```bash
curl -X POST http://192.168.1.50:8080/api/command \
  -H "Content-Type: application/json" \
  -d '{"command": "PING", "target_id": "node-1"}' | jq .
```

#### Master Proxy Command Routing:
Dispatch a command to `node-1` through the Master's proxy on port 9000:
```bash
curl -X POST http://192.168.1.10:9000/api/proxy/node-1/command \
  -H "Content-Type: application/json" \
  -d '{"command": "PING", "target_id": "node-1"}' | jq .
```

#### HTTP Data Transfer Benchmark:
```bash
curl -X POST http://192.168.1.50:8080/api/benchmark \
  -H "Content-Type: application/json" \
  -d '{"preset": "Custom", "client_timestamp": 1710000000.0, "data": "Sample benchmark payload content"}' | jq .
```

### 7.3 Real-Time WebSocket Inspection

#### Using `websocat` (CLI WebSocket tool):
```bash
# Listen to live telemetry updates from master:
websocat ws://192.168.1.10:9000/ws

# Send a benchmark action frame over WebSocket:
echo '{"action":"ws_benchmark","preset":"1KB","timestamp":1710000000.0,"data":"Benchmark test"}' | websocat ws://192.168.1.10:9000/ws
```

#### Using a Single-Line Python WebSocket Client:
```bash
python3 -c '
import asyncio, aiohttp

async def listen():
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect("http://127.0.0.1:9000/ws") as ws:
            msg = await ws.receive_json()
            print("Received frame:", msg["type"], "Peers:", list(msg.get("peers", {}).keys()))

asyncio.run(listen())
'
```
