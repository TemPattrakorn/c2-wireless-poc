/**
 * @file app.js — C2 Wireless Command & Control Dashboard Client
 * 
 * Implements the Alpine.js reactive component managing:
 * - Real-time WebSocket telemetry ingestion and automatic reconnection
 * - Live peer node registry rendering (metrics, latency, jitter, packet loss)
 * - Cluster-wide and targeted command dispatch (PING)
 * - Interactive data transfer throughput lab (HTTP POST / WebSocket)
 * - Real-time console activity log feed
 */

import Alpine from 'alpinejs';

export function c2Dashboard() {
  return {
    // State
    wsStatus: 'Connecting...',
    peers: {},
    logs: [],
    logCounter: 0,
    selectedTarget: '',
    selectedProtocol: 'HTTP',
    selectedSizeLabel: '1KB',
    selectedSizeBytes: 1024,
    payloadText: '',
    ws: null,
    statusInterval: null,
    reconnectTimer: null,
    bench: {
      status: 'Ready',
      statusColor: 'inherit',
      rtt: '-',
      throughput: '-',
      bytes: '-',
      preview: 'No transfer executed yet.'
    },

    get peerList() {
      return Object.keys(this.peers).map(id => ({
        id,
        ...this.peers[id]
      }));
    },

    get byteCountText() {
      const byteLen = new Blob([this.payloadText || '']).size;
      return `${byteLen.toLocaleString()} bytes (${(byteLen / 1024).toFixed(1)} KB) [${this.selectedSizeLabel}]`;
    },

    init() {
      this.payloadText = this.generateSyntheticData(this.selectedSizeBytes);
      this.addLog('System initialized. Connecting to C2 Master WebSocket...');
      this.connectWebSocket();
      this.fetchNodeStatus(true);
      if (this.statusInterval) clearInterval(this.statusInterval);
      this.statusInterval = setInterval(() => {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
          this.fetchNodeStatus(false);
        }
      }, 2000);
    },

    addLog(msg) {
      const timeStr = new Date().toLocaleTimeString();
      this.logs.push({
        id: ++this.logCounter,
        time: timeStr,
        msg: msg
      });
      this.$nextTick(() => {
        if (this.$refs.logFeed) {
          this.$refs.logFeed.scrollTop = this.$refs.logFeed.scrollHeight;
        }
      });
    },

    selectProtocol(proto) {
      this.selectedProtocol = proto;
      this.addLog(`Selected transfer protocol: ${proto}`);
    },

    selectPayloadSize(label, bytes) {
      this.selectedSizeLabel = label;
      this.selectedSizeBytes = bytes;
      this.payloadText = this.generateSyntheticData(bytes);
    },

    connectWebSocket() {
      if (this.reconnectTimer) {
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = null;
      }
      const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const wsUrl = `${proto}//${window.location.host}/ws`;
      this.ws = new WebSocket(wsUrl);

      this.ws.onopen = () => {
        this.wsStatus = 'Online (Live Stream)';
        this.addLog('Connected to C2 Telemetry WebSocket stream.');
      };

      this.ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          if (data.type === 'telemetry_update' && data.peers) {
            this.peers = data.peers;
            if (this.selectedTarget && !this.peers[this.selectedTarget]) {
              this.selectedTarget = '';
            }
          }
        } catch (e) {
          console.error('WS parse error:', e);
        }
      };

      this.ws.onclose = () => {
        this.wsStatus = 'Offline (Reconnecting...)';
        if (!this.reconnectTimer) {
          this.reconnectTimer = setTimeout(() => this.connectWebSocket(), 2500);
        }
      };
    },

    async fetchNodeStatus(force = false) {
      if (!force && this.ws && this.ws.readyState === WebSocket.OPEN) {
        return;
      }
      try {
        const resp = await fetch('/api/status');
        if (resp.ok) {
          const data = await resp.json();
          if (data.peers) {
            this.peers = data.peers;
          }
        }
      } catch (err) {
        // Server might be reloading
      }
    },

    async sendNodeCommand(nodeId, cmd) {
      const node = this.peers[nodeId];
      if (!node) return;
      this.addLog(`Dispatching command [${cmd}] to ${nodeId} via Master proxy...`);

      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 4000);

      try {
        const url = `/api/proxy/${encodeURIComponent(nodeId)}/command`;
        const resp = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ command: cmd, target_id: nodeId }),
          signal: controller.signal
        });
        clearTimeout(timer);
        const resJson = await resp.json();
        this.addLog(`[${nodeId}] Response: ${JSON.stringify(resJson)}`);
        this.fetchNodeStatus(true);
      } catch (err) {
        clearTimeout(timer);
        const errorMsg = err.name === 'AbortError' ? 'Request timed out (>4s)' : err;
        this.addLog(`Error executing command on ${nodeId}: ${errorMsg}`);
      }
    },

    async broadcastCommand(cmd) {
      this.addLog(`Broadcasting global command: [${cmd}]`);
      const nodeIds = Object.keys(this.peers);
      if (nodeIds.length === 0) {
        this.addLog('No active nodes discovered to broadcast to.');
        return;
      }
      await Promise.allSettled(nodeIds.map(id => this.sendNodeCommand(id, cmd)));
    },

    generateSyntheticData(targetBytes) {
      const nowTs = Date.now();
      const baseObj = {
        benchmark_id: `bench-${nowTs.toString(36)}`,
        preset: this.selectedSizeLabel,
        timestamp: nowTs / 1000.0,
        data: ""
      };
      const baseJson = JSON.stringify(baseObj, null, 2);
      const diff = Math.max(0, targetBytes - baseJson.length);
      baseObj.data = "X".repeat(diff);
      return JSON.stringify(baseObj, null, 2);
    },

    async runDataTransferBenchmark() {
      if (!this.selectedTarget) {
        this.bench.status = 'Select Target';
        this.bench.statusColor = 'var(--warning)';
        this.addLog('Benchmark transfer aborted: Please select a target node from the dropdown.');
        return;
      }
      const node = this.peers[this.selectedTarget];
      if (!node) return;

      const payloadText = this.payloadText || this.generateSyntheticData(this.selectedSizeBytes);

      this.bench.status = 'Transferring...';
      this.bench.statusColor = 'var(--primary)';

      if (this.selectedProtocol === 'HTTP') {
        const startTs = performance.now();
        const clientSendTs = Date.now() / 1000.0;
        const payload = {
          preset: this.selectedSizeLabel,
          client_timestamp: clientSendTs,
          data: payloadText
        };

        try {
          const url = `http://${node.ip}:${node.http_port}/api/benchmark`;
          const resp = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
          });
          const durationMs = performance.now() - startTs;
          const resJson = await resp.json();

          const totalBytes = new Blob([JSON.stringify(payload)]).size;
          const throughputKbps = (totalBytes / 1024) / (durationMs / 1000);

          this.bench.status = 'HTTP Success';
          this.bench.statusColor = 'var(--success)';
          this.bench.rtt = `${durationMs.toFixed(1)} ms`;
          this.bench.throughput = `${throughputKbps.toFixed(1)} KB/s (${(throughputKbps/1024).toFixed(2)} MB/s)`;
          this.bench.bytes = `${(totalBytes/1024).toFixed(1)} KB`;
          this.bench.preview = JSON.stringify(resJson, null, 2);

          this.addLog(`HTTP Benchmark to ${this.selectedTarget}: ${totalBytes} bytes in ${durationMs.toFixed(1)}ms (${throughputKbps.toFixed(1)} KB/s)`);
        } catch (err) {
          this.bench.status = 'HTTP Failed';
          this.bench.statusColor = 'var(--danger)';
          this.addLog(`HTTP Benchmark error: ${err}`);
        }
      } else {
        const wsProto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const targetWsUrl = `${wsProto}//${node.ip}:${node.http_port}/ws`;
        this.addLog(`Connecting direct WebSocket to ${this.selectedTarget} (${targetWsUrl})...`);

        let benchWs;
        try {
          benchWs = new WebSocket(targetWsUrl);
        } catch (e) {
          this.bench.status = 'WS Error';
          this.bench.statusColor = 'var(--danger)';
          this.addLog(`Failed to connect WebSocket to ${this.selectedTarget}: ${e}`);
          return;
        }

        const wsTimeout = setTimeout(() => {
          if (benchWs.readyState !== WebSocket.CLOSED) {
            benchWs.close();
            this.bench.status = 'WS Timeout';
            this.bench.statusColor = 'var(--danger)';
            this.addLog(`WebSocket benchmark to ${this.selectedTarget} timed out (> 10s).`);
          }
        }, 10000);

        benchWs.onopen = () => {
          const wsStartTime = performance.now();
          const payload = {
            action: 'ws_benchmark',
            target_id: this.selectedTarget,
            preset: this.selectedSizeLabel,
            timestamp: Date.now() / 1000.0,
            data: payloadText
          };
          const rawStr = JSON.stringify(payload);
          const rawBytes = new Blob([rawStr]).size;

          benchWs.onmessage = (event) => {
            try {
              const data = JSON.parse(event.data);
              if (data.type === 'ws_benchmark_ack') {
                clearTimeout(wsTimeout);
                const durationMs = performance.now() - wsStartTime;
                const throughputKbps = (rawBytes / 1024) / (durationMs / 1000);

                this.bench.status = 'WebSocket Success';
                this.bench.statusColor = 'var(--success)';
                this.bench.rtt = `${durationMs.toFixed(1)} ms`;
                this.bench.throughput = `${throughputKbps.toFixed(1)} KB/s (${(throughputKbps/1024).toFixed(2)} MB/s)`;
                this.bench.bytes = `${(rawBytes/1024).toFixed(1)} KB`;
                this.bench.preview = JSON.stringify(data, null, 2);

                this.addLog(`WebSocket Benchmark to ${this.selectedTarget}: ${rawBytes} bytes in ${durationMs.toFixed(1)}ms (${throughputKbps.toFixed(1)} KB/s)`);
                benchWs.close();
              }
            } catch (e) {
              console.error('Benchmark WS parse error:', e);
            }
          };

          benchWs.send(rawStr);
        };

        benchWs.onerror = (err) => {
          clearTimeout(wsTimeout);
          this.bench.status = 'WebSocket Failed';
          this.bench.statusColor = 'var(--danger)';
          this.addLog(`WebSocket Benchmark error on ${this.selectedTarget}`);
        };
      }
    }
  };
}

// Register component and start Alpine reactivity engine
Alpine.data('c2Dashboard', c2Dashboard);
window.Alpine = Alpine;
Alpine.start();
