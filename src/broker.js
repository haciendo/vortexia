import Aedes from 'aedes';
import { createServer } from 'aedes-server-factory';

const REGISTRY_URL = process.env.LAS_REGISTRY_URL || 'http://localhost:8700';

/**
 * Ask the local-agent-society port registry to claim a port.
 * Degrades gracefully (returns null) if the registry is unreachable —
 * vortexia should never hard-fail just because the backend isn't running.
 */
export async function claimPort(app, { start, end } = {}) {
  try {
    const body = {
      app,
      local_agent: 'vortexia',
      path: process.cwd(),
    };
    if (start !== undefined) body.start = start;
    if (end !== undefined) body.end = end;

    const res = await fetch(`${REGISTRY_URL}/ports/claim`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      console.warn(`[vortexia] port registry refused claim for ${app}: HTTP ${res.status}`);
      return null;
    }
    const data = await res.json();
    return data.port;
  } catch (err) {
    console.warn(`[vortexia] port registry unreachable (${err.message}) — proceeding without claiming a port for ${app}`);
    return null;
  }
}

/**
 * Release a previously claimed port. No-op (warns) if the registry is unreachable.
 *
 * The registry stores its state in a single JSON file with a naive
 * read-modify-write (no lock on delete), so a DELETE can occasionally lose
 * a race against a concurrent write from another agent on the same
 * machine. Retry a couple of times, confirming via GET, before giving up.
 */
export async function releasePort(port, { retries = 2, delayMs = 150 } = {}) {
  if (port == null) return;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await fetch(`${REGISTRY_URL}/ports/${port}`, { method: 'DELETE' });
      if (res.ok || res.status === 404) {
        return;
      }
      console.warn(`[vortexia] failed to release port ${port}: HTTP ${res.status}`);
    } catch (err) {
      console.warn(`[vortexia] port registry unreachable while releasing port ${port} (${err.message})`);
      return; // registry is down, not a transient race — no point retrying
    }
    if (attempt < retries) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
}

/**
 * Start the vortexia MQTT broker: a TCP listener (for paho-mqtt / las clients)
 * and a WebSocket listener (for an Electron renderer using mqtt.js).
 *
 * Ports are claimed from the local-agent-society port registry when possible.
 * If explicit ports are passed, those are claimed as pinned ports instead of
 * picked from the free range.
 */
export async function startBroker({ mqttPort: pinnedMqttPort, wsPort: pinnedWsPort } = {}) {
  const aedes = new Aedes();

  const claimOpts = { start: 9000, end: 9999 };

  const mqttPort = pinnedMqttPort ?? (await claimPort('vortexia-mqtt', claimOpts)) ?? 1883;
  const wsPort = pinnedWsPort ?? (await claimPort('vortexia-ws', claimOpts)) ?? 8883;

  const tcpServer = createServer(aedes, { ws: false });
  const wsServer = createServer(aedes, { ws: true });

  await new Promise((resolve, reject) => {
    tcpServer.once('error', reject);
    tcpServer.listen(mqttPort, () => {
      tcpServer.removeListener('error', reject);
      resolve();
    });
  });

  await new Promise((resolve, reject) => {
    wsServer.once('error', reject);
    wsServer.listen(wsPort, () => {
      wsServer.removeListener('error', reject);
      resolve();
    });
  });

  let closed = false;
  async function close() {
    if (closed) return;
    closed = true;
    await Promise.all([
      new Promise((resolve) => tcpServer.close(resolve)),
      new Promise((resolve) => wsServer.close(resolve)),
    ]);
    await new Promise((resolve) => aedes.close(resolve));
    await Promise.all([releasePort(mqttPort), releasePort(wsPort)]);
  }

  return { aedes, tcpServer, wsServer, mqttPort, wsPort, close };
}
