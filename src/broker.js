import Aedes from 'aedes';
import { createServer } from 'aedes-server-factory';
import { logger } from './logger.js';

// Read lazily (not a frozen module-level const) so a caller — notably a
// test file — can set process.env.LAS_REGISTRY_URL before startBroker()
// actually runs and have it take effect, regardless of import order (ESM
// imports resolve before the importing file's own top-level code runs, so
// a frozen constant here would already be baked in from the real default
// by the time a test file's own env-setting statement executes).
function registryUrl() {
  return process.env.LAS_REGISTRY_URL || 'http://localhost:8700';
}

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

    const res = await fetch(`${registryUrl()}/ports/claim`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      logger.warn(`[vortexia] port registry refused claim for ${app}: HTTP ${res.status}`);
      return null;
    }
    const data = await res.json();
    return data.port;
  } catch (err) {
    logger.warn(`[vortexia] port registry unreachable (${err.message}) — proceeding without claiming a port for ${app}`);
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
      const res = await fetch(`${registryUrl()}/ports/${port}`, { method: 'DELETE' });
      if (res.ok || res.status === 404) {
        return;
      }
      logger.warn(`[vortexia] failed to release port ${port}: HTTP ${res.status}`);
    } catch (err) {
      logger.warn(`[vortexia] port registry unreachable while releasing port ${port} (${err.message})`);
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

  // Diagnostics: these are the events that matter when a client (a `las`
  // CLI call, the widget, another agent's session) reports "connection
  // refused" or a dropped message — without this, an incident like that
  // leaves no trace once it's over. Keep it to connection lifecycle and
  // errors, not per-publish traffic, to avoid drowning the log.
  aedes.on('client', (client) => {
    logger.info(`[vortexia] client connected: ${client.id}`);
  });
  aedes.on('clientDisconnect', (client) => {
    logger.info(`[vortexia] client disconnected: ${client.id}`);
  });
  aedes.on('clientError', (client, err) => {
    logger.warn(`[vortexia] client error (${client?.id ?? 'unknown'}): ${err.message}`);
  });
  aedes.on('connectionError', (client, err) => {
    logger.warn(`[vortexia] connection error (${client?.id ?? 'unknown'}): ${err.message}`);
  });
  tcpServer.on('error', (err) => {
    logger.error(`[vortexia] TCP server error: ${err.message}`);
  });
  wsServer.on('error', (err) => {
    logger.error(`[vortexia] WS server error: ${err.message}`);
  });

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
    // aedes.close() first: it walks connected clients and closes each of
    // their sockets. tcpServer.close()/wsServer.close() only stop accepting
    // new connections and don't resolve until every existing connection has
    // ended — calling them before aedes.close() deadlocked shutdown forever
    // whenever a client (any connected agent) was still attached.
    await new Promise((resolve) => aedes.close(resolve));
    await Promise.all([
      new Promise((resolve) => tcpServer.close(resolve)),
      new Promise((resolve) => wsServer.close(resolve)),
    ]);
    await Promise.all([releasePort(mqttPort), releasePort(wsPort)]);
  }

  return { aedes, tcpServer, wsServer, mqttPort, wsPort, close };
}
