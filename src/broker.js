import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Aedes from 'aedes';
import { createServer } from 'aedes-server-factory';
import { logger } from './logger.js';
import { MailboxPersistence, DEFAULT_MAILBOX_CAP, DEFAULT_MAILBOX_TTL_MS } from './mailbox.js';
import { mailboxClientId, parseInboxTopic, sessionTopic } from './topics.js';
import {
  bindWithPolicy,
  readStickyPorts,
  writeStickyPorts,
  reconcileRegistry,
  releasePort,
  DEFAULT_RANGE,
} from './ports.js';

// Re-exported for callers/tests that used to import these from here.
export { releasePort } from './ports.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

export const MQTT_APP = 'vortexia-mqtt';
export const WS_APP = 'vortexia-ws';

function envInt(name, fallback) {
  const raw = process.env[name];
  if (raw == null || raw === '') return fallback;
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
}

/**
 * Where the broker keeps state that must outlive one process: the ports it
 * bound last time (data/ports.json) and the mailbox snapshot
 * (data/mailboxes.json). VORTEXIA_DATA_DIR overrides the default
 * <repo>/data — the test suite points it at a scratch directory so test
 * brokers never inherit the real broker's sticky ports or mailboxes.
 */
export function defaultDataDir() {
  return process.env.VORTEXIA_DATA_DIR || path.join(ROOT, 'data');
}

/**
 * Start the vortexia MQTT broker: a TCP listener (for paho-mqtt / las clients)
 * and a WebSocket listener (for an Electron renderer using mqtt.js).
 *
 * Ports: see ports.js for the policy (pin → last bound → registry claim →
 * local probe) and for how the registry is told what got bound without the
 * broker ever waiting on it. Explicit `mqttPort`/`wsPort` here (or
 * VORTEXIA_MQTT_PORT / VORTEXIA_WS_PORT) are pins: bound as given or the
 * start fails.
 *
 * Mailboxes: every `las/agent/<name>/inbox` publish is queued for the
 * persistent session `las-agent-<name>` (see mailbox.js). `mailbox` options
 * (or VORTEXIA_MAILBOX_CAP / VORTEXIA_MAILBOX_TTL_MS) tune the per-mailbox
 * cap and message TTL; `persist: false` keeps everything in memory.
 */
export async function startBroker({
  mqttPort: pinnedMqttPort,
  wsPort: pinnedWsPort,
  dataDir = defaultDataDir(),
  persist = true,
  mailbox = {},
} = {}) {
  const persistence = new MailboxPersistence({
    file: persist ? path.join(dataDir, 'mailboxes.json') : null,
    cap: mailbox.cap ?? envInt('VORTEXIA_MAILBOX_CAP', DEFAULT_MAILBOX_CAP),
    ttlMs: mailbox.ttlMs ?? envInt('VORTEXIA_MAILBOX_TTL_MS', DEFAULT_MAILBOX_TTL_MS),
  }).load();

  const aedes = new Aedes({
    persistence,
    // A mailbox comes into existence on the first publish to an inbox —
    // BEFORE aedes computes who to enqueue for, which is why this lives in
    // authorizePublish rather than in a 'publish' listener (that fires
    // after the enqueue decision, too late for the very first message).
    authorizePublish(client, packet, cb) {
      if (packet.topic.startsWith('$SYS')) return cb(new Error('$SYS topic is reserved'));
      const agentName = parseInboxTopic(packet.topic);
      if (!agentName) return cb(null);
      persistence.ensureMailbox(agentName, (err, created) => {
        // A brand-new mailbox gets its retained session state right away,
        // so a poll's "is someone holding this?" check answers instantly
        // instead of waiting out its timeout on an absent topic.
        if (!err && created) publishSession({ id: mailboxClientId(agentName) }, Boolean(aedes.clients[mailboxClientId(agentName)]));
        cb(err);
      });
    },
  });

  // Broker-published, retained consumer state per mailbox — the signal a
  // one-shot poll needs to NOT take the session over from a live listener
  // (which would kick it; with a client that auto-reconnects, the two
  // would then keep kicking each other). Absent/"disconnected" means the
  // mailbox is free to drain.
  const publishSession = (client, connected) => {
    const name = mailboxAgentName(client.id);
    if (!name) return;
    aedes.publish({
      topic: sessionTopic(name),
      payload: Buffer.from(JSON.stringify({ connected, clientId: client.id, ts: Date.now() })),
      qos: 0,
      retain: true,
    }, () => {});
  };

  // Diagnostics: these are the events that matter when a client (a `las`
  // CLI call, the widget, another agent's session) reports "connection
  // refused" or a dropped message — without this, an incident like that
  // leaves no trace once it's over. Keep it to connection lifecycle and
  // errors, not per-publish traffic, to avoid drowning the log.
  aedes.on('client', (client) => {
    logger.info(`[vortexia] client connected: ${client.id}${client.clean === false ? ' (persistent session)' : ''}`);
    if (client.clean === false) publishSession(client, true);
  });
  aedes.on('clientDisconnect', (client) => {
    logger.info(`[vortexia] client disconnected: ${client.id}`);
    if (client.clean === false) publishSession(client, false);
  });
  aedes.on('clientError', (client, err) => {
    logger.warn(`[vortexia] client error (${client?.id ?? 'unknown'}): ${err.message}`);
  });
  aedes.on('connectionError', (client, err) => {
    logger.warn(`[vortexia] connection error (${client?.id ?? 'unknown'}): ${err.message}`);
  });

  // Restored mailboxes start out with nobody connected — say so on their
  // retained session topics (retained state isn't snapshotted; see mailbox.js).
  for (const id of persistence.sessions) publishSession({ id }, false);

  const tcpServer = createServer(aedes, { ws: false });
  const wsServer = createServer(aedes, { ws: true });
  tcpServer.on('error', (err) => logger.error(`[vortexia] TCP server error: ${err.message}`));
  wsServer.on('error', (err) => logger.error(`[vortexia] WS server error: ${err.message}`));

  const stickyFile = path.join(dataDir, 'ports.json');
  let sticky = persist ? readStickyPorts(stickyFile) : {};
  // One-time migration from versions before data/ports.json existed: the
  // previous run's vortexia.port.json (left behind by an unclean stop, or
  // still present from the instance being replaced) is the best memory of
  // which ports this machine's clients already know.
  if (persist && sticky[MQTT_APP] == null) {
    try {
      const legacy = JSON.parse(fs.readFileSync(path.join(ROOT, 'vortexia.port.json'), 'utf8'));
      if (legacy?.mqttPort && legacy?.wsPort) sticky = { [MQTT_APP]: legacy.mqttPort, [WS_APP]: legacy.wsPort };
    } catch {
      // none — first run
    }
  }

  let mqtt;
  let ws;
  try {
    mqtt = await bindWithPolicy(tcpServer, MQTT_APP, {
      pinned: pinnedMqttPort ?? envInt('VORTEXIA_MQTT_PORT', null),
      sticky: sticky[MQTT_APP] ?? null,
      range: DEFAULT_RANGE,
    });
    ws = await bindWithPolicy(wsServer, WS_APP, {
      pinned: pinnedWsPort ?? envInt('VORTEXIA_WS_PORT', null),
      sticky: sticky[WS_APP] ?? null,
      range: DEFAULT_RANGE,
      exclude: [mqtt.port],
    });
  } catch (err) {
    // Don't leak a half-started broker (aedes keeps a heartbeat timer
    // alive until closed; a listener that did bind must be released).
    await new Promise((resolve) => aedes.close(resolve));
    for (const server of [tcpServer, wsServer]) {
      if (server.listening) await new Promise((resolve) => server.close(resolve));
    }
    throw err;
  }
  const mqttPort = mqtt.port;
  const wsPort = ws.port;

  if (persist) writeStickyPorts(stickyFile, { [MQTT_APP]: mqttPort, [WS_APP]: wsPort });

  // Whatever the registry didn't hand us itself, it gets told about in the
  // background — this never delays the broker being usable.
  const reconcilers = [];
  if (!mqtt.claimed) reconcilers.push(reconcileRegistry(MQTT_APP, mqttPort));
  if (!ws.claimed) reconcilers.push(reconcileRegistry(WS_APP, wsPort));

  let closed = false;
  async function close() {
    if (closed) return;
    closed = true;
    for (const r of reconcilers) r.stop();
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
    persistence.flush();
    // Best-effort, time-bounded (see ports.js): a registry that's down at
    // shutdown costs one warning line, never a hang. Sequential, not
    // parallel: the registry's file store is a naive read-modify-write, so
    // two concurrent DELETEs from us can make one of them vanish — seen
    // live, it left a stale 9007 entry behind on an otherwise clean stop.
    await releasePort(mqttPort);
    await releasePort(wsPort);
  }

  return { aedes, persistence, tcpServer, wsServer, mqttPort, wsPort, close, registryReconcile: Promise.all(reconcilers.map((r) => r.done)) };
}

function mailboxAgentName(clientId) {
  const prefix = mailboxClientId('');
  return typeof clientId === 'string' && clientId.startsWith(prefix) && clientId.length > prefix.length
    ? clientId.slice(prefix.length)
    : null;
}
