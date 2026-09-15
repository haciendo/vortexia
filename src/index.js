#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { startBroker } from './broker.js';
import { scanScopes } from './scope.js';
import { logger } from './logger.js';
import { VortexiaClient } from './client.js';
import { VortexRelayBridge } from './vortex-relay/bridge.js';
import { GistRelay, NostrRelay, MultiRelay } from './vortex-relay/relay.js';
import { discoverLocalRoster } from './vortex-relay/localRoster.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const PID_FILE = path.join(ROOT, 'vortexia.pid');
const PORT_FILE = path.join(ROOT, 'vortexia.port.json');

function readPidFile() {
  try {
    const raw = fs.readFileSync(PID_FILE, 'utf8').trim();
    return raw ? parseInt(raw, 10) : null;
  } catch {
    return null;
  }
}

function isRunning(pid) {
  if (!pid) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function readPortFile() {
  try {
    return JSON.parse(fs.readFileSync(PORT_FILE, 'utf8'));
  } catch {
    return null;
  }
}

/**
 * vortex-relay is opt-in: only starts if VORTEXIA_ENV_NAME and at least one
 * transport is configured (VORTEXIA_GIST_ID and/or
 * VORTEXIA_NOSTR_SECRET_KEY). An instance with neither (the common case —
 * a single-Mac society) runs exactly as before. See docs/vortex-relay-poc.md
 * for how to provision a Gist/token or a Nostr key, and
 * VORTEXIA_RELAY_ENV_NAMES (comma-separated) to list every
 * environment expected to publish to the shared directory — this
 * environment's own name is added automatically if omitted.
 *
 * When more than one transport is configured, they're combined via
 * MultiRelay (see relay.js): reads race all of them, writes fan out to
 * all of them, and one transport being down/rate-limited doesn't block
 * the other. VORTEXIA_NOSTR_SECRET_KEY is hex-encoded (see nostr-tools
 * generateSecretKey/bytesToHex) — it's this environment's own signing
 * identity, not a shared secret like the Gist token; losing it only lets
 * someone impersonate THIS environment's writes.
 */
async function startVortexRelay(mqttPort) {
  const envName = process.env.VORTEXIA_ENV_NAME;
  const gistId = process.env.VORTEXIA_GIST_ID;
  const nostrSecretHex = process.env.VORTEXIA_NOSTR_SECRET_KEY;
  if (!envName || (!gistId && !nostrSecretHex)) return null;

  const envNames = (process.env.VORTEXIA_RELAY_ENV_NAMES || envName)
    .split(',').map((s) => s.trim()).filter(Boolean);
  if (!envNames.includes(envName)) envNames.push(envName);

  const transports = [];
  if (gistId) transports.push(new GistRelay({ gistId, token: process.env.VORTEXIA_GIST_TOKEN }));
  let nostrRelay = null;
  if (nostrSecretHex) {
    // VORTEXIA_NOSTR_PEERS: "envName:pubkeyHex,envName2:pubkeyHex2" —
    // each peer environment's OWN pubkey (public by design, safe to share
    // in the open), exchanged out of band (e.g. `las agent inject`) since
    // there's no way to discover an unknown environment's identity from
    // the relay itself without this — see NostrRelay's doc comment.
    const knownPeerPubkeys = Object.fromEntries(
      (process.env.VORTEXIA_NOSTR_PEERS || '')
        .split(',').map((s) => s.trim()).filter(Boolean)
        .map((pair) => pair.split(':').map((s) => s.trim())),
    );
    nostrRelay = new NostrRelay({ secretKey: Buffer.from(nostrSecretHex, 'hex'), knownPeerPubkeys });
    transports.push(nostrRelay);
  }
  const relay = transports.length > 1 ? new MultiRelay(transports) : transports[0];
  if (nostrRelay) {
    logger.info(`[vortexia] Nostr identity pubkey (share this for peers to trust ${envName}): ${await nostrRelay.publicKeyHex()}`);
  }
  const gateway = new VortexiaClient({ port: mqttPort });
  await gateway.register(`${envName}-gateway`);

  const bridge = new VortexRelayBridge({ envName, relay, envNames }).attach(gateway);
  const roster = await discoverLocalRoster();
  // See bridge.js's startDirectorySync doc comment: publish (a Gist write)
  // hits GitHub's 100/hour gist_update secondary limit shared across every
  // environment using the same token, so it stays well below that; sync
  // (a read) doesn't share that budget and can run far more often.
  bridge.startDirectorySync(roster, { publishIntervalMs: 300000, syncIntervalMs: 15000 });
  bridge.startPolling(1000);

  logger.info(`[vortexia] vortex-relay enabled: env=${envName}, envNames=[${envNames.join(', ')}], local roster=${roster.length} agent(s)`);
  return { bridge, gateway };
}

async function cmdStart() {
  const existingPid = readPidFile();
  if (isRunning(existingPid)) {
    console.log(`vortexia is already running (pid ${existingPid}).`);
    return;
  }

  const { mqttPort, wsPort, close } = await startBroker();

  fs.writeFileSync(PID_FILE, String(process.pid));
  fs.writeFileSync(PORT_FILE, JSON.stringify({ mqttPort, wsPort, pid: process.pid }, null, 2));

  logger.info(`vortexia broker started (pid ${process.pid})`);
  logger.info(`  MQTT (TCP):     localhost:${mqttPort}`);
  logger.info(`  MQTT (WebSocket): localhost:${wsPort}`);

  const vortexRelay = await startVortexRelay(mqttPort).catch((err) => {
    logger.error(`[vortexia] vortex-relay failed to start: ${err.message}`);
    return null;
  });

  const shutdown = async (signal) => {
    logger.info(`vortexia: received ${signal}, shutting down...`);
    try {
      if (vortexRelay) {
        vortexRelay.bridge.stopPolling();
        vortexRelay.bridge.stopDirectorySync();
        await vortexRelay.gateway.close();
      }
      await close();
    } finally {
      try { fs.unlinkSync(PID_FILE); } catch {}
      try { fs.unlinkSync(PORT_FILE); } catch {}
      process.exit(0);
    }
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  // A restart-on-crash launchd/systemd unit only helps if the process
  // actually exits on a fatal error instead of hanging in a broken state —
  // log the cause before going down so the crash shows up in vortexia.log,
  // not just as a silent gap in uptime.
  process.on('uncaughtException', (err) => {
    logger.error(`vortexia: uncaught exception, exiting: ${err.stack || err.message}`);
    process.exit(1);
  });
  process.on('unhandledRejection', (reason) => {
    const message = reason instanceof Error ? (reason.stack || reason.message) : String(reason);
    logger.error(`vortexia: unhandled rejection, exiting: ${message}`);
    process.exit(1);
  });
}

function cmdStop() {
  const pid = readPidFile();
  if (!isRunning(pid)) {
    console.log('vortexia is not running.');
    try { fs.unlinkSync(PID_FILE); } catch {}
    try { fs.unlinkSync(PORT_FILE); } catch {}
    return;
  }
  process.kill(pid, 'SIGTERM');
  console.log(`vortexia: sent SIGTERM to pid ${pid}.`);
}

function cmdStatus() {
  const pid = readPidFile();
  const running = isRunning(pid);
  const ports = readPortFile();
  if (running) {
    console.log(`vortexia is running (pid ${pid}).`);
    if (ports) {
      console.log(`  MQTT (TCP):       localhost:${ports.mqttPort}`);
      console.log(`  MQTT (WebSocket): localhost:${ports.wsPort}`);
    }
  } else {
    console.log('vortexia is not running.');
  }
}

function cmdScopeScan(dir) {
  if (!dir) {
    console.log('Usage: vortexia scope scan <dir>');
    process.exit(1);
  }
  let result;
  try {
    result = scanScopes(path.resolve(dir));
  } catch (err) {
    console.error(`vortexia: could not scan '${dir}': ${err.message}`);
    process.exit(1);
  }
  const { rungs, warnings } = result;
  for (const w of warnings) console.warn(`warning: ${w}`);
  if (rungs.length === 0) {
    console.log('No .vxia-scope.*.md files or README found.');
    return;
  }
  for (const r of rungs) {
    console.log(`\n=== rung ${r.rung} (${r.source}) ===`);
    console.log(r.text);
  }
}

const cmd = process.argv[2];

switch (cmd) {
  case 'start':
    cmdStart();
    break;
  case 'stop':
    cmdStop();
    break;
  case 'status':
    cmdStatus();
    break;
  case 'scope':
    if (process.argv[3] === 'scan') {
      cmdScopeScan(process.argv[4]);
    } else {
      console.log('Usage: vortexia scope scan <dir>');
      process.exit(1);
    }
    break;
  default:
    console.log('Usage: vortexia <start|stop|status|scope scan <dir>>');
    process.exit(cmd ? 1 : 0);
}
