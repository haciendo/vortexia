#!/usr/bin/env node
// Live, real-internet run of the federation PoC. Two or more of these,
// run on the SAME machine or on different ones, join through nothing but
// a shared GitHub Gist — see docs/federation-poc.md for setup.
//
// Usage:
//   VORTEXIA_GIST_ID=... VORTEXIA_GIST_TOKEN=... \
//     node scripts/federation-demo.js <envName> <agentName> "<scopeText>"

import readline from 'node:readline';
import { startBroker } from '../src/broker.js';
import { VortexiaClient } from '../src/client.js';
import { FederationBridge, FEDERATION_KIND } from '../src/federation/bridge.js';
import { GistRelay } from '../src/federation/relay.js';

const [envName, agentName, scopeText] = process.argv.slice(2);
if (!envName || !agentName || !scopeText) {
  console.error('Usage: node scripts/federation-demo.js <envName> <agentName> "<scopeText>"');
  console.error('Requires env vars VORTEXIA_GIST_ID and VORTEXIA_GIST_TOKEN — see docs/federation-poc.md');
  process.exit(1);
}

const gistId = process.env.VORTEXIA_GIST_ID;
const token = process.env.VORTEXIA_GIST_TOKEN;
if (!gistId || !token) {
  console.error('Set VORTEXIA_GIST_ID and VORTEXIA_GIST_TOKEN — see docs/federation-poc.md');
  process.exit(1);
}

const relay = new GistRelay({ gistId, token });
const DIRECTORY_FILE = 'directory.json';

// NOTE: last-writer-wins on directory.json (read-modify-write, no version
// check) — fine for a demo run by one person joining a few agents at a
// time, not safe for many concurrent joins. See relay.js's own note on why
// outbox files avoid this (single writer per file) — the directory doesn't
// have that luxury since everyone needs to add themselves to it.
async function readDirectory() {
  try {
    return await relay.readFile(DIRECTORY_FILE);
  } catch {
    return [];
  }
}

async function writeDirectory(directory) {
  const res = await fetch(`https://api.github.com/gists/${gistId}`, {
    method: 'PATCH',
    headers: {
      authorization: `Bearer ${token}`,
      'content-type': 'application/json',
      accept: 'application/vnd.github+json',
    },
    body: JSON.stringify({ files: { [DIRECTORY_FILE]: { content: JSON.stringify(directory, null, 2) } } }),
  });
  if (!res.ok) throw new Error(`could not write ${DIRECTORY_FILE}: ${res.status}`);
}

async function joinDirectory() {
  const directory = (await readDirectory()).filter(
    (e) => !(e.envName === envName && e.agentName === agentName),
  );
  directory.push({ envName, agentName, scopeText });
  await writeDirectory(directory);
  return directory;
}

const broker = await startBroker();
console.log(`[${envName}] local broker on :${broker.mqttPort}`);

const agentClient = new VortexiaClient({ port: broker.mqttPort });
await agentClient.register(agentName);
agentClient.on('message', (envelope) => {
  if (envelope.kind === 'federation-delivery') {
    console.log(`\n[${agentName}] received: "${envelope.text}" (from ${envelope.from}, routed via ${envelope.routedFrom})`);
  }
});

const gateway = new VortexiaClient({ port: broker.mqttPort });
await gateway.register(`${envName}-gateway`);

const directory = await joinDirectory();
console.log(`[${envName}] joined the federation directory (${directory.length} agent(s) known so far):`);
for (const d of directory) console.log(`  - ${d.envName}/${d.agentName}: ${d.scopeText}`);

const bridge = new FederationBridge({ envName, relay, directory }).attach(gateway);
bridge.startPolling(1000);

// Other environments may join after this one starts — refresh periodically.
setInterval(async () => {
  try {
    bridge.directory = await readDirectory();
  } catch {
    // relay hiccup — keep the last known directory and try again next tick
  }
}, 5000);

console.log(`\n[${envName}] ready. Type an intent and press enter to broadcast it (Ctrl+C to quit):`);
const rl = readline.createInterface({ input: process.stdin });
rl.on('line', (line) => {
  const intent = line.trim();
  if (!intent) return;
  gateway.send('broadcast', intent, { kind: FEDERATION_KIND, intent });
  console.log(`[${envName}] sent intent: "${intent}"`);
});

// Node only reliably delivers SIGINT (Ctrl+C) on Windows, not SIGTERM — so
// this only listens for SIGINT, which is also what a normal Ctrl+C sends
// on macOS/Linux. Releases the claimed local-agent-society port registry
// entries so a killed demo doesn't leave a ghost claim behind.
process.on('SIGINT', async () => {
  console.log(`\n[${envName}] shutting down...`);
  rl.close();
  bridge.stopPolling();
  try {
    await agentClient.close();
    await gateway.close();
    await broker.close();
  } finally {
    process.exit(0);
  }
});
