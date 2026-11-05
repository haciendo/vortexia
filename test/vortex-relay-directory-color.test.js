// An agent's widget color crosses environments in the directory, and the
// merged directory is readable on the local broker (retained
// vortex-relay/directory) — so a LAS friend on another machine is painted
// in its own tone.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { InMemoryRelay } from '../src/vortex-relay/relay.js';
import { publishDirectory, mergeDirectories } from '../src/vortex-relay/directory.js';
import { VortexRelayBridge, LOCAL_DIRECTORY_TOPIC } from '../src/vortex-relay/bridge.js';
import { discoverLocalRoster } from '../src/vortex-relay/localRoster.js';

test('mergeDirectories keeps a valid color, drops a malformed one, and old rosters still merge', async () => {
  const relay = new InMemoryRelay();
  await publishDirectory(relay, 'uy-mac', [
    { agentName: 'coach', scopeText: 'c', color: '#aa5500' },
    { agentName: 'billing', scopeText: 'b', color: 'red' },
    { agentName: 'old', scopeText: 'o' },
  ]);
  const { entries } = await mergeDirectories(relay, ['uy-mac']);
  const byName = Object.fromEntries(entries.map((e) => [e.agentName, e]));
  assert.equal(byName.coach.color, '#aa5500');
  assert.equal('color' in byName.billing, false);
  assert.equal('color' in byName.old, false);
});

test('the local roster carries each agent\'s .las-agent.json color', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'roster-color-'));
  const a = path.join(dir, 'a'); const b = path.join(dir, 'b');
  fs.mkdirSync(a); fs.mkdirSync(b);
  fs.writeFileSync(path.join(a, '.las-agent.json'), JSON.stringify({ name: 'A', short_description: 'aa', color: '#123456' }));
  fs.writeFileSync(path.join(b, '.las-agent.json'), JSON.stringify({ name: 'B', short_description: 'bb', color: 'nope' }));
  const server = http.createServer((_req, res) => res.end(JSON.stringify({ A: { path: a }, B: { path: b } })));
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  try {
    const roster = await discoverLocalRoster(`http://127.0.0.1:${server.address().port}`);
    assert.deepEqual(roster, [
      { agentName: 'A', scopeText: 'aa', color: '#123456' },
      { agentName: 'B', scopeText: 'bb' },
    ]);
  } finally {
    server.close();
  }
});

test('syncDirectory publishes the merged directory retained on the local broker, only when it changes', async () => {
  const relay = new InMemoryRelay();
  await publishDirectory(relay, 'uy-mac', [{ agentName: 'coach', scopeText: 'c', color: '#aa5500' }]);
  const bridge = new VortexRelayBridge({ envName: 'ba-mac', relay, envNames: ['ba-mac', 'uy-mac'] });
  const published = [];
  bridge.localClient = { mqttClient: { publish: (topic, payload, opts) => published.push({ topic, payload: JSON.parse(payload), opts }) } };
  await bridge.syncDirectory();
  await bridge.syncDirectory();
  assert.equal(published.length, 1, 'unchanged directory: no republish');
  assert.equal(published[0].topic, LOCAL_DIRECTORY_TOPIC);
  assert.deepEqual(published[0].opts, { qos: 1, retain: true });
  assert.equal(published[0].payload.envName, 'ba-mac');
  assert.deepEqual(published[0].payload.entries, [{ envName: 'uy-mac', agentName: 'coach', scopeText: 'c', color: '#aa5500' }]);
  await publishDirectory(relay, 'uy-mac', [{ agentName: 'coach', scopeText: 'c', color: '#0000ff' }]);
  await bridge.syncDirectory();
  assert.equal(published.length, 2);
  assert.equal(published[1].payload.entries[0].color, '#0000ff', 'a color change on the other machine arrives');
});
