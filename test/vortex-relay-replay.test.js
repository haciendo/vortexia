import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { VortexRelayBridge, relayMessageKey } from '../src/vortex-relay/bridge.js';

// A relay whose reads return whatever the test says — the shape of a
// Nostr read that merges public relays answering (or not) within their
// timeout: the same log, longer or shorter from one poll to the next.
function scriptedRelay() {
  return { next: [], async readFile() { return this.next; } };
}
function bridgeWith(relay, opts = {}) {
  const delivered = [];
  const bridge = new VortexRelayBridge({ envName: 'uy-mac', relay, ...opts });
  bridge._deliverLocally = (m) => delivered.push(m.text);
  return { bridge, delivered };
}
const msg = (id, text, ts = Date.now()) => ({ id, from: 'LocalAgentSociety', text, agentNames: ['System'], routedFrom: 'ba-mac', ts });
const tmpFile = () => path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'vx-seen-')), 'relay-seen.json');

test('a read that comes back shorter and then full again does not re-deliver (2026-09-30 rollout replay)', async () => {
  const relay = scriptedRelay();
  const { bridge, delivered } = bridgeWith(relay);
  const a = msg('a', 'older'), b = msg('b', 'rollout request');
  relay.next = [a, b];
  await bridge._pollOnce();
  relay.next = [b]; // one relay slow: the older event missing
  await bridge._pollOnce();
  relay.next = [a, b];
  await bridge._pollOnce();
  relay.next = [b, a]; // different order
  await bridge._pollOnce();
  assert.deepEqual(delivered, ['older', 'rollout request']);
});

test('the seen-set survives a restart: a new bridge on the same file delivers only what is new', async () => {
  const seenFile = tmpFile();
  const relay = scriptedRelay();
  relay.next = [msg('a', 'one')];
  const first = bridgeWith(relay, { seenFile });
  first.bridge._bootstrapCutoff = null; // treat as an established install
  await first.bridge._pollOnce();
  assert.deepEqual(first.delivered, ['one']);

  relay.next = [msg('a', 'one'), msg('b', 'two')];
  const second = bridgeWith(relay, { seenFile });
  await second.bridge._pollOnce();
  assert.deepEqual(second.delivered, ['two']);
});

test('first run with no seen file: old stored messages are marked seen, recent ones still delivered', async () => {
  const seenFile = tmpFile();
  const relay = scriptedRelay();
  relay.next = [msg('old', 'yesterday', Date.now() - 24 * 3600 * 1000), msg('new', 'just now')];
  const { bridge, delivered } = bridgeWith(relay, { seenFile });
  await bridge._pollOnce();
  assert.deepEqual(delivered, ['just now']);
  assert.ok(fs.existsSync(seenFile));
});

test('relayMessageKey: id when present, content hash for older senders without one', () => {
  assert.equal(relayMessageKey({ id: 'x', text: 'a' }), 'id:x');
  const legacy = { from: 'A', text: 'hi', ts: 1 };
  assert.equal(relayMessageKey(legacy), relayMessageKey({ ...legacy }));
  assert.notEqual(relayMessageKey(legacy), relayMessageKey({ ...legacy, ts: 2 }));
});

test('first run: an empty first read (relays timing out) does not end the bootstrap window', async () => {
  const relay = scriptedRelay();
  const { bridge, delivered } = bridgeWith(relay, { seenFile: tmpFile() });
  relay.next = [];
  await bridge._pollOnce();
  relay.next = [msg('old', 'yesterday', Date.now() - 24 * 3600 * 1000), msg('new', 'just now')];
  await bridge._pollOnce();
  assert.deepEqual(delivered, ['just now']);
});
