// NostrRelay end-to-end encryption (docs/adr/0001-device-pairing.md §4),
// against an in-memory stand-in for the relay pool: reads accept plaintext
// and NIP-44 events alike, encrypted writes are readable only by their
// intended reader, and the rollout needs no flag day.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { generateSecretKey, getPublicKey, matchFilter } from 'nostr-tools';
import { NostrRelay } from '../src/vortex-relay/relay.js';
import { FriendsStore } from '../src/vortex-relay/friends.js';

class FakePool {
  events = [];
  publish(_urls, event) { this.events.push(event); return [Promise.resolve('ok')]; }
  async querySync(_urls, filter) {
    const matching = this.events.filter((e) => matchFilter(filter, e));
    // parameterized-replaceable: keep only the latest per (kind, author, d)
    const latest = new Map();
    const out = [];
    for (const e of matching) {
      if (e.kind >= 30000 && e.kind < 40000) {
        const key = `${e.kind}:${e.pubkey}:${e.tags.find((t) => t[0] === 'd')?.[1]}`;
        const prev = latest.get(key);
        if (!prev || prev.created_at <= e.created_at) latest.set(key, e);
      } else out.push(e);
    }
    return [...out, ...latest.values()];
  }
  destroy() {}
}

function makeEnv(envName, pool) {
  const sk = generateSecretKey();
  const store = new FriendsStore({ file: path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'vx-enc-')), 'friends.json') });
  const relay = new NostrRelay({ secretKey: sk, friends: store, envName });
  relay._pool = pool;
  relay._pubkey = getPublicKey(sk);
  relay._ensurePromise = Promise.resolve();
  return { envName, relay, store, pk: relay._pubkey };
}

function befriend(a, b) {
  a.store.add({ envName: b.envName, pubkey: b.pk });
  b.store.add({ envName: a.envName, pubkey: a.pk });
}

test('an encrypted inbox message is readable by its env, and nothing on the relay is plaintext', async () => {
  const pool = new FakePool();
  const ba = makeEnv('ba-mac', pool);
  const uy = makeEnv('uy-mac', pool);
  befriend(ba, uy);
  ba.relay.encrypt = true;

  await ba.relay.appendMessage('inbox-uy-mac.json', { to: 'coach', text: 'secreto' });
  assert.deepEqual(await uy.relay.readFile('inbox-uy-mac.json'), [{ to: 'coach', text: 'secreto' }]);
  assert.ok(pool.events.every((e) => !e.content.includes('secreto')));
  assert.ok(pool.events.every((e) => e.kind === 7879));
});

test('a third env, even a trusted one, cannot read a message encrypted to someone else', async () => {
  const pool = new FakePool();
  const ba = makeEnv('ba-mac', pool);
  const uy = makeEnv('uy-mac', pool);
  const ar = makeEnv('ar-pc', pool);
  befriend(ba, uy);
  befriend(ba, ar);
  ar.store.add({ envName: 'uy-mac', pubkey: uy.pk });
  ba.relay.encrypt = true;

  await ba.relay.appendMessage('inbox-uy-mac.json', { text: 'solo uy' });
  // ar-pc queries its own d tag: the event addressed to uy-mac isn't even returned
  assert.deepEqual(await ar.relay.readFile('inbox-uy-mac.json'), []);
});

test('an encrypted directory snapshot reaches every env friend and its writer', async () => {
  const pool = new FakePool();
  const ba = makeEnv('ba-mac', pool);
  const uy = makeEnv('uy-mac', pool);
  befriend(ba, uy);
  ba.relay.encrypt = true;

  const dir = { envName: 'ba-mac', roster: [{ agentName: 'Vortexia', scopeText: 'x' }] };
  await ba.relay.writeFile('directory-ba-mac.json', dir);
  assert.deepEqual(await uy.relay.readFile('directory-ba-mac.json'), dir);
  assert.deepEqual(await ba.relay.readFile('directory-ba-mac.json'), dir);
});

test('rollout: an upgraded reader reads a plaintext writer, and the newest snapshot wins across both kinds', async () => {
  const pool = new FakePool();
  const ba = makeEnv('ba-mac', pool);
  const uy = makeEnv('uy-mac', pool);
  befriend(ba, uy);

  await uy.relay.appendMessage('inbox-ba-mac.json', { n: 1 });
  await uy.relay.writeFile('directory-uy-mac.json', { v: 'plain' });
  assert.deepEqual(await ba.relay.readFile('inbox-ba-mac.json'), [{ n: 1 }]);

  uy.relay.encrypt = true;
  // same second would tie: make the encrypted snapshot strictly newer
  const realNow = Date.now;
  Date.now = () => realNow() + 2000;
  try {
    await uy.relay.appendMessage('inbox-ba-mac.json', { n: 2 });
    await uy.relay.writeFile('directory-uy-mac.json', { v: 'enc' });
  } finally {
    Date.now = realNow;
  }
  assert.deepEqual(await ba.relay.readFile('inbox-ba-mac.json'), [{ n: 1 }, { n: 2 }]);
  assert.deepEqual(await ba.relay.readFile('directory-uy-mac.json'), { v: 'enc' });
});

test('encrypting to an env with no known key fails loudly instead of leaking plaintext', async () => {
  const pool = new FakePool();
  const ba = makeEnv('ba-mac', pool);
  ba.relay.encrypt = true;
  await assert.rejects(ba.relay.appendMessage('inbox-nowhere.json', { text: 'x' }), /no known pubkey/);
  assert.equal(pool.events.length, 0);
});

test('a revoked friend gets nothing new and its events are no longer read', async () => {
  const pool = new FakePool();
  const ba = makeEnv('ba-mac', pool);
  const uy = makeEnv('uy-mac', pool);
  befriend(ba, uy);
  ba.relay.encrypt = true;
  uy.relay.encrypt = true;

  await uy.relay.appendMessage('inbox-ba-mac.json', { text: 'antes' });
  ba.store.revoke('uy-mac');
  assert.deepEqual(await ba.relay.readFile('inbox-ba-mac.json'), []);
  await assert.rejects(ba.relay.appendMessage('inbox-uy-mac.json', { text: 'x' }), /no known pubkey/);
});
