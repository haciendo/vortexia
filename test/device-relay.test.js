// The phone away from home (ADR 0001 §9.1): pairing, send, a large proxied
// read, a clip, the live outbox and sync, all through a Nostr relay — here
// an in-memory one that stores events and fans them out like a real relay.
import './helpers/isolate.js';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import mqtt from 'mqtt';
import { generateSecretKey, getPublicKey, matchFilter } from 'nostr-tools';
import { startBroker } from '../src/broker.js';
import { VortexiaClient } from '../src/client.js';
import { FriendsStore } from '../src/vortex-relay/friends.js';
import { PairingManager, encryptEventTo, openEvent } from '../src/vortex-relay/pairing.js';
import { DeviceGateway, encryptBlob } from '../src/vortex-relay/deviceGateway.js';
import { DeviceRelayLink, splitPayload, joinParts, PART_BYTES } from '../src/vortex-relay/deviceRelay.js';

class MemoryRelayPool {
  constructor() { this.events = []; this.subs = new Set(); this.down = false; }
  subscribeMany(_relays, filter, { onevent }) {
    const sub = { filter, onevent };
    this.subs.add(sub);
    for (const e of this.events) if (matchFilter(filter, e)) queueMicrotask(() => onevent(e));
    return { close: () => this.subs.delete(sub) };
  }
  async querySync(_relays, filter) { return this.events.filter((e) => matchFilter(filter, e)); }
  publish(relays, event) {
    if (this.down) return relays.map(() => Promise.reject(new Error('relay down')));
    this.events.push(event);
    for (const s of this.subs) if (matchFilter(s.filter, event)) queueMicrotask(() => s.onevent(event));
    return relays.map(() => Promise.resolve('ok'));
  }
  destroy() {}
}

/** The phone's side: publish to home, collect what home sends back (parts joined). */
function phone(pool, sk, homePk) {
  const pk = getPublicKey(sk);
  const got = [];
  const parts = new Map();
  const waiters = [];
  pool.subscribeMany([], { kinds: [7879], '#p': [pk], authors: [homePk] }, {
    onevent: async (e) => {
      let p = await openEvent(sk, e);
      if (p.type === 'part') {
        const list = parts.get(p.msgId) || [];
        list.push(p);
        parts.set(p.msgId, list);
        if (list.length < p.total) return;
        p = joinParts(list);
      }
      got.push(p);
      for (const w of [...waiters]) if (w.match(p)) { waiters.splice(waiters.indexOf(w), 1); w.resolve(p); }
    },
  });
  return {
    pk, got,
    async send(payload) {
      const ids = [];
      for (const part of splitPayload(payload)) {
        const e = await encryptEventTo(sk, homePk, part);
        pool.publish(['r'], e);
        ids.push(e.id);
      }
      return ids;
    },
    next(match) {
      const hit = got.find(match);
      if (hit) { got.splice(got.indexOf(hit), 1); return Promise.resolve(hit); }
      return new Promise((resolve, reject) => {
        const w = { match: (p) => { if (match(p)) { got.splice(got.indexOf(p), 1); return true; } return false; }, resolve };
        waiters.push(w);
        setTimeout(() => reject(new Error('timed out waiting for a payload')), 4000).unref();
      });
    },
  };
}

function nextMessage(port, topic) {
  const c = mqtt.connect(`mqtt://127.0.0.1:${port}`);
  const p = new Promise((resolve) => c.on('message', (t, m) => { if (t === topic) { resolve(JSON.parse(m.toString())); c.end(true); } }));
  return new Promise((r) => c.on('connect', () => c.subscribe(topic, { qos: 1 }, () => r({ message: p }))));
}

test('splitPayload / joinParts round-trip, and a small payload stays whole', () => {
  assert.deepEqual(splitPayload({ a: 1 }), [{ a: 1 }]);
  const big = { type: 'x', body: 'ñ'.repeat(PART_BYTES) };
  const parts = splitPayload(big);
  assert.ok(parts.length > 1);
  assert.ok(parts.every((p) => p.type === 'part' && p.total === parts.length));
  assert.deepEqual(joinParts(parts.reverse()), big);
});

test('the phone pairs, sends, reads, uploads and receives through a relay only', async () => {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'vx-drelay-'));
  const broker = await startBroker({ persist: false });
  const homeSk = generateSecretKey();
  const homePk = getPublicKey(homeSk);
  const friends = new FriendsStore({ file: path.join(dataDir, 'friends.json') });
  const pairing = new PairingManager({ envName: 'ba-mac', secretKey: homeSk, friends });

  const bigTree = { env: 'ba-mac', children: Array.from({ length: 900 }, (_, i) => ({ name: `agent-${i}`, scope: 'x'.repeat(60) })) };
  const las = http.createServer((req, res) => {
    if (req.url !== '/device/society/tree') { res.writeHead(404).end(); return; }
    res.writeHead(200, { etag: '"v1"', 'content-type': 'application/json' }).end(JSON.stringify(bigTree));
  });
  await new Promise((r) => las.listen(0, '127.0.0.1', r));

  const gw = new DeviceGateway({ envName: 'ba-mac', secretKey: homeSk, friends, pairing, mqttPort: broker.mqttPort, dataDir, lasUrl: `http://127.0.0.1:${las.address().port}`, dev: false, logger: {} });
  await gw.start();
  const pool = new MemoryRelayPool();
  const stateFile = path.join(dataDir, 'device-relay-state.json');
  const link = await new DeviceRelayLink({ gateway: gw, pairing, friends, secretKey: homeSk, relays: ['wss://r'], stateFile, pool, logger: {} }).start();

  const devSk = generateSecretKey();
  const dev = phone(pool, devSk, homePk);
  try {
    // ── pairing ──
    const { sessionId, uri } = await pairing.start({ role: 'device', label: 'iPhone' });
    const token = new URLSearchParams(uri.split('?')[1]).get('t');
    const [reqId] = await dev.send({ type: 'PAIR_REQUEST', token, envName: 'jose-iphone', label: 'iPhone de José' });
    const pend = await dev.next((p) => p.type === 'pair-pending');
    assert.equal(pend.ref, reqId);
    assert.equal(pend.sessionId, sessionId);
    await pairing.decide(sessionId, true);
    const ack = await dev.next((p) => p.type === 'PAIR_ACK');
    assert.equal(ack.homeEnvName, 'ba-mac');
    assert.equal(friends.byPubkey(dev.pk).envName, 'jose-iphone');

    // ── send ──
    const inbox = await nextMessage(broker.mqttPort, 'las/agent/Coach/inbox');
    const [sendId] = await dev.send({ type: 'send', to: 'Coach/claude', text: 'hola desde la calle' });
    const delivered = await inbox.message;
    assert.equal(delivered.text, 'hola desde la calle');
    assert.equal(delivered.from, 'jose-iphone');
    assert.equal(delivered.origin, 'human');
    assert.equal(delivered.session, 'claude');
    const sendAck = await dev.next((p) => p.ref === sendId);
    assert.equal(sendAck.type, 'ack');
    assert.equal(sendAck.ok, true);

    // ── the same event again (a relay resends it, or a catch-up query) is not processed twice ──
    const dup = pool.events.find((e) => e.id === sendId);
    let extra = 0;
    const watcher = mqtt.connect(`mqtt://127.0.0.1:${broker.mqttPort}`);
    await new Promise((r) => watcher.on('connect', () => watcher.subscribe('las/agent/Coach/inbox', { qos: 1 }, r)));
    watcher.on('message', () => { extra += 1; });
    await link.handleEvent(dup);
    await link._catchUp();
    await new Promise((r) => setTimeout(r, 200));
    watcher.end(true);
    assert.equal(extra, 0);

    // ── a large read comes back in parts ──
    const [httpId] = await dev.send({ type: 'http', id: 'h1', path: '/device/society/tree' });
    const reply = await dev.next((p) => p.type === 'http-response');
    assert.equal(reply.ref, httpId);
    assert.equal(reply.status, 200);
    assert.deepEqual(JSON.parse(reply.body), bigTree);
    assert.ok(pool.events.filter((e) => e.pubkey === homePk).length > 4, 'the reply needed several events');

    // ── a clip inline, split into parts ──
    const clip = crypto.randomBytes(70_000);
    const sha = crypto.createHash('sha256').update(clip).digest('hex');
    const key = crypto.randomBytes(32);
    const notice = await nextMessage(broker.mqttPort, 'las/agent/las-device-audio/inbox');
    await dev.send({ type: 'blob', sha256: sha, key: key.toString('base64'), data: encryptBlob(clip, key).toString('base64'), mime: 'audio/mp4', durationMs: 3000, capturedAt: 1, target: { agent: 'Coach' } });
    const n = JSON.parse((await notice.message).text);
    assert.equal(n.type, 'device-audio');
    assert.deepEqual(fs.readFileSync(n.path), clip);
    const clipAck = await dev.next((p) => p.type === 'ack' && p.sha256 === sha);
    assert.equal(clipAck.delivered, true);
    assert.ok(!fs.readFileSync(path.join(dataDir, 'device-inbox', 'jose-iphone', `${sha}.json`), 'utf8').includes('"data"'));

    // ── the outbox arrives live, and sync resends what was not acked ──
    const lasClient = new VortexiaClient({ port: broker.mqttPort });
    await lasClient.register('LocalAgentSociety', { presence: false });
    await lasClient.sendConfirmed('jose-iphone', JSON.stringify({ type: 'device-audio-result', sha256: sha, ok: true, chars: 12 }));
    const live = await dev.next((p) => p.type === 'message');
    assert.equal(live.from, 'LocalAgentSociety');
    await lasClient.sendConfirmed('jose-iphone', 'segundo');
    const second = await dev.next((p) => p.type === 'message' && p.text === 'segundo');
    await lasClient.close();
    const [syncId] = await dev.send({ type: 'sync', ack: live.seq });
    const resent = await dev.next((p) => p.type === 'message' && p.seq === second.seq);
    assert.equal(resent.text, 'segundo');
    const synced = await dev.next((p) => p.type === 'synced');
    assert.equal(synced.ref, syncId);
    assert.equal(synced.cursor, second.seq);

    // ── errors come back with the event's ref ──
    const [badId] = await dev.send({ type: 'send', to: 'Coach@uy-mac', text: 'x' });
    const err = await dev.next((p) => p.ref === badId);
    assert.equal(err.type, 'error');
    assert.equal(err.status, 400);

    // ── a stranger gets no answer; a revoked phone neither ──
    const strangerSk = generateSecretKey();
    const stranger = phone(pool, strangerSk, homePk);
    await stranger.send({ type: 'send', to: 'Coach', text: 'hola' });
    friends.revoke('jose-iphone');
    await dev.send({ type: 'send', to: 'Coach', text: 'después de revocar' });
    await new Promise((r) => setTimeout(r, 200));
    assert.equal(stranger.got.length, 0);
    assert.equal(dev.got.length, 0);

    // ── a restart remembers what it already handled ──
    link.close();
    const before = pool.events.length;
    const link2 = await new DeviceRelayLink({ gateway: gw, pairing, friends, secretKey: homeSk, relays: ['wss://r'], stateFile, pool, logger: {} }).start();
    await new Promise((r) => setTimeout(r, 200));
    assert.equal(pool.events.length, before, 'nothing was answered again');
    link2.close();
  } finally {
    link.close();
    await gw.close();
    las.close();
    await broker.close();
  }
});

test('a publish no relay accepts is retried until one does', async () => {
  const pool = new MemoryRelayPool();
  pool.down = true;
  const homeSk = generateSecretKey();
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'vx-drelay-r-'));
  const link = new DeviceRelayLink({ gateway: {}, friends: new FriendsStore({ file: path.join(dataDir, 'f.json') }), secretKey: homeSk, relays: ['wss://r'], stateFile: path.join(dataDir, 's.json'), pool, logger: {}, retryDelaysMs: [20, 20, 20] });
  const done = link.publishTo(getPublicKey(generateSecretKey()), { type: 'message', text: 'hola' });
  setTimeout(() => { pool.down = false; }, 30);
  await done;
  assert.equal(pool.events.length, 1);
});
