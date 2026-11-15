// Device gateway end to end (ADR 0001 §3, §6, §8, §9): a simulated phone
// pairs by QR token, the human accepts, and then it sends to an agent, reads
// LAS through the proxy, uploads a clip and drains its outbox. Every body is
// signed and encrypted.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import mqtt from 'mqtt';
import { generateSecretKey, getPublicKey } from 'nostr-tools';
import * as nip44 from 'nostr-tools/nip44';
import { startBroker } from '../src/broker.js';
import { VortexiaClient } from '../src/client.js';
import { FriendsStore } from '../src/vortex-relay/friends.js';
import { PairingManager, encryptEventTo, fingerprint } from '../src/vortex-relay/pairing.js';
import { DeviceGateway, buildHttpAuth, encryptBlob } from '../src/vortex-relay/deviceGateway.js';

function nextMessage(port, topic) {
  const c = mqtt.connect(`mqtt://127.0.0.1:${port}`);
  const p = new Promise((resolve) => c.on('message', (t, m) => { if (t === topic) { resolve(JSON.parse(m.toString())); c.end(true); } }));
  // Resolves once subscribed, with { message } — wrapped, because resolving
  // with a promise would chain and wait for the message itself.
  return new Promise((r) => c.on('connect', () => c.subscribe(topic, { qos: 1 }, () => r({ message: p }))));
}

test('pair, send, proxy, clip and outbox through the device gateway', async () => {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'vx-gw-'));
  const broker = await startBroker({ persist: false });
  const homeSk = generateSecretKey();
  const friends = new FriendsStore({ file: path.join(dataDir, 'friends.json') });
  const events = [];
  const pairing = new PairingManager({ envName: 'ba-mac', secretKey: homeSk, friends, endpoints: () => ({ lan: [`127.0.0.1:${broker.wsPort}`] }), onEvent: (e) => events.push(e) });

  // A stand-in LAS backend: /device/society/tree with an ETag.
  const seenHeaders = [];
  const las = http.createServer((req, res) => {
    seenHeaders.push(req.headers);
    if (req.url !== '/device/society/tree') { res.writeHead(404).end(); return; }
    if (req.headers['if-none-match'] === '"v1"') { res.writeHead(304, { etag: '"v1"' }).end(); return; }
    res.writeHead(200, { etag: '"v1"', 'content-type': 'application/json' }).end(JSON.stringify({ env: 'ba-mac', name: 'root', children: [] }));
  });
  await new Promise((r) => las.listen(0, '127.0.0.1', r));

  const gw = new DeviceGateway({ envName: 'ba-mac', secretKey: homeSk, friends, pairing, mqttPort: broker.mqttPort, dataDir, lasUrl: `http://127.0.0.1:${las.address().port}`, dev: false, logger: {} });
  await gw.start();
  gw.attach(broker.wsServer);
  const base = `http://127.0.0.1:${broker.wsPort}`;

  const devSk = generateSecretKey();
  const devPk = getPublicKey(devSk);
  const open = (event) => JSON.parse(nip44.decrypt(event.content, nip44.getConversationKey(devSk, event.pubkey)));
  const signed = async (method, url, body) => {
    const headers = { authorization: await buildHttpAuth(devSk, { url, method, body }) };
    const res = await fetch(url, { method, headers, ...(body ? { body } : {}) });
    return res;
  };

  try {
    // ── pairing ──
    const { sessionId, uri } = await pairing.start({ role: 'device', label: 'iPhone' });
    const q = new URLSearchParams(uri.split('?')[1]);
    const homePk = q.get('pk');
    assert.equal(homePk, getPublicKey(homeSk));
    assert.deepEqual(q.get('lan').split(','), [`127.0.0.1:${broker.wsPort}`]);

    const request = await encryptEventTo(devSk, homePk, { type: 'PAIR_REQUEST', token: q.get('t'), envName: 'jose-iphone', label: 'iPhone de José' });
    let r = await fetch(`${base}/device/events`, { method: 'POST', body: JSON.stringify(request) });
    assert.equal(r.status, 202);
    assert.equal((await r.json()).sessionId, sessionId);
    assert.equal(pairing.pending()[0].fingerprint, fingerprint(devPk));

    r = await fetch(`${base}/device/events`, { method: 'POST', body: JSON.stringify(request) });
    assert.equal(r.status, 409, 'the token is single use');

    assert.equal((await fetch(`${base}/device/pair/${sessionId}`)).status, 202);
    assert.deepEqual(await pairing.decide(sessionId, true), { ok: true, envName: 'jose-iphone' });
    r = await fetch(`${base}/device/pair/${sessionId}`);
    assert.equal(r.status, 200);
    const ack = open((await r.json()).event);
    assert.equal(ack.type, 'PAIR_ACK');
    assert.equal(ack.homeEnvName, 'ba-mac');
    assert.equal(friends.byPubkey(devPk).role, 'device');
    assert.deepEqual(events.map((e) => e.type), ['paired']);

    // ── auth failures ──
    assert.equal((await fetch(`${base}/device/society/tree`)).status, 401);
    const replayUrl = `${base}/device/society/tree`;
    const auth = await buildHttpAuth(devSk, { url: replayUrl, method: 'GET' });
    assert.equal((await fetch(replayUrl, { headers: { authorization: auth } })).status, 200);
    assert.equal((await fetch(replayUrl, { headers: { authorization: auth } })).status, 401, 'replay');

    // ── proxy ──
    r = await signed('GET', `${base}/device/society/tree`);
    assert.equal(r.status, 200);
    assert.equal(r.headers.get('content-type'), 'application/vnd.vortexia.nip44');
    const tree = JSON.parse(nip44.decrypt(await r.text(), nip44.getConversationKey(devSk, homePk)));
    assert.equal(tree.env, 'ba-mac');
    assert.equal(seenHeaders.at(-1)['x-vortexia-device'], 'jose-iphone');
    r = await fetch(`${base}/device/society/tree`, { headers: { authorization: await buildHttpAuth(devSk, { url: `${base}/device/society/tree`, method: 'GET' }), 'if-none-match': '"v1"' } });
    assert.equal(r.status, 304);

    // ── send ──
    const inbox = await nextMessage(broker.mqttPort, 'las/agent/Coach/inbox');
    const sendEvent = await encryptEventTo(devSk, homePk, { type: 'send', to: 'Coach/claude', text: 'hola desde el teléfono' });
    const sendBody = Buffer.from(JSON.stringify(sendEvent));
    r = await signed('POST', `${base}/device/events`, sendBody);
    assert.equal(r.status, 202);
    const delivered = await inbox.message;
    assert.equal(delivered.text, 'hola desde el teléfono');
    assert.equal(delivered.from, 'jose-iphone');
    assert.equal(delivered.origin, 'human');
    assert.equal(delivered.via, 'device');
    assert.equal(delivered.session, 'claude');

    // ── http event (the relay-shaped read) ──
    const httpEvent = await encryptEventTo(devSk, homePk, { type: 'http', id: 'h1', path: '/device/society/tree' });
    r = await signed('POST', `${base}/device/events`, Buffer.from(JSON.stringify(httpEvent)));
    assert.equal(r.status, 200);
    const reply = open((await r.json()).events[0]);
    assert.equal(reply.type, 'http-response');
    assert.equal(reply.id, 'h1');
    assert.equal(JSON.parse(reply.body).env, 'ba-mac');

    // ── clip: PUT first, key after ──
    const clip = crypto.randomBytes(5000);
    const sha = crypto.createHash('sha256').update(clip).digest('hex');
    const key = crypto.randomBytes(32);
    const notice = await nextMessage(broker.mqttPort, 'las/agent/las-device-audio/inbox');
    r = await signed('PUT', `${base}/device/blob/${sha}`, encryptBlob(clip, key));
    assert.equal(r.status, 201);
    assert.equal((await r.json()).delivered, false, 'waits for its key');
    const blobEvent = await encryptEventTo(devSk, homePk, { type: 'blob', sha256: sha, key: key.toString('base64'), mime: 'audio/mp4', durationMs: 4200, capturedAt: 1, target: { agent: 'Coach' } });
    r = await signed('POST', `${base}/device/events`, Buffer.from(JSON.stringify(blobEvent)));
    assert.equal(r.status, 202);
    const n = JSON.parse((await notice.message).text);
    assert.equal(n.type, 'device-audio');
    assert.equal(n.from, 'jose-iphone');
    assert.deepEqual(n.target, { agent: 'Coach' });
    assert.deepEqual(fs.readFileSync(n.path), clip);
    assert.deepEqual(JSON.parse(fs.readFileSync(path.join(dataDir, 'device-inbox', 'jose-iphone', `${sha}.json`), 'utf8')), n);
    r = await signed('PUT', `${base}/device/blob/${sha}`, encryptBlob(clip, key));
    assert.equal(r.status, 200, 're-upload is harmless');

    // ── outbox ──
    const las2 = new VortexiaClient({ port: broker.mqttPort });
    await las2.register('LocalAgentSociety', { presence: false });
    await las2.sendConfirmed('jose-iphone', JSON.stringify({ type: 'device-audio-result', sha256: sha, ok: true, chars: 42 }));
    await las2.close();
    let box;
    for (let i = 0; i < 20; i++) {
      r = await signed('GET', `${base}/device/events`);
      box = await r.json();
      if (box.events.length) break;
      await new Promise((res) => setTimeout(res, 100));
    }
    const msg = open(box.events[0]);
    assert.equal(msg.from, 'LocalAgentSociety');
    assert.equal(JSON.parse(msg.text).type, 'device-audio-result');
    r = await signed('GET', `${base}/device/events?ack=${box.cursor}`);
    assert.deepEqual((await r.json()).events, []);

    // ── revocation ──
    friends.revoke('jose-iphone');
    r = await signed('GET', `${base}/device/society/tree`);
    assert.equal(r.status, 403);
  } finally {
    await gw.close();
    las.close();
    await broker.close();
  }
});

test('a rejected or expired pairing never adds a friend', async () => {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'vx-pair-'));
  let t = 1_000_000;
  const friends = new FriendsStore({ file: path.join(dataDir, 'friends.json') });
  const homeSk = generateSecretKey();
  const pairing = new PairingManager({ envName: 'ba-mac', secretKey: homeSk, friends, now: () => t });
  const dev = generateSecretKey();

  const a = await pairing.start({ ttlMs: 60_000 });
  const tokenA = new URLSearchParams(a.uri.split('?')[1]).get('t');
  pairing.handleRequest({ pubkey: getPublicKey(dev) }, { type: 'PAIR_REQUEST', token: tokenA, envName: 'phone' });
  assert.deepEqual(await pairing.decide(a.sessionId, false), { ok: true, envName: 'phone' });
  assert.equal(pairing.status(a.sessionId).state, 'rejected');
  assert.equal(friends.list().length, 0);

  const b = await pairing.start({ ttlMs: 60_000 });
  const tokenB = new URLSearchParams(b.uri.split('?')[1]).get('t');
  pairing.handleRequest({ pubkey: getPublicKey(dev) }, { type: 'PAIR_REQUEST', token: tokenB, envName: 'phone' });
  t += 61_000;
  assert.deepEqual(await pairing.decide(b.sessionId, true), { ok: false, error: 'expired' });
  assert.equal(friends.list().length, 0);
  assert.throws(() => pairing.handleRequest({ pubkey: getPublicKey(dev) }, { type: 'PAIR_REQUEST', token: tokenB, envName: 'phone' }), /unknown or used token/);
});
