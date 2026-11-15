// Broker auth for non-loopback clients (ADR 0001 §5). Loopback can't be
// faked from a test without a second interface, so the hook is exercised
// with a fake remote address, and the verifier directly.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { generateSecretKey, getPublicKey } from 'nostr-tools';
import { BrokerAuthenticator, buildBrokerCredentials, isLoopback } from '../src/auth.js';
import { FriendsStore } from '../src/vortex-relay/friends.js';

const newStore = () => new FriendsStore({ file: path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'vx-auth-')), 'friends.json') });
const remote = (id = 'c1') => ({ id, connDetails: { ipAddress: '192.168.1.30' } });
const call = (hook, client, u, p) => new Promise((resolve) => hook(client, u, p, (err, ok) => resolve({ err, ok })));

test('isLoopback', () => {
  for (const a of ['127.0.0.1', '::1', '::ffff:127.0.0.1', '127.8.0.1']) assert.ok(isLoopback(a), a);
  for (const a of ['192.168.1.2', '::ffff:10.0.0.1', '', null, '100.64.0.1']) assert.ok(!isLoopback(a), a);
});

test('a friend env with a fresh signed event for this broker gets in; the same event replayed does not', async () => {
  const sk = generateSecretKey();
  const store = newStore();
  store.add({ envName: 'uy-mac', pubkey: getPublicKey(sk) });
  const auth = new BrokerAuthenticator({ envName: 'ba-mac', friends: store, mode: 'enforce', logger: {} });
  const { username, password } = await buildBrokerCredentials(sk, 'ba-mac');
  const client = remote();
  assert.deepEqual(await call(auth.hook(), client, username, Buffer.from(password)), { err: null, ok: true });
  assert.equal(client.vxFriend.envName, 'uy-mac');
  const again = await call(auth.hook(), remote('c2'), username, Buffer.from(password));
  assert.equal(again.ok, false);
  assert.match(again.err.message, /replayed/);
});

test('enforce rejects: no credentials, a stranger, the wrong challenge, a stale event, a device, a revoked friend', async () => {
  const sk = generateSecretKey();
  const store = newStore();
  store.add({ envName: 'uy-mac', pubkey: getPublicKey(sk) });
  const auth = new BrokerAuthenticator({ envName: 'ba-mac', friends: store, mode: 'enforce', logger: {} });
  const reason = async (u, p) => (await call(auth.hook(), remote(), u, p)).err?.message;

  assert.match(await reason(undefined, undefined), /no credentials/);
  const stranger = await buildBrokerCredentials(generateSecretKey(), 'ba-mac');
  assert.match(await reason(stranger.username, stranger.password), /not an active friend/);
  const wrong = await buildBrokerCredentials(sk, 'ar-pc');
  assert.match(await reason(wrong.username, wrong.password), /challenge/);
  const stale = await buildBrokerCredentials(sk, 'ba-mac', () => Date.now() - 10 * 60_000);
  assert.match(await reason(stale.username, stale.password), /off/);
  const tampered = JSON.parse((await buildBrokerCredentials(sk, 'ba-mac')).password);
  tampered.tags = [['challenge', 'ba-mac'], ['x', 'y']];
  assert.match(await reason(tampered.pubkey, JSON.stringify(tampered)), /signature/);

  const dsk = generateSecretKey();
  store.add({ envName: 'jose-iphone', pubkey: getPublicKey(dsk), role: 'device' });
  const dev = await buildBrokerCredentials(dsk, 'ba-mac');
  assert.match(await reason(dev.username, dev.password), /devices use the HTTP gateway/);

  store.revoke('uy-mac');
  const revoked = await buildBrokerCredentials(sk, 'ba-mac');
  assert.match(await reason(revoked.username, revoked.password), /not an active friend/);
});

test('warn mode lets a stranger in but logs it; loopback never needs credentials', async () => {
  const warnings = [];
  const auth = new BrokerAuthenticator({ envName: 'ba-mac', friends: newStore(), mode: 'warn', logger: { warn: (m) => warnings.push(m) } });
  assert.deepEqual(await call(auth.hook(), remote(), undefined, undefined), { err: null, ok: true });
  assert.match(warnings[0], /would reject/);

  const strict = new BrokerAuthenticator({ envName: 'ba-mac', friends: newStore(), mode: 'enforce', logger: {} });
  assert.deepEqual(await call(strict.hook(), { id: 'l', connDetails: { ipAddress: '::ffff:127.0.0.1' } }), { err: null, ok: true });
  assert.equal(strict.allowsControl(remote()), false);
  assert.equal(strict.allowsControl({ connDetails: { ipAddress: '127.0.0.1' } }), true);
});
