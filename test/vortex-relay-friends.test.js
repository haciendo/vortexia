// Friends store (docs/adr/0001-device-pairing.md §2): data/friends.json
// replaces VORTEXIA_NOSTR_PEERS as the source of trust, live-reloaded, with
// local and immediate revocation.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { FriendsStore, parsePeersEnv } from '../src/vortex-relay/friends.js';
import { NostrRelay } from '../src/vortex-relay/relay.js';

const PK_A = 'a'.repeat(64);
const PK_B = 'b'.repeat(64);
const PK_C = 'c'.repeat(64);

function tmpFile() {
  return path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'vx-friends-')), 'friends.json');
}

test('parsePeersEnv reads env:pubkey pairs and drops malformed ones', () => {
  assert.deepEqual(parsePeersEnv(`uy-mac:${PK_A.toUpperCase()}, bad, x:short ,`), [{ envName: 'uy-mac', pubkey: PK_A }]);
});

test('seeds are trusted as env friends until the file overrides them', () => {
  const store = new FriendsStore({ file: tmpFile(), seed: [{ envName: 'uy-mac', pubkey: PK_A }] });
  assert.deepEqual(store.activePubkeys(), [PK_A]);
  assert.equal(store.get('uy-mac').role, 'env');

  store.revoke('uy-mac');
  assert.deepEqual(store.activePubkeys(), [], 'a revoked seed stays revoked: the file wins');
  assert.equal(store.list().length, 1);
});

test('add, revoke, re-pair and rename, persisted with mode 0600', () => {
  const file = tmpFile();
  let t = 1000;
  const store = new FriendsStore({ file, now: () => t });
  store.add({ envName: 'jose-iphone', pubkey: PK_B, role: 'device', label: 'iPhone de José' });
  assert.equal(store.byPubkey(PK_B).envName, 'jose-iphone');
  assert.equal(fs.statSync(file).mode & 0o777, 0o600);

  t = 2000;
  store.revoke('jose-iphone');
  assert.equal(store.byPubkey(PK_B), null);
  assert.equal(store.get('jose-iphone').revokedAt, 2000);

  store.add({ envName: 'jose-iphone', pubkey: PK_C, role: 'device' });
  assert.equal(store.byPubkey(PK_C).revokedAt, null, 're-pairing clears the revocation');
  assert.equal(store.byPubkey(PK_B), null, 'the old key is gone');

  store.rename('jose-iphone', 'iPhone');
  assert.equal(store.get('jose-iphone').label, 'iPhone');
});

test('rejects bad input and a pubkey that already belongs to another friend', () => {
  const store = new FriendsStore({ file: tmpFile() });
  assert.throws(() => store.add({ envName: 'a b', pubkey: PK_A }), /envName/);
  assert.throws(() => store.add({ envName: 'x', pubkey: 'nope' }), /pubkey/);
  assert.throws(() => store.add({ envName: 'x', pubkey: PK_A, role: 'robot' }), /role/);
  store.add({ envName: 'x', pubkey: PK_A });
  assert.throws(() => store.add({ envName: 'y', pubkey: PK_A }), /already belongs/);
});

test('another process writing the file is picked up without a restart', () => {
  const file = tmpFile();
  const running = new FriendsStore({ file });
  assert.deepEqual(running.activePubkeys(), []);
  new FriendsStore({ file }).add({ envName: 'uy-mac', pubkey: PK_A });
  // mtime resolution can be coarse: force a visible change
  fs.utimesSync(file, new Date(), new Date(Date.now() + 5000));
  assert.deepEqual(running.activePubkeys(), [PK_A]);
});

test('toPublic never exposes keys', () => {
  const store = new FriendsStore({ file: tmpFile() });
  store.add({ envName: 'jose-iphone', pubkey: PK_B, role: 'device' });
  const [pub] = store.toPublic();
  assert.equal('pubkey' in pub, false);
  assert.equal(pub.role, 'device');
});

test('NostrRelay trusts the store\'s active friends, live', () => {
  const store = new FriendsStore({ file: tmpFile() });
  const relay = new NostrRelay({ secretKey: new Uint8Array(32).fill(1), friends: store, knownPeerPubkeys: { legacy: PK_C } });
  relay._pubkey = 'self';
  assert.deepEqual(relay._trustedAuthors(), ['self', PK_C]);
  store.add({ envName: 'uy-mac', pubkey: PK_A });
  assert.deepEqual(relay._trustedAuthors(), ['self', PK_C, PK_A]);
  store.revoke('uy-mac');
  assert.deepEqual(relay._trustedAuthors(), ['self', PK_C]);
});
