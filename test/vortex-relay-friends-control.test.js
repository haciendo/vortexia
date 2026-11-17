// Friends control over MQTT (ADR 0001 §10): revoke/rename by request/response.
import './helpers/isolate.js';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import mqtt from 'mqtt';
import { startBroker } from '../src/broker.js';
import { FriendsStore } from '../src/vortex-relay/friends.js';
import { handleFriendsControl, startFriendsControl, FRIENDS_REVOKE_TOPIC, FRIENDS_RENAME_TOPIC } from '../src/vortex-relay/control.js';

const PK = 'd'.repeat(64);
const store = () => new FriendsStore({ file: path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'vx-ctl-')), 'friends.json') });

test('handleFriendsControl: revoke, rename, unknown friend, bad payload', () => {
  const s = store();
  s.add({ envName: 'jose-iphone', pubkey: PK, role: 'device' });
  assert.deepEqual(handleFriendsControl(s, FRIENDS_RENAME_TOPIC, { id: 1, envName: 'jose-iphone', label: 'iPhone' }), { id: 1, ok: true, envName: 'jose-iphone', label: 'iPhone' });
  assert.deepEqual(handleFriendsControl(s, FRIENDS_REVOKE_TOPIC, { id: 2, envName: 'jose-iphone' }), { id: 2, ok: true, envName: 'jose-iphone' });
  assert.equal(s.byPubkey(PK), null);
  assert.deepEqual(handleFriendsControl(s, FRIENDS_REVOKE_TOPIC, { id: 3, envName: 'nadie' }), { id: 3, ok: false, error: 'unknown friend' });
  assert.equal(handleFriendsControl(s, FRIENDS_REVOKE_TOPIC, null).ok, false);
});

test('revoke over the broker answers on /result and calls onChange', async () => {
  const broker = await startBroker();
  const s = store();
  s.add({ envName: 'jose-iphone', pubkey: PK, role: 'device' });
  let changed = 0;
  const stop = startFriendsControl({ mqttPort: broker.mqttPort, store: s, onChange: () => changed++, logger: {} });
  const ui = mqtt.connect(`mqtt://127.0.0.1:${broker.mqttPort}`);
  try {
    await new Promise((r) => ui.on('connect', r));
    await ui.subscribeAsync(`${FRIENDS_REVOKE_TOPIC}/result`, { qos: 1 });
    const answer = await new Promise((resolve) => {
      ui.on('message', (_t, m) => resolve(JSON.parse(m.toString())));
      // the control client may still be subscribing: retry until answered
      const send = () => ui.publish(FRIENDS_REVOKE_TOPIC, JSON.stringify({ id: 'r1', envName: 'jose-iphone' }), { qos: 1 });
      send();
      const t = setInterval(send, 300);
      ui.once('message', () => clearInterval(t));
    });
    assert.equal(answer.id, 'r1');
    assert.ok(changed >= 1);
    assert.equal(s.byPubkey(PK), null);
  } finally {
    ui.end(true);
    stop();
    await broker.close();
  }
});
