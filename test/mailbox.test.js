import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { startBroker } from '../src/broker.js';
import { VortexiaClient, pollInbox, sessionState } from '../src/client.js';
import { inboxTopic, presenceTopic } from '../src/topics.js';
import mqtt from 'mqtt';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const tmpDir = () => fs.mkdtempSync(path.join(os.tmpdir(), 'vortexia-mailbox-'));

test('messages sent to an agent that has NEVER connected are queued and delivered, in order, to its first mailbox connect', async () => {
  const broker = await startBroker({ dataDir: tmpDir(), persist: false });
  const sender = new VortexiaClient({ port: broker.mqttPort });
  try {
    await sender.register('MbSender1');
    sender.send('MbNewcomer1', 'first');
    sender.send('MbNewcomer1', 'second');
    sender.send('MbNewcomer1', 'third');
    await sleep(100);
    assert.equal(broker.persistence.queuedFor('MbNewcomer1'), 3);

    const got = await pollInbox('MbNewcomer1', { port: broker.mqttPort, timeoutMs: 300 });
    assert.deepEqual(got.map((m) => m.text), ['first', 'second', 'third']);
    assert.ok(got.every((m) => typeof m.id === 'string' && m.id.length > 0), 'envelopes carry an id');

    // Consumed: a second poll finds nothing.
    const again = await pollInbox('MbNewcomer1', { port: broker.mqttPort, timeoutMs: 200 });
    assert.deepEqual(again, []);
    assert.equal(broker.persistence.queuedFor('MbNewcomer1'), 0);
  } finally {
    await sender.close();
    await broker.close();
  }
});

test('a viewer (clean session) sees live messages but consumes nothing — the mailbox still holds them', async () => {
  const broker = await startBroker({ dataDir: tmpDir(), persist: false });
  const sender = new VortexiaClient({ port: broker.mqttPort });
  const viewer = new VortexiaClient({ port: broker.mqttPort });
  try {
    await sender.register('MbSender2');
    const seen = [];
    viewer.on('message', (env, topic) => { if (topic === inboxTopic('MbWatched2')) seen.push(env.text); });
    await viewer.register('MbWatched2'); // default: viewer
    assert.equal(viewer.sessionPresent, false);

    sender.send('MbWatched2', 'hello');
    await sleep(100);
    assert.deepEqual(seen, ['hello']);
    assert.equal(broker.persistence.queuedFor('MbWatched2'), 1, 'still queued for the real consumer');

    const got = await pollInbox('MbWatched2', { port: broker.mqttPort, timeoutMs: 200 });
    assert.deepEqual(got.map((m) => m.text), ['hello']);
  } finally {
    await viewer.close();
    await sender.close();
    await broker.close();
  }
});

test('the mailbox session is present on reconnect; the queue survives a broker restart via the snapshot', async () => {
  const dataDir = tmpDir();
  let broker = await startBroker({ dataDir });
  let sender = new VortexiaClient({ port: broker.mqttPort });
  await sender.register('MbSender3');
  sender.send('MbSleeper3', 'before restart');
  await sleep(100);
  await sender.close();
  await broker.close(); // flushes the snapshot

  const snapshot = JSON.parse(fs.readFileSync(path.join(dataDir, 'mailboxes.json'), 'utf8'));
  assert.ok(snapshot.sessions['las-agent-MbSleeper3'], 'snapshot holds the sleeper mailbox');
  assert.equal(snapshot.sessions['las-agent-MbSleeper3'].queue.length, 1);

  broker = await startBroker({ dataDir });
  sender = new VortexiaClient({ port: broker.mqttPort });
  try {
    await sender.register('MbSender3b');
    sender.send('MbSleeper3', 'after restart');
    await sleep(100);

    const consumer = new VortexiaClient({ port: broker.mqttPort });
    const got = [];
    consumer.on('message', (env, topic) => { if (topic === inboxTopic('MbSleeper3')) got.push(env.text); });
    await consumer.register('MbSleeper3', { mailbox: true });
    assert.equal(consumer.sessionPresent, true, 'broker restored the session from the snapshot');
    await sleep(200);
    assert.deepEqual(got, ['before restart', 'after restart']);
    await consumer.close();
  } finally {
    await sender.close();
    await broker.close();
  }
});

test('cap: the oldest messages are dropped once a mailbox is full', async () => {
  const broker = await startBroker({ dataDir: tmpDir(), persist: false, mailbox: { cap: 3 } });
  const sender = new VortexiaClient({ port: broker.mqttPort });
  try {
    await sender.register('MbSender4');
    for (let i = 1; i <= 5; i++) sender.send('MbFull4', `m${i}`);
    await sleep(150);
    assert.equal(broker.persistence.queuedFor('MbFull4'), 3);
    const got = await pollInbox('MbFull4', { port: broker.mqttPort, timeoutMs: 200 });
    assert.deepEqual(got.map((m) => m.text), ['m3', 'm4', 'm5']);
    assert.equal(broker.persistence.stats.dropped, 2);
  } finally {
    await sender.close();
    await broker.close();
  }
});

test('ttl: a message older than the mailbox TTL is expired instead of delivered', async () => {
  const broker = await startBroker({ dataDir: tmpDir(), persist: false, mailbox: { ttlMs: 600 } });
  const sender = new VortexiaClient({ port: broker.mqttPort });
  try {
    await sender.register('MbSender5');
    sender.send('MbStale5', 'old news');
    await sleep(800);
    sender.send('MbStale5', 'fresh');
    await sleep(50);
    const got = await pollInbox('MbStale5', { port: broker.mqttPort, timeoutMs: 200, takeover: true });
    assert.deepEqual(got.map((m) => m.text), ['fresh']);
    assert.ok(broker.persistence.stats.expired >= 1);
  } finally {
    await sender.close();
    await broker.close();
  }
});

test('session topic: the broker publishes connected/disconnected for the mailbox consumer, and pollInbox yields to a live one', async () => {
  const broker = await startBroker({ dataDir: tmpDir(), persist: false });
  const sender = new VortexiaClient({ port: broker.mqttPort });
  try {
    await sender.register('MbSender6');
    assert.equal((await sessionState('MbListener6', { port: broker.mqttPort })).connected, false);

    const listener = new VortexiaClient({ port: broker.mqttPort });
    const heard = [];
    listener.on('message', (env, topic) => { if (topic === inboxTopic('MbListener6')) heard.push(env.text); });
    await listener.register('MbListener6', { mailbox: true });
    await sleep(50);
    assert.equal((await sessionState('MbListener6', { port: broker.mqttPort })).connected, true);

    sender.send('MbListener6', 'live one');
    await sleep(100);
    assert.deepEqual(heard, ['live one']);

    // A poll while the listener holds the session must not kick it.
    const polled = await pollInbox('MbListener6', { port: broker.mqttPort, timeoutMs: 100 });
    assert.deepEqual(polled, []);
    assert.equal(listener.mqttClient.connected, true, 'listener was not taken over');

    await listener.close();
    await sleep(50);
    assert.equal((await sessionState('MbListener6', { port: broker.mqttPort })).connected, false);
  } finally {
    await sender.close();
    await broker.close();
  }
});

test('broadcast is not queued for offline mailboxes', async () => {
  const broker = await startBroker({ dataDir: tmpDir(), persist: false });
  const sender = new VortexiaClient({ port: broker.mqttPort });
  try {
    await sender.register('MbSender7');
    // Create the mailbox session by delivering one direct message and draining it.
    sender.send('MbQuiet7', 'direct');
    await sleep(50);
    await pollInbox('MbQuiet7', { port: broker.mqttPort, timeoutMs: 150 });

    sender.send('broadcast', 'everyone!');
    await sleep(100);
    const got = await pollInbox('MbQuiet7', { port: broker.mqttPort, timeoutMs: 150 });
    assert.deepEqual(got, []);
  } finally {
    await sender.close();
    await broker.close();
  }
});

test('pollInbox does not touch presence: an agent registered "online" still reads online after a poll', async () => {
  const broker = await startBroker({ dataDir: tmpDir(), persist: false });
  const registrar = mqtt.connect(`mqtt://localhost:${broker.mqttPort}`, { clientId: 'reg-' + Math.random().toString(16).slice(2), reconnectPeriod: 0 });
  await new Promise((r, j) => { registrar.once('connect', r); registrar.once('error', j); });
  try {
    // What `las agent register` does: a one-shot retained "online".
    await new Promise((r) => registrar.publish(presenceTopic('MbPresent8'), 'online', { qos: 1, retain: true }, r));
    await pollInbox('MbPresent8', { port: broker.mqttPort, timeoutMs: 100, takeover: true });
    await sleep(100);

    const presence = await new Promise((resolve) => {
      registrar.on('message', (topic, payload) => { if (topic === presenceTopic('MbPresent8')) resolve(payload.toString()); });
      registrar.subscribe(presenceTopic('MbPresent8'), { qos: 1 });
    });
    assert.equal(presence, 'online');
  } finally {
    registrar.end(true);
    await broker.close();
  }
});
