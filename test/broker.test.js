import { test } from 'node:test';
import assert from 'node:assert/strict';
import { startBroker } from '../src/broker.js';
import { VortexiaClient } from '../src/client.js';
import { presenceTopic } from '../src/topics.js';
import mqtt from 'mqtt';

const REGISTRY_URL = process.env.LAS_REGISTRY_URL || 'http://localhost:8700';

async function registryReachable() {
  try {
    const res = await fetch(`${REGISTRY_URL}/ports`);
    return res.ok;
  } catch {
    return false;
  }
}

test('broker starts and claims ports from the registry (or degrades gracefully)', async () => {
  const broker = await startBroker();
  try {
    assert.ok(Number.isInteger(broker.mqttPort));
    assert.ok(Number.isInteger(broker.wsPort));
    assert.notEqual(broker.mqttPort, broker.wsPort);

    if (await registryReachable()) {
      const res = await fetch(`${REGISTRY_URL}/ports`);
      const ports = await res.json();
      assert.equal(ports[String(broker.mqttPort)]?.app, 'vortexia-mqtt');
      assert.equal(ports[String(broker.wsPort)]?.app, 'vortexia-ws');
    }
  } finally {
    // Spy on the DELETE calls vortexia's own close() issues, rather than
    // re-reading the shared registry file afterward: this machine runs
    // several other agents that write to that same JSON file concurrently
    // (no lock on the delete path), so a fresh GET can occasionally show a
    // port we just released as still present — a registry-side race, not
    // a vortexia bug. Asserting on our own DELETE requests/responses is a
    // deterministic way to confirm we *do* release cleanly on shutdown.
    const deletes = [];
    const origFetch = global.fetch;
    global.fetch = async (url, opts) => {
      const res = await origFetch(url, opts);
      if (opts?.method === 'DELETE') deletes.push({ url: String(url), status: res.status });
      return res;
    };
    try {
      await broker.close();
    } finally {
      global.fetch = origFetch;
    }

    if (await registryReachable()) {
      const deletedUrls = deletes.map((d) => d.url);
      assert.ok(deletedUrls.some((u) => u.endsWith(`/ports/${broker.mqttPort}`)));
      assert.ok(deletedUrls.some((u) => u.endsWith(`/ports/${broker.wsPort}`)));
      assert.ok(deletes.every((d) => d.status === 200 || d.status === 404));
    }
  }
});

test('two clients can exchange a direct message', async () => {
  const broker = await startBroker();
  const alice = new VortexiaClient({ port: broker.mqttPort });
  const bob = new VortexiaClient({ port: broker.mqttPort });

  try {
    await alice.register('Alice');
    await bob.register('Bob');

    const received = new Promise((resolve) => {
      alice.once('message', (envelope, topic) => resolve({ envelope, topic }));
    });

    bob.send('Alice', 'hello from bob', { source: 'agent' });

    const { envelope, topic } = await received;
    assert.equal(envelope.from, 'Bob');
    assert.equal(envelope.to, 'Alice');
    assert.equal(envelope.text, 'hello from bob');
    assert.equal(envelope.source, 'agent');
    assert.ok(Number.isInteger(envelope.ts));
    assert.equal(topic, 'las/agent/Alice/inbox');
  } finally {
    await alice.close();
    await bob.close();
    await broker.close();
  }
});

test('broadcast reaches all subscribed agents', async () => {
  const broker = await startBroker();
  const alice = new VortexiaClient({ port: broker.mqttPort });
  const bob = new VortexiaClient({ port: broker.mqttPort });

  try {
    await alice.register('Alice2');
    await bob.register('Bob2');

    const aliceGot = new Promise((resolve) => alice.once('message', resolve));
    const bobGot = new Promise((resolve) => bob.once('message', resolve));

    alice.send('broadcast', 'hi everyone');

    const [a, b] = await Promise.all([aliceGot, bobGot]);
    assert.equal(a.text, 'hi everyone');
    assert.equal(b.text, 'hi everyone');
  } finally {
    await alice.close();
    await bob.close();
    await broker.close();
  }
});

test('inbox messages are retained: a subscriber that connects AFTER the send still receives it', async () => {
  const broker = await startBroker();
  const sender = new VortexiaClient({ port: broker.mqttPort });

  try {
    await sender.register('Sender3');
    sender.send('LateJoiner', 'were you listening?', { source: 'human' });

    // Give the publish a moment to land, then connect the receiver — this
    // is the whole point: NOT subscribed at send time, unlike the other
    // direct-message test above.
    await new Promise((r) => setTimeout(r, 100));

    const receiver = new VortexiaClient({ port: broker.mqttPort });
    const received = new Promise((resolve) => receiver.once('message', resolve));
    await receiver.register('LateJoiner');

    const envelope = await received;
    assert.equal(envelope.text, 'were you listening?');
    await receiver.close();
  } finally {
    await sender.close();
    await broker.close();
  }
});

test('broadcast is NOT retained: a subscriber that connects after a broadcast does not receive it', async () => {
  const broker = await startBroker();
  const sender = new VortexiaClient({ port: broker.mqttPort });

  try {
    await sender.register('Sender4');
    sender.send('broadcast', 'anyone home?');
    await new Promise((r) => setTimeout(r, 100));

    const receiver = new VortexiaClient({ port: broker.mqttPort });
    let gotSomething = false;
    receiver.on('message', () => { gotSomething = true; });
    await receiver.register('LateJoiner2');
    await new Promise((r) => setTimeout(r, 200));

    assert.equal(gotSomething, false, 'a late-joining subscriber should not receive a stale broadcast');
    await receiver.close();
  } finally {
    await sender.close();
    await broker.close();
  }
});

test('presence LWT fires offline on an abrupt disconnect', async () => {
  const broker = await startBroker();
  const watcher = mqtt.connect(`mqtt://localhost:${broker.mqttPort}`, {
    clientId: `watcher-${Math.random().toString(16).slice(2)}`,
    reconnectPeriod: 0,
  });

  try {
    await new Promise((resolve, reject) => {
      watcher.once('connect', resolve);
      watcher.once('error', reject);
    });

    const ghost = new VortexiaClient({ port: broker.mqttPort });
    await ghost.register('Ghost');

    const onlineMsg = await new Promise((resolve, reject) => {
      watcher.subscribe(presenceTopic('Ghost'), { qos: 1 }, (err) => {
        if (err) reject(err);
      });
      watcher.on('message', (topic, payload) => {
        if (topic === presenceTopic('Ghost')) resolve(payload.toString());
      });
    });
    assert.equal(onlineMsg, 'online');

    const offlinePromise = new Promise((resolve) => {
      const handler = (topic, payload) => {
        if (topic === presenceTopic('Ghost') && payload.toString() === 'offline') {
          watcher.removeListener('message', handler);
          resolve();
        }
      };
      watcher.on('message', handler);
    });

    // Simulate a crash: destroy the underlying socket without sending
    // a proper MQTT DISCONNECT, so the broker fires the Last Will.
    ghost.mqttClient.stream.destroy();

    await offlinePromise;
  } finally {
    watcher.end(true);
    await broker.close();
  }
});
