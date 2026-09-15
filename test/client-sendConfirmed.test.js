import { test } from 'node:test';
import assert from 'node:assert/strict';
import { startBroker } from '../src/broker.js';
import { VortexiaClient } from '../src/client.js';

// sendConfirmed exists specifically because send() is fire-and-forget and
// mqtt.js silently QUEUES a QoS-1 publish (instead of erroring) when the
// client is disconnected but not .end()-ed — the exact shape of bug that
// let a DirectMqttTransport report success while a cross-machine message
// vanished. See client.js's doc comment and directTransport.js.

test('sendConfirmed resolves once the broker acks the publish', async () => {
  const broker = await startBroker();
  const sender = new VortexiaClient({ port: broker.mqttPort });
  const receiver = new VortexiaClient({ port: broker.mqttPort });
  await sender.register('Sender');
  await receiver.register('Receiver');

  try {
    const envelope = await sender.sendConfirmed('Receiver', 'hola', { timeoutMs: 1000 });
    assert.equal(envelope.text, 'hola');
    assert.equal(envelope.to, 'Receiver');
  } finally {
    await sender.close();
    await receiver.close();
    await broker.close();
  }
});

test('sendConfirmed rejects on timeout instead of hanging when the publish never acks', async () => {
  const client = new VortexiaClient();
  client.name = 'Ghost';
  // A stub whose publish callback is simply never called — reproduces
  // mqtt.js's real behavior of silently queuing a QoS-1 publish on a
  // half-dead connection (no error, no ack, ever) without needing an
  // actual dead socket in the test.
  client.mqttClient = { publish: () => {} };

  await assert.rejects(
    () => client.sendConfirmed('Someone', 'hola', { timeoutMs: 50 }),
    /no ack from broker within 50ms/,
  );
});

test('sendConfirmed rejects immediately if the broker itself errors the publish', async () => {
  const client = new VortexiaClient();
  client.name = 'Ghost';
  client.mqttClient = { publish: (topic, payload, opts, cb) => cb(new Error('not authorized')) };

  await assert.rejects(
    () => client.sendConfirmed('Someone', 'hola', { timeoutMs: 1000 }),
    /not authorized/,
  );
});
