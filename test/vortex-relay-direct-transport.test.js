import { test } from 'node:test';
import assert from 'node:assert/strict';
import { startBroker } from '../src/broker.js';
import { VortexiaClient } from '../src/client.js';
import { VortexRelayBridge, VORTEX_RELAY_DIRECT_KIND } from '../src/vortex-relay/bridge.js';
import { DirectMqttTransport } from '../src/vortex-relay/directTransport.js';
import { InMemoryRelay } from '../src/vortex-relay/relay.js';

// Direct transport (the "LAN, no lake" medium): a live connection to a
// peer's real broker, preferred over the relay when available (via
// ConnectionRouter — see connection.js), with per-env pin overrides that
// lock to one Connection by name. See directTransport.js and bridge.js's
// transportPins doc for the design.

let nextTestPort = 20500;
function pinnedPorts() {
  const mqttPort = nextTestPort++;
  const wsPort = nextTestPort++;
  return { mqttPort, wsPort };
}

async function setupEnv(envName, agents, relay, bridgeOpts = {}) {
  const broker = await startBroker(pinnedPorts());
  const clients = {};
  for (const agentName of Object.keys(agents)) {
    const c = new VortexiaClient({ port: broker.mqttPort });
    await c.register(agentName);
    clients[agentName] = c;
  }

  const gateway = new VortexiaClient({ port: broker.mqttPort });
  await gateway.register(`${envName}-gateway`);

  const bridge = new VortexRelayBridge({ envName, relay, envNames: ['mac-1', 'mac-2'], ...bridgeOpts }).attach(gateway);
  await bridge.publishSelf(Object.entries(agents).map(([agentName, scopeText]) => ({ agentName, scopeText })));
  await bridge.syncDirectory();

  return { broker, clients, gateway, bridge };
}

function waitForMessage(client, predicate, timeoutMs = 2000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      client.removeListener('message', onMessage);
      reject(new Error('timed out waiting for message'));
    }, timeoutMs);
    function onMessage(envelope) {
      if (predicate(envelope)) {
        clearTimeout(timer);
        client.removeListener('message', onMessage);
        resolve(envelope);
      }
    }
    client.on('message', onMessage);
  });
}

async function teardown(...envs) {
  for (const env of envs) {
    env.bridge.stopPolling?.();
    env.bridge.stopDirectorySync?.();
    await Promise.all([...env._directTransportsToClose ?? []].map((t) => t.disconnect()));
  }
  await Promise.all(
    envs.flatMap((env) => [
      ...Object.values(env.clients).map((c) => c.close()),
      env.gateway.close(),
      env.broker.close(),
    ]),
  );
}

test('attachDirectTransport: exact-name delivery goes straight to the peer broker, never touching the relay', async () => {
  let appendCalls = 0;
  const relay = new InMemoryRelay();
  const realAppend = relay.appendMessage.bind(relay);
  relay.appendMessage = (...args) => { appendCalls++; return realAppend(...args); };

  const a = await setupEnv('mac-1', { Robotics: 'brazos' }, relay);
  const b = await setupEnv('mac-2', { Facturas: 'facturacion' }, relay);
  await a.bridge.syncDirectory();
  a._directTransportsToClose = [];

  try {
    const direct = new DirectMqttTransport({ envName: 'mac-2', host: 'localhost', mqttPort: b.broker.mqttPort });
    await a.bridge.attachDirectTransport('mac-2', direct);
    a._directTransportsToClose.push(direct);

    const got = waitForMessage(b.clients.Facturas, (e) => e.kind === 'vortex-relay-delivery');
    a.gateway.send('mac-1-gateway', 'ver factura', { kind: VORTEX_RELAY_DIRECT_KIND, from: 'Robotics', to: 'Facturas' });

    const msg = await got;
    assert.equal(msg.text, 'ver factura');
    assert.equal(msg.transport, 'lan-direct');
    assert.equal(appendCalls, 0, 'a live direct transport must be preferred over the relay, not just tried first');
  } finally {
    await teardown(a, b);
  }
});

test('auto mode falls back to the relay when the direct transport send throws', async () => {
  const relay = new InMemoryRelay();
  const a = await setupEnv('mac-1', { Robotics: 'brazos' }, relay);
  const b = await setupEnv('mac-2', { Facturas: 'facturacion' }, relay);
  await a.bridge.syncDirectory();
  b.bridge.startPolling(30);

  try {
    const flaky = { name: 'flaky', connected: true, send: () => { throw new Error('socket reset'); } };
    a.bridge._directTransports.set('mac-2', flaky);

    const got = waitForMessage(b.clients.Facturas, (e) => e.kind === 'vortex-relay-delivery');
    a.gateway.send('mac-1-gateway', 'ver factura', { kind: VORTEX_RELAY_DIRECT_KIND, from: 'Robotics', to: 'Facturas' });

    const msg = await got;
    assert.equal(msg.text, 'ver factura');
    assert.equal(msg.transport, 'unknown', 'delivered via the relay-fed outbox, not the direct transport');
  } finally {
    b.bridge.stopPolling();
    await teardown(a, b);
  }
});

test('DirectMqttTransport.send rejects (bounded by timeoutMs) instead of hanging when the underlying publish never acks', async () => {
  // Reproduces the exact live bug: a connection that still LOOKS connected
  // (register() succeeded, no 'close' event fired yet) but whose publish
  // silently vanishes into mqtt.js's internal queue — see client.js's
  // sendConfirmed doc comment. Before the fix, DirectMqttTransport.send
  // was fire-and-forget and would have resolved as if it succeeded.
  class StuckClient {
    async register() { return this; }
    on() {}
    async sendConfirmed(toName, text, { timeoutMs = 3000 } = {}) {
      return new Promise((_, reject) => setTimeout(() => reject(new Error(`sendConfirmed: no ack from broker within ${timeoutMs}ms — connection is likely dead`)), timeoutMs));
    }
  }

  const transport = new DirectMqttTransport({ envName: 'mac-2', host: 'localhost', mqttPort: 1, ClientImpl: StuckClient });
  await transport.connect();

  await assert.rejects(
    () => transport.send('Facturas', 'hola', { timeoutMs: 50 }),
    /no ack from broker within 50ms/,
  );
});

test('transportPins pinned to a named connection that has no live match: drops (never falls back to the relay)', async () => {
  const relay = new InMemoryRelay();
  const realAppend = relay.appendMessage.bind(relay);
  let appendCalls = 0;
  relay.appendMessage = (...args) => { appendCalls++; return realAppend(...args); };

  const a = await setupEnv('mac-1', { Robotics: 'brazos' }, relay, { transportPins: { 'mac-2': 'lan-direct' } });

  try {
    a.gateway.send('mac-1-gateway', 'ver factura', { kind: VORTEX_RELAY_DIRECT_KIND, from: 'Robotics', to: 'Facturas@mac-2' });
    await new Promise((r) => setTimeout(r, 200));
    assert.equal(appendCalls, 0, 'a pin to an unavailable connection must never fall back to the relay');
  } finally {
    await teardown(a);
  }
});

test('transportPins pinned to "relay": ignores a live, connected direct transport', async () => {
  const relay = new InMemoryRelay();
  const a = await setupEnv('mac-1', { Robotics: 'brazos' }, relay, { transportPins: { 'mac-2': 'relay' } });
  const b = await setupEnv('mac-2', { Facturas: 'facturacion' }, relay);
  await a.bridge.syncDirectory();
  b.bridge.startPolling(30);

  try {
    let directSendCalled = false;
    a.bridge._directTransports.set('mac-2', { name: 'lan-direct', connected: true, send: () => { directSendCalled = true; } });

    const got = waitForMessage(b.clients.Facturas, (e) => e.kind === 'vortex-relay-delivery');
    a.gateway.send('mac-1-gateway', 'ver factura', { kind: VORTEX_RELAY_DIRECT_KIND, from: 'Robotics', to: 'Facturas' });

    await got;
    assert.equal(directSendCalled, false, 'relay-only must never call the attached direct transport');
  } finally {
    b.bridge.stopPolling();
    await teardown(a, b);
  }
});
