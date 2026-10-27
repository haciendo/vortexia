import { test } from 'node:test';
import assert from 'node:assert/strict';
import { startBroker } from '../src/broker.js';
import { VortexiaClient } from '../src/client.js';
import { VortexRelayBridge, VORTEX_RELAY_DIRECT_KIND } from '../src/vortex-relay/bridge.js';
import { InMemoryRelay } from '../src/vortex-relay/relay.js';

// Point-to-point delivery — the "las agent inject <exact name>, wherever it
// lives" path — as opposed to vortex-relay-bridge.test.js, which covers the
// no-named-recipient, scope-matched path. Three environments, matching
// José's "design for N, not just 2" requirement (a third machine is
// expected to join the real deployment).

// Pinned, incrementing ports — see the same comment in
// vortex-relay-bridge.test.js: these tests run several brokers concurrently
// within one process and must never fall back to the registry's
// auto-assign, which would hit the real, system-wide local-agent-society
// port registry and race/collide with whatever vortexia instance is
// actually in production.
let nextTestPort = 20300;
function pinnedPorts() {
  const mqttPort = nextTestPort++;
  const wsPort = nextTestPort++;
  return { mqttPort, wsPort };
}

async function setupEnv(envName, agents, relay) {
  const broker = await startBroker(pinnedPorts());
  const clients = {};
  for (const agentName of Object.keys(agents)) {
    const c = new VortexiaClient({ port: broker.mqttPort });
    await c.register(agentName);
    clients[agentName] = c;
  }

  const gateway = new VortexiaClient({ port: broker.mqttPort });
  await gateway.register(`${envName}-gateway`);

  const bridge = new VortexRelayBridge({ envName, relay, envNames: ['mac-1', 'mac-2', 'mac-3'] }).attach(gateway);
  await bridge.publishSelf(Object.entries(agents).map(([agentName, scopeText]) => ({ agentName, scopeText })));
  await bridge.syncDirectory();
  bridge.startPolling(50);

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
    env.bridge.stopPolling();
    env.bridge.stopDirectorySync?.();
  }
  await Promise.all(
    envs.flatMap((env) => [
      ...Object.values(env.clients).map((c) => c.close()),
      env.gateway.close(),
      env.broker.close(),
    ]),
  );
}

test('exact-name delivery crosses to the owning environment, bypassing scope matching', async () => {
  const relay = new InMemoryRelay();
  const a = await setupEnv('mac-1', { Robotics: 'brazos roboticos' }, relay);
  const b = await setupEnv('mac-2', { Facturas: 'facturacion, pagos' }, relay);
  await a.bridge.syncDirectory();

  try {
    const got = waitForMessage(b.clients.Facturas, (e) => e.kind === 'vortex-relay-delivery');

    a.gateway.send('mac-1-gateway', 'necesito ver la factura de este mes', {
      kind: VORTEX_RELAY_DIRECT_KIND,
      from: 'Robotics',
      to: 'Facturas',
    });

    const msg = await got;
    assert.equal(msg.text, 'necesito ver la factura de este mes');
    assert.equal(msg.from, 'Robotics@mac-1', 'a remote sender arrives qualified so it never reads as a local name');
    assert.equal(msg.routedFrom, 'mac-1');
  } finally {
    await teardown(a, b);
  }
});

test('exact-name delivery: a bare name that collides across environments resolves to the LOCAL one', async () => {
  const relay = new InMemoryRelay();
  const a = await setupEnv('mac-1', { System: 'sysadmin de mac-1' }, relay);
  const b = await setupEnv('mac-2', { System: 'sysadmin de mac-2' }, relay);
  await a.bridge.syncDirectory();
  await b.bridge.syncDirectory();

  try {
    const localGot = waitForMessage(a.clients.System, (e) => e.kind === 'vortex-relay-delivery');
    let remoteGotAnything = false;
    b.clients.System.on('message', () => { remoteGotAnything = true; });

    a.gateway.send('mac-1-gateway', 'reiniciá el servicio', {
      kind: VORTEX_RELAY_DIRECT_KIND,
      from: 'Robotics',
      to: 'System',
    });

    const msg = await localGot;
    assert.equal(msg.text, 'reiniciá el servicio');

    await new Promise((r) => setTimeout(r, 150));
    assert.equal(remoteGotAnything, false, 'the OTHER mac\'s System must never receive a bare-name send that resolved locally');
  } finally {
    await teardown(a, b);
  }
});

test('exact-name delivery: a bare name colliding across >1 REMOTE environments replies with an ambiguity error, never guesses', async () => {
  const relay = new InMemoryRelay();
  const asker = await setupEnv('mac-1', { Robotics: 'brazos' }, relay);
  const b = await setupEnv('mac-2', { System: 'sysadmin de mac-2' }, relay);
  const c = await setupEnv('mac-3', { System: 'sysadmin de mac-3' }, relay);
  await asker.bridge.syncDirectory();

  try {
    const errorGot = waitForMessage(asker.clients.Robotics, (e) => e.kind === 'vortex-relay-direct-error');
    let anyoneGotDelivery = false;
    b.clients.System.on('message', (e) => { if (e.kind === 'vortex-relay-delivery') anyoneGotDelivery = true; });
    c.clients.System.on('message', (e) => { if (e.kind === 'vortex-relay-delivery') anyoneGotDelivery = true; });

    asker.gateway.send('mac-1-gateway', 'reiniciá el servicio', {
      kind: VORTEX_RELAY_DIRECT_KIND,
      from: 'Robotics',
      to: 'System',
    });

    const err = await errorGot;
    assert.match(err.text, /System@mac-2/);
    assert.match(err.text, /System@mac-3/);

    await new Promise((r) => setTimeout(r, 150));
    assert.equal(anyoneGotDelivery, false, 'an ambiguous name must never be silently delivered to either candidate');
  } finally {
    await teardown(asker, b, c);
  }
});

test('exact-name delivery: an env-qualified name (name@env) resolves exactly even with a collision', async () => {
  const relay = new InMemoryRelay();
  const asker = await setupEnv('mac-1', { Robotics: 'brazos' }, relay);
  const b = await setupEnv('mac-2', { System: 'sysadmin de mac-2' }, relay);
  const c = await setupEnv('mac-3', { System: 'sysadmin de mac-3' }, relay);
  await asker.bridge.syncDirectory();

  try {
    const got = waitForMessage(c.clients.System, (e) => e.kind === 'vortex-relay-delivery');
    let bGotDelivery = false;
    b.clients.System.on('message', (e) => { if (e.kind === 'vortex-relay-delivery') bGotDelivery = true; });

    asker.gateway.send('mac-1-gateway', 'reiniciá el servicio', {
      kind: VORTEX_RELAY_DIRECT_KIND,
      from: 'Robotics',
      to: 'System@mac-3',
    });

    const msg = await got;
    assert.equal(msg.text, 'reiniciá el servicio');

    await new Promise((r) => setTimeout(r, 150));
    assert.equal(bGotDelivery, false);
  } finally {
    await teardown(asker, b, c);
  }
});

test('exact-name delivery: a session selector (name@env/<session>) routes on name@env and arrives as envelope.session', async () => {
  const relay = new InMemoryRelay();
  const asker = await setupEnv('mac-1', { Robotics: 'brazos' }, relay);
  const b = await setupEnv('mac-2', { System: 'sysadmin de mac-2' }, relay);
  await asker.bridge.syncDirectory();

  try {
    const got = waitForMessage(b.clients.System, (e) => e.kind === 'vortex-relay-delivery');
    asker.gateway.send('mac-1-gateway', 'uptime', {
      kind: VORTEX_RELAY_DIRECT_KIND,
      from: 'Robotics',
      to: 'System@mac-2/shell',
    });
    const msg = await got;
    assert.equal(msg.text, 'uptime');
    assert.equal(msg.to, 'System');
    assert.equal(msg.session, 'shell');
  } finally {
    await teardown(asker, b);
  }
});

test('exact-name delivery: an unknown name replies with a not-found error', async () => {
  const relay = new InMemoryRelay();
  const asker = await setupEnv('mac-1', { Robotics: 'brazos' }, relay);
  await asker.bridge.syncDirectory();

  try {
    const errorGot = waitForMessage(asker.clients.Robotics, (e) => e.kind === 'vortex-relay-direct-error');

    asker.gateway.send('mac-1-gateway', 'hola', {
      kind: VORTEX_RELAY_DIRECT_KIND,
      from: 'Robotics',
      to: 'Ghost',
    });

    const err = await errorGot;
    assert.match(err.text, /no agent named "Ghost"/);
  } finally {
    await teardown(asker);
  }
});

test('a delivery received from X@E teaches the bridge X@E, so a reply resolves even when E\'s directory is missing X', async () => {
  const relay = new InMemoryRelay();
  const a = await setupEnv('mac-1', { Alpha: 'alpha' }, relay);
  const b = await setupEnv('mac-2', { Beta: 'beta' }, relay);
  // Ghost never published a directory entry on mac-1: resolving Ghost@mac-1
  // from mac-2 would be not-found without the learned entry.
  try {
    assert.equal(b.bridge._resolveEntries().some((e) => e.agentName === 'Ghost'), false);
    b.bridge._learnSender('Ghost', 'mac-1');
    const resolved = b.bridge._resolveEntries().find((e) => e.agentName === 'Ghost' && e.envName === 'mac-1');
    assert.ok(resolved, 'learned entry is searched by resolution');
    b.bridge._learned.get('Ghost@mac-1').expires = Date.now() - 1;
    assert.equal(b.bridge._resolveEntries().some((e) => e.agentName === 'Ghost'), false, 'expired entries drop out');
  } finally {
    await teardown(a, b);
  }
});

test('startDirectorySync re-reads a roster function every tick and republishes only on change', async () => {
  const relay = new InMemoryRelay();
  const a = await setupEnv('mac-1', { Alpha: 'alpha' }, relay);
  const published = [];
  a.bridge.publishSelf = async (roster) => { published.push(roster.map((r) => r.agentName)); };
  let roster = [];
  a.bridge.startDirectorySync(async () => roster, { publishIntervalMs: 3600000, syncIntervalMs: 30 });
  try {
    await new Promise((r) => setTimeout(r, 120));
    roster = [{ agentName: 'Late', scopeText: 'late' }];
    await new Promise((r) => setTimeout(r, 150));
    const lateCount = published.filter((p) => p.includes('Late')).length;
    assert.equal(lateCount, 1, 'late agent is published once, not on every tick');
  } finally {
    a.bridge.stopDirectorySync();
    await teardown(a);
  }
});
