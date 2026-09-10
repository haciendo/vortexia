import { test } from 'node:test';
import assert from 'node:assert/strict';
import { startBroker } from '../src/broker.js';
import { VortexiaClient } from '../src/client.js';
import { FederationBridge, FEDERATION_KIND } from '../src/federation/bridge.js';
import { InMemoryRelay } from '../src/federation/relay.js';
import { bagOfWordsEmbedder } from '../src/federation/embeddings.js';

// Same directory used in federation-router.test.js: env-a/env-c are both
// "weather", env-b is "billing" — lets one test prove fan-out (one message
// reaching two environments) and another prove exclusivity (reaching only
// one), same as the router unit tests, but now end-to-end across three
// REAL, separate MQTT brokers that only share the InMemoryRelay — nothing
// about routing "knows" these are meant to simulate three different
// machines; the bridges only ever talk to each other through the relay,
// exactly like the real cross-machine (Gist-backed) version would.
const directory = [
  { envName: 'env-a', agentName: 'Clima', scopeText: 'pronostico del tiempo, temperatura, lluvia, viento' },
  { envName: 'env-b', agentName: 'Facturas', scopeText: 'facturacion, factura, pagos, pagar, cobros, dinero' },
  { envName: 'env-c', agentName: 'Meteo', scopeText: 'clima, tiempo, temperatura, humedad, viento, pronostico' },
];

async function setupEnv(envName, agentName, relay) {
  const broker = await startBroker();
  const agentClient = new VortexiaClient({ port: broker.mqttPort });
  await agentClient.register(agentName);

  const gateway = new VortexiaClient({ port: broker.mqttPort });
  await gateway.register(`${envName}-gateway`);

  const bridge = new FederationBridge({
    envName,
    relay,
    directory,
    matchOpts: { embedder: bagOfWordsEmbedder, closeness: 0.6 },
  }).attach(gateway);
  bridge.startPolling(50);

  return { broker, agentClient, gateway, bridge };
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

test('a federated weather intent reaches env-a AND env-c, three independent brokers joined only by the relay', async () => {
  const relay = new InMemoryRelay();
  const a = await setupEnv('env-a', 'Clima', relay);
  const b = await setupEnv('env-b', 'Facturas', relay);
  const c = await setupEnv('env-c', 'Meteo', relay);

  try {
    const climaGot = waitForMessage(a.agentClient, (e) => e.kind === 'federation-delivery');
    const meteoGot = waitForMessage(c.agentClient, (e) => e.kind === 'federation-delivery');
    let facturasGotAnything = false;
    b.agentClient.on('message', () => { facturasGotAnything = true; });

    // Broadcast the intent on env-a's own broker — as if a human/agent in
    // that environment asked for help with no named recipient.
    a.gateway.send('broadcast', 'pronostico del tiempo y viento para mañana', {
      kind: FEDERATION_KIND,
      intent: 'pronostico del tiempo y viento para mañana',
    });

    const [climaMsg, meteoMsg] = await Promise.all([climaGot, meteoGot]);
    assert.equal(climaMsg.text, 'pronostico del tiempo y viento para mañana');
    assert.equal(meteoMsg.text, 'pronostico del tiempo y viento para mañana');
    assert.equal(meteoMsg.routedFrom, 'env-a', 'env-c received it via the relay, routed from env-a');

    await new Promise((r) => setTimeout(r, 150));
    assert.equal(facturasGotAnything, false, 'the billing agent should never see the weather intent');
  } finally {
    a.bridge.stopPolling();
    b.bridge.stopPolling();
    c.bridge.stopPolling();
    await Promise.all([
      a.agentClient.close(), a.gateway.close(), a.broker.close(),
      b.agentClient.close(), b.gateway.close(), b.broker.close(),
      c.agentClient.close(), c.gateway.close(), c.broker.close(),
    ]);
  }
});

test('a federated billing intent reaches only env-b, even though it started on env-c', async () => {
  const relay = new InMemoryRelay();
  const a = await setupEnv('env-a', 'Clima', relay);
  const b = await setupEnv('env-b', 'Facturas', relay);
  const c = await setupEnv('env-c', 'Meteo', relay);

  try {
    const facturasGot = waitForMessage(b.agentClient, (e) => e.kind === 'federation-delivery');
    let othersGotAnything = false;
    const markDelivery = (e) => { if (e.kind === 'federation-delivery') othersGotAnything = true; };
    a.agentClient.on('message', markDelivery);
    c.agentClient.on('message', markDelivery);

    // This time the request originates on env-c, a DIFFERENT environment
    // than the one that ends up handling it — proving routing is by scope
    // match, not "whichever environment happened to ask."
    c.gateway.send('broadcast', 'necesito pagar mi factura', {
      kind: FEDERATION_KIND,
      intent: 'quiero pagar mi factura de este mes',
    });

    const facturasMsg = await facturasGot;
    assert.equal(facturasMsg.text, 'necesito pagar mi factura');
    assert.equal(facturasMsg.routedFrom, 'env-c');

    await new Promise((r) => setTimeout(r, 150));
    assert.equal(othersGotAnything, false, 'weather agents should never see the billing intent');
  } finally {
    a.bridge.stopPolling();
    b.bridge.stopPolling();
    c.bridge.stopPolling();
    await Promise.all([
      a.agentClient.close(), a.gateway.close(), a.broker.close(),
      b.agentClient.close(), b.gateway.close(), b.broker.close(),
      c.agentClient.close(), c.gateway.close(), c.broker.close(),
    ]);
  }
});
