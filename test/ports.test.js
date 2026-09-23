import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import net from 'node:net';
import { startBroker, MQTT_APP, WS_APP } from '../src/broker.js';
import { readStickyPorts } from '../src/ports.js';

const tmpDir = () => fs.mkdtempSync(path.join(os.tmpdir(), 'vortexia-ports-'));

/**
 * A tiny stand-in for the local-agent-society port registry: same three
 * endpoints vortexia uses (GET/POST /ports, DELETE /ports/:port,
 * POST /ports/claim) over a JSON object in memory. `listen()` can be
 * called late to simulate the registry coming up after the broker.
 */
function fakeRegistry() {
  const ports = {};
  const server = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', () => {
      const json = (status, data) => { res.writeHead(status, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(data)); };
      if (req.method === 'GET' && req.url === '/ports') return json(200, ports);
      if (req.method === 'POST' && req.url === '/ports') {
        const reg = JSON.parse(body);
        ports[String(reg.port)] = { ...reg, registered_at: new Date().toISOString() };
        return json(200, { ok: true, port: reg.port });
      }
      if (req.method === 'DELETE' && req.url.startsWith('/ports/')) {
        const p = req.url.slice('/ports/'.length);
        if (!ports[p]) return json(404, { detail: 'Port not registered' });
        delete ports[p];
        return json(200, { ok: true });
      }
      if (req.method === 'POST' && req.url === '/ports/claim') {
        const req_ = JSON.parse(body);
        if (req_.port != null && ports[String(req_.port)] && ports[String(req_.port)].app !== req_.app) return json(409, { detail: 'taken' });
        const chosen = req_.port ?? (() => { for (let p = req_.start; p < req_.end; p++) if (!ports[String(p)]) return p; })();
        for (const [k, v] of Object.entries(ports)) if (v.app === req_.app && v.local_agent === req_.local_agent && k !== String(chosen)) delete ports[k];
        ports[String(chosen)] = { port: chosen, app: req_.app, local_agent: req_.local_agent, path: req_.path, registered_at: new Date().toISOString() };
        return json(200, { port: chosen });
      }
      json(404, {});
    });
  });
  return {
    ports,
    server,
    listen: (port) => new Promise((resolve) => server.listen(port, '127.0.0.1', resolve)),
    close: () => new Promise((resolve) => server.close(resolve)),
  };
}

async function freeTcpPort() {
  return new Promise((resolve) => {
    const s = net.createServer();
    s.listen(0, '127.0.0.1', () => { const { port } = s.address(); s.close(() => resolve(port)); });
  });
}

test('with the registry down, a broker re-uses the ports it bound last time (sticky), never 1883', async () => {
  const dataDir = tmpDir();
  const first = await startBroker({ dataDir });
  const { mqttPort, wsPort } = first;
  await first.close();
  assert.ok(mqttPort >= 9000 && mqttPort <= 9999, `first run picked ${mqttPort} from the range, not a fixed default`);
  assert.deepEqual(readStickyPorts(path.join(dataDir, 'ports.json'))[MQTT_APP], mqttPort);

  const second = await startBroker({ dataDir });
  try {
    assert.equal(second.mqttPort, mqttPort);
    assert.equal(second.wsPort, wsPort);
  } finally {
    await second.close();
  }
});

test('a sticky port that is now busy is skipped, not fought over', async () => {
  const dataDir = tmpDir();
  const first = await startBroker({ dataDir });
  const { mqttPort } = first;
  await first.close();

  const squatter = net.createServer();
  await new Promise((resolve) => squatter.listen(mqttPort, resolve));
  const second = await startBroker({ dataDir });
  try {
    assert.notEqual(second.mqttPort, mqttPort);
    assert.equal(readStickyPorts(path.join(dataDir, 'ports.json'))[MQTT_APP], second.mqttPort, 'sticky file follows the port actually bound');
  } finally {
    await second.close();
    await new Promise((resolve) => squatter.close(resolve));
  }
});

test('an explicit pin is bound as given, and a busy pin fails loudly instead of silently moving', async () => {
  const pin = await freeTcpPort();
  const broker = await startBroker({ dataDir: tmpDir(), persist: false, mqttPort: pin });
  try {
    assert.equal(broker.mqttPort, pin);
  } finally {
    await broker.close();
  }
  const squatter = net.createServer();
  await new Promise((resolve) => squatter.listen(pin, resolve));
  try {
    await assert.rejects(() => startBroker({ dataDir: tmpDir(), persist: false, mqttPort: pin }), /EADDRINUSE/);
  } finally {
    await new Promise((resolve) => squatter.close(resolve));
  }
});

test('boot race: the registry comes up AFTER the broker, and still learns the port actually bound (superseding a stale claim)', async () => {
  const registryPort = await freeTcpPort();
  const prevUrl = process.env.LAS_REGISTRY_URL;
  process.env.LAS_REGISTRY_URL = `http://127.0.0.1:${registryPort}`;
  const registry = fakeRegistry();
  // What the real registry held after the 2026-09-23 reboot: last run's claims.
  registry.ports['9014'] = { port: 9014, app: MQTT_APP, local_agent: 'vortexia', path: '/old' };
  registry.ports['9019'] = { port: 9019, app: WS_APP, local_agent: 'vortexia', path: '/old' };

  const dataDir = tmpDir();
  const broker = await startBroker({ dataDir });
  try {
    // Registry not listening yet: the broker is up anyway.
    assert.ok(broker.mqttPort);
    await new Promise((r) => setTimeout(r, 300));
    await registry.listen(registryPort);
    const reconciled = await Promise.race([
      broker.registryReconcile,
      new Promise((r) => setTimeout(() => r('timeout'), 8000)),
    ]);
    assert.notEqual(reconciled, 'timeout', 'background registration completed once the registry was up');

    const mine = Object.values(registry.ports).filter((p) => p.app === MQTT_APP);
    assert.deepEqual(mine.map((p) => p.port), [broker.mqttPort], 'exactly one entry for vortexia-mqtt: the bound port, stale 9014 gone');
    const ws = Object.values(registry.ports).filter((p) => p.app === WS_APP);
    assert.deepEqual(ws.map((p) => p.port), [broker.wsPort]);
  } finally {
    await broker.close();
    await registry.close();
    if (prevUrl === undefined) delete process.env.LAS_REGISTRY_URL; else process.env.LAS_REGISTRY_URL = prevUrl;
  }
  assert.deepEqual(Object.keys(registry.ports), [], 'clean shutdown released both ports');
});

test('shutdown with a hung registry is time-bounded', async () => {
  const registryPort = await freeTcpPort();
  const prevUrl = process.env.LAS_REGISTRY_URL;
  process.env.LAS_REGISTRY_URL = `http://127.0.0.1:${registryPort}`;
  // Accepts connections, never answers.
  const blackhole = http.createServer(() => {});
  await new Promise((resolve) => blackhole.listen(registryPort, '127.0.0.1', resolve));
  const broker = await startBroker({ dataDir: tmpDir(), persist: false });
  try {
    const started = Date.now();
    await broker.close();
    assert.ok(Date.now() - started < 4000, 'close() returned despite the registry never answering');
  } finally {
    blackhole.closeAllConnections?.();
    await new Promise((resolve) => blackhole.close(resolve));
    if (prevUrl === undefined) delete process.env.LAS_REGISTRY_URL; else process.env.LAS_REGISTRY_URL = prevUrl;
  }
});
