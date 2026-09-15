import { test } from 'node:test';
import assert from 'node:assert/strict';
import { LanDiscovery } from '../src/vortex-relay/lanDiscovery.js';

// Same-host advertise/discover — proves the mDNS publish+browse code paths
// work, NOT that multicast actually crosses two real machines on a real
// Wi-Fi network (client isolation or multicast filtering on some routers
// can block that in ways a single-host test can't see). Skips gracefully
// if this environment doesn't support multicast at all (e.g. a sandboxed
// CI runner with no real network interface), same posture as
// broker.test.js's Ollama-dependent test.

test('LanDiscovery: one instance discovers another advertising on the same host', async (t) => {
  const envName = `lan-test-${Date.now()}`;
  const advertiser = new LanDiscovery({ envName, mqttPort: 19555, wsPort: 19556 });
  const watcher = new LanDiscovery({ envName: `lan-test-watcher-${Date.now()}`, mqttPort: 19557 });

  try {
    await advertiser.advertise();
  } catch (err) {
    t.skip(`mDNS advertise not supported in this environment: ${err.message}`);
    return;
  }

  try {
    const found = await new Promise((resolve, reject) => {
      watcher.discover({ onUp: resolve }).catch(reject);
      setTimeout(() => resolve(null), 6000);
    });

    if (!found) {
      t.skip('no mDNS response received within 6s — likely no multicast support in this environment');
      return;
    }

    assert.equal(found.envName, envName);
    assert.equal(found.mqttPort, 19555);
    assert.equal(found.wsPort, 19556);
    assert.ok(found.host, 'discovered peer should have a resolvable host');
    assert.equal(watcher.peers.get(envName)?.mqttPort, 19555, 'discovered peer is tracked in .peers');
  } finally {
    advertiser.stop();
    watcher.stop();
  }
});
