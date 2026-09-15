import { test } from 'node:test';
import assert from 'node:assert/strict';
import { VortexRelayBridge } from '../src/vortex-relay/bridge.js';
import { InMemoryRelay } from '../src/vortex-relay/relay.js';
import { publishDirectory } from '../src/vortex-relay/directory.js';

// Reproduces a real incident: syncDirectory() runs on a 15s timer, and a
// real relay read can take anywhere from milliseconds to tens of seconds
// (Nostr especially). If an EARLIER-started call happens to resolve AFTER
// a LATER-started one, naively "last call to finish wins" lets a slow,
// now-stale response clobber fresher data that a later call already
// applied — even though a collision/merge log moments earlier proved the
// fresh data really was there. This wraps InMemoryRelay to inject that
// exact ordering: the FIRST read of uy-mac's directory captures an empty
// snapshot immediately, then "delivers" it late (simulating a slow
// network response for stale data), while a second, later-started sync
// reads (and applies) the real data first.
class SlowFirstReadRelay {
  constructor(base, { delayForFile, delayMs }) {
    this.base = base;
    this.delayForFile = delayForFile;
    this.delayMs = delayMs;
    this._seenFirstRead = false;
  }

  async readFile(filename) {
    if (filename === this.delayForFile && !this._seenFirstRead) {
      this._seenFirstRead = true;
      const snapshot = await this.base.readFile(filename); // capture NOW, before any concurrent write
      await new Promise((r) => setTimeout(r, this.delayMs)); // deliver it late
      return snapshot;
    }
    return this.base.readFile(filename);
  }

  writeFile(...args) { return this.base.writeFile(...args); }
  appendMessage(...args) { return this.base.appendMessage(...args); }
}

test('syncDirectory: an earlier-started call resolving LATE with stale data must not clobber a later call\'s fresh result', async () => {
  const base = new InMemoryRelay();
  await publishDirectory(base, 'ba-mac', [{ agentName: 'Robotics', scopeText: 'brazos' }]);
  // uy-mac has NOT published yet at this point — directory-uy-mac.json doesn't exist.

  const relay = new SlowFirstReadRelay(base, { delayForFile: 'directory-uy-mac.json', delayMs: 150 });
  const bridge = new VortexRelayBridge({ envName: 'ba-mac', relay, envNames: ['ba-mac', 'uy-mac'] });

  // Call A starts first (generation 1): its read of uy-mac's file captures
  // "not published yet" immediately, then sleeps 150ms before resolving.
  const callA = bridge.syncDirectory();

  // uy-mac publishes shortly after call A already captured its (empty)
  // snapshot of that file, but before call A's delayed response lands.
  await new Promise((r) => setTimeout(r, 20));
  await publishDirectory(base, 'uy-mac', [{ agentName: 'System', scopeText: 'sysadmin uy-mac' }]);

  // Call B starts after the publish (generation 2) and resolves fast —
  // its read of uy-mac's file is the SECOND ever, so no artificial delay.
  const callB = bridge.syncDirectory();
  await callB;

  assert.ok(
    bridge._mergedEntries.some((e) => e.envName === 'uy-mac' && e.agentName === 'System'),
    'the later, fresher sync must be reflected in _mergedEntries right after it resolves',
  );

  // Call A finally resolves ~150ms after it started, well after call B —
  // its stale (uy-mac-less) result must NOT overwrite what call B set.
  await callA;
  assert.ok(
    bridge._mergedEntries.some((e) => e.envName === 'uy-mac' && e.agentName === 'System'),
    'a late-arriving, earlier-started sync must not clobber a fresher result with stale data',
  );
});

// A third incident, also seen live in logs/launchd.err.log ("could not read
// directory for uy-mac: GistRelay: read failed with 403", 265 times over one
// day): every configured relay can throw for ONE environment on a given
// sync cycle (e.g. a simultaneous Gist rate-limit + Nostr timeout) even
// while every other environment reads fine. mergeDirectories correctly
// skips that one environment for the cycle, but syncDirectory() must not
// let that translate into "this environment has no agents" — a
// vortex-relay-direct lookup for name@env landing in that ~15s window before
// the fix would get a false not-found even though nothing changed on the
// far side. See resolveDirectoryName in vortex-relay-directory.test.js for
// the same guarantee at the pure-function level; this covers the bridge's
// handling of the failure signal end to end.
class FlakyOnceRelay {
  constructor(base, { failForFile }) {
    this.base = base;
    this.failForFile = failForFile;
    this._failed = false;
  }

  async readFile(filename) {
    if (filename === this.failForFile && !this._failed) {
      this._failed = true;
      throw new Error('GistRelay: read failed with 403');
    }
    return this.base.readFile(filename);
  }

  writeFile(...args) { return this.base.writeFile(...args); }
  appendMessage(...args) { return this.base.appendMessage(...args); }
}

test('syncDirectory: a one-cycle read failure for one environment keeps its stale entries instead of dropping them', async () => {
  const base = new InMemoryRelay();
  await publishDirectory(base, 'ba-mac', [{ agentName: 'System', scopeText: 'sysadmin ba-mac' }]);
  await publishDirectory(base, 'uy-mac', [{ agentName: 'System', scopeText: 'sysadmin uy-mac' }]);

  const bridge = new VortexRelayBridge({ envName: 'ba-mac', relay: base, envNames: ['ba-mac', 'uy-mac'] });

  // Cycle 1: both environments read fine (straight off `base`, no flakiness
  // yet), establishing a good baseline.
  await bridge.syncDirectory();
  assert.ok(bridge._mergedEntries.some((e) => e.envName === 'uy-mac' && e.agentName === 'System'));

  // Cycle 2: swap in a relay whose FIRST read of uy-mac's directory throws
  // (every relay failed that cycle). Without the fallback, uy-mac's
  // entries would vanish from _mergedEntries even though its roster
  // hasn't actually changed.
  bridge.relay = new FlakyOnceRelay(base, { failForFile: 'directory-uy-mac.json' });
  await bridge.syncDirectory();
  assert.ok(
    bridge._mergedEntries.some((e) => e.envName === 'uy-mac' && e.agentName === 'System'),
    'a transient full-relay read failure for uy-mac must not drop its entries from _mergedEntries',
  );

  // Cycle 3: the read succeeds again — confirms the fallback doesn't wedge
  // stale data in permanently once a real read succeeds.
  await bridge.syncDirectory();
  assert.equal(
    bridge._mergedEntries.filter((e) => e.envName === 'uy-mac' && e.agentName === 'System').length,
    1,
    'a recovered read must not leave duplicate stale + fresh entries',
  );
});

// A second, related incident on the same day: with Gist 403ing on every
// call, every outbox poll tick fell through to a real multi-relay Nostr
// query — and with no overlap guard, a slow query meant the NEXT tick
// fired before the previous one finished, piling up more and more
// concurrent in-flight queries against the same relays until they all
// started timing out. startPolling/startDirectorySync now skip a tick
// entirely while the previous one is still in flight.
class SlowRelay {
  constructor(delayMs) {
    this.delayMs = delayMs;
    this.readCalls = 0;
  }
  async readFile() {
    this.readCalls++;
    await new Promise((r) => setTimeout(r, this.delayMs));
    return [];
  }
  async writeFile() { return {}; }
  async appendMessage() { return []; }
}

test('startPolling: a slow relay read must not let poll ticks pile up concurrently', async () => {
  const relay = new SlowRelay(200); // slower than the 30ms poll interval below
  const bridge = new VortexRelayBridge({ envName: 'ba-mac', relay, envNames: ['ba-mac'] });
  bridge.localClient = { send() {} }; // _deliverLocally target; unused here since readFile always returns []

  bridge.startPolling(30);
  await new Promise((r) => setTimeout(r, 650)); // ~21 ticks' worth of interval time, but reads take 200ms each
  bridge.stopPolling();

  // Without the guard, ~21 ticks over 650ms would each fire readFile
  // immediately (interval << read latency), piling up many concurrent
  // in-flight reads. With it, only one read is ever in flight — roughly
  // 650/200 ≈ 3 completed, never anywhere near 21.
  assert.ok(relay.readCalls <= 4, `expected at most ~4 non-overlapping reads in 650ms of 200ms reads, got ${relay.readCalls}`);
});
