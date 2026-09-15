import { test } from 'node:test';
import assert from 'node:assert/strict';
import { FederationBridge } from '../src/federation/bridge.js';
import { InMemoryRelay } from '../src/federation/relay.js';
import { publishDirectory } from '../src/federation/directory.js';

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
  const bridge = new FederationBridge({ envName: 'ba-mac', relay, envNames: ['ba-mac', 'uy-mac'] });

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
