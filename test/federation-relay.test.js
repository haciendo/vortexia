import { test } from 'node:test';
import assert from 'node:assert/strict';
import { InMemoryRelay, MultiRelay, NostrRelay } from '../src/federation/relay.js';

test('NostrRelay.readFile: picks the NEWEST snapshot event by created_at, not the first one returned', async () => {
  // No real network: initialize the pool, then swap in a stub querySync
  // that returns two "replaceable" events for the same filename out of
  // order — deterministic reproduction of what real relays disagreeing on
  // propagation looks like from querySync's point of view. Found live: a
  // directory snapshot kept reading back stale/empty long after a real,
  // non-empty publish had gone out, because .find() took whichever came
  // back first in the array, not the latest by timestamp.
  const relay = new NostrRelay({ secretKey: new Uint8Array(32).fill(7) });
  await relay._ensure();
  const pubkey = await relay.publicKeyHex();

  relay._pool.querySync = async () => [
    { kind: 30078, created_at: 1000, tags: [['d', 'directory-ba-mac.json']], content: JSON.stringify({ envName: 'ba-mac', agents: [] }), pubkey },
    { kind: 30078, created_at: 2000, tags: [['d', 'directory-ba-mac.json']], content: JSON.stringify({ envName: 'ba-mac', agents: [{ agentName: 'System', scopeText: 'x' }] }), pubkey },
  ];

  const result = await relay.readFile('directory-ba-mac.json');
  assert.deepEqual(result, { envName: 'ba-mac', agents: [{ agentName: 'System', scopeText: 'x' }] });
  relay.close();
});

// A relay double that always rejects, to prove fallback actually engages
// rather than just happening to work because the first relay in the list
// is the real one.
class FailingRelay {
  async readFile() { throw new Error('FailingRelay: read always fails'); }
  async appendMessage() { throw new Error('FailingRelay: append always fails'); }
  async writeFile() { throw new Error('FailingRelay: write always fails'); }
}

test('MultiRelay.readFile: falls through to the next relay when the first errors', async () => {
  const good = new InMemoryRelay();
  await good.writeFile('directory-env-a.json', { envName: 'env-a', agents: [] });
  const multi = new MultiRelay([new FailingRelay(), good]);

  const result = await multi.readFile('directory-env-a.json');
  assert.deepEqual(result, { envName: 'env-a', agents: [] });
});

test('MultiRelay.readFile: skips a relay that succeeds but returns nothing (not the same as real data)', async () => {
  const empty = new InMemoryRelay(); // never written to — readFile resolves to []
  const good = new InMemoryRelay();
  await good.appendMessage('inbox-env-b.json', { text: 'hi' });
  const multi = new MultiRelay([empty, good]);

  const result = await multi.readFile('inbox-env-b.json');
  assert.deepEqual(result, [{ text: 'hi' }]);
});

test('MultiRelay.readFile: throws only when every relay fails', async () => {
  const multi = new MultiRelay([new FailingRelay(), new FailingRelay()]);
  await assert.rejects(() => multi.readFile('whatever.json'), /always fails/);
});

test('MultiRelay.readFile: an earlier relay erroring must not mask a later relay\'s valid empty result', async () => {
  // Live bug: Gist 403'd (rate limited) while Nostr had nothing yet for a
  // brand new file (a real, valid "empty" — not an error) — this must
  // resolve to [], not re-throw the unrelated Gist error.
  const empty = new InMemoryRelay(); // never written to — readFile resolves to [], not an error
  const multi = new MultiRelay([new FailingRelay(), empty]);
  const result = await multi.readFile('never-published.json');
  assert.deepEqual(result, []);
});

test('MultiRelay.writeFile: fans out to every relay, succeeds if at least one does', async () => {
  const good = new InMemoryRelay();
  const multi = new MultiRelay([new FailingRelay(), good]);

  await multi.writeFile('directory-env-a.json', { envName: 'env-a', agents: [] });
  assert.deepEqual(await good.readFile('directory-env-a.json'), { envName: 'env-a', agents: [] });
});

test('MultiRelay.writeFile: throws only when every relay fails', async () => {
  const multi = new MultiRelay([new FailingRelay(), new FailingRelay()]);
  await assert.rejects(() => multi.writeFile('x.json', {}), /always fails/);
});

test('MultiRelay.appendMessage: fans out to every relay, succeeds if at least one does', async () => {
  const good = new InMemoryRelay();
  const multi = new MultiRelay([new FailingRelay(), good]);

  await multi.appendMessage('inbox-env-b.json', { text: 'hi' });
  assert.deepEqual(await good.readFile('inbox-env-b.json'), [{ text: 'hi' }]);
});

// Real network test against live public Nostr relays — same "skip if
// unreachable" posture as broker.test.js's Ollama-dependent test, since a
// CI box or an offline dev machine may not have outbound access.
async function nostrReachable() {
  try {
    const res = await fetch('https://relay.damus.io', { method: 'GET' });
    return res.ok || res.status === 426; // 426 Upgrade Required is the expected reply from an HTTP GET to a ws-only endpoint — still proves reachability
  } catch {
    return false;
  }
}

test('NostrRelay: real round trip against live public relays (writeFile + readFile, appendMessage log)', async (t) => {
  if (!(await nostrReachable())) {
    t.skip('no network access to Nostr relays from this environment');
    return;
  }

  const { generateSecretKey } = await import('nostr-tools');
  const relay = new NostrRelay({ secretKey: generateSecretKey(), queryTimeoutMs: 8000 });
  const suffix = Date.now();

  try {
    // Snapshot (directory-style) round trip.
    const snapshotFile = `vortexia-test-directory-${suffix}.json`;
    await relay.writeFile(snapshotFile, { envName: 'test-env', agents: [{ agentName: 'X', scopeText: 'x' }] });
    const readBack = await relay.readFile(snapshotFile);
    assert.deepEqual(readBack, { envName: 'test-env', agents: [{ agentName: 'X', scopeText: 'x' }] });

    // Overwrite proves it's actually replaceable (last write wins), not append.
    await relay.writeFile(snapshotFile, { envName: 'test-env', agents: [] });
    const readBack2 = await relay.readFile(snapshotFile);
    assert.deepEqual(readBack2, { envName: 'test-env', agents: [] });

    // Log (outbox-style) round trip: two independent appends, no read-modify-write.
    const logFile = `vortexia-test-inbox-${suffix}.json`;
    await relay.appendMessage(logFile, { from: 'A', text: 'first' });
    const afterFirst = await relay.appendMessage(logFile, { from: 'B', text: 'second' });
    assert.equal(afterFirst.length, 2);
    assert.deepEqual(afterFirst.map((m) => m.text), ['first', 'second']);
  } finally {
    relay.close();
  }
});

test('NostrRelay: reads a known peer\'s events (cross-identity), but not an unknown identity\'s', async (t) => {
  if (!(await nostrReachable())) {
    t.skip('no network access to Nostr relays from this environment');
    return;
  }

  const { generateSecretKey } = await import('nostr-tools');
  const suffix = Date.now();
  const filename = `vortexia-test-peer-${suffix}.json`;

  const peer = new NostrRelay({ secretKey: generateSecretKey(), queryTimeoutMs: 8000 });
  const stranger = new NostrRelay({ secretKey: generateSecretKey(), queryTimeoutMs: 8000 });
  const reader = new NostrRelay({ secretKey: generateSecretKey(), queryTimeoutMs: 8000 });

  try {
    await peer.writeFile(filename, { from: 'peer' });
    await stranger.writeFile(filename, { from: 'stranger' });

    // Reader doesn't know either yet — its own file is empty (never wrote it).
    const beforeTrust = await reader.readFile(filename);
    assert.deepEqual(beforeTrust, []);

    // After exchanging pubkeys with `peer` only, reader can read peer's
    // event, but a stranger who happens to reuse the same filename is
    // still invisible — trust is explicit, per environment, not "anyone
    // who used this tag."
    reader.addPeer('peer-env', await peer.publicKeyHex());
    const afterTrust = await reader.readFile(filename);
    assert.deepEqual(afterTrust, { from: 'peer' });
  } finally {
    peer.close();
    stranger.close();
    reader.close();
  }
});
