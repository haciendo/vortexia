import { test } from 'node:test';
import assert from 'node:assert/strict';
import { InMemoryRelay, MultiRelay, NostrRelay } from '../src/vortex-relay/relay.js';

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

test('MultiRelay.appendMessage: chains — falls through to the next relay when an earlier one fails', async () => {
  const good = new InMemoryRelay();
  const multi = new MultiRelay([new FailingRelay(), good]);

  await multi.appendMessage('inbox-env-b.json', { text: 'hi' });
  assert.deepEqual(await good.readFile('inbox-env-b.json'), [{ text: 'hi' }]);
  assert.equal(multi.lastWriteVia, good.name, 'only the relay that actually succeeded should be reported');
});

test('MultiRelay.appendMessage: stops at the first success, never calls a later relay too', async () => {
  const first = new InMemoryRelay();
  let secondCalls = 0;
  const second = { name: 'second', async appendMessage() { secondCalls++; return []; } };
  const multi = new MultiRelay([first, second]);

  await multi.appendMessage('inbox-env-b.json', { text: 'hi' });
  assert.equal(secondCalls, 0, 'a chain must not fan out once an earlier relay already succeeded');
});

test('MultiRelay.appendMessage: skips a circuit-open relay on later calls, not retried every time', async () => {
  const flaky = new CountingRelay('flaky'); // always fails
  const good = new InMemoryRelay();
  const multi = new MultiRelay([flaky, good], { failureThreshold: 2, cooldownMs: 60000 });

  await multi.appendMessage('inbox-env-b.json', { text: 'a' }); // flaky fails (1), good succeeds
  await multi.appendMessage('inbox-env-b.json', { text: 'b' }); // flaky fails (2) -> circuit opens; good succeeds
  assert.equal(flaky.calls, 2);

  await multi.appendMessage('inbox-env-b.json', { text: 'c' }); // circuit open -> flaky must be SKIPPED entirely
  assert.equal(flaky.calls, 2, 'an open circuit must not be attempted again before its cooldown elapses');
  assert.deepEqual((await good.readFile('inbox-env-b.json')).map((m) => m.text), ['a', 'b', 'c']);
});

test('MultiRelay.appendMessage: retries an open relay as a last resort rather than dropping the message', async () => {
  // With only one relay and threshold 1, its very first failure opens its
  // own circuit mid-call — the second pass immediately retries it (nothing
  // else to fall back to), so a single call can attempt twice.
  const onlyOne = new CountingRelay('only', { failUntilCall: 2 }); // fails first two calls, then works
  const multi = new MultiRelay([onlyOne], { failureThreshold: 1, cooldownMs: 60000 });

  // First appendMessage: attempt #1 fails (opens the circuit), the
  // same-call second pass retries as attempt #2 — also still within
  // failUntilCall, so it fails too.
  await assert.rejects(() => multi.appendMessage('x.json', { text: 'first' }));
  assert.equal(onlyOne.calls, 2);

  // Second appendMessage: the relay is circuit-open and its cooldown
  // hasn't elapsed — pass one has nothing to try, so pass two retries it
  // anyway rather than refusing the write outright. Attempt #3 is past
  // failUntilCall, so it succeeds.
  const result = await multi.appendMessage('x.json', { text: 'second' });
  assert.deepEqual(result, {});
  assert.equal(onlyOne.calls, 3);
});

test('MultiRelay.readFile: merges messages that ended up split across different relays', async () => {
  const a = new InMemoryRelay();
  const b = new InMemoryRelay();
  await a.appendMessage('inbox-env-b.json', { text: 'first', ts: 1 });
  await b.appendMessage('inbox-env-b.json', { text: 'second', ts: 2 });
  const multi = new MultiRelay([a, b]);

  const result = await multi.readFile('inbox-env-b.json');
  assert.deepEqual(result.map((m) => m.text), ['first', 'second']);
});

// Real network tests against live public Nostr relays — opt-in only
// (VORTEXIA_TEST_NETWORK=1), not part of the default `npm test` run.
// Found live: these two tests alone account for ~60s of an ~85s full
// suite run, and repeatedly running them (dozens of times over one long
// debugging session) adds real, unnecessary load to shared free public
// infrastructure other environments depend on — the same category of
// mistake as the earlier incident where unpinned test ports hit the real
// port registry. `npm test` should be fast and side-effect-free by
// default; real-network verification is still available, deliberately,
// via `VORTEXIA_TEST_NETWORK=1 npm test`.
async function nostrReachable() {
  if (process.env.VORTEXIA_TEST_NETWORK !== '1') return false;
  try {
    const res = await fetch('https://relay.damus.io', { method: 'GET' });
    return res.ok || res.status === 426; // 426 Upgrade Required is the expected reply from an HTTP GET to a ws-only endpoint — still proves reachability
  } catch {
    return false;
  }
}

test('NostrRelay: real round trip against live public relays (writeFile + readFile, appendMessage log)', async (t) => {
  if (!(await nostrReachable())) {
    t.skip('real-network test opt-in (VORTEXIA_TEST_NETWORK=1) or unreachable');
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
    t.skip('real-network test opt-in (VORTEXIA_TEST_NETWORK=1) or unreachable');
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

// Circuit breaker: "chain multiple transports, fall back automatically"
// only really means something if a persistently-failing one stops being
// retried on every single call — otherwise every operation keeps paying
// for (and waiting on) a transport that's been down for an hour. José's
// ask directly: this must be automatic, not something a human/agent has
// to fix by editing config on the other end.
class CountingRelay {
  constructor(name, { failUntilCall = Infinity } = {}) {
    this.name = name;
    this.failUntilCall = failUntilCall;
    this.calls = 0;
  }
  async readFile() {
    this.calls++;
    if (this.calls <= this.failUntilCall) throw new Error(`${this.name}: fail #${this.calls}`);
    return { ok: this.name };
  }
  async writeFile() {
    this.calls++;
    if (this.calls <= this.failUntilCall) throw new Error(`${this.name}: fail #${this.calls}`);
    return {};
  }
  async appendMessage(...args) { return this.writeFile(...args); }
}

test('MultiRelay circuit breaker: a relay failing past the threshold is skipped on later reads, not retried every time', async () => {
  const flaky = new CountingRelay('flaky'); // always fails
  const good = new InMemoryRelay();
  await good.writeFile('directory-ba-mac.json', { envName: 'ba-mac', agents: [{ agentName: 'X', scopeText: 'x' }] });
  const multi = new MultiRelay([flaky, good], { failureThreshold: 2, cooldownMs: 5000 });

  await multi.readFile('directory-ba-mac.json'); // flaky fails (1)
  await multi.readFile('directory-ba-mac.json'); // flaky fails (2) -> circuit opens
  assert.equal(flaky.calls, 2);

  await multi.readFile('directory-ba-mac.json'); // circuit open -> flaky must be SKIPPED entirely
  assert.equal(flaky.calls, 2, 'an open circuit must not be attempted again before its cooldown elapses');
});

test('MultiRelay circuit breaker: a relay recovers after its cooldown and closes on success', async () => {
  const recovers = new CountingRelay('recovers', { failUntilCall: 2 }); // fails twice, then works
  const other = new CountingRelay('other', { failUntilCall: Infinity }); // always fails — forces MultiRelay to actually need `recovers`
  const multi = new MultiRelay([recovers, other], { failureThreshold: 2, cooldownMs: 30 });

  await assert.rejects(() => multi.readFile('x.json')); // recovers fails (1), other fails -> every relay failed
  await assert.rejects(() => multi.readFile('x.json')); // recovers fails (2) -> circuit opens; other fails again
  assert.equal(recovers.calls, 2);

  await new Promise((r) => setTimeout(r, 40)); // past cooldownMs
  const result = await multi.readFile('x.json'); // circuit half-open -> tried again -> succeeds (call #3 > failUntilCall)
  assert.deepEqual(result, { ok: 'recovers' });
  assert.equal(recovers.calls, 3);
});
