import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ConnectionRouter } from '../src/vortex-relay/connection.js';

// ConnectionRouter is the generalized fallback chain: MultiRelay's
// circuit-breaker pattern, one level up, working across heterogeneous
// Connection kinds (not just within the relay kind). These tests cover
// the router in isolation — vortex-relay-direct-transport.test.js covers
// it wired into VortexRelayBridge against real brokers.

function fakeConnection(name, priority, { onDeliver } = {}) {
  return {
    name,
    priority,
    calls: [],
    isAvailable() { return true; },
    async deliver(envName, routed) {
      this.calls.push([envName, routed]);
      if (onDeliver) await onDeliver();
    },
  };
}

test('deliver: picks the lowest-priority (cheapest/fastest) available connection', async () => {
  const cheap = fakeConnection('local', 0);
  const expensive = fakeConnection('relay', 100);
  const router = new ConnectionRouter();

  await router.deliver('env-b', { text: 'hi' }, [expensive, cheap]);

  assert.equal(cheap.calls.length, 1);
  assert.equal(expensive.calls.length, 0);
});

test('deliver: falls back to the next connection when the first throws', async () => {
  const flaky = fakeConnection('lan-direct', 10, { onDeliver: () => { throw new Error('reset'); } });
  const fallback = fakeConnection('relay', 100);
  const router = new ConnectionRouter();

  await router.deliver('env-b', { text: 'hi' }, [flaky, fallback]);

  assert.equal(fallback.calls.length, 1);
});

test('deliver: throws only when every connection fails', async () => {
  const a = fakeConnection('a', 0, { onDeliver: () => { throw new Error('a down'); } });
  const b = fakeConnection('b', 1, { onDeliver: () => { throw new Error('b down'); } });
  const router = new ConnectionRouter();

  await assert.rejects(() => router.deliver('env-b', {}, [a, b]), /b down/);
});

test('deliver: skips an unavailable connection without waiting for it to fail', async () => {
  const unavailable = fakeConnection('lan-direct', 10);
  unavailable.isAvailable = () => false;
  const fallback = fakeConnection('relay', 100);
  const router = new ConnectionRouter();

  await router.deliver('env-b', {}, [unavailable, fallback]);

  assert.equal(unavailable.calls.length, 0);
  assert.equal(fallback.calls.length, 1);
});

test('deliver: pin locks to one named connection, never falls back on failure', async () => {
  const pinned = fakeConnection('lan-direct', 10, { onDeliver: () => { throw new Error('reset'); } });
  const other = fakeConnection('relay', 100);
  const router = new ConnectionRouter();

  await assert.rejects(() => router.deliver('env-b', {}, [pinned, other], { pin: 'lan-direct' }), /reset/);
  assert.equal(other.calls.length, 0, 'a pin must never fall through to another connection');
});

test('deliver: pin to a name with no matching connection throws immediately', async () => {
  const only = fakeConnection('relay', 100);
  const router = new ConnectionRouter();

  await assert.rejects(
    () => router.deliver('env-b', {}, [only], { pin: 'lan-direct' }),
    /pinned to "lan-direct" but no such connection applies/,
  );
  assert.equal(only.calls.length, 0);
});

test('circuit breaker: a connection failing past the threshold is skipped on later calls, not retried every time', async () => {
  let attempts = 0;
  const flaky = fakeConnection('lan-direct', 10, { onDeliver: () => { attempts++; throw new Error('down'); } });
  const fallback = fakeConnection('relay', 100);
  const router = new ConnectionRouter({ failureThreshold: 2, cooldownMs: 60000 });

  await router.deliver('env-b', {}, [flaky, fallback]);
  await router.deliver('env-b', {}, [flaky, fallback]);
  assert.equal(attempts, 2, 'threshold reached — circuit should now be open');

  await router.deliver('env-b', {}, [flaky, fallback]);
  assert.equal(attempts, 2, 'an open circuit must be skipped, not attempted again immediately');
  assert.equal(fallback.calls.length, 3);
});

test('circuit breaker health is scoped per envName: a connection failing for one peer stays usable for another', async () => {
  let attemptsForB = 0;
  let successesForC = 0;
  const conn = {
    name: 'lan-direct',
    priority: 10,
    isAvailable: () => true,
    async deliver(envName) {
      if (envName === 'env-b') { attemptsForB++; throw new Error('env-b unreachable'); }
      successesForC++;
    },
  };
  const fallback = fakeConnection('relay', 100);
  const router = new ConnectionRouter({ failureThreshold: 1, cooldownMs: 60000 });

  await router.deliver('env-b', {}, [conn, fallback]);
  await router.deliver('env-b', {}, [conn, fallback]); // circuit now open for env-b
  assert.equal(attemptsForB, 1, 'env-b\'s circuit is open — no second attempt');
  assert.equal(fallback.calls.length, 2, 'both env-b calls fell back to the relay');

  await router.deliver('env-c', {}, [conn, fallback]); // env-c must not be affected by env-b's breaker
  assert.equal(successesForC, 1, 'env-c delivered via the direct connection, unaffected by env-b\'s open circuit');
  assert.equal(fallback.calls.length, 2, 'env-c never touched the fallback');
});
