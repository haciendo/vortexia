import { test } from 'node:test';
import assert from 'node:assert/strict';
import { InMemoryRelay } from '../src/federation/relay.js';
import { publishDirectory, mergeDirectories, resolveDirectoryName } from '../src/federation/directory.js';

test('mergeDirectories combines every published environment, skipping ones that never published', async () => {
  const relay = new InMemoryRelay();
  await publishDirectory(relay, 'env-a', [{ agentName: 'Clima', scopeText: 'tiempo' }]);
  await publishDirectory(relay, 'env-b', [{ agentName: 'Facturas', scopeText: 'pagos' }]);

  const { entries, collisions } = await mergeDirectories(relay, ['env-a', 'env-b', 'env-c-never-published']);

  assert.deepEqual(
    entries.map((e) => [e.envName, e.agentName]).sort(),
    [['env-a', 'Clima'], ['env-b', 'Facturas']],
  );
  assert.equal(collisions.size, 0);
});

test('mergeDirectories flags a same-name agent across environments as a collision, not silently deduped', async () => {
  const relay = new InMemoryRelay();
  await publishDirectory(relay, 'mac-1', [{ agentName: 'System', scopeText: 'sysadmin local mac-1' }]);
  await publishDirectory(relay, 'mac-2', [{ agentName: 'System', scopeText: 'sysadmin local mac-2' }]);

  const { entries, collisions } = await mergeDirectories(relay, ['mac-1', 'mac-2']);

  assert.equal(entries.length, 2);
  assert.deepEqual([...collisions.get('System')].sort(), ['mac-1', 'mac-2']);
});

test('resolveDirectoryName: bare name prefers the local environment over any remote match', () => {
  const entries = [
    { envName: 'mac-1', agentName: 'System', scopeText: 'a' },
    { envName: 'mac-2', agentName: 'System', scopeText: 'b' },
  ];
  const resolved = resolveDirectoryName('System', 'mac-2', entries);
  assert.deepEqual(resolved, { status: 'resolved', envName: 'mac-2', agentName: 'System' });
});

test('resolveDirectoryName: bare name with no local match and exactly one remote match resolves there', () => {
  const entries = [{ envName: 'env-b', agentName: 'Facturas', scopeText: 'pagos' }];
  const resolved = resolveDirectoryName('Facturas', 'env-a', entries);
  assert.deepEqual(resolved, { status: 'resolved', envName: 'env-b', agentName: 'Facturas' });
});

test('resolveDirectoryName: bare name colliding across >1 remote environments is ambiguous, never guessed', () => {
  const entries = [
    { envName: 'mac-1', agentName: 'System', scopeText: 'a' },
    { envName: 'mac-2', agentName: 'System', scopeText: 'b' },
    { envName: 'mac-3', agentName: 'System', scopeText: 'c' },
  ];
  const resolved = resolveDirectoryName('System', 'mac-4-someone-else', entries);
  assert.equal(resolved.status, 'ambiguous');
  assert.deepEqual(resolved.candidates.sort(), ['System@mac-1', 'System@mac-2', 'System@mac-3']);
});

test('resolveDirectoryName: env-qualified name (name@env) resolves exactly, bypassing collision handling', () => {
  const entries = [
    { envName: 'mac-1', agentName: 'System', scopeText: 'a' },
    { envName: 'mac-2', agentName: 'System', scopeText: 'b' },
  ];
  const resolved = resolveDirectoryName('System@mac-2', 'mac-3-someone-else', entries);
  assert.deepEqual(resolved, { status: 'resolved', envName: 'mac-2', agentName: 'System' });
});

test('resolveDirectoryName: unknown name (bare or qualified) is not-found', () => {
  const entries = [{ envName: 'env-a', agentName: 'Clima', scopeText: 'tiempo' }];
  assert.deepEqual(resolveDirectoryName('Ghost', 'env-a', entries), { status: 'not-found' });
  assert.deepEqual(resolveDirectoryName('Ghost@env-a', 'env-b', entries), { status: 'not-found' });
});
