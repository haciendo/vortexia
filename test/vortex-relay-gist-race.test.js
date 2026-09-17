import { test } from 'node:test';
import assert from 'node:assert/strict';
import { GistRelay } from '../src/vortex-relay/relay.js';

// GistRelay.appendMessage is read-then-append-then-writeFile against a
// single shared Gist file — a genuine read-modify-write. Two calls for the
// SAME file racing (a poll retry and a fresh send landing close together,
// or just two sends fired back to back) must not let the later PATCH
// clobber the earlier one's append with a stale read. See relay.js's
// _writeQueues doc.

function makeFakeGistFetch(filename, { readDelayMs = 20 } = {}) {
  let content = JSON.stringify([]);
  let getCalls = 0;
  let patchCalls = 0;
  const fetchImpl = async (url, opts = {}) => {
    const method = opts.method ?? 'GET';
    if (method === 'GET') {
      getCalls++;
      // Simulate real network latency — long enough that two calls into
      // appendMessage launched back to back would both have their read
      // in flight at once if appendMessage didn't serialize them.
      await new Promise((r) => setTimeout(r, readDelayMs));
      return { ok: true, json: async () => ({ files: { [filename]: { content } } }) };
    }
    if (method === 'PATCH') {
      patchCalls++;
      const body = JSON.parse(opts.body);
      content = body.files[filename].content;
      return { ok: true, json: async () => ({}) };
    }
    throw new Error(`unexpected method ${method}`);
  };
  return { fetchImpl, counts: () => ({ getCalls, patchCalls }) };
}

test('GistRelay.appendMessage: two near-concurrent appends to the same file both survive, none clobbered', async () => {
  const filename = 'inbox-uy-mac.json';
  const { fetchImpl } = makeFakeGistFetch(filename);
  const relay = new GistRelay({ gistId: 'g', token: 't', fetchImpl });

  await Promise.all([
    relay.appendMessage(filename, { id: 1 }),
    relay.appendMessage(filename, { id: 2 }),
    relay.appendMessage(filename, { id: 3 }),
  ]);

  const final = await relay.readFile(filename);
  assert.deepEqual(final.map((m) => m.id).sort(), [1, 2, 3], 'every concurrent append must land, none lost to a stale-read overwrite');
});

test('GistRelay.appendMessage: one call failing (e.g. a 403) does not wedge later appends to the same file', async () => {
  const filename = 'inbox-uy-mac.json';
  let fail = true;
  const fetchImpl = async (url, opts = {}) => {
    const method = opts.method ?? 'GET';
    if (method === 'GET') return { ok: true, json: async () => ({ files: {} }) };
    if (method === 'PATCH') {
      if (fail) { fail = false; return { ok: false, status: 403 }; }
      return { ok: true, json: async () => ({}) };
    }
    throw new Error(`unexpected method ${method}`);
  };
  const relay = new GistRelay({ gistId: 'g', token: 't', fetchImpl });

  await assert.rejects(() => relay.appendMessage(filename, { id: 1 }), /write failed with 403/);
  await assert.doesNotReject(() => relay.appendMessage(filename, { id: 2 }));
});
