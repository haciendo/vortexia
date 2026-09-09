import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { scanScopes, smallestFibonacciAtLeast } from '../src/scope.js';

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'vxia-scope-'));
}

test('smallestFibonacciAtLeast finds the smallest cap >= n', () => {
  assert.equal(smallestFibonacciAtLeast(0), 1);
  assert.equal(smallestFibonacciAtLeast(1), 1);
  assert.equal(smallestFibonacciAtLeast(2), 2);
  assert.equal(smallestFibonacciAtLeast(4), 5);
  assert.equal(smallestFibonacciAtLeast(90), 144);
  assert.equal(smallestFibonacciAtLeast(144), 144);
  assert.equal(smallestFibonacciAtLeast(145), 233);
});

test('empty directory has an empty ladder', () => {
  const dir = tmpDir();
  const { rungs, warnings } = scanScopes(dir);
  assert.deepEqual(rungs, []);
  assert.deepEqual(warnings, []);
});

test('reads .vxia-scope.<N>.md files in ascending rung order', () => {
  const dir = tmpDir();
  fs.writeFileSync(path.join(dir, '.vxia-scope.377.md'), 'the long version');
  fs.writeFileSync(path.join(dir, '.vxia-scope.144.md'), 'the short version');

  const { rungs, warnings } = scanScopes(dir);
  assert.deepEqual(warnings, []);
  assert.equal(rungs.length, 2);
  assert.deepEqual(rungs.map((r) => r.rung), [144, 377]);
  assert.equal(rungs[0].source, '.vxia-scope.144.md');
  assert.equal(rungs[0].text, 'the short version');
});

test('README.md slots into the ladder at the smallest Fibonacci rung >= its length', () => {
  const dir = tmpDir();
  const text = 'x'.repeat(100);
  fs.writeFileSync(path.join(dir, 'README.md'), text);

  const { rungs, warnings } = scanScopes(dir);
  assert.deepEqual(warnings, []);
  assert.equal(rungs.length, 1);
  assert.equal(rungs[0].rung, 144);
  assert.equal(rungs[0].source, 'README.md');
  assert.equal(rungs[0].text, text);
});

test('explicit rung beats a README that lands on the same rung by coincidence', () => {
  const dir = tmpDir();
  fs.writeFileSync(path.join(dir, '.vxia-scope.144.md'), 'explicit rung 144');
  fs.writeFileSync(path.join(dir, 'README.md'), 'y'.repeat(100)); // also rounds up to 144

  const { rungs, warnings } = scanScopes(dir);
  assert.equal(rungs.length, 1);
  assert.equal(rungs[0].source, '.vxia-scope.144.md');
  assert.equal(warnings.length, 1);
  assert.match(warnings[0], /rung 144/);
  assert.match(warnings[0], /README\.md/);
});

test('a rung collision never deletes or modifies the losing file', () => {
  const dir = tmpDir();
  fs.writeFileSync(path.join(dir, '.vxia-scope.144.md'), 'explicit rung 144');
  fs.writeFileSync(path.join(dir, 'README.md'), 'y'.repeat(100));

  scanScopes(dir);

  assert.equal(fs.readFileSync(path.join(dir, '.vxia-scope.144.md'), 'utf8'), 'explicit rung 144');
  assert.equal(fs.readFileSync(path.join(dir, 'README.md'), 'utf8'), 'y'.repeat(100));
});

test('ignores unrelated files', () => {
  const dir = tmpDir();
  fs.writeFileSync(path.join(dir, 'notes.md'), 'irrelevant');
  fs.writeFileSync(path.join(dir, '.vxia-scope.144.md'), 'relevant');

  const { rungs } = scanScopes(dir);
  assert.equal(rungs.length, 1);
  assert.equal(rungs[0].text, 'relevant');
});
