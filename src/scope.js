// vxia-scope ladder scanner — see docs/vxia-scope-ladder.md for the spec.
//
// Reads a directory's .vxia-scope.<N>.md files plus an optional README,
// resolves rung collisions, and returns an ordered ladder. Pure fs, no MQTT —
// callers merge this with any of their own pre-existing fields themselves
// (see docs/future-las-agent-scope-router.md for how local-agent-society
// does that with .las-agent.json).

import fs from 'node:fs';
import path from 'node:path';

const SCOPE_FILE_RE = /^\.vxia-scope\.(\d+)\.md$/;
const README_RE = /^readme(\.md)?$/i;

/**
 * Smallest number in the 1,2,3,5,8,13,... Fibonacci-style sequence that is
 * >= n (the sequence used throughout the ladder for rung length caps).
 */
export function smallestFibonacciAtLeast(n) {
  if (n <= 1) return 1;
  let a = 1;
  let b = 2;
  while (b < n) {
    [a, b] = [b, a + b];
  }
  return b;
}

/**
 * Scan `dir` for a .vxia-scope ladder. Returns `{ rungs, warnings }`:
 * - `rungs`: ordered array of `{ rung, source, text }`, ascending by rung.
 * - `warnings`: human-readable strings describing any rung collisions
 *   found (nothing is deleted or modified — see the protocol doc).
 */
export function scanScopes(dir) {
  const warnings = [];
  const entries = fs.readdirSync(dir);

  const fileRungs = [];
  for (const entry of entries) {
    const m = entry.match(SCOPE_FILE_RE);
    if (!m) continue;
    const rung = parseInt(m[1], 10);
    const text = fs.readFileSync(path.join(dir, entry), 'utf8').trim();
    fileRungs.push({ rung, source: entry, text });
  }
  fileRungs.sort((a, b) => a.rung - b.rung);

  const readmeEntry = entries.find((e) => README_RE.test(e));

  const byRung = new Map();
  for (const fr of fileRungs) {
    const existing = byRung.get(fr.rung);
    if (existing) {
      warnings.push(
        `rung ${fr.rung}: collision between ${existing.source} and ${fr.source} — keeping ${existing.source}`,
      );
      continue;
    }
    byRung.set(fr.rung, fr);
  }

  if (readmeEntry) {
    const text = fs.readFileSync(path.join(dir, readmeEntry), 'utf8').trim();
    const rung = smallestFibonacciAtLeast(text.length);
    const existing = byRung.get(rung);
    if (existing) {
      warnings.push(
        `rung ${rung}: collision between ${existing.source} and ${readmeEntry} — keeping ${existing.source} (explicit rung beats README)`,
      );
    } else {
      byRung.set(rung, { rung, source: readmeEntry, text });
    }
  }

  const rungs = [...byRung.values()].sort((a, b) => a.rung - b.rung);
  return { rungs, warnings };
}
