// Imported first by every test that starts a broker. `npm test` already
// isolates the suite (fake registry URL, temp data dir); this makes a bare
// `node --test test/x.test.js` just as safe. Without it, a test broker claims
// vortexia's ports in the REAL LAS registry, live agents follow it there, and
// closing it releases the daemon's registry entries (2026-11-17 incident).
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

if (!process.env.LAS_REGISTRY_URL) process.env.LAS_REGISTRY_URL = 'http://127.0.0.1:39217';
if (!process.env.VORTEXIA_DATA_DIR) process.env.VORTEXIA_DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'vortexia-test-data-'));
if (!process.env.VORTEXIA_LOG_FILE) process.env.VORTEXIA_LOG_FILE = path.join(os.tmpdir(), 'vortexia-test.log');
