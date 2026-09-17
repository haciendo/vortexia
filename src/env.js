// Loads ROOT/vortexia.env (gitignored, KEY=VALUE per line) into process.env
// before anything else in the module graph reads it. Needed because launchd
// LaunchAgents don't inherit a shell's exported env vars — install-service.sh
// generates a plist with no EnvironmentVariables key, so vortex-relay's
// VORTEXIA_ENV_NAME/VORTEXIA_GIST_ID/VORTEXIA_NOSTR_SECRET_KEY (previously
// only ever `export`ed by hand in a Claude Code terminal) silently vanished
// every time the unmanaged process was handed off to the service — see
// scripts/install-service.sh's "stop the unmanaged instance" step.
//
// MUST be the first import in src/index.js: ES module evaluation runs each
// imported module's top-level code before the importing module's own body,
// in import order — so as long as this is imported first, its process.env
// writes land before logger.js/client.js's module-level `process.env.X ||
// default` reads execute. An explicit shell export still wins (this only
// fills in what's not already set), so the unmanaged/manual-export path
// keeps working exactly as before.

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ENV_FILE = path.join(__dirname, '..', 'vortexia.env');

function parseEnvFile(text) {
  const out = {};
  for (const rawLine of text.split('\n')) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq === -1) continue;
    const key = line.slice(0, eq).trim();
    let value = line.slice(eq + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    if (key) out[key] = value;
  }
  return out;
}

let text;
try {
  text = fs.readFileSync(ENV_FILE, 'utf8');
} catch {
  text = null; // no vortexia.env — fine, nothing to load
}

if (text !== null) {
  for (const [key, value] of Object.entries(parseEnvFile(text))) {
    if (!(key in process.env)) process.env[key] = value;
  }
}
