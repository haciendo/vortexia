// Relay abstraction for the federation PoC: "a shared external mailbox two
// vortexia garantes both read/write to, that isn't either of their own
// MQTT brokers." Every implementation exposes the same two operations —
// FederationBridge (bridge.js) only depends on this shape, not on Gists
// specifically, so the transport can be swapped later (see
// docs/future-las-agent-scope-router.md section 4, cross-machine
// federation) without touching routing logic.
//
// Each environment owns exactly one file (its "inbox from the federation")
// and is the ONLY writer to it — every other environment only reads it.
// That sidesteps the read-modify-write race a shared multi-writer file
// would have (see GistRelay.appendMessage).

/**
 * @typedef {object} Relay
 * @property {(filename: string) => Promise<any[]>} readFile
 * @property {(filename: string, message: any) => Promise<any[]>} appendMessage
 * @property {(filename: string, data: any) => Promise<any>} writeFile
 */

/** In-memory relay — for deterministic tests with no network dependency. */
export class InMemoryRelay {
  constructor() {
    /** @type {Map<string, any[]>} */
    this.files = new Map();
  }

  async readFile(filename) {
    const value = this.files.get(filename);
    if (value === undefined) return [];
    // Mirror GistRelay.readFile: return whatever shape was written
    // (an append-log array, or an object like a directory snapshot),
    // not force-cast to an array.
    return Array.isArray(value) ? [...value] : value;
  }

  async appendMessage(filename, message) {
    const arr = this.files.get(filename) ?? [];
    arr.push(message);
    this.files.set(filename, arr);
    return arr;
  }

  // Full-replace write, for single-owner snapshot files (e.g. a per-env
  // directory roster) rather than an append-only log. Same "exactly one
  // writer per file" rule as appendMessage sidesteps a race here too.
  async writeFile(filename, data) {
    this.files.set(filename, data);
    return data;
  }
}

/**
 * Real relay backed by a GitHub Gist — the actual "internet in the
 * middle" for the live, cross-machine version of this PoC. Reads work
 * unauthenticated (fine for a public gist); writes need a token with the
 * `gist` scope. See docs/federation-poc.md for how to set one up and run
 * this for real between two machines.
 */
export class GistRelay {
  constructor({ gistId, token, apiUrl = 'https://api.github.com', fetchImpl = fetch }) {
    if (!gistId) throw new Error('GistRelay requires a gistId');
    this.gistId = gistId;
    this.token = token;
    this.apiUrl = apiUrl;
    this.fetchImpl = fetchImpl;
  }

  _headers(extra = {}) {
    const headers = { accept: 'application/vnd.github+json', ...extra };
    if (this.token) headers.authorization = `Bearer ${this.token}`;
    return headers;
  }

  async _getGist() {
    const res = await this.fetchImpl(`${this.apiUrl}/gists/${this.gistId}`, { headers: this._headers() });
    if (!res.ok) throw new Error(`GistRelay: read failed with ${res.status}`);
    return res.json();
  }

  async readFile(filename) {
    const gist = await this._getGist();
    const file = gist.files?.[filename];
    if (!file) return [];
    // Gist API truncates file content over ~1MB; for a message log that
    // large this PoC's read-everything-every-poll approach has bigger
    // problems anyway, but fetch the untruncated raw content if it happens.
    const content = file.truncated ? await (await this.fetchImpl(file.raw_url)).text() : file.content;
    return content?.trim() ? JSON.parse(content) : [];
  }

  async appendMessage(filename, message) {
    if (!this.token) throw new Error('GistRelay: appendMessage requires a token with the gist scope');
    const current = await this.readFile(filename).catch(() => []);
    current.push(message);
    const res = await this.fetchImpl(`${this.apiUrl}/gists/${this.gistId}`, {
      method: 'PATCH',
      headers: this._headers({ 'content-type': 'application/json' }),
      body: JSON.stringify({ files: { [filename]: { content: JSON.stringify(current, null, 2) } } }),
    });
    if (!res.ok) throw new Error(`GistRelay: write failed with ${res.status}`);
    return current;
  }

  // Full-replace write — no read-modify-write, so no race to sidestep as
  // long as each file still has exactly one writer (true for a per-env
  // directory snapshot: only that env ever writes its own file).
  async writeFile(filename, data) {
    if (!this.token) throw new Error('GistRelay: writeFile requires a token with the gist scope');
    const res = await this.fetchImpl(`${this.apiUrl}/gists/${this.gistId}`, {
      method: 'PATCH',
      headers: this._headers({ 'content-type': 'application/json' }),
      body: JSON.stringify({ files: { [filename]: { content: JSON.stringify(data, null, 2) } } }),
    });
    if (!res.ok) throw new Error(`GistRelay: write failed with ${res.status}`);
    return data;
  }
}
