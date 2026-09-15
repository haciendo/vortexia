// Relay abstraction for the federation PoC: "a shared external mailbox two
// vortexia garantes both read/write to, that isn't either of their own
// MQTT brokers." Every implementation exposes the same two operations —
// FederationBridge (bridge.js) only depends on this shape, not on Gists
// specifically, so the transport can be swapped later (see
// docs/future-las-agent-scope-router.md section 4, cross-machine
// federation) without touching routing logic. See also MultiRelay below,
// for combining several of these with automatic fallback.
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

// Default public relays for NostrRelay — well-established, free, no
// account/API key needed. Override via the `relays` option for a private
// or self-hosted set.
const DEFAULT_NOSTR_RELAYS = ['wss://relay.damus.io', 'wss://nos.lol', 'wss://relay.nostr.band'];

// NIP-78 "application-specific data": a parameterized-replaceable event
// (last publish with the same `d` tag WINS, older ones are dropped by the
// relay) — exact match for a directory snapshot file's single-owner,
// last-writer-wins semantics (see publishDirectory in directory.js).
const SNAPSHOT_KIND = 30078;

// A plain custom "regular" kind (stored and replayable, unlike the
// 20000-29999 "ephemeral" range which relays don't persist) — every
// publish is its OWN event, never replaced. Filtering by the `d` tag
// across every event of this kind gives exactly an append-only log, i.e.
// an outbox file — and unlike GistRelay.appendMessage, there is no
// read-modify-write: each append is a single independent publish, so two
// environments (or two processes) appending at "the same time" can never
// race or clobber each other.
const LOG_KIND = 7878;

/**
 * Real relay backed by the Nostr network — a set of independent, free,
 * publicly-run relays (see DEFAULT_NOSTR_RELAYS) built specifically for
 * this "publish small signed events, anyone can subscribe" pattern. Unlike
 * GistRelay, this isn't fighting the transport's own design: no
 * GitHub-specific secondary rate limit (gist_update, 100/hour shared
 * across every writer on a token), no read-modify-write race on appends,
 * and every event is signed by this environment's own key instead of
 * everyone sharing one bearer token that can write (or impersonate)
 * anything. querySync/publish talk to every configured relay at once and
 * de-duplicate/return-on-first-success — one relay being down or slow
 * doesn't block delivery as long as at least one of the others is up.
 *
 * Trust model: `secretKey` is this environment's own identity, not a
 * shared secret — losing it only lets someone impersonate THIS
 * environment's writes, not every environment's, unlike a shared Gist
 * token. Keep it as private as the Gist token was.
 */
export class NostrRelay {
  /**
   * @param {object} opts
   * @param {Uint8Array} opts.secretKey - this environment's own signing
   *   identity (see nostr-tools generateSecretKey()) — never shared
   *   between environments; each one generates and keeps its own.
   * @param {Record<string, string>} [opts.knownPeerPubkeys] - hex pubkeys
   *   of OTHER environments' NostrRelay identities, keyed by whatever
   *   label is convenient (typically envName) — reads are only trusted
   *   from this environment's own pubkey plus these. A pubkey is public
   *   by design (safe to exchange in the open, e.g. via `las agent
   *   inject`, unlike a Gist token); without it here, this environment
   *   simply can't read that peer's events — there's no way to
   *   discover an unknown peer's identity from the relay itself, by
   *   design (anyone could otherwise claim to be any environment).
   */
  constructor({ secretKey, relays = DEFAULT_NOSTR_RELAYS, queryTimeoutMs = 5000, knownPeerPubkeys = {} } = {}) {
    if (!secretKey) throw new Error('NostrRelay requires a secretKey (see nostr-tools generateSecretKey())');
    this.secretKey = secretKey;
    this.relayUrls = relays;
    this.queryTimeoutMs = queryTimeoutMs;
    this.knownPeerPubkeys = { ...knownPeerPubkeys };
    this._pool = null;
    this._pubkey = null;
    this._ensurePromise = null;
  }

  /** Learn (or update) a peer environment's pubkey after construction — no restart needed to start trusting a newly-exchanged peer. */
  addPeer(label, pubkeyHex) {
    this.knownPeerPubkeys[label] = pubkeyHex;
  }

  /** This environment's own public key (hex) — safe to share openly for a peer to add via addPeer/knownPeerPubkeys. */
  async publicKeyHex() {
    await this._ensure();
    return this._pubkey;
  }

  _trustedAuthors() {
    return [this._pubkey, ...Object.values(this.knownPeerPubkeys)];
  }

  // Memoized in-flight promise, not a synchronous null-check: bridge.js
  // fires publish and sync on independent timers (see
  // FederationBridge.startDirectorySync), so two calls into a fresh
  // NostrRelay can easily land before the first `await import(...)`
  // resolves — a bare `if (this._pool) return` lets both proceed, each
  // creating and assigning its own SimplePool, the second silently
  // orphaning the first.
  async _ensure() {
    if (this._pool) return;
    if (!this._ensurePromise) {
      this._ensurePromise = (async () => {
        const { SimplePool, getPublicKey } = await import('nostr-tools');
        this._pool = new SimplePool();
        this._pubkey = getPublicKey(this.secretKey);
      })();
    }
    await this._ensurePromise;
  }

  async _publish(event) {
    await this._ensure();
    const results = await Promise.allSettled(this._pool.publish(this.relayUrls, event));
    if (!results.some((r) => r.status === 'fulfilled')) {
      throw new Error(`NostrRelay: publish to ${this.relayUrls.join(', ')} failed on every relay`);
    }
  }

  async readFile(filename) {
    await this._ensure();
    const events = await this._pool.querySync(
      this.relayUrls,
      { kinds: [SNAPSHOT_KIND, LOG_KIND], authors: this._trustedAuthors(), '#d': [filename] },
      { maxWait: this.queryTimeoutMs },
    );

    const snapshot = events.find((e) => e.kind === SNAPSHOT_KIND);
    if (snapshot) return JSON.parse(snapshot.content);

    return events
      .filter((e) => e.kind === LOG_KIND)
      .sort((a, b) => a.created_at - b.created_at)
      .map((e) => JSON.parse(e.content));
  }

  async appendMessage(filename, message) {
    const { finalizeEvent } = await import('nostr-tools');
    await this._ensure();
    const event = finalizeEvent(
      { kind: LOG_KIND, created_at: Math.floor(Date.now() / 1000), tags: [['d', filename]], content: JSON.stringify(message) },
      this.secretKey,
    );
    await this._publish(event);
    return this.readFile(filename);
  }

  async writeFile(filename, data) {
    const { finalizeEvent } = await import('nostr-tools');
    await this._ensure();
    const event = finalizeEvent(
      { kind: SNAPSHOT_KIND, created_at: Math.floor(Date.now() / 1000), tags: [['d', filename]], content: JSON.stringify(data) },
      this.secretKey,
    );
    await this._publish(event);
    return data;
  }

  close() {
    this._pool?.destroy();
    this._pool = null;
  }
}

/**
 * Combines several Relay implementations with automatic fallback — "buscar
 * por varios conectores, usar el más veloz, fallback si falla uno." Reads
 * race every relay and return the first successful, non-empty result
 * (falling through to the next relay on error, and treating an empty
 * result as "keep trying the rest" since a relay that's merely behind on
 * propagation shouldn't look identical to real data). Writes fan out to
 * EVERY relay in parallel — a write should land wherever it can, not just
 * the fastest one, since a reader might only be listening on a relay that
 * wasn't first. A write only fails if every relay's write failed.
 */
export class MultiRelay {
  constructor(relays) {
    if (!relays?.length) throw new Error('MultiRelay requires at least one relay');
    this.relays = relays;
  }

  async readFile(filename) {
    let lastErr;
    // "Every relay failed" must mean every relay actually THREW — a later
    // relay succeeding with a legitimately empty result (nothing published
    // there yet) is a real, valid answer and must not be masked by an
    // earlier, unrelated relay's error. Found live: Gist 403'd (rate
    // limited) while Nostr correctly had nothing yet for a brand new file —
    // without tracking success separately from emptiness, that re-threw
    // the Gist error instead of returning the true (empty) result.
    let sawSuccess = false;
    for (const relay of this.relays) {
      try {
        const result = await relay.readFile(filename);
        sawSuccess = true;
        const isEmpty = Array.isArray(result) ? result.length === 0 : result == null;
        if (!isEmpty) return result;
      } catch (err) {
        lastErr = err;
      }
    }
    if (!sawSuccess && lastErr) throw lastErr;
    return [];
  }

  async appendMessage(filename, message) {
    const results = await Promise.allSettled(this.relays.map((r) => r.appendMessage(filename, message)));
    const fulfilled = results.find((r) => r.status === 'fulfilled');
    if (!fulfilled) throw this._aggregateError(results);
    return fulfilled.value;
  }

  async writeFile(filename, data) {
    const results = await Promise.allSettled(this.relays.map((r) => r.writeFile(filename, data)));
    const fulfilled = results.find((r) => r.status === 'fulfilled');
    if (!fulfilled) throw this._aggregateError(results);
    return data;
  }

  // Every relay failed — surface every relay's reason, not just the
  // first. Found live: with only the first reason shown, a Nostr-specific
  // failure was invisible behind an unrelated "GistRelay: write failed
  // with 403" message, which looked like a Gist-only problem it wasn't.
  _aggregateError(results) {
    const reasons = results.map((r) => r.reason?.message ?? String(r.reason)).join('; ');
    return new Error(`MultiRelay: every relay failed — ${reasons}`);
  }

  close() {
    for (const relay of this.relays) relay.close?.();
  }
}
