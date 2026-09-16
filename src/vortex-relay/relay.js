// Relay abstraction for vortex-relay: "a shared external mailbox two
// vortexia garantes both read/write to, that isn't either of their own
// MQTT brokers." Every implementation exposes the same two operations —
// VortexRelayBridge (bridge.js) only depends on this shape, not on Gists
// specifically, so the transport can be swapped later (see
// docs/future-las-agent-scope-router.md section 4, cross-machine
// vortex-relay) without touching routing logic. See also MultiRelay below,
// for combining several of these with automatic fallback.
//
// Each environment owns exactly one file (its "inbox from vortex-relay")
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
  // Short, stable label for "which transport carried this" message
  // metadata (see VortexRelayBridge._pollOnce / MultiRelay.lastReadVia) —
  // every Relay implementation has one, even a fake used only in tests.
  name = 'memory';

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
 * `gist` scope. See docs/vortex-relay-poc.md for how to set one up and run
 * this for real between two machines.
 */
export class GistRelay {
  name = 'gist';

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
  name = 'nostr';

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
  // VortexRelayBridge.startDirectorySync), so two calls into a fresh
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

    // querySync fans out across every configured relay and merges whatever
    // each one has — public relays don't all converge to the latest
    // replacement instantly, so more than one (differently stale) copy of
    // a "replaceable" event can legitimately come back at once. Picking
    // the FIRST one found (array order, not recency) risked silently
    // serving a relay's stale cached copy instead of the actual latest
    // write — found live: a directory snapshot kept reading back empty
    // long after a real, non-empty publish had gone out.
    const snapshots = events.filter((e) => e.kind === SNAPSHOT_KIND).sort((a, b) => b.created_at - a.created_at);
    if (snapshots[0]) return JSON.parse(snapshots[0].content);

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
 * por varios conectores, usar el más veloz, fallback si falla uno."
 *
 * appendMessage (the per-message outbox write, on the hot path — every
 * routed message, every poll) is a genuine CHAIN, same shape as
 * ConnectionRouter one level up: try relays in order (constructor order =
 * priority — cheapest/most-reliable first), stop at the first success, and
 * SKIP a circuit-open relay entirely rather than paying for a request
 * that's very likely to fail again. Found live: fanning every append out to
 * every relay in parallel meant a Gist token stuck at 403 got hit on every
 * single message even once Nostr alone was carrying traffic fine — this is
 * the fix. A relay is only ever fully passed over if every OTHER relay
 * failed too (a message should never be silently dropped just because
 * every relay happened to be in cooldown at once — see the second pass
 * below).
 *
 * Because appendMessage no longer guarantees every relay ends up with the
 * same log, readFile can't just return the first non-empty result anymore
 * (a later message chained to a different relay than an earlier one would
 * go missing). Instead it fetches from every non-open relay in parallel and
 * MERGES: log-shaped (array) results are unioned and deduped; snapshot-
 * shaped (object, e.g. a directory file) results resolve to whichever has
 * the newest `updatedAt`.
 *
 * writeFile (full-replace snapshot writes — directory publish only, every
 * ~5min, not the hot path) still fans out to every relay: it's cheap at
 * that cadence, and mirroring the snapshot everywhere means any relay a
 * peer happens to be reading from has it, without leaning on readFile's
 * merge to reconstruct anything.
 *
 * Circuit breaker, per relay: a relay that fails `failureThreshold` times
 * in a row is marked "open" for `cooldownMs`, instead of every single call
 * wasting a round-trip attempting a transport that's been failing for the
 * last hour. It's never permanently excluded: after the cooldown, the next
 * call tries it again (a real request, not a side-channel health check) —
 * success closes the circuit and resets it to normal priority, another
 * failure reopens it for another cooldown. This is what makes "chain
 * multiple transports, fall back automatically" actually automatic — found
 * live: without this, degrading Gist required a human/agent to manually
 * tell the OTHER environment to drop it from config, because every call
 * kept retrying it first no matter how long it had been down.
 */
export class MultiRelay {
  name = 'multi';

  constructor(relays, { failureThreshold = 3, cooldownMs = 60000 } = {}) {
    if (!relays?.length) throw new Error('MultiRelay requires at least one relay');
    this.relays = relays;
    this.failureThreshold = failureThreshold;
    this.cooldownMs = cooldownMs;
    this._health = new Map(relays.map((r) => [r, { consecutiveFailures: 0, openUntil: 0 }]));
    // Which underlying relay actually served the MOST RECENT readFile /
    // writeFile+appendMessage — read immediately after the awaited call by
    // VortexRelayBridge (no other await happens in between, so nothing else
    // can overwrite it first) to tag delivered messages with "how this
    // actually got here" (see docs request: transport as message
    // metadata). Not meaningful before the first call; null until then.
    this.lastReadVia = null;
    this.lastWriteVia = null;
  }

  _isOpen(relay) {
    return this._health.get(relay).openUntil > Date.now();
  }

  _recordSuccess(relay) {
    const h = this._health.get(relay);
    h.consecutiveFailures = 0;
    h.openUntil = 0;
  }

  _recordFailure(relay) {
    const h = this._health.get(relay);
    h.consecutiveFailures++;
    if (h.consecutiveFailures >= this.failureThreshold) {
      h.openUntil = Date.now() + this.cooldownMs;
    }
  }

  // Closed/half-open relays first (their original relative order
  // preserved), open ones last. Open relays stay in the list — a fully-
  // open set still attempts something instead of failing outright, and
  // this is also what lets a genuinely-recovered relay be noticed again:
  // it gets tried last, but it still gets tried.
  _orderedRelays() {
    const usable = [];
    const open = [];
    for (const r of this.relays) (this._isOpen(r) ? open : usable).push(r);
    return [...usable, ...open];
  }

  async readFile(filename) {
    // Skip open (recently-failing) relays entirely — a read only needs
    // good answers from whatever's actually up, so there's no benefit to
    // burning time on a relay that's very likely still down; it still
    // gets retried once its cooldown elapses.
    const candidates = this._orderedRelays().filter((r) => !this._isOpen(r));
    const settled = await Promise.allSettled(candidates.map((r) => r.readFile(filename)));

    let lastErr;
    const successes = [];
    settled.forEach((s, i) => {
      if (s.status === 'fulfilled') {
        this._recordSuccess(candidates[i]);
        successes.push({ relay: candidates[i], result: s.value });
      } else {
        this._recordFailure(candidates[i]);
        lastErr = s.reason;
      }
    });

    // "Every relay failed" must mean every relay actually THREW — found
    // live: Gist 403'd (rate limited) while Nostr correctly had nothing yet
    // for a brand new file (a real, valid empty result, not an error).
    if (!successes.length) {
      if (lastErr) throw lastErr;
      this.lastReadVia = null;
      return [];
    }
    this.lastReadVia = successes.map((s) => s.relay.name).join('+');

    const results = successes.map((s) => s.result);
    if (results.every((r) => Array.isArray(r))) {
      // Log-shaped: appendMessage now chains rather than fanning out (see
      // class doc), so different messages can legitimately live on
      // different relays — union them all, deduped, oldest first.
      const merged = results.flat();
      merged.sort((a, b) => (a?.ts ?? 0) - (b?.ts ?? 0));
      const seen = new Set();
      return merged.filter((m) => {
        const key = JSON.stringify(m);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    }

    // Snapshot-shaped (e.g. a directory file): writeFile still fans out to
    // every relay, so these should usually agree — but pick the newest by
    // `updatedAt` rather than an arbitrary relay, just in case one lagged.
    const withUpdatedAt = results.filter((r) => r && typeof r === 'object' && !Array.isArray(r) && typeof r.updatedAt === 'number');
    if (withUpdatedAt.length) {
      return withUpdatedAt.reduce((latest, r) => (r.updatedAt > latest.updatedAt ? r : latest));
    }
    return results.find((r) => r != null) ?? results[0];
  }

  async appendMessage(filename, message) {
    // First pass: only relays that aren't circuit-open, in priority order —
    // stop at the first success (see class doc: this is a chain, not a
    // fan-out). Second pass, only reached if that whole pass failed (or
    // every relay is currently open): try the open ones too — a message
    // should never be silently dropped just because every relay happened
    // to be in cooldown at once.
    let attempt = await this._tryAppend(filename, message, (r) => !this._isOpen(r));
    if (!attempt.ok) attempt = await this._tryAppend(filename, message, (r) => this._isOpen(r));
    if (!attempt.ok) throw attempt.err ?? new Error(`MultiRelay: no relay available for "${filename}"`);
    return attempt.result;
  }

  // Tries each relay matching `predicate`, in priority order, stopping at
  // the first success.
  async _tryAppend(filename, message, predicate) {
    let err;
    for (const relay of this._orderedRelays()) {
      if (!predicate(relay)) continue;
      try {
        const result = await relay.appendMessage(filename, message);
        this._recordSuccess(relay);
        this.lastWriteVia = relay.name;
        return { ok: true, result };
      } catch (e) {
        this._recordFailure(relay);
        err = e;
      }
    }
    return { ok: false, err };
  }

  async writeFile(filename, data) {
    const ordered = this._orderedRelays();
    const results = await Promise.allSettled(ordered.map((r) => r.writeFile(filename, data)));
    results.forEach((r, i) => (r.status === 'fulfilled' ? this._recordSuccess(ordered[i]) : this._recordFailure(ordered[i])));
    const index = results.findIndex((r) => r.status === 'fulfilled');
    if (index === -1) throw this._aggregateError(results);
    this.lastWriteVia = ordered[index].name;
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
