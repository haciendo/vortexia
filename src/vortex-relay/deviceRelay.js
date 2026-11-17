// Device link over Nostr relays (docs/adr/0001-device-pairing.md §9.1): the
// same conversation as the HTTP gateway, for a phone away from home with no
// direct path (no LAN, no Tailscale).
//
// Every message is a kind-7879 event, NIP-44 encrypted to its recipient and
// p-tagged with it. The device publishes to the home pubkey; home answers to
// the device pubkey. Payloads are the gateway's own (send, blob, http,
// PAIR_REQUEST), plus:
//
//   device → home  {type:'sync', ack?}     trim the outbox up to `ack`, resend the rest
//   home → device  {type:'message', seq…}  what an agent sent to the device (live, as it arrives)
//                  {type:'ack', ref, …}    an event was processed (ref = its event id or msgId)
//                  {type:'error', ref, status, error}
//                  {type:'pair-pending', ref, sessionId}, then PAIR_ACK | PAIR_REJECT
//                  {type:'synced', ref, cursor}
//
// Relays cap event size, so a payload whose JSON is over PART_BYTES travels as
// {type:'part', msgId, index, total, data} events (data = a base64 slice of
// the UTF-8 JSON), both ways. The receiver joins them and handles the whole
// payload under ref = msgId. A clip goes as one `blob` payload with its
// encrypted bytes inline in `data` (base64), which splits like anything else.
//
// Reliability: the pool reconnects and resumes the subscription, a catch-up
// query runs every minute, and every event id is remembered (persisted), so
// nothing is processed twice. A publish that no relay accepts is retried.

import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { DEVICE_EVENT_KIND, encryptEventTo, openEvent } from './pairing.js';

export const PART_BYTES = 32_000;
export const MAX_PARTS = 400; // ~9.6 MB of base64 JSON per payload
const MAX_AGE_S = 7 * 24 * 3600;
const SEEN_CAP = 5000;
const PART_TTL_MS = 15 * 60_000;
const CATCHUP_MS = 60_000;

/** A payload → one or more payloads, each small enough for one event. */
export function splitPayload(payload, { partBytes = PART_BYTES, msgId = crypto.randomBytes(8).toString('hex') } = {}) {
  const json = JSON.stringify(payload);
  if (Buffer.byteLength(json) <= partBytes) return [payload];
  const b64 = Buffer.from(json).toString('base64');
  const total = Math.ceil(b64.length / partBytes);
  if (total > MAX_PARTS) throw new Error(`payload too large for the relay path (${total} parts, max ${MAX_PARTS})`);
  const parts = [];
  for (let i = 0; i < total; i++) parts.push({ type: 'part', msgId, index: i, total, data: b64.slice(i * partBytes, (i + 1) * partBytes) });
  return parts;
}

/** Joins `part` payloads (any order, all present) back into the original payload. */
export function joinParts(parts) {
  const sorted = [...parts].sort((a, b) => a.index - b.index);
  return JSON.parse(Buffer.from(sorted.map((p) => p.data).join(''), 'base64').toString());
}

export class DeviceRelayLink {
  /**
   * @param {object} opts
   * @param {import('./deviceGateway.js').DeviceGateway} opts.gateway
   * @param {import('./pairing.js').PairingManager|null} opts.pairing
   * @param {import('./friends.js').FriendsStore} opts.friends
   * @param {Uint8Array} opts.secretKey
   * @param {string[]} opts.relays
   * @param {string} opts.stateFile - seen event ids + the newest created_at
   * @param {object} [opts.pool] - a SimplePool-like {subscribeMany, querySync, publish, destroy} (tests)
   */
  constructor({ gateway, pairing = null, friends, secretKey, relays, stateFile, pool = null, now = Date.now, logger = console, retryDelaysMs = [2000, 10_000, 60_000, 300_000] }) {
    this.gateway = gateway;
    this.pairing = pairing;
    this.friends = friends;
    this.secretKey = secretKey;
    this.relays = relays;
    this.stateFile = stateFile;
    this.pool = pool;
    this.now = now;
    this.logger = logger;
    this.retryDelaysMs = retryDelaysMs;
    this._parts = new Map(); // msgId → {pubkey, total, parts: Map, firstAt}
    this._relaySessions = new Map(); // pairing sessionId → requester pubkey
    this._timers = new Set();
    this._sub = null;
    this._closed = false;
    this._state = this._loadState();
  }

  async start() {
    const { getPublicKey } = await import('nostr-tools');
    this.pubkey = getPublicKey(this.secretKey);
    if (!this.pool) {
      const { SimplePool } = await import('nostr-tools');
      const websocketImplementation = typeof globalThis.WebSocket === 'undefined' ? (await import('ws')).default : undefined;
      this.pool = new SimplePool({ enablePing: true, enableReconnect: true, ...(websocketImplementation ? { websocketImplementation } : {}) });
    }
    if (this.pairing) {
      const prev = this.pairing.onEvent;
      this.pairing.onEvent = (evt) => {
        prev?.(evt);
        this._onPairEvent(evt).catch((err) => this.logger.warn?.(`[vortexia] device relay: pairing reply: ${err.message}`));
      };
    }
    this.gateway.onOutbox = (envName, payload) => this._pushOutbox(envName, payload);
    this._subscribe();
    const catchup = setInterval(() => this._catchUp(), CATCHUP_MS);
    catchup.unref?.();
    this._timers.add(catchup);
    this.logger.info?.(`[vortexia] device relay link on ${this.relays.length} relay(s) for ${this.pubkey.slice(0, 12)}…`);
    return this;
  }

  close() {
    this._closed = true;
    for (const t of this._timers) { clearInterval(t); clearTimeout(t); }
    this._timers.clear();
    try { this._sub?.close(); } catch { /* already closed */ }
    try { this.pool?.destroy?.(); } catch { /* best effort */ }
  }

  _filter(sinceS) {
    return { kinds: [DEVICE_EVENT_KIND], '#p': [this.pubkey], since: sinceS };
  }

  _since() {
    const floor = Math.floor(this.now() / 1000) - MAX_AGE_S;
    return Math.max(floor, (this._state.since || 0) - 600);
  }

  _subscribe() {
    this._sub = this.pool.subscribeMany(this.relays, this._filter(this._since()), {
      onevent: (event) => this.handleEvent(event).catch((err) => this.logger.warn?.(`[vortexia] device relay: ${err.message}`)),
    });
  }

  async _catchUp() {
    if (this._closed) return;
    try {
      const events = await this.pool.querySync(this.relays, this._filter(Math.floor(this.now() / 1000) - 900), { maxWait: 8000 });
      for (const e of events.sort((a, b) => a.created_at - b.created_at)) await this.handleEvent(e).catch(() => {});
    } catch (err) {
      this.logger.warn?.(`[vortexia] device relay: catch-up query failed: ${err.message}`);
    }
  }

  // ── state ────────────────────────────────────────────────────────────

  _loadState() {
    try {
      const s = JSON.parse(fs.readFileSync(this.stateFile, 'utf8'));
      return { since: s.since || 0, seen: new Map(Object.entries(s.seen || {})) };
    } catch {
      return { since: 0, seen: new Map() };
    }
  }

  _saveState() {
    const floor = Math.floor(this.now() / 1000) - MAX_AGE_S;
    for (const [id, at] of this._state.seen) if (at < floor) this._state.seen.delete(id);
    if (this._state.seen.size > SEEN_CAP) {
      const keep = [...this._state.seen].sort((a, b) => b[1] - a[1]).slice(0, SEEN_CAP);
      this._state.seen = new Map(keep);
    }
    fs.mkdirSync(path.dirname(this.stateFile), { recursive: true });
    const tmp = `${this.stateFile}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify({ since: this._state.since, seen: Object.fromEntries(this._state.seen) }), { mode: 0o600 });
    fs.renameSync(tmp, this.stateFile);
  }

  // ── incoming ─────────────────────────────────────────────────────────

  /** One event from a relay. Public for tests. */
  async handleEvent(event) {
    if (event?.kind !== DEVICE_EVENT_KIND || this._state.seen.has(event.id)) return;
    if (event.created_at < Math.floor(this.now() / 1000) - MAX_AGE_S) return;
    let payload;
    try {
      payload = await openEvent(this.secretKey, event);
    } catch {
      return; // not for us, forged, or garbage: say nothing
    }
    this._state.seen.set(event.id, event.created_at);
    this._state.since = Math.max(this._state.since, event.created_at);
    this._saveState();

    let ref = event.id;
    if (payload?.type === 'part') {
      const joined = this._collectPart(event.pubkey, payload);
      if (!joined) return;
      payload = joined;
      ref = payload.__msgId;
      delete payload.__msgId;
    }

    if (payload?.type === 'PAIR_REQUEST') return this._pairRequest(event, payload, ref);

    const friend = this.friends.byPubkey(event.pubkey);
    if (!friend || friend.role !== 'device') return; // strangers get no answer
    await this.gateway._ensureConsumer(friend.envName).catch(() => {});
    let reply;
    try {
      if (payload?.type === 'sync') {
        const cursor = await this._sync(friend, payload.ack);
        reply = { type: 'synced', ref, cursor };
      } else {
        const result = await this.gateway._dispatch(friend, payload);
        reply = result.reply ? { ...result.reply, ref } : { ...result, type: 'ack', ref };
      }
    } catch (err) {
      reply = { type: 'error', ref, status: err.status || 500, error: err.message };
      this.logger.warn?.(`[vortexia] device relay: ${payload?.type} from ${friend.envName}: ${err.message}`);
    }
    await this.publishTo(friend.pubkey, reply);
  }

  _collectPart(pubkey, p) {
    const t = this.now();
    for (const [id, x] of this._parts) if (t - x.firstAt > PART_TTL_MS) this._parts.delete(id);
    // Parts only from a paired device: a stranger cannot make us buffer megabytes.
    const friend = this.friends.byPubkey(pubkey);
    if (!friend || friend.role !== 'device') return null;
    if (typeof p.msgId !== 'string' || !Number.isInteger(p.index) || !Number.isInteger(p.total) || p.total < 1 || p.total > MAX_PARTS || p.index < 0 || p.index >= p.total || typeof p.data !== 'string') return null;
    const key = `${pubkey}:${p.msgId}`;
    let entry = this._parts.get(key);
    if (!entry) { entry = { total: p.total, parts: new Map(), firstAt: t }; this._parts.set(key, entry); }
    if (entry.total !== p.total) return null;
    entry.parts.set(p.index, p);
    if (entry.parts.size < entry.total) return null;
    this._parts.delete(key);
    try {
      const payload = joinParts([...entry.parts.values()]);
      return { ...payload, __msgId: p.msgId };
    } catch (err) {
      this.logger.warn?.(`[vortexia] device relay: parts of ${p.msgId} do not join: ${err.message}`);
      return null;
    }
  }

  async _pairRequest(event, payload, ref) {
    if (!this.pairing) return;
    let sessionId;
    try {
      sessionId = this.pairing.handleRequest(event, payload);
    } catch (err) {
      return this.publishTo(event.pubkey, { type: 'error', ref, status: 409, error: err.message });
    }
    this._relaySessions.set(sessionId, event.pubkey);
    return this.publishTo(event.pubkey, { type: 'pair-pending', ref, sessionId });
  }

  async _onPairEvent(evt) {
    const pubkey = this._relaySessions.get(evt.sessionId);
    if (!pubkey) return;
    if (evt.type !== 'paired' && evt.type !== 'rejected') {
      if (evt.type === 'expired' || evt.type === 'cancelled') this._relaySessions.delete(evt.sessionId);
      return;
    }
    this._relaySessions.delete(evt.sessionId);
    const st = this.pairing.status(evt.sessionId);
    // The PAIR_ACK/PAIR_REJECT event is already sealed for the device: publish it as is.
    if (st?.event) await this._publishEvent(st.event);
  }

  async _sync(friend, ack) {
    const box = this.gateway.ackOutbox(friend.envName, Number(ack));
    for (const item of box.items) await this.publishTo(friend.pubkey, this.gateway.outboxPayload(item));
    return box.seq;
  }

  // ── outgoing ─────────────────────────────────────────────────────────

  _pushOutbox(envName, payload) {
    const friend = this.friends.get(envName);
    if (!friend || friend.revokedAt || friend.role !== 'device') return;
    this.publishTo(friend.pubkey, payload).catch((err) => this.logger.warn?.(`[vortexia] device relay: outbox to ${envName}: ${err.message}`));
  }

  /** Seal `payload` (split if needed) to `pubkey` and publish it, retrying while no relay takes it. */
  async publishTo(pubkey, payload) {
    for (const p of splitPayload(payload)) {
      await this._publishEvent(await encryptEventTo(this.secretKey, pubkey, p));
    }
  }

  async _publishEvent(event, attempt = 0) {
    if (this._closed) return false;
    const results = await Promise.allSettled(this.pool.publish(this.relays, event));
    if (results.some((r) => r.status === 'fulfilled')) return true;
    const reasons = results.map((r) => r.reason?.message || String(r.reason)).join('; ');
    if (attempt >= this.retryDelaysMs.length) {
      this.logger.error?.(`[vortexia] device relay: gave up publishing ${event.id.slice(0, 12)} after ${attempt + 1} attempts: ${reasons}`);
      return false;
    }
    this.logger.warn?.(`[vortexia] device relay: no relay took ${event.id.slice(0, 12)} (${reasons}); retrying`);
    await new Promise((resolve) => {
      const t = setTimeout(() => { this._timers.delete(t); resolve(); }, this.retryDelaysMs[attempt]);
      t.unref?.();
      this._timers.add(t);
    });
    return this._publishEvent(event, attempt + 1);
  }
}
