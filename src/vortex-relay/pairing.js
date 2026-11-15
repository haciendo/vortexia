// Pairing sessions (docs/adr/0001-device-pairing.md §3, §10).
//
// A session starts when the LAS UI (or the CLI) asks for one: a single-use
// token goes into a `vxpair:` URI shown as a QR. A signed PAIR_REQUEST that
// carries the token moves it to `pending`, and a human decision then makes
// it `paired` (friend added, PAIR_ACK ready) or `rejected`. Past expiresAt,
// an undecided session becomes `expired`. Tokens are burned on first use.
//
// State lives in memory: a restart drops pending sessions. That is
// harmless, since the QR simply expires and is shown again.

import crypto from 'node:crypto';

export const DEFAULT_PAIR_TTL_MS = 600_000;
export const MAX_PAIR_TTL_MS = 900_000;
export const DEVICE_EVENT_KIND = 7879;

/** "3F9A·C21E·77B0·4D18" — first 8 bytes of a pubkey, for humans to compare. */
export function fingerprint(pubkeyHex) {
  return String(pubkeyHex).slice(0, 16).toUpperCase().match(/.{4}/g).join('·');
}

export function buildPairUri({ envName, pubkey, token, role, lan = [], ts = [], relays = [] }) {
  const q = new URLSearchParams({ env: envName, pk: pubkey, t: token, role });
  if (lan.length) q.set('lan', lan.join(','));
  if (ts.length) q.set('ts', ts.join(','));
  if (relays.length) q.set('r', relays.join(','));
  return `vxpair:1?${q.toString()}`;
}

export class PairingManager {
  /**
   * @param {object} opts
   * @param {string} opts.envName
   * @param {Uint8Array} opts.secretKey - this env's Nostr key (signs PAIR_ACK)
   * @param {import('./friends.js').FriendsStore} opts.friends
   * @param {() => {lan?: string[], ts?: string[]}} [opts.endpoints]
   * @param {string[]} [opts.relays]
   * @param {(pending: object[]) => void} [opts.onPendingChange]
   * @param {(event: object) => void} [opts.onEvent] - paired/rejected/expired/cancelled
   */
  constructor({ envName, secretKey, friends, endpoints = () => ({}), relays = [], now = Date.now, onPendingChange = () => {}, onEvent = () => {} }) {
    this.envName = envName;
    this.secretKey = secretKey;
    this.friends = friends;
    this.endpoints = endpoints;
    this.relays = relays;
    this.now = now;
    this.onPendingChange = onPendingChange;
    this.onEvent = onEvent;
    this.sessions = new Map();
    this._pubkey = null;
  }

  async pubkey() {
    if (!this._pubkey) this._pubkey = (await import('nostr-tools')).getPublicKey(this.secretKey);
    return this._pubkey;
  }

  async start({ role = 'device', label, ttlMs } = {}) {
    if (!['device', 'env'].includes(role)) throw new Error('role must be device or env');
    const ttl = Math.min(Math.max(Number(ttlMs) || DEFAULT_PAIR_TTL_MS, 30_000), MAX_PAIR_TTL_MS);
    const sessionId = crypto.randomUUID();
    const token = crypto.randomBytes(16).toString('base64url');
    const expiresAt = this.now() + ttl;
    const { lan = [], ts = [] } = this.endpoints() || {};
    const uri = buildPairUri({ envName: this.envName, pubkey: await this.pubkey(), token, role, lan, ts, relays: this.relays });
    this.sessions.set(sessionId, { sessionId, token, role, label, expiresAt, state: 'waiting', requester: null, reply: null });
    return { sessionId, uri, expiresAt };
  }

  cancel(sessionId) {
    const s = this.sessions.get(sessionId);
    if (!s) return false;
    if (s.state === 'waiting' || s.state === 'pending') {
      const wasPending = s.state === 'pending';
      s.state = 'cancelled';
      s.token = null;
      this.onEvent({ type: 'cancelled', sessionId });
      if (wasPending) this._emitPending();
    }
    return true;
  }

  /**
   * A signed event (already signature-checked by the caller) whose
   * decrypted payload is a PAIR_REQUEST. Returns the sessionId it moved to
   * pending.
   * @param {{pubkey: string}} event
   * @param {{type: 'PAIR_REQUEST', token: string, envName: string, label?: string}} payload
   */
  handleRequest(event, payload) {
    this.sweep();
    if (payload?.type !== 'PAIR_REQUEST') throw new Error('not a PAIR_REQUEST');
    const s = [...this.sessions.values()].find((x) => x.token && x.token === payload.token);
    if (!s) throw new Error('unknown or used token');
    if (s.state !== 'waiting') throw new Error(`session is ${s.state}`);
    const envName = String(payload.envName || '');
    if (!envName || /[\s/@,:#+|]/.test(envName)) throw new Error(`invalid envName "${envName}"`);
    if (envName === this.envName) throw new Error('envName is this environment');
    const holder = this.friends.byPubkey(event.pubkey);
    if (holder && holder.envName !== envName) throw new Error(`this key is already paired as "${holder.envName}"`);
    s.token = null; // single use, burned now
    s.state = 'pending';
    s.requester = { pubkey: event.pubkey, envName, label: payload.label || s.label || envName, requestedAt: this.now() };
    this._emitPending();
    return s.sessionId;
  }

  /** @returns {Promise<{ok: true, envName: string} | {ok: false, error: string}>} */
  async decide(sessionId, accept) {
    this.sweep();
    const s = this.sessions.get(sessionId);
    if (!s) return { ok: false, error: 'unknown session' };
    if (s.state === 'expired') return { ok: false, error: 'expired' };
    if (s.state !== 'pending') return { ok: false, error: `session is ${s.state}` };
    const { envName, pubkey, label } = s.requester;
    if (accept) {
      this.friends.add({ envName, pubkey, role: s.role, label });
      s.state = 'paired';
      s.reply = await this._reply(pubkey, {
        type: 'PAIR_ACK', homeEnvName: this.envName, homePubkey: await this.pubkey(), role: s.role,
        endpoints: this.endpoints() || {}, relays: this.relays,
      });
      this.onEvent({ type: 'paired', sessionId, envName });
    } else {
      s.state = 'rejected';
      s.reply = await this._reply(pubkey, { type: 'PAIR_REJECT', homeEnvName: this.envName });
      this.onEvent({ type: 'rejected', sessionId, envName });
    }
    this._emitPending();
    return { ok: true, envName };
  }

  /** What a requester polling its session sees: {state, event?}. */
  status(sessionId) {
    this.sweep();
    const s = this.sessions.get(sessionId);
    if (!s) return null;
    return { state: s.state, ...(s.reply ? { event: s.reply } : {}) };
  }

  pending() {
    return [...this.sessions.values()].filter((s) => s.state === 'pending').map((s) => ({
      sessionId: s.sessionId, role: s.role, envName: s.requester.envName, label: s.requester.label,
      fingerprint: fingerprint(s.requester.pubkey), requestedAt: s.requester.requestedAt, expiresAt: s.expiresAt,
    }));
  }

  /** Expire undecided sessions; forget finished ones an hour after expiry. */
  sweep() {
    const t = this.now();
    let pendingChanged = false;
    for (const s of this.sessions.values()) {
      if ((s.state === 'waiting' || s.state === 'pending') && t > s.expiresAt) {
        if (s.state === 'pending') pendingChanged = true;
        s.state = 'expired';
        s.token = null;
        this.onEvent({ type: 'expired', sessionId: s.sessionId, ...(s.requester ? { envName: s.requester.envName } : {}) });
      }
      if (t > s.expiresAt + 3_600_000) this.sessions.delete(s.sessionId);
    }
    if (pendingChanged) this._emitPending();
  }

  _emitPending() {
    this.onPendingChange(this.pending());
  }

  async _reply(recipientPubkey, payload) {
    return encryptEventTo(this.secretKey, recipientPubkey, payload);
  }
}

/** A signed kind-7879 event whose content is `payload` NIP-44 encrypted to `recipientPubkey`. */
export async function encryptEventTo(secretKey, recipientPubkey, payload) {
  const { finalizeEvent } = await import('nostr-tools');
  const nip44 = await import('nostr-tools/nip44');
  const key = nip44.getConversationKey(secretKey, recipientPubkey);
  return finalizeEvent({
    kind: DEVICE_EVENT_KIND,
    created_at: Math.floor(Date.now() / 1000),
    tags: [['p', recipientPubkey]],
    content: nip44.encrypt(JSON.stringify(payload), key),
  }, secretKey);
}

/** Verify a 7879 event and decrypt its payload with our key. Throws on anything invalid. */
export async function openEvent(secretKey, event) {
  const { verifyEvent } = await import('nostr-tools');
  const nip44 = await import('nostr-tools/nip44');
  if (!event || event.kind !== DEVICE_EVENT_KIND) throw new Error(`event kind must be ${DEVICE_EVENT_KIND}`);
  if (!verifyEvent(event)) throw new Error('bad signature');
  const payload = JSON.parse(nip44.decrypt(event.content, nip44.getConversationKey(secretKey, event.pubkey)));
  return payload;
}
