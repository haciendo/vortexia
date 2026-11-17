// Device gateway (docs/adr/0001-device-pairing.md §6, §8, §9): plain HTTP on
// the broker's WS port, for paired phones on a direct path (LAN/Tailscale).
//
//   POST /device/events          one signed 7879 event (send | blob | http | PAIR_REQUEST)
//   GET  /device/events?ack=N    the device's outbox (messages to it), as 7879 events
//   GET  /device/pair/<id>       pairing status for a requester (no auth: the reply is encrypted)
//   GET  /device/<path>          proxied to LAS at 127.0.0.1:8700/device/<path>, body NIP-44 encrypted
//   PUT  /device/blob/<sha256>   an AES-256-GCM encrypted clip (its key comes in a `blob` event)
//
// Every request except PAIR_REQUEST and pairing status carries
// `Authorization: Nostr <base64 event>` (NIP-98: kind 27235, u, method,
// payload) signed by an active device friend. VORTEXIA_DEVICE_DEV=1 drops
// auth and encryption for a dev build, but only for loopback or
// private-range sources.

import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { VortexiaClient } from '../client.js';
import { DEVICE_AUDIO_MAILBOX } from '../topics.js';
import { isLoopback } from '../auth.js';
import { DEVICE_EVENT_KIND, encryptEventTo, openEvent } from './pairing.js';

export const HTTP_AUTH_KIND = 27235;
const AUTH_WINDOW_S = 120;
const MAX_EVENT_BYTES = 256 * 1024;
const MAX_BLOB_BYTES = 64 * 1024 * 1024;
const HEX64 = /^[0-9a-f]{64}$/;
const EXT = { 'audio/mp4': 'm4a', 'audio/m4a': 'm4a', 'audio/x-m4a': 'm4a', 'audio/aac': 'aac', 'audio/mpeg': 'mp3', 'audio/wav': 'wav', 'audio/webm': 'webm', 'audio/ogg': 'ogg' };

export function isPrivateAddress(address) {
  const a = String(address || '').replace(/^::ffff:/, '');
  if (isLoopback(a)) return true;
  if (/^10\./.test(a) || /^192\.168\./.test(a)) return true;
  if (/^172\.(1[6-9]|2\d|3[01])\./.test(a)) return true;
  const m = /^100\.(\d+)\./.exec(a); // Tailscale CGNAT 100.64.0.0/10
  if (m && Number(m[1]) >= 64 && Number(m[1]) <= 127) return true;
  return /^f[cd]/i.test(a) || /^fe80:/i.test(a);
}

const sha256hex = (buf) => crypto.createHash('sha256').update(buf).digest('hex');

/** AES-256-GCM with body = nonce(12) ‖ ciphertext ‖ tag(16). */
export function encryptBlob(plaintext, key) {
  const nonce = crypto.randomBytes(12);
  const c = crypto.createCipheriv('aes-256-gcm', key, nonce);
  return Buffer.concat([nonce, c.update(plaintext), c.final(), c.getAuthTag()]);
}

export function decryptBlob(body, key) {
  if (body.length < 28) throw new Error('blob too short');
  const d = crypto.createDecipheriv('aes-256-gcm', key, body.subarray(0, 12));
  d.setAuthTag(body.subarray(body.length - 16));
  return Buffer.concat([d.update(body.subarray(12, body.length - 16)), d.final()]);
}

/** Build the NIP-98 Authorization header value (used by tests and by any Node client). */
export async function buildHttpAuth(secretKey, { url, method, body }) {
  const { finalizeEvent } = await import('nostr-tools');
  // nonce: two identical requests in the same second would otherwise be the
  // same event, and the second one would be rejected as a replay.
  const tags = [['u', url], ['method', method], ['nonce', crypto.randomBytes(8).toString('hex')]];
  if (body && body.length) tags.push(['payload', sha256hex(body)]);
  const event = finalizeEvent({ kind: HTTP_AUTH_KIND, created_at: Math.floor(Date.now() / 1000), tags, content: '' }, secretKey);
  return `Nostr ${Buffer.from(JSON.stringify(event)).toString('base64')}`;
}

function readBody(req, limit) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) { reject(Object.assign(new Error('body too large'), { status: 413 })); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

class HttpError extends Error {
  constructor(status, message) { super(message); this.status = status; }
}

export class DeviceGateway {
  constructor({
    envName, secretKey, friends, pairing, mqttPort, dataDir,
    lasUrl = process.env.LAS_BACKEND_URL || 'http://127.0.0.1:8700',
    dev = /^(1|true|yes)$/i.test(process.env.VORTEXIA_DEVICE_DEV || ''),
    logger = console, ClientImpl = VortexiaClient,
  }) {
    this.envName = envName;
    this.secretKey = secretKey;
    this.friends = friends;
    this.pairing = pairing;
    this.mqttPort = mqttPort;
    this.dataDir = dataDir;
    this.inboxDir = path.join(dataDir, 'device-inbox');
    this.outboxDir = path.join(dataDir, 'device-outbox');
    this.lasUrl = lasUrl.replace(/\/$/, '');
    this.dev = dev;
    this.logger = logger;
    this._ClientImpl = ClientImpl;
    this._seenAuth = new Map();
    this._consumers = new Map(); // device envName → VortexiaClient (mailbox consumer)
    this.client = null;
    /** (deviceEnvName, messagePayload) → void: set by the relay link to push outbox items live. */
    this.onOutbox = null;
  }

  async start() {
    this.client = new this._ClientImpl({ port: this.mqttPort });
    await this.client.register(`${this.envName}-device-gateway`, { presence: false });
    for (const f of this.friends.active().filter((x) => x.role === 'device')) await this._ensureConsumer(f.envName).catch(() => {});
    if (this.dev) this.logger.warn?.('[vortexia] device gateway DEV MODE: no auth, no encryption, private sources only (VORTEXIA_DEVICE_DEV)');
    return this;
  }

  attach(server) {
    server.on('request', (req, res) => {
      if (!req.url.startsWith('/device/')) { res.writeHead(404).end(); return; }
      this.handle(req, res).catch((err) => {
        const status = err.status || 500;
        if (status >= 500) this.logger.error?.(`[vortexia] device gateway: ${req.method} ${req.url}: ${err.stack || err.message}`);
        if (!res.headersSent) res.writeHead(status, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      });
    });
    return this;
  }

  async close() {
    for (const c of this._consumers.values()) await c.close().catch(() => {});
    await this.client?.close().catch(() => {});
  }

  // ── request dispatch ─────────────────────────────────────────────────

  async handle(req, res) {
    const address = req.socket.remoteAddress;
    if (this.dev && !isPrivateAddress(address)) throw new HttpError(403, 'dev mode only serves loopback/private addresses');
    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const p = url.pathname;

    const pair = /^\/device\/pair\/([\w-]+)$/.exec(p);
    if (req.method === 'GET' && pair) return this._pairStatus(res, pair[1]);

    if (req.method === 'POST' && p === '/device/events') {
      const body = await readBody(req, MAX_EVENT_BYTES);
      return this._postEvent(req, res, url, body);
    }
    const blob = /^\/device\/blob\/([0-9a-f]{64})$/.exec(p);
    if (req.method === 'PUT' && blob) {
      const body = await readBody(req, MAX_BLOB_BYTES);
      const device = await this._authenticate(req, url, body);
      return this._putBlob(res, device, blob[1], body, url);
    }
    if (req.method === 'GET' && p === '/device/events') {
      const device = await this._authenticate(req, url, null);
      return this._getOutbox(res, device, url);
    }
    if (req.method === 'GET') {
      const device = await this._authenticate(req, url, null);
      return this._proxy(req, res, device, `${p}${url.search}`);
    }
    throw new HttpError(405, `${req.method} ${p} not supported`);
  }

  /** NIP-98 → the device friend, or the dev identity. */
  async _authenticate(req, url, body) {
    if (this.dev) {
      const envName = String(req.headers['x-vortexia-dev-device'] || 'dev-device');
      return { envName, label: envName, role: 'device', pubkey: null, dev: true };
    }
    const header = String(req.headers.authorization || '');
    if (!header.startsWith('Nostr ')) throw new HttpError(401, 'missing Authorization: Nostr …');
    let event;
    try { event = JSON.parse(Buffer.from(header.slice(6), 'base64').toString()); } catch { throw new HttpError(401, 'bad Authorization payload'); }
    const { verifyEvent } = await import('nostr-tools');
    if (event?.kind !== HTTP_AUTH_KIND || !verifyEvent(event)) throw new HttpError(401, 'bad auth event');
    if (Math.abs(Math.floor(Date.now() / 1000) - event.created_at) > AUTH_WINDOW_S) throw new HttpError(401, 'auth event too old or in the future');
    const tag = (k) => event.tags.find((t) => t[0] === k)?.[1];
    if (tag('method') !== req.method) throw new HttpError(401, 'auth method mismatch');
    if (tag('u') !== url.href) throw new HttpError(401, `auth url mismatch (signed ${tag('u')}, got ${url.href})`);
    if (body && body.length && tag('payload') !== sha256hex(body)) throw new HttpError(401, 'auth payload hash mismatch');
    for (const [id, exp] of this._seenAuth) if (exp < Date.now()) this._seenAuth.delete(id);
    if (this._seenAuth.has(event.id)) throw new HttpError(401, 'replayed auth event');
    const friend = this.friends.list().find((f) => f.pubkey === event.pubkey);
    if (!friend) throw new HttpError(401, 'not a friend');
    if (friend.revokedAt) throw new HttpError(403, 'revoked');
    if (friend.role !== 'device') throw new HttpError(403, 'only devices use the gateway');
    this._seenAuth.set(event.id, Date.now() + AUTH_WINDOW_S * 2000);
    this._ensureConsumer(friend.envName).catch(() => {});
    return friend;
  }

  _json(res, status, value) {
    res.writeHead(status, { 'content-type': 'application/json' });
    res.end(JSON.stringify(value));
  }

  // ── pairing ──────────────────────────────────────────────────────────

  _pairStatus(res, sessionId) {
    const st = this.pairing?.status(sessionId);
    if (!st) throw new HttpError(404, 'unknown session');
    const code = st.state === 'paired' ? 200 : (st.state === 'waiting' || st.state === 'pending') ? 202 : 410;
    return this._json(res, code, st);
  }

  // ── events ───────────────────────────────────────────────────────────

  async _postEvent(req, res, url, body) {
    let parsed;
    try { parsed = JSON.parse(body.toString()); } catch { throw new HttpError(400, 'body is not JSON'); }

    if (this.dev && parsed && parsed.kind !== DEVICE_EVENT_KIND) {
      const device = await this._authenticate(req, url, body);
      return this._json(res, 200, { events: [], results: [await this._dispatch(device, parsed)] });
    }

    let payload;
    try { payload = await openEvent(this.secretKey, parsed); } catch (err) { throw new HttpError(400, err.message); }

    if (payload?.type === 'PAIR_REQUEST') {
      if (!this.pairing) throw new HttpError(503, 'pairing not available');
      try {
        const sessionId = this.pairing.handleRequest(parsed, payload);
        return this._json(res, 202, { sessionId });
      } catch (err) {
        throw new HttpError(409, err.message);
      }
    }

    const device = await this._authenticate(req, url, body);
    if (!device.dev && parsed.pubkey !== device.pubkey) throw new HttpError(403, 'event signer is not the authenticated device');
    const result = await this._dispatch(device, payload);
    const events = result.reply ? [await this._seal(device, result.reply)] : [];
    return this._json(res, result.reply ? 200 : 202, { events, ...(this.dev ? { results: [result] } : {}) });
  }

  async _seal(device, payload) {
    return device.dev ? payload : encryptEventTo(this.secretKey, device.pubkey, payload);
  }

  async _dispatch(device, payload) {
    switch (payload?.type) {
      case 'send': return this._send(device, payload);
      case 'blob': return this._blobKey(device, payload);
      case 'http': return this._httpEvent(device, payload);
      default: throw new HttpError(400, `unknown event type ${payload?.type}`);
    }
  }

  /** {type:'send', to:'Agent'|'Agent/<sid|runtime>', text, kind?} → that agent's inbox, as the human on the device. */
  async _send(device, { to, text, kind = 'message' }) {
    if (!to || typeof to !== 'string' || typeof text !== 'string' || !text) throw new HttpError(400, 'send needs to and text');
    const [target, session] = to.split('/', 2);
    if (target.includes('@') && !target.endsWith(`@${this.envName}`)) throw new HttpError(400, 'a device can only address its home environment');
    const agent = target.replace(`@${this.envName}`, '');
    const envelope = this.client.send(agent, text, {
      from: device.envName, source: 'human', origin: 'human', via: 'device',
      ...(kind === 'command' ? { kind: 'command' } : {}),
      ...(session ? { session } : {}),
    });
    return { ok: true, type: 'send', id: envelope.id };
  }

  async _httpEvent(device, { id, method = 'GET', path: p, ifNoneMatch }) {
    if (method !== 'GET') throw new HttpError(405, 'only GET is proxied');
    const r = await this._fetchLas(device, p, ifNoneMatch);
    return { ok: true, reply: { type: 'http-response', id, status: r.status, headers: r.headers, body: r.body } };
  }

  // ── proxy to LAS ─────────────────────────────────────────────────────

  async _fetchLas(device, p, ifNoneMatch) {
    if (typeof p !== 'string' || !p.startsWith('/device/') || p.includes('..')) throw new HttpError(400, 'only /device/* paths are proxied');
    const headers = { 'x-vortexia-device': device.envName, 'x-vortexia-device-label': encodeURIComponent(device.label || device.envName) };
    if (ifNoneMatch) headers['if-none-match'] = ifNoneMatch;
    let r;
    try {
      r = await fetch(`${this.lasUrl}${p}`, { headers });
    } catch (err) {
      throw new HttpError(502, `LAS backend unreachable: ${err.message}`);
    }
    const body = r.status === 304 ? '' : await r.text();
    return { status: r.status, headers: { etag: r.headers.get('etag'), 'content-type': r.headers.get('content-type') }, body };
  }

  async _proxy(req, res, device, p) {
    const r = await this._fetchLas(device, p, req.headers['if-none-match']);
    const head = { ...(r.headers.etag ? { etag: r.headers.etag } : {}) };
    if (r.status === 304) { res.writeHead(304, head); res.end(); return; }
    if (device.dev) {
      res.writeHead(r.status, { ...head, 'content-type': r.headers['content-type'] || 'application/octet-stream' });
      res.end(r.body);
      return;
    }
    const nip44 = await import('nostr-tools/nip44');
    const sealed = nip44.encrypt(r.body || '', nip44.getConversationKey(this.secretKey, device.pubkey));
    res.writeHead(r.status, { ...head, 'content-type': 'application/vnd.vortexia.nip44', 'x-vortexia-content-type': r.headers['content-type'] || '' });
    res.end(sealed);
  }

  // ── clips ────────────────────────────────────────────────────────────

  _incoming(envName) {
    const dir = path.join(this.inboxDir, envName, '.incoming');
    fs.mkdirSync(dir, { recursive: true });
    return dir;
  }

  /** {type:'blob', sha256, key(base64), mime, durationMs, capturedAt, target:{agent, session?}} */
  async _blobKey(device, meta) {
    if (!HEX64.test(meta?.sha256 || '')) throw new HttpError(400, 'blob needs sha256');
    if (!meta.target?.agent) throw new HttpError(400, 'blob needs target.agent (there is no general mic)');
    if (!device.dev && Buffer.from(String(meta.key || ''), 'base64').length !== 32) throw new HttpError(400, 'blob key must be 32 bytes, base64');
    const { data, ...rest } = meta;
    // The relay path carries the encrypted bytes inline (base64), with no PUT.
    if (typeof data === 'string' && data && !this._findFinal(device.envName, meta.sha256)) {
      const body = Buffer.from(data, 'base64');
      if (body.length > MAX_BLOB_BYTES) throw new HttpError(413, 'clip too large');
      writeAtomic(path.join(this._incoming(device.envName), `${meta.sha256}.bin`), body, 0o600);
    }
    const file = path.join(this._incoming(device.envName), `${meta.sha256}.key.json`);
    writeAtomic(file, JSON.stringify(rest), 0o600);
    const delivered = await this._tryFinalize(device, meta.sha256);
    return { ok: true, type: 'blob', sha256: meta.sha256, delivered };
  }

  async _putBlob(res, device, sha, body, url) {
    if (!body.length) throw new HttpError(400, 'empty body');
    const final = this._findFinal(device.envName, sha);
    if (final) return this._json(res, 200, { sha256: sha, already: true });
    const incoming = this._incoming(device.envName);
    if (device.dev) {
      // Dev: plaintext body, metadata in the query string.
      const q = url.searchParams;
      if (!q.get('agent')) throw new HttpError(400, 'dev upload needs ?agent=');
      if (sha256hex(body) !== sha) throw new HttpError(400, 'sha256 does not match the body');
      writeAtomic(path.join(incoming, `${sha}.plain`), body);
      writeAtomic(path.join(incoming, `${sha}.key.json`), JSON.stringify({
        sha256: sha, mime: q.get('mime') || 'audio/mp4', durationMs: Number(q.get('durationMs')) || null,
        capturedAt: Number(q.get('capturedAt')) || Date.now(),
        target: { agent: q.get('agent'), ...(q.get('session') ? { session: q.get('session') } : {}) },
      }));
    } else {
      writeAtomic(path.join(incoming, `${sha}.bin`), body, 0o600);
    }
    const delivered = await this._tryFinalize(device, sha);
    return this._json(res, 201, { sha256: sha, delivered });
  }

  _findFinal(envName, sha) {
    const dir = path.join(this.inboxDir, envName);
    try {
      return fs.readdirSync(dir).find((f) => f.startsWith(`${sha}.`) && f.endsWith('.json')) ? true : false;
    } catch {
      return false;
    }
  }

  /** Once both the bytes and the key are here: decrypt, verify, write clip + sidecar, notify LAS. */
  async _tryFinalize(device, sha) {
    const incoming = this._incoming(device.envName);
    const keyFile = path.join(incoming, `${sha}.key.json`);
    const binFile = path.join(incoming, `${sha}.bin`);
    const plainFile = path.join(incoming, `${sha}.plain`);
    if (!fs.existsSync(keyFile) || (!fs.existsSync(binFile) && !fs.existsSync(plainFile))) return false;
    const meta = JSON.parse(fs.readFileSync(keyFile, 'utf8'));
    let clip;
    if (fs.existsSync(plainFile)) {
      clip = fs.readFileSync(plainFile);
    } else {
      try {
        clip = decryptBlob(fs.readFileSync(binFile), Buffer.from(meta.key, 'base64'));
      } catch (err) {
        throw new HttpError(400, `clip does not decrypt with its key: ${err.message}`);
      }
    }
    if (sha256hex(clip) !== sha) throw new HttpError(400, 'decrypted clip does not match its sha256');

    const dir = path.join(this.inboxDir, device.envName);
    const ext = EXT[meta.mime] || 'bin';
    const clipPath = path.join(dir, `${sha}.${ext}`);
    const notice = {
      type: 'device-audio', from: device.envName, label: device.label || device.envName,
      path: clipPath, sha256: sha, mime: meta.mime || 'application/octet-stream',
      durationMs: meta.durationMs ?? null, capturedAt: meta.capturedAt ?? null,
      target: { agent: meta.target.agent, ...(meta.target.session ? { session: meta.target.session } : {}) },
    };
    // Order matters (ADR §6): clip, then sidecar, then the notice.
    writeAtomic(clipPath, clip);
    writeAtomic(path.join(dir, `${sha}.json`), JSON.stringify(notice, null, 2));
    for (const f of [keyFile, binFile, plainFile]) fs.rmSync(f, { force: true });
    this.client.send(DEVICE_AUDIO_MAILBOX, JSON.stringify(notice), { from: device.envName, source: 'system', kind: 'device-audio' });
    this.logger.info?.(`[vortexia] device gateway: clip ${sha.slice(0, 12)} from ${device.envName} → ${notice.target.agent}`);
    return true;
  }

  // ── outbox: what agents send to the device ───────────────────────────

  _outboxFile(envName) {
    return path.join(this.outboxDir, `${envName}.json`);
  }

  _readOutbox(envName) {
    try { return JSON.parse(fs.readFileSync(this._outboxFile(envName), 'utf8')); } catch { return { seq: 0, items: [] }; }
  }

  /** Hold the device's mailbox (las-agent-<device>) and copy what arrives to a file the device drains. */
  async _ensureConsumer(envName) {
    if (this._consumers.has(envName)) return;
    const c = new this._ClientImpl({ port: this.mqttPort });
    this._consumers.set(envName, c);
    c.on('message', (envelope) => {
      if (envelope?.to !== envName) return;
      const box = this._readOutbox(envName);
      box.seq += 1;
      box.items.push({ seq: box.seq, envelope });
      if (box.items.length > 500) box.items = box.items.slice(-500);
      fs.mkdirSync(this.outboxDir, { recursive: true });
      writeAtomic(this._outboxFile(envName), JSON.stringify(box), 0o600);
      try { this.onOutbox?.(envName, this.outboxPayload(box.items.at(-1))); } catch (err) { this.logger.warn?.(`[vortexia] device gateway: onOutbox: ${err.message}`); }
    });
    c.on('close', () => { this._consumers.delete(envName); });
    try {
      await c.register(envName, { mailbox: true, presence: false });
    } catch (err) {
      this._consumers.delete(envName);
      throw err;
    }
  }

  /** Drop outbox items up to `ack` (when it is a positive number); returns what is left. */
  ackOutbox(envName, ack) {
    const box = this._readOutbox(envName);
    if (Number.isFinite(ack) && ack > 0) {
      box.items = box.items.filter((i) => i.seq > ack);
      fs.mkdirSync(this.outboxDir, { recursive: true });
      writeAtomic(this._outboxFile(envName), JSON.stringify(box), 0o600);
    }
    return box;
  }

  /** The `message` payload a device receives for one outbox item (same on HTTP and relay). */
  outboxPayload(item) {
    const { from, text, ts, id, kind, source } = item.envelope;
    return { type: 'message', seq: item.seq, id, from, text, ts, kind: kind || 'message', source };
  }

  async _getOutbox(res, device, url) {
    const box = this.ackOutbox(device.envName, Number(url.searchParams.get('ack')));
    const events = [];
    for (const i of box.items) events.push(await this._seal(device, this.outboxPayload(i)));
    return this._json(res, 200, { cursor: box.seq, events });
  }
}

function writeAtomic(file, data, mode) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = `${file}.${process.pid}.${crypto.randomBytes(4).toString('hex')}.tmp`;
  fs.writeFileSync(tmp, data, mode ? { mode } : undefined);
  fs.renameSync(tmp, file);
}
