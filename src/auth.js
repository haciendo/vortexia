// Broker authentication for non-loopback clients (docs/adr/0001-device-pairing.md §5).
//
// A client that is not on loopback connects with
//   username = its pubkey (hex), password = JSON of a signed Nostr event
//   { kind 22242, created_at within ±AUTH_WINDOW_S, tags [["challenge", <this broker's envName>]] }
// and its pubkey must be an active friend of role `env` (another broker's
// vortex-relay, e.g. the LAN direct transport). Each event is accepted once
// (replay cache for the window), since LAN MQTT is plain TCP.
//
// Modes (VORTEXIA_BROKER_AUTH): `warn` (default) logs what enforce would
// reject and lets it in, so peers can be upgraded first; `enforce` rejects;
// `off` skips the check. Loopback (local agents, LAS, the widget) is never
// asked for anything.

export const AUTH_KIND = 22242;
export const AUTH_WINDOW_S = 120;

export function isLoopback(address) {
  if (!address) return false;
  const a = String(address).replace(/^::ffff:/, '');
  return a === '::1' || a === 'localhost' || /^127\./.test(a);
}

export function remoteAddressOf(client) {
  return client?.connDetails?.ipAddress
    ?? client?.conn?.remoteAddress
    ?? client?.conn?._socket?.remoteAddress
    ?? client?.conn?.socket?.remoteAddress
    ?? null;
}

export function authMode(value = process.env.VORTEXIA_BROKER_AUTH) {
  const v = String(value || 'warn').toLowerCase();
  return ['off', 'warn', 'enforce'].includes(v) ? v : 'warn';
}

/** Credentials for connecting to the broker of `targetEnvName`, signed with `secretKey`. */
export async function buildBrokerCredentials(secretKey, targetEnvName, now = Date.now) {
  const { finalizeEvent, getPublicKey } = await import('nostr-tools');
  const event = finalizeEvent({
    kind: AUTH_KIND,
    created_at: Math.floor(now() / 1000),
    tags: [['challenge', targetEnvName]],
    content: '',
  }, secretKey);
  return { username: getPublicKey(secretKey), password: JSON.stringify(event) };
}

export class BrokerAuthenticator {
  /**
   * @param {object} opts
   * @param {string|null} opts.envName - this broker's env (the expected challenge)
   * @param {{byPubkey(pk: string): any}|null} opts.friends - FriendsStore
   * @param {'off'|'warn'|'enforce'} [opts.mode]
   */
  constructor({ envName, friends, mode = authMode(), now = Date.now, logger = console } = {}) {
    this.envName = envName;
    this.friends = friends;
    this.mode = mode;
    this.now = now;
    this.logger = logger;
    this._seen = new Map(); // event id → expiry ms
  }

  /** @returns {Promise<{ok: true, friend: any} | {ok: false, reason: string}>} */
  async verify(username, password) {
    if (!this.envName) return { ok: false, reason: 'this broker has no VORTEXIA_ENV_NAME, so it cannot check a challenge' };
    if (!this.friends) return { ok: false, reason: 'no friends store' };
    if (!username || !password) return { ok: false, reason: 'no credentials' };
    let event;
    try { event = JSON.parse(Buffer.isBuffer(password) ? password.toString() : String(password)); } catch { return { ok: false, reason: 'password is not a JSON event' }; }
    const { verifyEvent } = await import('nostr-tools');
    if (event?.kind !== AUTH_KIND) return { ok: false, reason: `event kind must be ${AUTH_KIND}` };
    if (event.pubkey !== String(username).toLowerCase()) return { ok: false, reason: 'username does not match the event pubkey' };
    if (!verifyEvent(event)) return { ok: false, reason: 'bad signature' };
    const skew = Math.abs(Math.floor(this.now() / 1000) - event.created_at);
    if (skew > AUTH_WINDOW_S) return { ok: false, reason: `created_at is ${skew}s off (max ${AUTH_WINDOW_S}s)` };
    const challenge = event.tags?.find((t) => t[0] === 'challenge')?.[1];
    if (challenge !== this.envName) return { ok: false, reason: `challenge "${challenge}" is not this broker (${this.envName})` };
    const friend = this.friends.byPubkey(event.pubkey);
    if (!friend) return { ok: false, reason: 'not an active friend' };
    if (friend.role !== 'env') return { ok: false, reason: `role ${friend.role} cannot connect over MQTT (devices use the HTTP gateway)` };
    this._sweep();
    if (this._seen.has(event.id)) return { ok: false, reason: 'replayed auth event' };
    this._seen.set(event.id, this.now() + AUTH_WINDOW_S * 2000);
    return { ok: true, friend };
  }

  _sweep() {
    const t = this.now();
    for (const [id, exp] of this._seen) if (exp < t) this._seen.delete(id);
  }

  /** aedes `authenticate` hook. */
  hook() {
    return (client, username, password, cb) => {
      const address = remoteAddressOf(client);
      if (this.mode === 'off' || isLoopback(address)) return cb(null, true);
      this.verify(username, password).then((result) => {
        if (result.ok) {
          client.vxFriend = result.friend;
          return cb(null, true);
        }
        if (this.mode === 'warn') {
          this.logger.warn?.(`[vortexia] auth (warn mode): would reject ${client?.id} from ${address}: ${result.reason}`);
          return cb(null, true);
        }
        this.logger.warn?.(`[vortexia] auth: rejected ${client?.id} from ${address}: ${result.reason}`);
        const err = new Error(result.reason);
        err.returnCode = 5; // not authorized
        return cb(err, false);
      }, (err) => {
        const e = new Error(`auth check failed: ${err.message}`);
        e.returnCode = 3;
        cb(e, false);
      });
    };
  }

  /** True when a non-loopback client may use this control topic (never, in enforce mode). */
  allowsControl(client) {
    return this.mode !== 'enforce' || isLoopback(remoteAddressOf(client));
  }
}
