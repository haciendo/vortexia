// Friends store — who this environment trusts (docs/adr/0001-device-pairing.md §2).
//
// One file, data/friends.json, owned by vortexia. Each friend is an endpoint
// identified by its public key: another `env` (a Mac/PC running its own broker)
// or a `device` (a phone: keypair, no broker). Revoking is local and instant —
// the entry stays with `revokedAt` set, so a revoked key is never re-trusted
// by a later seed.
//
// VORTEXIA_NOSTR_PEERS still works as a seed: those peers are trusted as
// role `env` unless the file has an entry with the same envName (the file
// wins, including a revocation). Seeds are never written to the file.
//
// The store re-reads the file whenever its mtime changes, so `vortexia
// friends …` run from another process takes effect without a restart.

import fs from 'node:fs';
import path from 'node:path';

/** Retained on the local broker: the key-free friends list (FriendsStore.toPublic). */
export const FRIENDS_TOPIC = 'vortex-relay/friends';

const HEX_PUBKEY = /^[0-9a-f]{64}$/;
const ROLES = new Set(['env', 'device']);

/** Parse "envName:pubkeyHex,envName2:pubkeyHex2" (VORTEXIA_NOSTR_PEERS). */
export function parsePeersEnv(value = '') {
  return value
    .split(',').map((s) => s.trim()).filter(Boolean)
    .map((pair) => {
      const i = pair.indexOf(':');
      return i < 0 ? null : { envName: pair.slice(0, i).trim(), pubkey: pair.slice(i + 1).trim().toLowerCase() };
    })
    .filter((p) => p && p.envName && HEX_PUBKEY.test(p.pubkey));
}

export class FriendsStore {
  /**
   * @param {object} opts
   * @param {string} opts.file - path to friends.json
   * @param {Array<{envName: string, pubkey: string}>} [opts.seed] - trusted as role env (VORTEXIA_NOSTR_PEERS)
   * @param {() => number} [opts.now]
   */
  constructor({ file, seed = [], now = Date.now } = {}) {
    if (!file) throw new Error('FriendsStore requires a file');
    this.file = file;
    this.seed = seed.map((s) => ({ envName: s.envName, pubkey: s.pubkey.toLowerCase(), role: 'env', label: s.envName, addedAt: 0, revokedAt: null, source: 'env' }));
    this.now = now;
    this._mtimeMs = -1;
    this._fileFriends = [];
  }

  _reload() {
    let mtimeMs;
    try {
      mtimeMs = fs.statSync(this.file).mtimeMs;
    } catch {
      this._mtimeMs = -1;
      this._fileFriends = [];
      return;
    }
    if (mtimeMs === this._mtimeMs) return;
    try {
      const parsed = JSON.parse(fs.readFileSync(this.file, 'utf8'));
      this._fileFriends = Array.isArray(parsed?.friends) ? parsed.friends.filter((f) => f?.envName && HEX_PUBKEY.test(f.pubkey || '')) : [];
      this._mtimeMs = mtimeMs;
    } catch {
      // A half-written or corrupt file: keep what we had rather than
      // dropping every friendship (writes are atomic, so this is rare).
    }
  }

  _write(friends) {
    fs.mkdirSync(path.dirname(this.file), { recursive: true });
    const tmp = `${this.file}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, `${JSON.stringify({ friends }, null, 2)}\n`, { mode: 0o600 });
    fs.renameSync(tmp, this.file);
    this._mtimeMs = -1;
    this._reload();
  }

  /** Every friend, file entries first, then seeds not overridden by the file. Includes revoked ones. */
  list() {
    this._reload();
    const named = new Set(this._fileFriends.map((f) => f.envName));
    return [...this._fileFriends.map((f) => ({ ...f, source: 'file' })), ...this.seed.filter((s) => !named.has(s.envName))];
  }

  /** Friends currently trusted (not revoked). */
  active() {
    return this.list().filter((f) => !f.revokedAt);
  }

  get(envName) {
    return this.list().find((f) => f.envName === envName) ?? null;
  }

  /** The active friend holding this pubkey, or null — what auth checks ask. */
  byPubkey(pubkey) {
    const pk = String(pubkey || '').toLowerCase();
    return this.active().find((f) => f.pubkey === pk) ?? null;
  }

  activePubkeys() {
    return [...new Set(this.active().map((f) => f.pubkey))];
  }

  /**
   * Add a friend, or re-pair one (a new pubkey or role for the same envName
   * replaces it and clears any revocation). Throws on invalid input or when
   * the pubkey already belongs to a different active friend.
   */
  add({ envName, pubkey, role = 'env', label }) {
    if (!envName || /[\s/@,:#+]/.test(envName)) throw new Error(`invalid envName "${envName}"`);
    const pk = String(pubkey || '').toLowerCase();
    if (!HEX_PUBKEY.test(pk)) throw new Error('pubkey must be 64 hex characters (x-only secp256k1)');
    if (!ROLES.has(role)) throw new Error(`role must be one of ${[...ROLES].join(', ')}`);
    const clash = this.active().find((f) => f.pubkey === pk && f.envName !== envName);
    if (clash) throw new Error(`pubkey already belongs to friend "${clash.envName}"`);
    this._reload();
    const friend = { envName, pubkey: pk, role, label: label || envName, addedAt: this.now(), revokedAt: null };
    this._write([...this._fileFriends.filter((f) => f.envName !== envName), friend]);
    return friend;
  }

  /** Revoke a friend (file entry or seed). Returns the revoked entry, or null if unknown. */
  revoke(envName) {
    const current = this.get(envName);
    if (!current) return null;
    this._reload();
    const { source, ...rest } = current;
    const revoked = { ...rest, revokedAt: this.now() };
    this._write([...this._fileFriends.filter((f) => f.envName !== envName), revoked]);
    return revoked;
  }

  rename(envName, label) {
    this._reload();
    const current = this._fileFriends.find((f) => f.envName === envName);
    if (!current) {
      const seeded = this.get(envName);
      if (!seeded) return null;
      const { source, ...rest } = seeded;
      this._write([...this._fileFriends, { ...rest, label }]);
    } else {
      this._write(this._fileFriends.map((f) => (f.envName === envName ? { ...f, label } : f)));
    }
    return this.get(envName);
  }

  /** What other local processes may see (retained vortex-relay/friends): no keys. */
  toPublic() {
    return this.list().map(({ envName, role, label, addedAt, revokedAt, lastSeen }) => ({
      envName, role, label, addedAt, revokedAt, ...(lastSeen ? { lastSeen } : {}),
    }));
  }
}
