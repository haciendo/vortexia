// Connection: the umbrella abstraction for "how one vortex-relay bridge
// reaches a given peer environment." The relay (relay.js: GistRelay,
// NostrRelay, MultiRelay) is ONE Connection type — always reachable,
// always the slowest, rate-limited. A DirectTransport (directTransport.js:
// same-LAN today, a wired/BLE peer later) is another — fast and free once
// connected, but not always there. Same machine (no network at all) is a
// third, and the cheapest of all.
//
// ConnectionRouter picks the cheapest/fastest AVAILABLE Connection for a
// given peer environment and falls back down the list on failure — the
// same circuit-breaker shape MultiRelay already uses to fall back across
// Gist/Nostr, generalized one level up so it works across connection
// KINDS, not just within the relay kind. A Connection only ever sees an
// envName and a routed-message shape — nothing about scope text or the
// physical id (envName+agentName) lives here (that stays in directory.js/
// router.js), so this layer isn't tied to las-agent/LocalAgentSociety and
// could carry any app sharing the same horizontal network later.

/**
 * @typedef {object} Connection
 * @property {string} name - stable label: message metadata (`routed.transport`) and pin target
 * @property {number} priority - lower tries first (cost/speed rank): local < direct < relay
 * @property {() => boolean} isAvailable - cheap synchronous check: worth trying right now?
 * @property {(envName: string, routed: object) => Promise<void>} deliver
 */

/** Same machine, no network at all — always available, always wins. */
export class LocalConnection {
  name = 'local';
  priority = 0;

  /** @param {(routed: object) => void} deliverLocallyFn - VortexRelayBridge._deliverLocally, reused so the transport tag/agentNames-fanout logic lives in exactly one place. */
  constructor(deliverLocallyFn) {
    this._deliverLocallyFn = deliverLocallyFn;
  }

  isAvailable() { return true; }

  async deliver(envName, routed) {
    this._deliverLocallyFn({ ...routed, transport: 'local' });
  }
}

/** Adapts a DirectTransport (connect/send/disconnect — see directTransport.js) into the Connection shape the router selects between. */
export class DirectConnection {
  priority = 10;

  constructor(transport) {
    this.transport = transport;
    this.name = transport.name;
  }

  isAvailable() { return this.transport.connected; }

  async deliver(envName, routed) {
    for (const agentName of routed.agentNames) {
      this.transport.send(agentName, routed.text ?? routed.intent, {
        from: routed.from,
        kind: 'vortex-relay-delivery',
        routedFrom: routed.routedFrom,
        transport: this.transport.name,
      });
    }
  }
}

/**
 * Adapts a Relay (readFile/appendMessage/writeFile — see relay.js) into
 * the Connection shape. `relayRef()` is a live getter, not a snapshot: a
 * test (or future hot-swap) may reassign `bridge.relay` after this
 * Connection is built, and delivery must see the CURRENT relay, not
 * whichever one existed at construction time.
 */
export class RelayConnection {
  name = 'relay';
  priority = 100;

  constructor(relayRef, { outboxFileFor }) {
    this._relayRef = relayRef;
    this.outboxFileFor = outboxFileFor;
  }

  // The guaranteed fallback — Gist/Nostr rate limits are handled a level
  // down, inside MultiRelay's own circuit breaker, not here.
  isAvailable() { return true; }

  async deliver(envName, routed) {
    await this._relayRef().appendMessage(this.outboxFileFor(envName), routed);
  }
}

const DEFAULT_FAILURE_THRESHOLD = 3;
const DEFAULT_COOLDOWN_MS = 60000;

/**
 * Picks the cheapest/fastest available Connection for a peer environment,
 * falling back down the (priority-sorted) list on failure. Per-connection
 * health is keyed by `${envName}:${connection.name}` rather than object
 * identity, since callers are free to rebuild the candidate list (e.g. a
 * new DirectConnection wrapper) between calls without losing circuit
 * state — see VortexRelayBridge._connectionsFor.
 */
export class ConnectionRouter {
  constructor({ failureThreshold = DEFAULT_FAILURE_THRESHOLD, cooldownMs = DEFAULT_COOLDOWN_MS } = {}) {
    this.failureThreshold = failureThreshold;
    this.cooldownMs = cooldownMs;
    this._health = new Map();
  }

  _healthFor(envName, conn) {
    const key = `${envName}:${conn.name}`;
    let h = this._health.get(key);
    if (!h) {
      h = { consecutiveFailures: 0, openUntil: 0 };
      this._health.set(key, h);
    }
    return h;
  }

  _isOpen(envName, conn) {
    return this._healthFor(envName, conn).openUntil > Date.now();
  }

  _recordSuccess(envName, conn) {
    const h = this._healthFor(envName, conn);
    h.consecutiveFailures = 0;
    h.openUntil = 0;
  }

  _recordFailure(envName, conn) {
    const h = this._healthFor(envName, conn);
    h.consecutiveFailures++;
    if (h.consecutiveFailures >= this.failureThreshold) {
      h.openUntil = Date.now() + this.cooldownMs;
    }
  }

  /**
   * @param {string} envName
   * @param {object} routed
   * @param {Connection[]} connections - candidates that apply to `envName` (caller filters — see VortexRelayBridge._connectionsFor)
   * @param {object} [opts]
   * @param {string} [opts.pin] - lock to one connection by name, no fallback, no circuit-breaker skip (a pin means "always try this one, every time")
   * @returns {Promise<string>} the name of the connection that actually delivered it
   */
  async deliver(envName, routed, connections, { pin } = {}) {
    const ordered = [...connections].sort((a, b) => a.priority - b.priority);

    if (pin) {
      const conn = ordered.find((c) => c.name === pin);
      if (!conn) throw new Error(`ConnectionRouter: "${envName}" is pinned to "${pin}" but no such connection applies to it`);
      await conn.deliver(envName, routed);
      return conn.name;
    }

    let lastErr;
    for (const conn of ordered) {
      if (this._isOpen(envName, conn)) continue;
      if (!conn.isAvailable()) continue;
      try {
        await conn.deliver(envName, routed);
        this._recordSuccess(envName, conn);
        return conn.name;
      } catch (err) {
        this._recordFailure(envName, conn);
        lastErr = err;
      }
    }
    throw lastErr ?? new Error(`ConnectionRouter: no available connection for "${envName}"`);
  }
}
