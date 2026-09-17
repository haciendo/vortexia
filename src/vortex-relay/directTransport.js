// Direct transport for vortex-relay: skip the relay/lake entirely and
// deliver straight into a peer environment's own broker, when there's a
// live connection to reach it (same LAN today, via LanDiscovery — a direct
// socket or WebSocket peer later fits the same shape). "un relay es como
// un lago": Gist/Nostr are one slow, store-and-forward medium among
// several, not the only way two environments can talk.
//
// Unlike Relay (store: readFile/appendMessage/writeFile against a shared
// mailbox, polled), a DirectTransport is a live connection: connect once,
// then send() publishes straight onto the peer's broker — the peer's own
// VortexiaClient.on('message') delivers it exactly like a local agent's
// message, no vortex-relay envelope/outbox file involved. There's no
// readFile side to this: the peer receives on their OWN inbox topic, not
// ours, so nothing here ever polls.

import { VortexiaClient } from '../client.js';

/**
 * @typedef {object} DirectTransport
 * @property {string} name
 * @property {boolean} connected
 * @property {() => Promise<void>} connect
 * @property {(agentName: string, text: string, extra?: object) => void} send
 * @property {() => Promise<void>} disconnect
 */

/**
 * A DirectTransport backed by a second VortexiaClient connected straight to
 * a peer environment's broker (host + mqttPort, as discovered by
 * LanDiscovery). One instance per peer environment.
 */
const DEFAULT_RECONNECT_BASE_MS = 1000;
const DEFAULT_RECONNECT_MAX_MS = 30000;

export class DirectMqttTransport {
  name = 'lan-direct';

  constructor({
    envName, host, mqttPort, ClientImpl = VortexiaClient,
    reconnectBaseMs = DEFAULT_RECONNECT_BASE_MS, reconnectMaxMs = DEFAULT_RECONNECT_MAX_MS,
  }) {
    if (!envName || !host || !mqttPort) throw new Error('DirectMqttTransport requires envName, host, and mqttPort');
    this.envName = envName;
    this.host = host;
    this.mqttPort = mqttPort;
    this._ClientImpl = ClientImpl;
    this._reconnectBaseMs = reconnectBaseMs;
    this._reconnectMaxMs = reconnectMaxMs;
    this._reconnectAttempts = 0;
    this._reconnectTimer = null;
    // Set by disconnect() to distinguish a deliberate teardown (LAN peer
    // actually left, bridge.js detachDirectTransport()) from an
    // involuntary drop (MQTT keepalive timeout) — only the latter should
    // trigger a reconnect attempt.
    this._deliberatelyClosed = false;
    this.client = null;
    this.connected = false;
  }

  async connect() {
    if (this.connected) return;
    this._deliberatelyClosed = false;
    this.client = new this._ClientImpl({ host: this.host, port: this.mqttPort });
    // A bridge-only client identity, not a real agent — no inbox worth
    // draining, so its own name is just diagnostic (visible to the peer
    // broker's other subscribers as "who published this").
    await this.client.register(`vortex-relay-bridge-${this.envName}-${Math.random().toString(16).slice(2)}`);
    this.connected = true;
    this._reconnectAttempts = 0;
    // A dropped connection must stop looking connected so bridge.js falls
    // back to the relay instead of silently calling send() on a dead
    // socket (mqtt.js queues/drops publishes on a closed client rather
    // than throwing) — see VortexRelayBridge._deliverToEnv. But a
    // keepalive timeout drops this WITHOUT the peer leaving the LAN (its
    // mDNS advertisement stays up, so bridge.js's onUp/onDown never fires
    // again) — left at connected=false permanently, isAvailable() would
    // never recover and every future delivery falls through to the relay
    // even with the peer still reachable. Self-heal instead: schedule a
    // reconnect with backoff, same as any other transient network drop.
    this.client.on('close', () => {
      this.connected = false;
      if (!this._deliberatelyClosed) this._scheduleReconnect();
    });
  }

  _scheduleReconnect() {
    if (this._reconnectTimer) return;
    const delay = Math.min(this._reconnectBaseMs * 2 ** this._reconnectAttempts, this._reconnectMaxMs);
    this._reconnectAttempts++;
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer = null;
      this.connect().catch(() => this._scheduleReconnect());
    }, delay);
    // Don't hold the process open just to retry a background reconnect.
    this._reconnectTimer.unref?.();
  }

  /**
   * Publish straight into the peer's real inbox topic — bypasses
   * vortex-relay entirely. Uses sendConfirmed (see client.js), NOT
   * send(): fire-and-forget would let a message vanish into mqtt.js's
   * internal queue on a half-dead connection without ever throwing,
   * silently defeating ConnectionRouter's fallback to the relay.
   */
  async send(agentName, text, extra = {}) {
    if (!this.connected) throw new Error(`DirectMqttTransport(${this.envName}): not connected`);
    await this.client.sendConfirmed(agentName, text, extra);
  }

  async disconnect() {
    this._deliberatelyClosed = true;
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
    if (!this.client) return;
    await this.client.close();
    this.connected = false;
    this.client = null;
  }
}
