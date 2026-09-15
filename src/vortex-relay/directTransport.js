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
export class DirectMqttTransport {
  name = 'lan-direct';

  constructor({ envName, host, mqttPort, ClientImpl = VortexiaClient }) {
    if (!envName || !host || !mqttPort) throw new Error('DirectMqttTransport requires envName, host, and mqttPort');
    this.envName = envName;
    this.host = host;
    this.mqttPort = mqttPort;
    this._ClientImpl = ClientImpl;
    this.client = null;
    this.connected = false;
  }

  async connect() {
    if (this.connected) return;
    this.client = new this._ClientImpl({ host: this.host, port: this.mqttPort });
    // A bridge-only client identity, not a real agent — no inbox worth
    // draining, so its own name is just diagnostic (visible to the peer
    // broker's other subscribers as "who published this").
    await this.client.register(`vortex-relay-bridge-${this.envName}-${Math.random().toString(16).slice(2)}`);
    this.connected = true;
    // A dropped connection must stop looking connected so bridge.js falls
    // back to the relay instead of silently calling send() on a dead
    // socket (mqtt.js queues/drops publishes on a closed client rather
    // than throwing) — see VortexRelayBridge._deliverToEnv.
    this.client.on('close', () => { this.connected = false; });
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
    if (!this.client) return;
    await this.client.close();
    this.connected = false;
    this.client = null;
  }
}
