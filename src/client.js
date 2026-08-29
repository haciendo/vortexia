import mqtt from 'mqtt';
import { EventEmitter } from 'node:events';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { BROADCAST_TOPIC, SPEAK_TOPIC, inboxTopic, presenceTopic, buildEnvelope } from './topics.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_PORT_FILE = path.resolve(__dirname, '..', 'vortexia.port.json');
const REGISTRY_URL = process.env.LAS_REGISTRY_URL || 'http://localhost:8700';

/**
 * Resolve the broker's current TCP MQTT port.
 * Order of precedence: explicit option -> local vortexia.port.json -> port registry -> default 1883.
 */
async function resolvePort(explicitPort) {
  if (explicitPort) return explicitPort;

  try {
    const data = JSON.parse(fs.readFileSync(DEFAULT_PORT_FILE, 'utf8'));
    if (data.mqttPort) return data.mqttPort;
  } catch {
    // fall through
  }

  try {
    const res = await fetch(`${REGISTRY_URL}/ports`);
    if (res.ok) {
      const ports = await res.json();
      const match = Object.values(ports).find((p) => p.app === 'vortexia-mqtt');
      if (match) return match.port;
    }
  } catch {
    // fall through
  }

  return 1883;
}

export class VortexiaClient extends EventEmitter {
  constructor({ host = 'localhost', port } = {}) {
    super();
    this.host = host;
    this.port = port;
    this.name = null;
    this.mqttClient = null;
  }

  /**
   * Connect and register this client as `name`. Sets up Last Will and
   * Testament so presence flips to offline automatically on disconnect,
   * and subscribes to this agent's inbox + the broadcast topic + the
   * shared speak-request topic (callers must filter envelope.to === name
   * on that last one, since it's shared by every agent's widget).
   */
  async register(name) {
    this.name = name;
    const resolvedPort = await resolvePort(this.port);
    const url = `mqtt://${this.host}:${resolvedPort}`;

    this.mqttClient = mqtt.connect(url, {
      clientId: `vortexia-${name}-${Math.random().toString(16).slice(2)}`,
      // Keep this a simple, explicit client: no background auto-reconnect
      // loop. Callers that want reconnection should call register() again.
      reconnectPeriod: 0,
      will: {
        topic: presenceTopic(name),
        payload: 'offline',
        qos: 1,
        retain: true,
      },
    });

    await new Promise((resolve, reject) => {
      this.mqttClient.once('connect', resolve);
      this.mqttClient.once('error', reject);
    });

    this.mqttClient.publish(presenceTopic(name), 'online', { qos: 1, retain: true });

    await new Promise((resolve, reject) => {
      this.mqttClient.subscribe([inboxTopic(name), BROADCAST_TOPIC, SPEAK_TOPIC], { qos: 1 }, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    this.mqttClient.on('message', (topic, payload) => {
      let envelope;
      try {
        envelope = JSON.parse(payload.toString());
      } catch {
        return;
      }
      this.emit('message', envelope, topic);
    });

    return this;
  }

  /**
   * Send a message to another agent (or 'broadcast' to send to everyone).
   *
   * Direct inbox messages are published RETAINED: without this, a message
   * sent while nobody happens to be subscribed at that exact instant (e.g.
   * a CLI poll that runs later, or a Claude Code session that only drains
   * its inbox at its own next start — see PROTOCOL.md) is simply gone,
   * since plain MQTT delivery only reaches currently-connected subscribers.
   * Retained delivery means a later subscriber (or poll_inbox) still finds
   * it. The consumer is responsible for clearing the retained flag once
   * it's actually been read (poll_inbox does this) — a live viewer like the
   * Electron widget's own register()/message handler intentionally does
   * NOT clear it, so it's still there for the "real" consumer later.
   * Broadcast is NOT retained — the same "latest value replayed forever"
   * behavior doesn't make sense for a fan-out channel.
   */
  send(toName, text, { from = this.name, source = 'agent' } = {}) {
    if (!this.mqttClient) throw new Error('client not registered — call register(name) first');
    const envelope = buildEnvelope({ from, to: toName, source, text });
    const isBroadcast = toName === 'broadcast';
    const topic = isBroadcast ? BROADCAST_TOPIC : inboxTopic(toName);
    this.mqttClient.publish(topic, JSON.stringify(envelope), { qos: 1, retain: !isBroadcast });
    return envelope;
  }

  async close() {
    if (!this.mqttClient) return;
    if (this.name) {
      // Clean, deliberate disconnect: publish offline ourselves instead of
      // relying on the LWT (LWT is for unexpected drops).
      await new Promise((resolve) => {
        this.mqttClient.publish(presenceTopic(this.name), 'offline', { qos: 1, retain: true }, () => resolve());
      });
    }
    await new Promise((resolve) => this.mqttClient.end(false, {}, resolve));
  }
}

export default VortexiaClient;
