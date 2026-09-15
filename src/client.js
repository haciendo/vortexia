import mqtt from 'mqtt';
import { EventEmitter } from 'node:events';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  BROADCAST_TOPIC,
  SPEAK_TOPIC,
  inboxTopic,
  presenceTopic,
  buildEnvelope,
  SCOPE_QUERY_KIND,
  SCOPE_REPLY_KIND,
  buildScopeQueryEnvelope,
  buildScopeReplyEnvelope,
} from './topics.js';

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

    // reconnectPeriod is 0 (see above) — this client never retries on its
    // own. Proxy just 'close' (not 'error' — Node's EventEmitter throws if
    // an 'error' event has no listener, and most existing callers of this
    // client don't attach one; 'close' fires on any disconnection,
    // including after a transport error, so it's the safe single signal)
    // so a caller that DOES want to recover from a dropped connection
    // (e.g. the broker restarting under its own crash-restart supervisor)
    // can tell the difference between "still connected" and "silently
    // dead" instead of discovering it only when a send/self-test goes
    // nowhere.
    this.mqttClient.on('close', () => this.emit('close'));

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
  send(toName, text, { from = this.name, source = 'agent', ...extra } = {}) {
    if (!this.mqttClient) throw new Error('client not registered — call register(name) first');
    const envelope = { ...buildEnvelope({ from, to: toName, source, text }), ...extra };
    const isBroadcast = toName === 'broadcast';
    const topic = isBroadcast ? BROADCAST_TOPIC : inboxTopic(toName);
    this.mqttClient.publish(topic, JSON.stringify(envelope), { qos: 1, retain: !isBroadcast });
    return envelope;
  }

  /**
   * Like send(), but resolves only once the broker has actually
   * acknowledged the publish (PUBACK, since this always uses QoS 1), and
   * REJECTS — rather than hanging silently forever — if that doesn't
   * happen within `timeoutMs`.
   *
   * send() is fire-and-forget by design, and that's fine for the vast
   * majority of callers (same-process local delivery, where the broker
   * connection is never in doubt). It's a real bug for anything crossing
   * a connection that can die without warning — e.g. DirectMqttTransport
   * (see directTransport.js) publishing across machines: mqtt.js QUEUES a
   * QoS-1 publish instead of erroring when the client is disconnected but
   * not `.end()`-ed (a socket that died without a clean FIN, or — as here
   * — reconnectPeriod: 0 meaning it will never retry to flush that
   * queue), so the message can vanish silently while the caller believes
   * it succeeded. Found live: a peer's direct connection still looked
   * "connected" after the other side restarted; the message never
   * arrived and nothing ever threw, so ConnectionRouter never got the
   * chance to fall back to the relay.
   */
  sendConfirmed(toName, text, { from = this.name, source = 'agent', timeoutMs = 3000, ...extra } = {}) {
    if (!this.mqttClient) throw new Error('client not registered — call register(name) first');
    const envelope = { ...buildEnvelope({ from, to: toName, source, text }), ...extra };
    const isBroadcast = toName === 'broadcast';
    const topic = isBroadcast ? BROADCAST_TOPIC : inboxTopic(toName);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`sendConfirmed: no ack from broker within ${timeoutMs}ms — connection is likely dead`)),
        timeoutMs,
      );
      this.mqttClient.publish(topic, JSON.stringify(envelope), { qos: 1, retain: !isBroadcast }, (err) => {
        clearTimeout(timer);
        if (err) reject(err);
        else resolve(envelope);
      });
    });
  }

  /**
   * Ask another agent for its scope-ladder text at a given detail level
   * ('short' | 'more' | 'full' | {maxChars: N} — meaning is up to the
   * target's own onScopeQuery handler). Resolves with
   * `{ rung, source, text }` from that agent's scope-reply, or rejects on
   * timeout if the target never answers (e.g. it doesn't call
   * onScopeQuery at all).
   *
   * Each call generates its own `queryId` and only resolves on a reply
   * that echoes it — this is what lets two concurrent requestScope() calls
   * to the same agent (or a late reply arriving after a prior call already
   * timed out) each resolve with their own answer instead of the first
   * reply satisfying whichever promise happened to still be listening.
   */
  requestScope(toName, detail = 'short', { timeout = 3000 } = {}) {
    if (!this.mqttClient) throw new Error('client not registered — call register(name) first');
    const queryId = crypto.randomUUID();
    const envelope = buildScopeQueryEnvelope({ from: this.name, to: toName, detail, queryId });
    const myInbox = inboxTopic(this.name);

    return new Promise((resolve, reject) => {
      const onMessage = (env, topic) => {
        if (
          topic === myInbox &&
          env.kind === SCOPE_REPLY_KIND &&
          env.queryId === queryId &&
          env.from === toName &&
          env.to === this.name
        ) {
          clearTimeout(timer);
          this.removeListener('message', onMessage);
          resolve({ rung: env.rung, source: env.scopeSource, text: env.text });
        }
      };
      const timer = setTimeout(() => {
        this.removeListener('message', onMessage);
        reject(new Error(`scope query to ${toName} timed out after ${timeout}ms`));
      }, timeout);

      this.on('message', onMessage);
      this.mqttClient.publish(inboxTopic(toName), JSON.stringify(envelope), { qos: 1, retain: false });
    });
  }

  /**
   * Register this client as an answerer for incoming scope-query messages.
   * `handler(detail, envelope)` should return `{ rung, source, text }` (or
   * a falsy value to decline answering) — how it picks a rung for a given
   * `detail` is entirely up to the caller (e.g. walking scanScopes()
   * output, merged with local-agent-society's own name/short_description/
   * long_description fields). vortexia only carries the request/reply.
   * `handler` may be async.
   *
   * Only one handler is active at a time — calling this again replaces the
   * previous one rather than stacking a second listener (which would
   * otherwise publish two replies per query). Returns an unsubscribe
   * function.
   */
  onScopeQuery(handler) {
    if (!this.mqttClient) throw new Error('client not registered — call register(name) first');
    if (this._scopeQueryListener) this.removeListener('message', this._scopeQueryListener);

    const myInbox = inboxTopic(this.name);
    const listener = async (envelope, topic) => {
      if (topic !== myInbox || envelope.kind !== SCOPE_QUERY_KIND || envelope.to !== this.name) return;
      const result = await handler(envelope.detail, envelope);
      if (!result) return;
      const reply = buildScopeReplyEnvelope({
        from: this.name,
        to: envelope.from,
        queryId: envelope.queryId,
        rung: result.rung,
        scopeSource: result.source,
        text: result.text,
      });
      this.mqttClient.publish(inboxTopic(envelope.from), JSON.stringify(reply), { qos: 1, retain: false });
    };

    this._scopeQueryListener = listener;
    this.on('message', listener);
    return () => {
      this.removeListener('message', listener);
      if (this._scopeQueryListener === listener) this._scopeQueryListener = null;
    };
  }

  async close() {
    if (!this.mqttClient) return;
    if (this.name) {
      // Clean, deliberate disconnect: publish offline ourselves instead of
      // relying on the LWT (LWT is for unexpected drops). But don't let a
      // QoS 1 publish that never gets PUBACK'd hang close() forever — if
      // the broker's own shutdown races ours and kills the socket before
      // acking (or the connection just drops for any other reason), the
      // publish callback never fires. A short timeout, or the socket
      // closing on its own, both mean "move on" here.
      await Promise.race([
        new Promise((resolve) => {
          this.mqttClient.publish(presenceTopic(this.name), 'offline', { qos: 1, retain: true }, () => resolve());
        }),
        new Promise((resolve) => this.mqttClient.once('close', resolve)),
        new Promise((resolve) => setTimeout(resolve, 1000)),
      ]);
    }
    await Promise.race([
      new Promise((resolve) => this.mqttClient.end(false, {}, resolve)),
      new Promise((resolve) => setTimeout(resolve, 1000)),
    ]);
  }
}

export default VortexiaClient;
