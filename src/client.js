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
  sessionTopic,
  mailboxClientId,
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
    this.mailbox = false;
    // Set from the CONNACK: true when the broker already held a persistent
    // session for our client id (mailbox mode only — always false for a
    // viewer, whose session is clean by definition).
    this.sessionPresent = false;
  }

  /**
   * Connect and register this client as `name`. Sets up Last Will and
   * Testament so presence flips to offline automatically on disconnect,
   * and subscribes to this agent's inbox + the broadcast topic + the
   * shared speak-request topic (callers must filter envelope.to === name
   * on that last one, since it's shared by every agent's widget).
   *
   * Two ways to attach to an inbox (see PROTOCOL.md "Mailboxes"):
   *
   * - viewer (default): a clean session with a random client id. Sees
   *   every message that arrives WHILE connected, consumes nothing. This
   *   is the widget: it displays the conversation, it doesn't own it.
   * - `{ mailbox: true }`: THE consumer. Connects as the agent's
   *   persistent session (client id `las-agent-<name>`, clean=false), so
   *   whatever was queued while nobody was connected is delivered first,
   *   in order, and each message is acknowledged as it's handled. Only
   *   one connection can hold the session at a time — a later one takes
   *   it over (check `sessionState(name)` first if that matters, e.g. a
   *   one-shot poll while a live listener may be running).
   */
  async register(name, { mailbox = false, presence = true } = {}) {
    this.name = name;
    this.mailbox = mailbox;
    // `presence: false` skips the LWT and the online/offline publishes —
    // for a short-lived connection (pollInbox) that must not overwrite
    // the retained presence a longer-lived registration already set:
    // connect-then-disconnect would leave the agent reading "offline"
    // seconds after `las agent register` said "online".
    this.presence = presence;
    const resolvedPort = await resolvePort(this.port);
    const url = `mqtt://${this.host}:${resolvedPort}`;

    this.mqttClient = mqtt.connect(url, {
      clientId: mailbox ? mailboxClientId(name) : `vortexia-${name}-${Math.random().toString(16).slice(2)}`,
      clean: !mailbox,
      // Keep this a simple, explicit client: no background auto-reconnect
      // loop. Callers that want reconnection should call register() again.
      reconnectPeriod: 0,
      // mqtt.js's default keepalive is 60s — found live to line up almost
      // exactly with DirectMqttTransport's cross-machine LAN drops (ba-mac
      // <-> uy-mac, 8 reconnects/restart, symmetric on both sides): a PING
      // only every 60s of idle time is long enough for some router/NAT idle
      // timeouts to expire the connection's table entry first, so the
      // keepalive never gets a chance to keep it alive. A shorter interval
      // keeps the path busy enough to stay under those idle timeouts. Local
      // same-process/localhost connections don't need this, but there's no
      // real cost to it either.
      keepalive: 15,
      ...(presence ? {
        will: {
          topic: presenceTopic(name),
          payload: 'offline',
          qos: 1,
          retain: true,
        },
      } : {}),
    });

    // Handlers go on BEFORE the connection completes: a mailbox session's
    // queued backlog starts flowing the instant the broker sends CONNACK,
    // and anything delivered before a 'message' listener exists is simply
    // not seen (mqtt.js acks it regardless — it would be consumed and
    // lost).
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

    const connack = await new Promise((resolve, reject) => {
      this.mqttClient.once('connect', resolve);
      this.mqttClient.once('error', reject);
    });
    this.sessionPresent = Boolean(connack?.sessionPresent);

    if (presence) this.mqttClient.publish(presenceTopic(name), 'online', { qos: 1, retain: true });

    // Mailbox: the inbox subscription is part of the persistent session —
    // the broker restores it on connect (and creates it itself on the
    // first publish to the inbox), so only subscribe when the session is
    // brand new. Re-subscribing an existing session would replay any
    // retained message still on the topic (from a pre-mailbox sender)
    // on top of the queued copy. Broadcast/speak are subscribed at QoS 0
    // in mailbox mode: live-only, never queued for an offline agent —
    // replaying a fan-out or a stale "speak this" later would be wrong.
    const subs = this.mailbox
      ? { ...(this.sessionPresent ? {} : { [inboxTopic(name)]: { qos: 1 } }), [BROADCAST_TOPIC]: { qos: 0 }, [SPEAK_TOPIC]: { qos: 0 } }
      : { [inboxTopic(name)]: { qos: 1 }, [BROADCAST_TOPIC]: { qos: 1 }, [SPEAK_TOPIC]: { qos: 1 } };
    await new Promise((resolve, reject) => {
      this.mqttClient.subscribe(subs, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    return this;
  }

  /**
   * Send a message to another agent (or 'broadcast' to send to everyone).
   *
   * Direct inbox messages are plain QoS 1 publishes — NOT retained. The
   * broker queues them in the recipient's mailbox (its persistent session,
   * see PROTOCOL.md "Mailboxes") if nobody holds that session right now,
   * so a message sent while the agent's Claude Code session isn't running
   * is delivered, in order and alongside every other one sent meanwhile,
   * the next time it polls or listens. Retaining used to be how this was
   * done, and it kept exactly ONE message per recipient (the latest
   * overwrote the rest); `retain: true` is still accepted for a caller
   * that knowingly wants the old single-slot behavior. Broadcast is never
   * retained — replaying the last fan-out to every new subscriber forever
   * makes no sense for that channel.
   */
  send(toName, text, { from = this.name, source = 'agent', retain = false, ...extra } = {}) {
    if (!this.mqttClient) throw new Error('client not registered — call register(name) first');
    const envelope = { ...buildEnvelope({ from, to: toName, source, text }), ...extra };
    const isBroadcast = toName === 'broadcast';
    const topic = isBroadcast ? BROADCAST_TOPIC : inboxTopic(toName);
    this.mqttClient.publish(topic, JSON.stringify(envelope), { qos: 1, retain: retain && !isBroadcast });
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
  sendConfirmed(toName, text, { from = this.name, source = 'agent', timeoutMs = 3000, retain = false, ...extra } = {}) {
    if (!this.mqttClient) throw new Error('client not registered — call register(name) first');
    const envelope = { ...buildEnvelope({ from, to: toName, source, text }), ...extra };
    const isBroadcast = toName === 'broadcast';
    const topic = isBroadcast ? BROADCAST_TOPIC : inboxTopic(toName);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`sendConfirmed: no ack from broker within ${timeoutMs}ms — connection is likely dead`)),
        timeoutMs,
      );
      this.mqttClient.publish(topic, JSON.stringify(envelope), { qos: 1, retain: retain && !isBroadcast }, (err) => {
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
    if (this.name && this.presence !== false) {
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

/**
 * Is `name`'s mailbox session currently held by a live connection? Reads
 * the retained, broker-published `las/agent/<name>/session` topic with a
 * throwaway clean client. Resolves `{ connected, clientId, ts }`, with
 * `connected: false` when the broker has never seen that session (or has
 * restarted since — retained session state isn't persisted, and a
 * consumer that's really connected republishes it by reconnecting).
 */
export async function sessionState(name, { host = 'localhost', port, timeoutMs = 700 } = {}) {
  const resolvedPort = await resolvePort(port);
  const client = mqtt.connect(`mqtt://${host}:${resolvedPort}`, {
    clientId: `vortexia-session-${Math.random().toString(16).slice(2)}`,
    clean: true,
    reconnectPeriod: 0,
  });
  try {
    await new Promise((resolve, reject) => {
      client.once('connect', resolve);
      client.once('error', reject);
    });
    return await new Promise((resolve) => {
      const timer = setTimeout(() => resolve({ connected: false }), timeoutMs);
      client.on('message', (topic, payload) => {
        if (topic !== sessionTopic(name)) return;
        clearTimeout(timer);
        try {
          const state = JSON.parse(payload.toString());
          resolve({ connected: Boolean(state.connected), clientId: state.clientId, ts: state.ts });
        } catch {
          resolve({ connected: false });
        }
      });
      client.subscribe(sessionTopic(name), { qos: 0 });
    });
  } finally {
    await new Promise((resolve) => client.end(true, {}, resolve));
  }
}

/**
 * One-shot mailbox drain: connect as `name`'s consumer, hand back every
 * queued (and, during the window, newly arriving) message, disconnect.
 * Each message is acknowledged as it's received, so a second poll does not
 * see it again. The counterpart of the Python client's poll_inbox for a
 * CLI/session that can't stay subscribed.
 *
 * If a live consumer already holds the session (a `listen` running under
 * the agent's Monitor, say), this returns [] WITHOUT connecting as the
 * mailbox — taking the session over would kick that listener, and with a
 * reconnecting listener the two would keep kicking each other. Pass
 * `takeover: true` to insist.
 */
export async function pollInbox(name, { host = 'localhost', port, timeoutMs = 2000, takeover = false } = {}) {
  if (!takeover) {
    const state = await sessionState(name, { host, port });
    if (state.connected) return [];
  }
  const client = new VortexiaClient({ host, port });
  const collected = [];
  client.on('message', (envelope, topic) => {
    if (topic === inboxTopic(name)) collected.push(envelope);
  });
  await client.register(name, { mailbox: true, presence: false });
  await new Promise((resolve) => setTimeout(resolve, timeoutMs));
  await client.close();
  return collected;
}

export default VortexiaClient;
