// Per-agent mailboxes on top of MQTT persistent sessions.
//
// Why not retained messages: MQTT `retain` keeps the LAST value per topic —
// it's a state cache ("what's the current temperature"), not a queue. Using
// it as the inbox meant a second message sent before the first was read
// silently overwrote it (visible the moment `las agent send --children`
// fanned one message out to nine subordinates, most without an open
// session). What MQTT actually provides for "deliver this later to a
// subscriber that's offline right now" is the persistent session: a client
// that connects with clean=false and a stable client id gets, on its next
// connect, every QoS 1 message published to its subscriptions while it was
// away — in order, each one acknowledged individually, none overwriting
// another. That's the mailbox. The broker owns the queue, consumers own
// the acks, and nothing on disk or in a client needs a "clear" step.
//
// This module is the aedes persistence for those sessions. It composes the
// stock in-memory persistence (retained messages, subscriptions, wills,
// inflight packets — all unchanged) and owns only the outgoing queues, so
// it can add what a society of agents needs and aedes 0.5x (MQTT 3.1.1) has
// no protocol-level notion of:
//
//   - a cap per mailbox (the oldest message is dropped once it's full);
//   - a TTL per message (an agent that never wakes up doesn't accumulate
//     forever — MQTT 5's message-expiry, applied broker-side);
//   - a JSON snapshot on disk (data/mailboxes.json), written a moment after
//     every change and flushed on shutdown, so a broker restart — a crash
//     under launchd, a deploy — doesn't drop what was waiting. Retained
//     messages and wills are deliberately NOT snapshotted: presence is
//     rebuilt by the clients themselves on reconnect, and a stale retained
//     "online" from before a crash would be wrong.
//
// A mailbox exists as soon as anyone publishes to `las/agent/<name>/inbox`
// (see ensureMailbox, wired into the broker's authorizePublish): the
// broker adds the inbox subscription to the session `las-agent-<name>`
// itself, so the very first message to an agent that has never connected
// is queued rather than lost. A consumer connecting later with that client
// id and clean=false finds the session present and the queue waiting.

import fs from 'node:fs';
import path from 'node:path';
import { Readable } from 'node:stream';
import Packet from 'aedes-packet';
import memoryPersistence from 'aedes-persistence';
import { logger } from './logger.js';
import { inboxTopic, mailboxClientId, parseInboxTopic } from './topics.js';

export const DEFAULT_MAILBOX_CAP = 500;
export const DEFAULT_MAILBOX_TTL_MS = 30 * 24 * 60 * 60 * 1000; // 30 days
const SNAPSHOT_VERSION = 1;

function serializePacket(p) {
  return {
    cmd: p.cmd,
    brokerId: p.brokerId,
    brokerCounter: p.brokerCounter,
    topic: p.topic,
    payload: Buffer.isBuffer(p.payload) ? p.payload.toString('base64') : Buffer.from(String(p.payload ?? '')).toString('base64'),
    qos: p.qos,
    retain: p.retain,
    dup: p.dup,
    messageId: p.messageId,
    enqueuedAt: p.enqueuedAt,
  };
}

function deserializePacket(s) {
  const p = new Packet({
    cmd: s.cmd,
    brokerId: s.brokerId,
    brokerCounter: s.brokerCounter,
    topic: s.topic,
    payload: Buffer.from(s.payload || '', 'base64'),
    qos: s.qos,
    retain: s.retain,
    dup: s.dup,
  });
  if (s.messageId !== undefined && s.messageId !== null) p.messageId = s.messageId;
  p.enqueuedAt = s.enqueuedAt ?? Date.now();
  return p;
}

export class MailboxPersistence {
  /**
   * @param {object} [opts]
   * @param {string} [opts.file] - snapshot path; omit for a purely in-memory instance (tests)
   * @param {number} [opts.cap] - max queued messages per mailbox
   * @param {number} [opts.ttlMs] - drop a queued message older than this
   * @param {number} [opts.flushDelayMs] - debounce for snapshot writes
   */
  constructor({ file = null, cap = DEFAULT_MAILBOX_CAP, ttlMs = DEFAULT_MAILBOX_TTL_MS, flushDelayMs = 250 } = {}) {
    this.inner = memoryPersistence();
    this.file = file;
    this.cap = cap;
    this.ttlMs = ttlMs;
    this.flushDelayMs = flushDelayMs;
    // Map(clientId -> Packet[]) — queue order is delivery order.
    this.outgoing = new Map();
    // Session ids that hold persistent subscriptions (what the snapshot
    // covers). Populated by addSubscriptions/ensureMailbox, emptied by
    // cleanSubscriptions.
    this.sessions = new Set();
    this._flushTimer = null;
    this._dirty = false;
    this._broker = null;
    this.stats = { dropped: 0, expired: 0 };
  }

  // aedes assigns `persistence.broker = this` after construction; the inner
  // persistence wants it too ($SYS topics, packet ids).
  get broker() { return this._broker; }
  set broker(b) { this._broker = b; this.inner.broker = b; }

  // ── snapshot ─────────────────────────────────────────────────────────────

  /** Restore sessions and queues from the snapshot file, if any. Call before the broker starts. */
  load() {
    if (!this.file) return this;
    let data;
    try {
      data = JSON.parse(fs.readFileSync(this.file, 'utf8'));
    } catch (err) {
      if (err.code !== 'ENOENT') logger.warn(`[vortexia] mailbox snapshot ${this.file} unreadable (${err.message}) — starting with empty mailboxes`);
      return this;
    }
    if (!data || data.version !== SNAPSHOT_VERSION || typeof data.sessions !== 'object') return this;
    let restoredQueued = 0;
    for (const [clientId, session] of Object.entries(data.sessions)) {
      const subs = Array.isArray(session.subscriptions) ? session.subscriptions : [];
      if (subs.length) {
        this.inner.addSubscriptions({ id: clientId }, subs, () => {});
        this.sessions.add(clientId);
      }
      const queue = (Array.isArray(session.queue) ? session.queue : []).map(deserializePacket);
      const live = this._prune(queue);
      if (live.length) {
        this.outgoing.set(clientId, live);
        restoredQueued += live.length;
      }
    }
    logger.info(`[vortexia] restored ${this.sessions.size} mailbox session(s), ${restoredQueued} queued message(s) from ${this.file}`);
    return this;
  }

  snapshot() {
    const sessions = {};
    const ids = new Set([...this.sessions, ...this.outgoing.keys()]);
    for (const id of ids) {
      let subscriptions = [];
      this.inner.subscriptionsByClient({ id }, (err, subs) => { if (!err && subs) subscriptions = subs; });
      const queue = this.outgoing.get(id) || [];
      if (!subscriptions.length && !queue.length) continue;
      sessions[id] = { subscriptions, queue: queue.map(serializePacket) };
    }
    return { version: SNAPSHOT_VERSION, savedAt: new Date().toISOString(), sessions };
  }

  _scheduleFlush() {
    this._dirty = true;
    if (!this.file || this._flushTimer) return;
    this._flushTimer = setTimeout(() => {
      this._flushTimer = null;
      this.flush();
    }, this.flushDelayMs);
    this._flushTimer.unref?.();
  }

  /** Write the snapshot now (synchronously — safe to call from a shutdown path). */
  flush() {
    if (!this.file) return;
    if (this._flushTimer) { clearTimeout(this._flushTimer); this._flushTimer = null; }
    if (!this._dirty) return;
    try {
      fs.mkdirSync(path.dirname(this.file), { recursive: true });
      const tmp = `${this.file}.tmp`;
      fs.writeFileSync(tmp, JSON.stringify(this.snapshot()));
      fs.renameSync(tmp, this.file);
      this._dirty = false;
    } catch (err) {
      logger.warn(`[vortexia] could not write mailbox snapshot ${this.file}: ${err.message}`);
    }
  }

  // ── mailbox provisioning ─────────────────────────────────────────────────

  /**
   * Make sure the session for `agentName` holds a QoS 1 subscription to its
   * own inbox, so publishes to it are queued even if that agent has never
   * connected. Idempotent; cheap after the first call.
   */
  ensureMailbox(agentName, cb = () => {}) {
    const id = mailboxClientId(agentName);
    if (this.sessions.has(id)) return cb(null, false);
    const topic = inboxTopic(agentName);
    this.inner.addSubscriptions({ id }, [{ topic, qos: 1 }], (err) => {
      if (!err) {
        this.sessions.add(id);
        this._scheduleFlush();
      }
      cb(err, !err);
    });
  }

  /** How many messages are waiting for `agentName`. */
  queuedFor(agentName) {
    return (this.outgoing.get(mailboxClientId(agentName)) || []).length;
  }

  // ── outgoing queues (ours) ───────────────────────────────────────────────

  _prune(queue) {
    if (!this.ttlMs || this.ttlMs <= 0) return queue;
    const cutoff = Date.now() - this.ttlMs;
    const live = queue.filter((p) => (p.enqueuedAt ?? cutoff) >= cutoff);
    this.stats.expired += queue.length - live.length;
    return live;
  }

  _queueFor(clientId) {
    let queue = this.outgoing.get(clientId);
    if (!queue) {
      queue = [];
      this.outgoing.set(clientId, queue);
    }
    return queue;
  }

  _enqueueOne(sub, packet) {
    const id = sub.clientId;
    let queue = this._prune(this._queueFor(id));
    if (this.cap > 0 && queue.length >= this.cap) {
      const drop = queue.length - this.cap + 1;
      queue = queue.slice(drop);
      this.stats.dropped += drop;
      logger.warn(`[vortexia] mailbox ${id} is full (${this.cap}) — dropped ${drop} oldest message(s)`);
    }
    const copy = new Packet(packet);
    copy.enqueuedAt = Date.now();
    queue.push(copy);
    this.outgoing.set(id, queue);
  }

  outgoingEnqueue(sub, packet, cb) {
    this._enqueueOne(sub, packet);
    this._scheduleFlush();
    process.nextTick(cb);
  }

  outgoingEnqueueCombi(subs, packet, cb) {
    for (const sub of subs) this._enqueueOne(sub, packet);
    if (subs.length) this._scheduleFlush();
    process.nextTick(cb);
  }

  outgoingUpdate(client, packet, cb) {
    const queue = this._queueFor(client.id);
    for (let i = 0; i < queue.length; i++) {
      const temp = queue[i];
      if (temp.brokerId === packet.brokerId) {
        if (temp.brokerCounter === packet.brokerCounter) {
          temp.messageId = packet.messageId;
          this._scheduleFlush();
          return cb(null, client, packet);
        }
      } else if (temp.messageId === packet.messageId) {
        const replacement = new Packet(packet);
        replacement.enqueuedAt = temp.enqueuedAt;
        queue[i] = replacement;
        this._scheduleFlush();
        return cb(null, client, packet);
      }
    }
    cb(new Error('no such packet'), client, packet);
  }

  outgoingClearMessageId(client, packet, cb) {
    const queue = this._queueFor(client.id);
    for (let i = 0; i < queue.length; i++) {
      if (queue[i].messageId === packet.messageId) {
        const [removed] = queue.splice(i, 1);
        if (!queue.length) this.outgoing.delete(client.id);
        this._scheduleFlush();
        return cb(null, removed);
      }
    }
    cb();
  }

  outgoingStream(client) {
    const queue = this._prune(this._queueFor(client.id));
    this.outgoing.set(client.id, queue);
    // Shallow copy: delivery mutates the queue (outgoingUpdate/Clear) while
    // this stream is being consumed.
    return Readable.from([...queue]);
  }

  // ── subscriptions (delegated, plus session bookkeeping) ──────────────────

  addSubscriptions(client, subs, cb) {
    this.inner.addSubscriptions(client, subs, (err, c) => {
      if (!err) { this.sessions.add(client.id); this._scheduleFlush(); }
      cb(err, c);
    });
  }

  removeSubscriptions(client, subs, cb) {
    this.inner.removeSubscriptions(client, subs, (err, c) => {
      if (!err) this._scheduleFlush();
      cb(err, c);
    });
  }

  /**
   * A clean-session connect (or explicit cleanup) discards the whole
   * session, queue included — MQTT 3.1.1 §3.1.2.4. The stock memory
   * persistence keeps the queue around; here it goes too.
   */
  cleanSubscriptions(client, cb) {
    this.inner.cleanSubscriptions(client, (err, c) => {
      if (!err) {
        this.sessions.delete(client.id);
        this.outgoing.delete(client.id);
        this._scheduleFlush();
      }
      cb(err, c);
    });
  }

  subscriptionsByClient(client, cb) { return this.inner.subscriptionsByClient(client, cb); }
  subscriptionsByTopic(pattern, cb) { return this.inner.subscriptionsByTopic(pattern, cb); }
  countOffline(cb) { return this.inner.countOffline(cb); }
  getClientList(topic) { return this.inner.getClientList(topic); }

  // ── everything else: straight through ────────────────────────────────────

  storeRetained(pkt, cb) { return this.inner.storeRetained(pkt, cb); }
  createRetainedStream(pattern) { return this.inner.createRetainedStream(pattern); }
  createRetainedStreamCombi(patterns) { return this.inner.createRetainedStreamCombi(patterns); }
  incomingStorePacket(client, packet, cb) { return this.inner.incomingStorePacket(client, packet, cb); }
  incomingGetPacket(client, packet, cb) { return this.inner.incomingGetPacket(client, packet, cb); }
  incomingDelPacket(client, packet, cb) { return this.inner.incomingDelPacket(client, packet, cb); }
  putWill(client, packet, cb) { return this.inner.putWill(client, packet, cb); }
  getWill(client, cb) { return this.inner.getWill(client, cb); }
  delWill(client, cb) { return this.inner.delWill(client, cb); }
  streamWill(brokers) { return this.inner.streamWill(brokers); }

  destroy(cb) {
    this.flush();
    this.outgoing.clear();
    this.sessions.clear();
    return this.inner.destroy(cb);
  }
}

/** True when `topic` is some agent's inbox — the topics that get a mailbox. */
export function isInboxTopic(topic) {
  return parseInboxTopic(topic) !== null;
}
