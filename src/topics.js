// Topic schema for vortexia — kept minimal and stable.
// See PROTOCOL.md for the full contract.

import crypto from 'node:crypto';

export const BROADCAST_TOPIC = 'las/broadcast';

// TTS request topic (see PROTOCOL.md "Extension: kind field and las/speak").
// Shared by all agents' widgets; consumers must filter on envelope.to.
export const SPEAK_TOPIC = 'las/speak';

export function inboxTopic(name) {
  return `las/agent/${name}/inbox`;
}

export function presenceTopic(name) {
  return `las/agent/${name}/presence`;
}

/**
 * Retained, broker-published state of `name`'s mailbox consumer: whether
 * the persistent session `las-agent-<name>` currently has a live
 * connection. A one-shot poll checks this before taking the session over
 * (see PROTOCOL.md "Mailboxes").
 */
export function sessionTopic(name) {
  return `las/agent/${name}/session`;
}

/**
 * The MQTT client id of `name`'s mailbox session. Whoever connects with
 * this id and clean=false IS the consumer of that agent's inbox — there is
 * exactly one at a time (a later connection takes the session over from
 * an earlier one, per MQTT-3.1.4-2).
 */
export function mailboxClientId(name) {
  return `las-agent-${name}`;
}

/** Inverse of inboxTopic: the agent name, or null if `topic` isn't an inbox. */
export function parseInboxTopic(topic) {
  if (typeof topic !== 'string') return null;
  const parts = topic.split('/');
  if (parts.length !== 4 || parts[0] !== 'las' || parts[1] !== 'agent' || parts[3] !== 'inbox') return null;
  return parts[2] || null;
}

/**
 * Nested mailboxes (PROTOCOL.md "Sessions"): one mailbox per connected
 * runtime of an agent, owned by the persistent client id
 * `las-agent-<name>-<sid>`. Unlike the agent inbox, the broker does NOT
 * create these on first publish — a session mailbox exists once its
 * consumer has subscribed, so a dropped one stays dropped.
 */
export function sessionInboxTopic(name, sid) {
  return `las/agent/${name}/sessions/${sid}/inbox`;
}

export function sessionMailboxClientId(name, sid) {
  return `las-agent-${name}-${sid}`;
}

/** Inverse of sessionInboxTopic: { name, sid }, or null. */
export function parseSessionInboxTopic(topic) {
  if (typeof topic !== 'string') return null;
  const parts = topic.split('/');
  if (parts.length !== 6 || parts[0] !== 'las' || parts[1] !== 'agent' || parts[3] !== 'sessions' || parts[5] !== 'inbox') return null;
  if (!parts[2] || !parts[4]) return null;
  return { name: parts[2], sid: parts[4] };
}

/**
 * Retained JSON { sid, runtime, ts }, published by the environment's
 * backend (LAS), naming the session that holds the agent-level mailbox.
 * vortexia defines the topic; it never publishes or reads it.
 */
export function defaultSessionTopic(name) {
  return `las/agent/${name}/default-session`;
}

/**
 * The client id whose mailbox `topic` is (agent inbox or session inbox),
 * or null when `topic` is no mailbox's own inbox.
 */
export function mailboxOwnerOf(topic) {
  const name = parseInboxTopic(topic);
  if (name) return mailboxClientId(name);
  const s = parseSessionInboxTopic(topic);
  return s ? sessionMailboxClientId(s.name, s.sid) : null;
}

// Control: drop a mailbox (persistent session + queue) by client id — see
// PROTOCOL.md "Dropping a mailbox". Request JSON { clientId, id?, force? },
// the broker answers on the result topic with { id, clientId, dropped, reason? }.
export const MAILBOX_DROP_TOPIC = 'vortexia/control/mailbox/drop';
export const MAILBOX_DROP_RESULT_TOPIC = 'vortexia/control/mailbox/drop/result';

/**
 * Build a standard message envelope.
 * @param {object} opts
 * @param {string} opts.from - sender agent name
 * @param {string} opts.to - recipient agent name (or 'broadcast')
 * @param {string} [opts.source] - 'agent' | 'human' | 'system'
 * @param {string} opts.text - message body
 * @param {number} [opts.ts] - epoch ms, defaults to now
 * @param {string} [opts.id] - unique message id, defaults to a UUID (lets a
 *   consumer dedupe the rare QoS 1 redelivery)
 */
export function buildEnvelope({ from, to, source = 'agent', text, ts = Date.now(), id = crypto.randomUUID() }) {
  return { id, from, to, source, text, ts };
}

// Scope-ladder query protocol (see docs/future-las-agent-scope-router.md,
// section 1). Sent over the target agent's own inbox topic like any other
// message — `kind` distinguishes it from a normal chat envelope.
export const SCOPE_QUERY_KIND = 'scope-query';
export const SCOPE_REPLY_KIND = 'scope-reply';

/**
 * @param {object} opts
 * @param {string} opts.from - requesting agent name
 * @param {string} opts.to - target agent name
 * @param {string|{maxChars:number}} [opts.detail] - 'short' | 'more' | 'full' | {maxChars:N}
 * @param {string} opts.queryId - correlates this query with its reply
 */
export function buildScopeQueryEnvelope({ from, to, detail = 'short', queryId, ts = Date.now() }) {
  return { from, to, source: 'agent', kind: SCOPE_QUERY_KIND, detail, queryId, ts };
}

/**
 * @param {object} opts
 * @param {string} opts.from - replying agent name
 * @param {string} opts.to - original requester's name
 * @param {string} opts.queryId - the id from the query being answered
 * @param {number} opts.rung - which ladder rung this reply came from
 * @param {string} opts.scopeSource - where that rung's text came from (e.g. a filename)
 * @param {string} opts.text - the rung's text
 * @param {Array<{id: string, scope: string, accepts: string[]}>} [opts.children] - the
 *   agent's sessions, one rung down (PROTOCOL.md "Sessions"); omitted when absent
 */
export function buildScopeReplyEnvelope({ from, to, queryId, rung, scopeSource, text, children, ts = Date.now() }) {
  return { from, to, source: 'agent', kind: SCOPE_REPLY_KIND, queryId, rung, scopeSource, text, ...(Array.isArray(children) ? { children } : {}), ts };
}
