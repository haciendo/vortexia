// Topic schema for vortexia — kept minimal and stable.
// See PROTOCOL.md for the full contract.

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
 * Build a standard message envelope.
 * @param {object} opts
 * @param {string} opts.from - sender agent name
 * @param {string} opts.to - recipient agent name (or 'broadcast')
 * @param {string} [opts.source] - 'agent' | 'human' | 'system'
 * @param {string} opts.text - message body
 * @param {number} [opts.ts] - epoch ms, defaults to now
 */
export function buildEnvelope({ from, to, source = 'agent', text, ts = Date.now() }) {
  return { from, to, source, text, ts };
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
 */
export function buildScopeReplyEnvelope({ from, to, queryId, rung, scopeSource, text, ts = Date.now() }) {
  return { from, to, source: 'agent', kind: SCOPE_REPLY_KIND, queryId, rung, scopeSource, text, ts };
}
