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
