// Friends control over the local broker (docs/adr/0001-device-pairing.md §10):
// request/response topics the LAS UI uses instead of running the CLI.
// Same pattern as vortexia/control/mailbox/drop: the request carries a
// caller-chosen `id`, and the answer goes on `<topic>/result`, QoS 1, not retained.
//
// Pairing: vortexia/control/pair/{start,cancel,decide}, retained
// vortexia/pair/pending, and outcome events on vortexia/pair/events.

import mqtt from 'mqtt';

export const FRIENDS_REVOKE_TOPIC = 'vortexia/control/friends/revoke';
export const FRIENDS_RENAME_TOPIC = 'vortexia/control/friends/rename';
export const PAIR_START_TOPIC = 'vortexia/control/pair/start';
export const PAIR_CANCEL_TOPIC = 'vortexia/control/pair/cancel';
export const PAIR_DECIDE_TOPIC = 'vortexia/control/pair/decide';
export const PAIR_PENDING_TOPIC = 'vortexia/pair/pending';
export const PAIR_EVENTS_TOPIC = 'vortexia/pair/events';

/** Pure handler for the pairing control topics: (topic, request) → Promise<answer>. */
export async function handlePairControl(pairing, topic, req) {
  const id = req && typeof req === 'object' ? req.id ?? null : null;
  if (!req || typeof req !== 'object') return { id, ok: false, error: 'payload is not a JSON object' };
  if (!pairing) return { id, ok: false, error: 'pairing is not available (vortex-relay with a Nostr key is required)' };
  try {
    if (topic === PAIR_START_TOPIC) {
      const r = await pairing.start({ role: req.role || 'device', label: req.label, ttlMs: req.ttlMs });
      return { id, ok: true, ...r };
    }
    if (topic === PAIR_CANCEL_TOPIC) {
      return pairing.cancel(req.sessionId) ? { id, ok: true } : { id, ok: false, error: 'unknown session' };
    }
    if (topic === PAIR_DECIDE_TOPIC) {
      return { id, ...(await pairing.decide(req.sessionId, req.accept === true)) };
    }
  } catch (err) {
    return { id, ok: false, error: err.message };
  }
  return { id, ok: false, error: `unknown control topic ${topic}` };
}

/** Pure handler: (topic, request) → answer. Exported for tests. */
export function handleFriendsControl(store, topic, req) {
  const id = req && typeof req === 'object' ? req.id ?? null : null;
  if (!req || typeof req !== 'object') return { id, ok: false, error: 'payload is not a JSON object' };
  try {
    if (topic === FRIENDS_REVOKE_TOPIC) {
      const f = store.revoke(req.envName);
      return f ? { id, ok: true, envName: f.envName } : { id, ok: false, error: 'unknown friend' };
    }
    if (topic === FRIENDS_RENAME_TOPIC) {
      if (!req.label || typeof req.label !== 'string') return { id, ok: false, error: 'label is required' };
      const f = store.rename(req.envName, req.label);
      return f ? { id, ok: true, envName: f.envName, label: f.label } : { id, ok: false, error: 'unknown friend' };
    }
  } catch (err) {
    return { id, ok: false, error: err.message };
  }
  return { id, ok: false, error: `unknown control topic ${topic}` };
}

/**
 * Serve the friends control topics on the local broker. Uses its own
 * auto-reconnecting client, so a broker hiccup never leaves the UI without
 * answers. `onChange` runs after every successful change (republish the
 * retained friends list right away instead of waiting for its timer).
 */
export function startFriendsControl({ mqttPort, store, pairing = null, onChange = () => {}, logger = console }) {
  const client = mqtt.connect(`mqtt://127.0.0.1:${mqttPort}`, {
    clientId: `vortexia-friends-control-${process.pid}`,
    reconnectPeriod: 2000,
  });
  const friendTopics = [FRIENDS_REVOKE_TOPIC, FRIENDS_RENAME_TOPIC];
  const pairTopics = [PAIR_START_TOPIC, PAIR_CANCEL_TOPIC, PAIR_DECIDE_TOPIC];
  const topics = [...friendTopics, ...pairTopics];
  const publishPending = (pending) => client.publish(PAIR_PENDING_TOPIC, JSON.stringify({ updatedAt: Date.now(), pending }), { qos: 1, retain: true });
  if (pairing) {
    pairing.onPendingChange = publishPending;
    const prevOnEvent = pairing.onEvent;
    pairing.onEvent = (evt) => {
      prevOnEvent?.(evt);
      client.publish(PAIR_EVENTS_TOPIC, JSON.stringify(evt), { qos: 1, retain: false });
      if (evt.type === 'paired') onChange();
    };
    const sweep = setInterval(() => pairing.sweep(), 5000);
    sweep.unref?.();
    client.on('end', () => clearInterval(sweep));
  }
  client.on('connect', () => {
    client.subscribe(topics, { qos: 1 });
    if (pairing) publishPending(pairing.pending());
  });
  client.on('error', (err) => logger.warn?.(`[vortexia] friends control: ${err.message}`));
  client.on('message', (topic, payload) => {
    if (!topics.includes(topic)) return;
    let req;
    try { req = JSON.parse(payload.toString()); } catch { req = null; }
    if (pairTopics.includes(topic)) {
      handlePairControl(pairing, topic, req).then((answer) => client.publish(`${topic}/result`, JSON.stringify(answer), { qos: 1, retain: false }));
      return;
    }
    const answer = handleFriendsControl(store, topic, req);
    client.publish(`${topic}/result`, JSON.stringify(answer), { qos: 1, retain: false });
    if (answer.ok) {
      logger.info?.(`[vortexia] friends control: ${topic.split('/').pop()} ${answer.envName}`);
      onChange();
    }
  });
  return () => client.end(true);
}
