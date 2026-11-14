// Friends control over the local broker (docs/adr/0001-device-pairing.md §10):
// request/response topics the LAS UI uses instead of running the CLI.
// Same pattern as vortexia/control/mailbox/drop: the request carries a
// caller-chosen `id`, and the answer goes on `<topic>/result`, QoS 1, not retained.
//
// Pairing (vortexia/control/pair/*) joins this module in ADR step 4.

import mqtt from 'mqtt';

export const FRIENDS_REVOKE_TOPIC = 'vortexia/control/friends/revoke';
export const FRIENDS_RENAME_TOPIC = 'vortexia/control/friends/rename';

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
export function startFriendsControl({ mqttPort, store, onChange = () => {}, logger = console }) {
  const client = mqtt.connect(`mqtt://127.0.0.1:${mqttPort}`, {
    clientId: `vortexia-friends-control-${process.pid}`,
    reconnectPeriod: 2000,
  });
  const topics = [FRIENDS_REVOKE_TOPIC, FRIENDS_RENAME_TOPIC];
  client.on('connect', () => client.subscribe(topics, { qos: 1 }));
  client.on('error', (err) => logger.warn?.(`[vortexia] friends control: ${err.message}`));
  client.on('message', (topic, payload) => {
    if (!topics.includes(topic)) return;
    let req;
    try { req = JSON.parse(payload.toString()); } catch { req = null; }
    const answer = handleFriendsControl(store, topic, req);
    client.publish(`${topic}/result`, JSON.stringify(answer), { qos: 1, retain: false });
    if (answer.ok) {
      logger.info?.(`[vortexia] friends control: ${topic.split('/').pop()} ${answer.envName}`);
      onChange();
    }
  });
  return () => client.end(true);
}
