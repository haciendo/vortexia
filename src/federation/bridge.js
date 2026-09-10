// Bridges one environment's local vortexia broker to the federation: a
// local agent publishes a "federation intent" (no named recipient — just
// what it needs, per docs/future-las-agent-scope-router.md section 1b),
// the bridge decides which environment(s) actually match via router.js,
// and either delivers locally (same environment) or writes it into the
// target environment's file on the shared relay (a different environment
// — potentially a different machine, connected only through that relay).
// Meanwhile it polls its OWN file on the relay for messages other
// environments routed to it, and delivers those to the matched local
// agent(s).

import { pickTargets } from './router.js';

export const FEDERATION_KIND = 'federation-intent';

export class FederationBridge {
  /**
   * @param {object} opts
   * @param {string} opts.envName - this environment's name (must match a
   *   directory entry's envName for its own agents)
   * @param {import('./relay.js').Relay} opts.relay
   * @param {Array<{envName: string, agentName: string, scopeText: string}>} opts.directory -
   *   every known agent across every environment (shared config, same on
   *   all bridges — see docs/federation-poc.md for how this gets
   *   distributed in the real cross-machine version)
   * @param {object} [opts.matchOpts] - passed through to pickTargets (minScore, closeness, embedder)
   */
  constructor({ envName, relay, directory, matchOpts = {} }) {
    this.envName = envName;
    this.relay = relay;
    this.directory = directory;
    this.matchOpts = matchOpts;
    this.localClient = null;
    this._pollTimer = null;
    this._lastIndex = -1;
  }

  outboxFileFor(envName) {
    return `inbox-${envName}.json`;
  }

  /** Attach the local VortexiaClient this bridge listens on / delivers into. */
  attach(localClient) {
    this.localClient = localClient;
    localClient.on('message', (envelope) => {
      this._onLocalMessage(envelope).catch((err) => {
        console.error(`[federation:${this.envName}] failed handling local message:`, err);
      });
    });
    return this;
  }

  async _onLocalMessage(envelope) {
    if (envelope.kind !== FEDERATION_KIND) return;

    const targets = await pickTargets(envelope.intent, this.directory, this.matchOpts);
    const agentNamesByEnv = new Map();
    for (const t of targets) {
      if (!agentNamesByEnv.has(t.envName)) agentNamesByEnv.set(t.envName, []);
      agentNamesByEnv.get(t.envName).push(t.agentName);
    }

    for (const [envName, agentNames] of agentNamesByEnv) {
      const routed = {
        from: envelope.from,
        intent: envelope.intent,
        text: envelope.text,
        agentNames,
        routedFrom: this.envName,
        ts: Date.now(),
      };
      if (envName === this.envName) {
        this._deliverLocally(routed);
      } else {
        await this.relay.appendMessage(this.outboxFileFor(envName), routed);
      }
    }
  }

  _deliverLocally(routed) {
    for (const agentName of routed.agentNames) {
      this.localClient.send(agentName, routed.text ?? routed.intent, {
        from: routed.from,
        kind: 'federation-delivery',
        routedFrom: routed.routedFrom,
      });
    }
  }

  /** Start polling this environment's own file on the relay for incoming federated messages. */
  startPolling(intervalMs = 300) {
    this._pollTimer = setInterval(() => {
      this._pollOnce().catch((err) => console.error(`[federation:${this.envName}] poll failed:`, err));
    }, intervalMs);
    return this;
  }

  stopPolling() {
    clearInterval(this._pollTimer);
    this._pollTimer = null;
  }

  async _pollOnce() {
    const messages = await this.relay.readFile(this.outboxFileFor(this.envName));
    for (let i = this._lastIndex + 1; i < messages.length; i++) {
      this._deliverLocally(messages[i]);
    }
    this._lastIndex = messages.length - 1;
  }
}
