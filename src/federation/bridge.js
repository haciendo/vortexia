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
import { publishDirectory, mergeDirectories, resolveDirectoryName } from './directory.js';

export const FEDERATION_KIND = 'federation-intent';

// Point-to-point: "deliver to this exact name, wherever it lives" — as
// opposed to FEDERATION_KIND, which has no named recipient and routes by
// scope match instead. A local backend (`las agent inject`) that can't
// find `targetName` in its own local registry publishes one of these to
// the environment's gateway agent instead of 404ing; the bridge resolves
// the name (local-first, then federated) and either delivers locally or
// relays to the owning environment, bypassing pickTargets entirely — an
// exact name is exact, it doesn't need a semantic match.
export const FEDERATION_DIRECT_KIND = 'federation-direct';

export class FederationBridge {
  /**
   * @param {object} opts
   * @param {string} opts.envName - this environment's name (must match a
   *   directory entry's envName for its own agents)
   * @param {import('./relay.js').Relay} opts.relay
   * @param {Array<{envName: string, agentName: string, scopeText: string}>} [opts.directory] -
   *   a static seed directory (used as-is by tests, and merged with
   *   whatever syncDirectory() pulls from the relay). Optional if
   *   opts.envNames is given — the live merge covers the same ground for
   *   real (N-environment, no hand-maintained array) deployments.
   * @param {string[]} [opts.envNames] - every environment expected to
   *   publish its own directory file on the relay (this one included);
   *   enables syncDirectory()/startDirectorySync() for exact-name
   *   (FEDERATION_DIRECT_KIND) resolution across N environments.
   * @param {object} [opts.matchOpts] - passed through to pickTargets (minScore, closeness, embedder)
   */
  constructor({ envName, relay, directory = [], envNames, matchOpts = {} }) {
    this.envName = envName;
    this.relay = relay;
    this.directory = directory;
    this.envNames = envNames ?? [envName];
    this.matchOpts = matchOpts;
    this.localClient = null;
    this._pollTimer = null;
    this._directoryTimer = null;
    this._lastIndex = -1;
    // What resolveDirectoryName() searches — starts as the static seed,
    // widened by syncDirectory() once it's run at least once.
    this._mergedEntries = directory;
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
    if (envelope.kind === FEDERATION_DIRECT_KIND) {
      return this._onDirectMessage(envelope);
    }
    if (envelope.kind !== FEDERATION_KIND) return;

    const targets = await pickTargets(envelope.intent, this._mergedEntries, this.matchOpts);
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

  /**
   * Point-to-point delivery for `envelope.to` (bare name or `name@env`),
   * bypassing pickTargets entirely — an exact name doesn't need a
   * semantic match. Resolution is local-first (see resolveDirectoryName):
   * a bare name that also exists locally always means "my local one," the
   * same way two colleagues named the same thing on different machines
   * each answer to their own name without confusion. An ambiguous or
   * unresolvable name gets a reply back to the sender instead of being
   * silently dropped or guessed at.
   */
  async _onDirectMessage(envelope) {
    const resolved = resolveDirectoryName(envelope.to, this.envName, this._mergedEntries);

    if (resolved.status === 'not-found') {
      this._replyDirectError(envelope, `no agent named "${envelope.to}" is known in the federation`);
      return;
    }
    if (resolved.status === 'ambiguous') {
      this._replyDirectError(
        envelope,
        `"${envelope.to}" exists in more than one environment — use one of: ${resolved.candidates.join(', ')}`,
      );
      return;
    }

    const routed = {
      from: envelope.from,
      text: envelope.text,
      agentNames: [resolved.agentName],
      routedFrom: this.envName,
      ts: Date.now(),
    };
    if (resolved.envName === this.envName) {
      this._deliverLocally(routed);
    } else {
      await this.relay.appendMessage(this.outboxFileFor(resolved.envName), routed);
    }
  }

  _replyDirectError(envelope, reason) {
    if (!envelope.from) return;
    this.localClient.send(envelope.from, reason, {
      kind: 'federation-direct-error',
      to: envelope.to,
    });
  }

  /** Publish this environment's own agent roster to the relay (see directory.js). */
  async publishSelf(agents) {
    await publishDirectory(this.relay, this.envName, agents);
  }

  /**
   * Refresh the merged, all-environments directory used by exact-name
   * resolution (and, for anything not passed a static `directory` seed,
   * by pickTargets too). Logs — doesn't throw — on a name collision
   * across environments; resolveDirectoryName is what actually enforces
   * "never guess" at delivery time.
   */
  async syncDirectory() {
    const { entries, collisions } = await mergeDirectories(this.relay, this.envNames);
    for (const [name, envs] of collisions) {
      console.warn(`[federation:${this.envName}] "${name}" exists in multiple environments: ${envs.join(', ')} — exact-name delivery to the bare name will require a name@env qualifier unless one is local`);
    }
    // Static seed entries (if any) stay available too, e.g. for tests that
    // never call publishSelf/syncDirectory against a real relay directory.
    this._mergedEntries = [...this.directory, ...entries];
    return { entries: this._mergedEntries, collisions };
  }

  /** Periodically republish this env's roster and refresh the merged directory. */
  startDirectorySync(agents, intervalMs = 5000) {
    const tick = () => {
      this.publishSelf(agents)
        .then(() => this.syncDirectory())
        .catch((err) => console.error(`[federation:${this.envName}] directory sync failed:`, err));
    };
    tick();
    this._directoryTimer = setInterval(tick, intervalMs);
    return this;
  }

  stopDirectorySync() {
    clearInterval(this._directoryTimer);
    this._directoryTimer = null;
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
