// The federated agent directory: who exists, in which environment, and
// what their scope is — built from each environment's own live roster
// instead of a hand-maintained array (the known limitation flagged in
// docs/federation-poc.md). Every environment publishes its own snapshot
// file on the relay (sole writer, so no read-modify-write race — same
// rule GistRelay.appendMessage already relies on for outbox files) and
// merges everyone else's snapshot to get the full picture. Works for any
// number of environments: each new one is just one more file to read.

function directoryFileFor(envName) {
  return `directory-${envName}.json`;
}

/**
 * Publish this environment's own agent roster to the relay. Call whenever
 * the local roster changes (an agent registers/unregisters) or on a
 * refresh interval — see FederationBridge.startDirectorySync.
 * @param {import('./relay.js').Relay} relay
 * @param {string} envName
 * @param {Array<{agentName: string, scopeText: string}>} agents
 */
export async function publishDirectory(relay, envName, agents) {
  await relay.writeFile(directoryFileFor(envName), {
    envName,
    agents,
    updatedAt: Date.now(),
  });
}

/**
 * Read and merge every environment's published roster into one flat
 * directory, suitable for pickTargets(). Environments that have never
 * published (or whose file is unreadable) are silently skipped — a
 * missing environment degrades that environment's reachability, not the
 * whole merge.
 *
 * @param {import('./relay.js').Relay} relay
 * @param {string[]} envNames - every environment expected to publish a
 *   directory file, this one included
 * @returns {Promise<{entries: Array<{envName: string, agentName: string, scopeText: string}>, collisions: Map<string, string[]>}>}
 *   `collisions` maps a bare agentName to the list of envNames that both
 *   claim it — same name, different environments, same as two colleagues
 *   who happen to share a job title on different machines (e.g. "System"
 *   on each Mac). Callers must not silently pick one; see
 *   resolveDirectoryName below.
 */
export async function mergeDirectories(relay, envNames) {
  const entries = [];
  const byName = new Map();

  for (const envName of envNames) {
    let snapshot;
    try {
      snapshot = await relay.readFile(directoryFileFor(envName));
    } catch (err) {
      // Missing/unreadable is expected for an environment that hasn't
      // published yet — degrade that one environment's reachability, not
      // the whole merge. Still surfaced (not swallowed silently) so a real
      // bug here doesn't look identical to "hasn't published yet".
      console.warn(`[federation] could not read directory for ${envName}: ${err.message}`);
      continue;
    }
    const agents = Array.isArray(snapshot) ? snapshot : snapshot?.agents;
    if (!Array.isArray(agents)) continue;

    for (const agent of agents) {
      if (!agent?.agentName || !agent?.scopeText) continue;
      const entry = { envName, agentName: agent.agentName, scopeText: agent.scopeText };
      entries.push(entry);
      if (!byName.has(agent.agentName)) byName.set(agent.agentName, []);
      byName.get(agent.agentName).push(envName);
    }
  }

  const collisions = new Map();
  for (const [name, envs] of byName) {
    if (envs.length > 1) collisions.set(name, envs);
  }

  return { entries, collisions };
}

/**
 * Resolve a `to` addressee for exact-name, point-to-point delivery.
 * Accepts either a bare name ("System") or an env-qualified one
 * ("System@uy-mac"). Never guesses across a same-name collision:
 *
 *  - Qualified (`name@env`): resolves to exactly that env, or 'not-found'
 *    if that env never published that name.
 *  - Bare name, local match exists: resolves locally — "talk to *my*
 *    System" is the unqualified default, matching how these names were
 *    never meant to be globally unique in the first place.
 *  - Bare name, no local match, exactly one remote match: resolves there.
 *  - Bare name, no local match, multiple remote matches (a real
 *    collision): 'ambiguous' — caller must ask for the `name@env` form
 *    rather than picking one.
 *  - No match anywhere: 'not-found'.
 *
 * @param {string} to - bare name or `name@env`
 * @param {string} localEnvName
 * @param {Array<{envName: string, agentName: string, scopeText: string}>} entries - from mergeDirectories, PLUS the local roster (a bridge's own agents may not be in its own merged snapshot if it hasn't republished since a new local agent joined)
 * @returns {{status: 'resolved', envName: string, agentName: string} | {status: 'ambiguous', candidates: string[]} | {status: 'not-found'}}
 */
export function resolveDirectoryName(to, localEnvName, entries) {
  const at = to.lastIndexOf('@');
  if (at > 0) {
    const bareName = to.slice(0, at);
    const envName = to.slice(at + 1);
    const match = entries.find((e) => e.agentName === bareName && e.envName === envName);
    return match ? { status: 'resolved', envName, agentName: bareName } : { status: 'not-found' };
  }

  const matches = entries.filter((e) => e.agentName === to);
  if (matches.length === 0) return { status: 'not-found' };

  const local = matches.find((e) => e.envName === localEnvName);
  if (local) return { status: 'resolved', envName: localEnvName, agentName: to };

  const remoteEnvs = [...new Set(matches.map((e) => e.envName))];
  if (remoteEnvs.length === 1) return { status: 'resolved', envName: remoteEnvs[0], agentName: to };

  return { status: 'ambiguous', candidates: remoteEnvs.map((env) => `${to}@${env}`) };
}
