// Builds this environment's vortex-relay roster (what publishDirectory
// sends to the relay) from the SAME source of truth `las status` already
// uses — the local-agent-society backend's /agents listing — rather than
// a hand-maintained list. Each entry there has a `path`; that path's
// .las-agent.json short_description (rung 1 of the scope ladder, see
// docs/adr/0001-las-agent-scope-ladder.md) is this agent's scopeText.
//
// Best-effort by design, matching broker.js's claimPort: an unreachable
// registry or an unreadable .las-agent.json means one fewer agent in the
// roster, not a hard failure — vortex-relay should never be the reason a
// local vortexia instance won't start.

import fs from 'node:fs';
import path from 'node:path';

export async function discoverLocalRoster(registryUrl = process.env.LAS_REGISTRY_URL || 'http://localhost:8700') {
  let agents;
  try {
    const res = await fetch(`${registryUrl}/agents`);
    if (!res.ok) return [];
    agents = await res.json();
  } catch {
    return [];
  }

  const roster = [];
  for (const [agentName, info] of Object.entries(agents ?? {})) {
    if (!info?.path) continue;
    let scopeText;
    try {
      const raw = fs.readFileSync(path.join(info.path, '.las-agent.json'), 'utf8');
      const parsed = JSON.parse(raw);
      scopeText = parsed.short_description || parsed.long_description;
    } catch {
      // No .las-agent.json (or unreadable) — still list the agent so
      // exact-name delivery works, just without a scope match target.
    }
    roster.push({ agentName, scopeText: scopeText || agentName });
  }
  return roster;
}
