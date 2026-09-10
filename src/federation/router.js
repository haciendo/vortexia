// Routes a message to whichever known agent(s) — local or remote — best
// match its intent, per docs/future-las-agent-scope-router.md section 1b:
// "a message shouldn't have to name a specific agent to reach the right
// one." Comparison is by embedding similarity against each directory
// entry's scope descriptor (see embeddings.js).

import { resolveEmbedder, cosineSimilarity } from './embeddings.js';

/**
 * @typedef {object} DirectoryEntry
 * @property {string} envName - which environment/broker this agent lives in
 * @property {string} agentName
 * @property {string} scopeText - the agent's scope descriptor to match against
 */

/**
 * @param {string} intentText - the message's routing intent, e.g. "necesito el pronóstico del tiempo"
 * @param {DirectoryEntry[]} directory - every known agent across every environment
 * @param {object} [opts]
 * @param {number} [opts.minScore] - entries scoring below this are never a match
 * @param {number} [opts.closeness] - any entry scoring >= closeness * bestScore
 *   is included too, not just the single best match — this is what lets one
 *   message legitimately reach more than one agent/environment when several
 *   are semantically close (e.g. two weather agents), while a message with
 *   one clear best match reaches only that one.
 * @param {{name: string, embed: (text: string) => Promise<number[]>}} [opts.embedder] -
 *   pass a specific embedder (e.g. bagOfWordsEmbedder in tests, for
 *   deterministic results with no network dependency) — otherwise resolved
 *   automatically (Ollama if reachable, bag-of-words fallback otherwise).
 *   Resolved ONCE for the whole call so every vector compared here comes
 *   from the same embedder — mixing vectors from two different embedders
 *   would silently produce meaningless similarity scores.
 * @returns {Promise<Array<DirectoryEntry & {score: number}>>} matches, best first
 */
export async function pickTargets(intentText, directory, { minScore = 0.05, closeness = 0.75, embedder } = {}) {
  const embed = (embedder ?? (await resolveEmbedder())).embed;

  const intentVec = await embed(intentText);
  const scored = [];
  for (const entry of directory) {
    const vec = await embed(entry.scopeText);
    scored.push({ ...entry, score: cosineSimilarity(intentVec, vec) });
  }
  scored.sort((a, b) => b.score - a.score);

  const filtered = scored.filter((entry) => entry.score >= minScore);
  if (filtered.length === 0) return [];
  const best = filtered[0].score;
  return filtered.filter((entry) => entry.score >= best * closeness);
}
