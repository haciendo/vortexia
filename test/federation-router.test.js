import { test } from 'node:test';
import assert from 'node:assert/strict';
import { pickTargets } from '../src/federation/router.js';
import { bagOfWordsEmbedder, cosineSimilarity, resolveEmbedder, ollamaEmbedder } from '../src/federation/embeddings.js';

// Three simulated environments, one agent each — mirrors the federation PoC:
// weather-ish agents in A and C should both match a weather intent, while
// the billing agent in B should be the sole match for a billing intent.
// Bag-of-words has no stemming — it only sees shared literal tokens, not
// paraphrases or morphological variants (that's exactly the gap a real
// embedding model like Ollama's all-minilm closes; see embeddings.js). So
// these fixtures share literal vocabulary with the intents on purpose,
// the same way two independently-written-but-topically-similar scope
// descriptors would in practice.
const directory = [
  { envName: 'env-a', agentName: 'Clima', scopeText: 'pronostico del tiempo, temperatura, lluvia, viento' },
  { envName: 'env-b', agentName: 'Facturas', scopeText: 'facturacion, factura, pagos, pagar, cobros, dinero' },
  { envName: 'env-c', agentName: 'Meteo', scopeText: 'clima, tiempo, temperatura, humedad, viento, pronostico' },
];

test('a weather intent matches both weather agents (env-a and env-c), not billing', async () => {
  const targets = await pickTargets('pronostico del tiempo y viento para mañana', directory, {
    embedder: bagOfWordsEmbedder,
    closeness: 0.6,
  });
  const envs = targets.map((t) => t.envName).sort();
  assert.deepEqual(envs, ['env-a', 'env-c']);
});

test('a billing intent matches only the billing agent (env-b)', async () => {
  const targets = await pickTargets('quiero pagar mi factura de este mes', directory, {
    embedder: bagOfWordsEmbedder,
  });
  const envs = targets.map((t) => t.envName);
  assert.deepEqual(envs, ['env-b']);
});

test('an intent matching nothing returns no targets', async () => {
  const targets = await pickTargets('xyzabc qwerty nonsense unrelated', directory, {
    embedder: bagOfWordsEmbedder,
  });
  assert.deepEqual(targets, []);
});

test('bag-of-words cosineSimilarity: identical text scores 1, unrelated text scores near 0', async () => {
  const a = await bagOfWordsEmbedder.embed('pronostico del tiempo y temperatura');
  const b = await bagOfWordsEmbedder.embed('pronostico del tiempo y temperatura');
  const c = await bagOfWordsEmbedder.embed('factura y pagos de dinero');
  assert.ok(Math.abs(cosineSimilarity(a, b) - 1) < 1e-9);
  assert.ok(cosineSimilarity(a, c) < 0.2);
});

test('real embeddings (Ollama) catch a paraphrase bag-of-words would miss — skips if Ollama is unreachable', async (t) => {
  const embedder = await resolveEmbedder({ timeoutMs: 500 });
  if (embedder !== ollamaEmbedder) {
    t.skip('Ollama not reachable on this machine — bag-of-words fallback covers the routing logic above');
    return;
  }

  // No literal word overlap at all — pure paraphrase. Bag-of-words would
  // score this ~0; a real embedding model should still see it's about
  // weather.
  const targets = await pickTargets('¿va a llover mañana?', directory, { embedder: ollamaEmbedder });
  const envs = targets.map((t2) => t2.envName);
  assert.ok(envs.includes('env-a') || envs.includes('env-c'), `expected a weather env, got ${JSON.stringify(envs)}`);
});
