// Text embeddings for the federation routing PoC (see
// docs/future-las-agent-scope-router.md, section 1b — routing a message by
// comparing it against agents' scope descriptors instead of a named
// recipient).
//
// Two embedders, same shape (`embed(text) -> Promise<number[]>`, a dense
// vector), so callers can swap one for the other without touching
// cosineSimilarity or router.js:
//   - `ollamaEmbedder`: real embeddings via a local Ollama (all-minilm,
//     384-dim) — see `las agent inject LocalModels` for how this got set up.
//   - `bagOfWordsEmbedder`: zero-dependency fallback using the classic
//     hashing trick (tokens hashed into a fixed-size vector) so it's ALSO a
//     plain dense array, comparable with cosineSimilarity the same way.
// `resolveEmbedder()` picks whichever is actually reachable, once per
// routing decision — see router.js for why it's resolved once rather than
// per-call (mixing vectors from two different embedders mid-comparison
// would silently produce garbage similarity scores).

const OLLAMA_URL = process.env.VORTEXIA_OLLAMA_URL || 'http://localhost:11434';
const OLLAMA_MODEL = process.env.VORTEXIA_OLLAMA_MODEL || 'all-minilm';
const BAG_OF_WORDS_DIM = 256;

const STOPWORDS = new Set([
  'el', 'la', 'los', 'las', 'un', 'una', 'unos', 'unas', 'de', 'del', 'al',
  'y', 'o', 'u', 'para', 'por', 'que', 'en', 'con', 'sin', 'a', 'mi', 'me',
  'tu', 'su', 'lo', 'le', 'se', 'es', 'soy', 'esta', 'este', 'esto',
  'necesito', 'quiero', 'quisiera', 'hola', 'porfavor', 'favor',
]);

function stripAccents(s) {
  return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
}

function tokenize(text) {
  return stripAccents(text.toLowerCase()).match(/[a-z0-9]+/g) || [];
}

function hashToken(tok) {
  let h = 0;
  for (let i = 0; i < tok.length; i++) h = (h * 31 + tok.charCodeAt(i)) >>> 0;
  return h % BAG_OF_WORDS_DIM;
}

/** Zero-dependency fallback: hashing-trick bag-of-words, as a dense vector. */
export async function bagOfWordsEmbed(text) {
  const vec = new Array(BAG_OF_WORDS_DIM).fill(0);
  for (const tok of tokenize(text)) {
    if (STOPWORDS.has(tok) || tok.length < 3) continue;
    vec[hashToken(tok)] += 1;
  }
  return vec;
}

/** Real embeddings via a local Ollama instance running `all-minilm`. */
export async function ollamaEmbed(text) {
  const res = await fetch(`${OLLAMA_URL}/api/embed`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: OLLAMA_MODEL, input: text }),
  });
  if (!res.ok) throw new Error(`ollama embed failed: ${res.status}`);
  const data = await res.json();
  const vec = data.embeddings?.[0];
  if (!Array.isArray(vec)) throw new Error('ollama embed: unexpected response shape');
  return vec;
}

export const bagOfWordsEmbedder = { name: 'bag-of-words', embed: bagOfWordsEmbed };
export const ollamaEmbedder = { name: `ollama:${OLLAMA_MODEL}`, embed: ollamaEmbed };

/**
 * Pick whichever embedder is actually usable right now: try Ollama with a
 * short timeout, fall back to bag-of-words if it's unreachable or errors.
 */
export async function resolveEmbedder({ timeoutMs = 1000 } = {}) {
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const res = await fetch(`${OLLAMA_URL}/api/tags`, { signal: controller.signal });
    clearTimeout(timer);
    if (res.ok) return ollamaEmbedder;
  } catch {
    // fall through to bag-of-words
  }
  return bagOfWordsEmbedder;
}

/** Cosine similarity between two same-length dense vectors. */
export function cosineSimilarity(a, b) {
  let dot = 0;
  let normA = 0;
  let normB = 0;
  const len = Math.min(a.length, b.length);
  for (let i = 0; i < len; i++) {
    dot += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }
  if (normA === 0 || normB === 0) return 0;
  return dot / (Math.sqrt(normA) * Math.sqrt(normB));
}
