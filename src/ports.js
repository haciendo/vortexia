// Port selection and port-registry reconciliation for the broker.
//
// The boot race this fixes (seen 2026-09-23, after a reboot): vortexia and
// the local-agent-society backend are both launchd jobs. vortexia came up
// while the :8700 registry wasn't listening yet, so its claim failed and it
// fell back to 1883/8883 — while the registry, once up, still advertised the
// previous run's 9014/9019. Every backend/CLI publish went to a port nobody
// listened on for an hour.
//
// The rule an infrastructure service follows: its listening port is
// configuration, not something re-negotiated on every start. So:
//
//   1. Precedence for WHICH port to bind: an explicit pin (option or
//      VORTEXIA_MQTT_PORT / VORTEXIA_WS_PORT) → the port bound last time
//      (data/ports.json, "sticky") → a fresh claim from the registry → a
//      locally probed free port in the 9000–9999 range. The registry being
//      down never changes the answer for a broker that has run before.
//   2. Binding is the source of truth. The registry is told what was
//      actually bound — immediately if it's reachable, otherwise from a
//      background loop with exponential backoff that keeps trying until
//      the registry answers (which, at boot, is "a few seconds after the
//      backend job comes up"). Registering supersedes any stale entry the
//      registry still holds for the same app.
//   3. Release on shutdown is best-effort and time-bounded: a registry
//      that's already down (or hung) at shutdown must never stall or crash
//      the exit. Its stale entry is superseded on the next start (rule 2).

import fs from 'node:fs';
import path from 'node:path';
import { logger } from './logger.js';

export const LOCAL_AGENT = 'vortexia';
export const DEFAULT_RANGE = { start: 9000, end: 9999 };

// Read lazily so a test can set process.env.LAS_REGISTRY_URL before the
// broker starts, regardless of ESM import order.
export function registryUrl() {
  return process.env.LAS_REGISTRY_URL || 'http://localhost:8700';
}

function withTimeout(ms) {
  return AbortSignal.timeout(ms);
}

// ── sticky ports ──────────────────────────────────────────────────────────

export function readStickyPorts(file) {
  try {
    const data = JSON.parse(fs.readFileSync(file, 'utf8'));
    return data && typeof data === 'object' ? data : {};
  } catch {
    return {};
  }
}

export function writeStickyPorts(file, ports) {
  try {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const tmp = `${file}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify({ ...ports, updatedAt: new Date().toISOString() }, null, 2));
    fs.renameSync(tmp, file);
  } catch (err) {
    logger.warn(`[vortexia] could not persist sticky ports to ${file}: ${err.message}`);
  }
}

// ── registry calls ────────────────────────────────────────────────────────

/**
 * Ask the registry for a port. With `port`, it's a pinned claim (the
 * registry answers 409 if that port is registered to someone else or
 * something is already listening on it); without, a claim from `range`.
 *
 * Resolves `{ port }` on success, `{ conflict: true }` on 409 (the port is
 * taken — pick another), or `{ unreachable: true }` when the registry can't
 * be reached or fails — in which case the caller decides without it.
 */
export async function claimPort(app, { port, range = DEFAULT_RANGE, timeoutMs = 2000 } = {}) {
  const body = { app, local_agent: LOCAL_AGENT, path: process.cwd() };
  if (port != null) body.port = port;
  else { body.start = range.start; body.end = range.end; }
  try {
    const res = await fetch(`${registryUrl()}/ports/claim`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal: withTimeout(timeoutMs),
    });
    if (res.status === 409) return { conflict: true };
    if (!res.ok) {
      logger.warn(`[vortexia] port registry refused claim for ${app}: HTTP ${res.status}`);
      return { unreachable: true };
    }
    const data = await res.json();
    return { port: data.port };
  } catch (err) {
    return { unreachable: true, error: err.message };
  }
}

/**
 * Tell the registry that `app` is bound to `port` — used AFTER binding, so
 * the plain register endpoint is the right one (a pinned /ports/claim would
 * 409 on its own liveness probe, since we're the ones listening). Any other
 * entry the registry still holds for this app (a stale claim from a run
 * that couldn't release, or a different port from before) is removed
 * first, mirroring the registry's own supersede rule.
 *
 * Returns true when the registry now holds the entry, false otherwise.
 */
export async function registerBoundPort(app, port, { timeoutMs = 2000 } = {}) {
  const base = registryUrl();
  try {
    const listRes = await fetch(`${base}/ports`, { signal: withTimeout(timeoutMs) });
    if (!listRes.ok) return false;
    const ports = await listRes.json();
    for (const [key, info] of Object.entries(ports || {})) {
      const stale = info?.app === app && info?.local_agent === LOCAL_AGENT && Number(key) !== port;
      if (!stale) continue;
      await fetch(`${base}/ports/${key}`, { method: 'DELETE', signal: withTimeout(timeoutMs) }).catch(() => {});
      logger.info(`[vortexia] superseded stale registry entry ${app}@${key} (now on ${port})`);
    }
    const existing = ports?.[String(port)];
    if (existing && (existing.app !== app || existing.local_agent !== LOCAL_AGENT)) {
      logger.warn(`[vortexia] registry had port ${port} recorded for ${existing.app}/${existing.local_agent} — overriding, since this process is what's actually listening on it`);
    }
    const res = await fetch(`${base}/ports`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ port, app, local_agent: LOCAL_AGENT, path: process.cwd() }),
      signal: withTimeout(timeoutMs),
    });
    return res.ok;
  } catch {
    return false;
  }
}

/**
 * Release a port from the registry. Best-effort and time-bounded: a
 * registry that is down or hung at shutdown gets one warning line, never a
 * hang or a throw. The stale entry it leaves behind is superseded by the
 * next start's registerBoundPort().
 *
 * The registry's file store has a naive read-modify-write on delete, so a
 * DELETE can lose a race against another agent's concurrent write — retry
 * a couple of times on a non-404 error status.
 */
export async function releasePort(port, { retries = 2, delayMs = 150, timeoutMs = 1500 } = {}) {
  if (port == null) return false;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await fetch(`${registryUrl()}/ports/${port}`, { method: 'DELETE', signal: withTimeout(timeoutMs) });
      if (res.ok || res.status === 404) return true;
      logger.warn(`[vortexia] failed to release port ${port}: HTTP ${res.status}`);
    } catch (err) {
      logger.warn(`[vortexia] port registry unreachable while releasing port ${port} (${err.message}) — its entry will be superseded on the next start`);
      return false;
    }
    if (attempt < retries) await new Promise((resolve) => setTimeout(resolve, delayMs));
  }
  return false;
}

// ── background reconciliation ─────────────────────────────────────────────

/**
 * Keep trying registerBoundPort(app, port) with exponential backoff (with
 * jitter, capped at `maxDelayMs`) until it succeeds or stop() is called.
 * This is what closes the boot race: the broker binds immediately, and the
 * registry learns the real port as soon as it's up, without the broker
 * ever having depended on it.
 *
 * Returns { done: Promise<boolean>, stop() }.
 */
export function reconcileRegistry(app, port, { initialDelayMs = 1000, maxDelayMs = 30000, maxAttempts = Infinity } = {}) {
  let stopped = false;
  let timer = null;
  let wake = null;

  const done = (async () => {
    let delay = initialDelayMs;
    for (let attempt = 1; !stopped && attempt <= maxAttempts; attempt++) {
      if (await registerBoundPort(app, port)) {
        logger.info(`[vortexia] registered ${app} -> port ${port} with the port registry${attempt > 1 ? ` (after ${attempt} attempts)` : ''}`);
        return true;
      }
      if (attempt === 1) {
        logger.warn(`[vortexia] port registry unreachable — ${app} is listening on ${port}; will keep retrying registration in the background`);
      }
      const jitter = delay * (0.8 + Math.random() * 0.4);
      await new Promise((resolve) => {
        wake = resolve;
        timer = setTimeout(resolve, jitter);
      });
      timer = null;
      wake = null;
      delay = Math.min(delay * 2, maxDelayMs);
    }
    return false;
  })();

  return {
    done,
    stop() {
      stopped = true;
      if (timer) clearTimeout(timer);
      if (wake) wake();
    },
  };
}

// ── binding ───────────────────────────────────────────────────────────────

function listenOnce(server, port) {
  return new Promise((resolve, reject) => {
    const onError = (err) => {
      server.removeListener('listening', onListening);
      reject(err);
    };
    const onListening = () => {
      server.removeListener('error', onError);
      resolve();
    };
    server.once('error', onError);
    server.once('listening', onListening);
    server.listen(port);
  });
}

/**
 * Decide and bind one listener. `server` is a not-yet-listening net/http
 * server; the port it ends up on is the return value. See the module doc
 * for the precedence. `exclude` lists ports already taken by this process
 * (the other listener), so a local scan never hands out the same port twice.
 *
 * Returns { port, claimed } — `claimed` is true when the registry already
 * holds the entry (a successful claim), false when it still has to be told
 * (pin, sticky-with-registry-down, or local scan) via reconcileRegistry().
 */
export async function bindWithPolicy(server, app, { pinned, sticky, range = DEFAULT_RANGE, exclude = [] } = {}) {
  const excluded = new Set(exclude);

  // 1. Explicit pin: bind it or fail loudly. Silently moving off a port the
  //    operator configured would defeat the point of configuring it.
  if (pinned != null) {
    await listenOnce(server, pinned);
    return { port: pinned, claimed: false };
  }

  // 2. Sticky: the port from last time. Ask the registry to re-claim it
  //    (supersedes our own stale entry; 409 means someone else has it now).
  if (sticky != null && !excluded.has(sticky)) {
    const claim = await claimPort(app, { port: sticky });
    if (!claim.conflict) {
      try {
        await listenOnce(server, sticky);
        if (claim.unreachable) {
          logger.info(`[vortexia] port registry unreachable — re-using last bound port ${sticky} for ${app}`);
        }
        return { port: sticky, claimed: !claim.unreachable };
      } catch (err) {
        if (err.code !== 'EADDRINUSE') throw err;
        logger.warn(`[vortexia] last bound port ${sticky} for ${app} is in use by something else — picking another`);
        excluded.add(sticky);
      }
    } else {
      logger.warn(`[vortexia] registry says last bound port ${sticky} for ${app} is now taken — picking another`);
      excluded.add(sticky);
    }
  }

  // 3. Fresh claim from the registry, if it's up.
  for (let attempt = 0; attempt < 3; attempt++) {
    const claim = await claimPort(app, { range });
    if (claim.unreachable || claim.conflict) break;
    if (excluded.has(claim.port)) continue;
    try {
      await listenOnce(server, claim.port);
      return { port: claim.port, claimed: true };
    } catch (err) {
      if (err.code !== 'EADDRINUSE') throw err;
      excluded.add(claim.port);
    }
  }

  // 4. Registry unavailable (or kept handing out busy ports): probe locally.
  for (let port = range.start; port <= range.end; port++) {
    if (excluded.has(port)) continue;
    try {
      await listenOnce(server, port);
      return { port, claimed: false };
    } catch (err) {
      if (err.code !== 'EADDRINUSE') throw err;
    }
  }
  throw new Error(`no free port for ${app} in ${range.start}-${range.end}`);
}
