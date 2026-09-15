# vortex-relay PoC: joining two (or more) vortexia queues over the internet

Proof of concept for docs/future-las-agent-scope-router.md sections 1b and
4: routing a message with no named recipient, by matching it against known
agents' scope descriptors, across environments that don't share a broker —
possibly on different machines, connected by nothing but a small shared
mailbox on the internet.

## Why not GitHub Pages as the connection point

A static site (GitHub Pages included) can't accept incoming writes or hold
a persistent connection — it can only serve fixed files. So it can't be the
"meeting point" between two garantes. What it *can* be is a read-only
viewer of whatever the real meeting point holds. For this PoC, the real
meeting point is a **GitHub Gist**: free, stable, no server to run or
maintain, and reads (the hot path — every environment polls it) are
unauthenticated and CDN-served.

## What's implemented

- `src/vortex-relay/embeddings.js` — text embeddings, two interchangeable
  backends: real ones via a local Ollama (`all-minilm`, 384-dim) when
  reachable, a zero-dependency bag-of-words fallback otherwise. Every
  environment picks whichever it has; they don't need to agree.
- `src/vortex-relay/router.js` — `pickTargets(intent, directory)`: compares
  an intent against every known agent's scope descriptor (regardless of
  which environment it's in) and returns whichever one(s) are close enough
  to the best match — one match when there's a clear winner, several when
  more than one is a close semantic fit.
- `src/vortex-relay/relay.js` — the shared mailbox abstraction. `InMemoryRelay`
  (used by the automated tests, `test/vortex-relay-bridge.test.js` — no
  network involved) and `GistRelay` (the real one, for this doc).
- `src/vortex-relay/bridge.js` — `VortexRelayBridge`: per environment, listens
  for local "vortex-relay intent" broadcasts, routes them via `pickTargets`,
  delivers locally or writes into the target environment's file on the
  relay, and polls its own file for what other environments routed to it.

The automated tests already prove the core claim end-to-end with three real,
separate MQTT brokers joined only through an in-memory relay (standing in
for "the internet") — see `test/vortex-relay-bridge.test.js`. This doc is for
running the same thing for real, over an actual Gist, optionally across two
actual machines.

## Setup

1. **Create a Gist.** Any gist.github.com gist, public or secret — content
   doesn't matter, it gets overwritten. Note its id (the hex string in the
   URL).
2. **Create a token scoped ONLY to `gist`.** GitHub → Settings → Developer
   settings → Fine-grained tokens (or classic, with just the `gist` scope
   checked). Never use a broader token here — this token is going into a
   plain env var on whatever machine runs the demo.
3. Export both:
   ```bash
   export VORTEXIA_GIST_ID=<the gist id>
   export VORTEXIA_GIST_TOKEN=<the token>
   ```

## Running it

One process per simulated (or real) environment. On one machine, three
terminals:

```bash
node scripts/vortex-relay-demo.js env-a Clima "pronostico del tiempo, temperatura, lluvia, viento"
node scripts/vortex-relay-demo.js env-b Facturas "facturacion, factura, pagos, pagar, cobros, dinero"
node scripts/vortex-relay-demo.js env-c Meteo "clima, tiempo, temperatura, humedad, viento, pronostico"
```

Each prints the agents it now knows about (itself plus whoever already
joined the shared directory). In env-a's terminal, type:

```
pronostico del tiempo y viento para mañana
```

Expect: env-a's own Clima gets it immediately (local match), and — a
second or two later, once env-c's bridge polls the relay — env-c's Meteo
terminal prints the same message with `routed via env-a`. Facturas' terminal
stays silent. Type a billing intent instead and only Facturas should react,
regardless of which terminal you typed it in.

**Across two real machines**: same commands, just run some of them on the
other machine, with the same `VORTEXIA_GIST_ID`/`VORTEXIA_GIST_TOKEN`
exported there too. There is no other coordination needed — the Gist is
the only thing they share.

## Known limitations (fine for a PoC, not for production)

- `directory.json` (who exists, where) is last-writer-wins with no
  concurrency control — several agents joining at the exact same moment
  could clobber each other's entry. Outbox files don't have this problem
  (each environment is the only writer to its own).
- Polling, not push — delivery latency is bounded by each bridge's poll
  interval (1s in the script), plus the Gist API/CDN's own propagation
  delay.
- No authentication of *messages* — anyone with the gist id can read
  everything in it, and anyone with write access to the gist (i.e. holding
  a token with `gist` scope for it) can inject arbitrary messages. The
  trust/signing layer discussed for real cross-machine vortex-relay
  (docs/future-las-agent-scope-router.md section 4) is intentionally not
  built here.
