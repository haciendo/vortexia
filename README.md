# vortexia

A small local MQTT broker/server used for inter-agent communication in the
**Local Agent Society** system. It gives `las-agent` clients (and, later, an
Electron widget) a working local message queue: register a name, send a
direct message to another agent, broadcast to everyone, and see who's
currently online.

Scope is deliberately small for now — just the broker, a JS client, a
Python client, and a CLI. No auth, no persistence beyond what the broker
gives for free, no routing logic. It will grow later.

See [PROTOCOL.md](./PROTOCOL.md) for the exact topic schema and message
format.

## Install

```bash
npm install
```

## Run

```bash
npm start          # or: node src/index.js start
npm run status      # or: node src/index.js status
npm stop           # or: node src/index.js stop
```

`vortexia start` runs in the foreground and writes `vortexia.pid` +
`vortexia.port.json` in the project root. Background it yourself
(`vortexia start &`) or send it a signal / run `vortexia stop`, which reads
the pidfile and sends `SIGTERM`.

On startup vortexia claims two ports from the local-agent-society port
registry (`http://localhost:8700`):

- `vortexia-mqtt` — TCP MQTT listener, for `las`/Python clients (paho-mqtt)
- `vortexia-ws` — WebSocket MQTT listener, for an Electron renderer using
  `mqtt.js` directly (no native deps)

If the registry is unreachable (e.g. the local-agent-society backend isn't
running), vortexia logs a warning and falls back to ports 1883/8883 rather
than failing to start — the registry is a nice-to-have, not a hard
dependency.

`vortexia.port.json` records whichever ports actually got used, so clients
that can't reach the registry can still find the broker locally.

On shutdown (`SIGINT`/`SIGTERM`), vortexia closes the broker cleanly and
releases both claimed ports back to the registry.

## Using it from JS/Node

```js
import { VortexiaClient } from 'vortexia/src/client.js';

const client = new VortexiaClient(); // defaults: localhost, port from vortexia.port.json or the registry
await client.register('MyAgent');

client.on('message', (envelope, topic) => {
  console.log(envelope.from, '->', envelope.text);
});

client.send('OtherAgent', 'hello there');
```

## Using it from Python

Requires `paho-mqtt` (not a hard dependency of this repo — install it
yourself if you need the Python client):

```bash
pip install paho-mqtt
```

```python
from vortexia_client import VortexiaClient, poll_inbox

client = VortexiaClient(host="localhost", port=1883)
client.register("MyAgent")
client.send("OtherAgent", "hello there")
client.close()

# One-shot CLI usage: drain whatever's waiting in your inbox and exit.
messages = poll_inbox("MyAgent", timeout=2.0)
for m in messages:
    print(m["from"], "->", m["text"])
```

## Topic schema (summary)

- `las/agent/<name>/inbox` — direct message to one agent
- `las/broadcast` — message to all agents
- `las/agent/<name>/presence` — retained online/offline status (via LWT)

Full details, including the JSON message envelope, are in
[PROTOCOL.md](./PROTOCOL.md).

## Scope-ladder queries

First piece of `docs/future-las-agent-scope-router.md`'s router work: any
agent can ask another for a description of itself at a given detail level
(`VortexiaClient.requestScope(name, detail)`), and answer such queries about
itself (`client.onScopeQuery(handler)`). See PROTOCOL.md's "scope-query /
scope-reply" section for the message shape.

`scanScopes(dir)` (CLI: `vortexia scope scan <dir>`) reads a directory's
`.vxia-scope.<N>.md` ladder files plus README into an ordered list per the
convention in `docs/vxia-scope-ladder.md` — this is the piece a consumer's
`onScopeQuery` handler would use to answer queries about files on disk.

## Federation PoC

Two or more environments (possibly on different machines) can join into
one logical network with no shared broker, routing messages by matching
intent against agents' scope descriptors instead of a named recipient —
see [docs/federation-poc.md](./docs/federation-poc.md) for how to run it,
including across a real Mac/Windows pair over a shared GitHub Gist.
Requires Node ≥18 (uses the global `fetch`) — no native/platform-specific
dependencies, so the same `scripts/federation-demo.js` runs unmodified on
macOS, Linux, or Windows.

## Tests

```bash
npm test
```

Runs on Node's built-in test runner (`node --test`). Tests claim their own
ports from the registry (or fall back gracefully if it's unreachable) so
they never collide with a real running agent's app.

## Status

This is step 1 of a migration: vortexia standing on its own, not yet wired
into local-agent-society's backend or an Electron widget. That integration
is a later phase.
