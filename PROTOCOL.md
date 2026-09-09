# vortexia protocol

vortexia is a plain MQTT broker (aedes) with a small, fixed topic schema.
Nothing in the protocol below is aedes- or vortexia-specific — any MQTT
client (paho-mqtt, mqtt.js, mosquitto_pub, etc.) can speak it.

## Topics

| Topic | Purpose | QoS | Retained |
|---|---|---|---|
| `las/agent/<name>/inbox` | Direct message to one agent | 1 | no |
| `las/broadcast` | Message to all agents | 1 | no |
| `las/agent/<name>/presence` | Online/offline status for `<name>` | 1 | **yes** |

`<name>` is an agent name, matching the `name` field in that agent's
`.agent.json` (see local-agent-society's `CLAUDE.md`).

## Message envelope

Every message published to an `inbox` or `broadcast` topic is a JSON object:

```json
{
  "from": "SenderAgent",
  "to": "ReceiverAgent",
  "source": "agent",
  "text": "hello",
  "ts": 1787958159823
}
```

- `from` — sender agent name (string)
- `to` — recipient agent name, or `"broadcast"` for broadcast messages (string)
- `source` — one of `"agent"`, `"human"`, `"system"` — who/what originated the message
- `text` — message body (string)
- `ts` — epoch milliseconds (number)

## Presence

Presence is **not** a JSON envelope — it's a plain retained payload of
either the literal string `"online"` or `"offline"` on
`las/agent/<name>/presence`.

- On `register(name)`, a client sets a Last Will and Testament (LWT) of
  `"offline"` (QoS 1, retained) on its own presence topic, then immediately
  publishes `"online"` (QoS 1, retained).
- On a clean disconnect, the client should publish `"offline"` itself
  before disconnecting (the LWT is a safety net for unexpected drops, not
  the primary mechanism).
- Because the payload is retained, any client can subscribe to
  `las/agent/+/presence` and immediately learn who is currently online
  without waiting for a fresh publish.

## Delivery notes

- QoS 1 is used throughout (at-least-once). Consumers should be prepared
  for the rare duplicate.
- `inbox` messages **are retained** (`retain: true` on publish). A client
  that isn't subscribed at publish time still gets the message the next
  time it subscribes (a live viewer registering via `VortexiaClient.register`)
  or polls (`poll_inbox()`). This is a **single-slot mailbox per recipient,
  not a queue**: MQTT retain only keeps the *latest* value per topic, so a
  second inject before the first is consumed overwrites it — there is no
  backlog of multiple unread messages. `poll_inbox()` is the "real" consumer:
  once it collects a retained message, it clears the retained flag (an empty
  retained publish) so the same message isn't handed out again on a later
  poll. A live subscriber via `register()` (e.g. the Electron widget)
  deliberately does NOT clear it — it just displays what arrives, leaving the
  retained copy for `poll_inbox()` to actually consume later.
- `broadcast` messages are **not** retained — a client that isn't subscribed
  at publish time misses them, and that's intentional: replaying the single
  last broadcast to every new subscriber forever doesn't make sense for a
  fan-out channel the way it does for a per-recipient mailbox.
- If a real multi-message backlog per recipient is needed later (this
  version only keeps the latest one), that's a job for a future phase (e.g.
  a small store-and-forward queue) — not in scope for this version of
  vortexia.

## Connecting

- TCP MQTT listener: for `las`/Python (`paho-mqtt`) clients and anything
  else that isn't a browser/Electron renderer.
- WebSocket MQTT listener: for an Electron renderer connecting directly
  with `mqtt.js`, no native dependencies required.

Both ports are claimed dynamically from the local-agent-society port
registry (`http://localhost:8700`) under app names `vortexia-mqtt` and
`vortexia-ws` — see README.md for how to discover the current ports.

## Extension: `kind` field and `las/speak` (added by local-agent-society)

local-agent-society's backend (`POST /queue/speak`) publishes TTS requests
over vortexia instead of shelling out to macOS `say`, so a cross-platform
Electron widget can pick them up and actually speak them (Web Speech API).
This adds one optional field to the envelope and one new topic — both are
backward compatible with the base protocol above (an envelope without
`kind` is implicitly `kind: "message"`, i.e. a normal chat message).

| Topic | Purpose | QoS | Retained |
|---|---|---|---|
| `las/speak` | TTS request — an agent's widget should actually say this aloud | 1 | no |

Envelope (same shape as above, plus `kind`, `voice`):

```json
{
  "from": "queue",
  "to": "AgentName",
  "source": "system",
  "kind": "speak",
  "text": "hello, task complete",
  "voice": "Samantha",
  "ts": 1787958159823
}
```

- `kind` — `"message"` (default, omittable) for normal chat, `"speak"` for
  a TTS request. Other kinds may be added later; unknown consumers should
  ignore envelopes whose `kind` they don't recognize rather than erroring.
- `voice` — (speak only) the macOS/TTS voice name to use, matching the
  `voice` field in the target agent's `.agent.json`.
- Published to `las/speak` (not the agent's own inbox) since any number of
  widgets could in principle be listening; a widget should filter on `to`
  matching its own agent name before speaking. Not retained, same
  at-least-once-while-connected semantics as `inbox`/`broadcast` above.

As of this writing, nothing consumes `las/speak` yet — local-agent-society
publishes to it, but the Electron widget that would actually speak it is a
separate, not-yet-built piece of work.

## Extension: scope-ladder query (`scope-query` / `scope-reply`)

Implements section 1 of `docs/future-las-agent-scope-router.md` — asking an
agent for a description of itself at a given detail level, per the
`.vxia-scope` ladder protocol (`docs/vxia-scope-ladder.md`). Sent over the
target's own inbox topic, not a dedicated topic — `kind` distinguishes it
from a normal chat envelope.

Query:

```json
{
  "from": "SomeRouter",
  "to": "TargetAgent",
  "source": "agent",
  "kind": "scope-query",
  "detail": "short",
  "queryId": "b3f1...",
  "ts": 1787958159823
}
```

Reply (published to the *requester's* inbox):

```json
{
  "from": "TargetAgent",
  "to": "SomeRouter",
  "source": "agent",
  "kind": "scope-reply",
  "queryId": "b3f1...",
  "rung": 55,
  "scopeSource": "short_description",
  "text": "a small agent",
  "ts": 1787958159824
}
```

- `detail` — `"short"` | `"more"` | `"full"` | `{"maxChars": N}`. Meaning is
  entirely up to the target's own handler (`VortexiaClient.onScopeQuery`) —
  vortexia only carries the request/reply, it does not interpret `detail`
  or walk any ladder itself.
- `queryId` — generated per call by `VortexiaClient.requestScope()` (a
  UUID) and echoed back verbatim in the reply. This is what lets two
  concurrent queries to the same agent, or a reply that arrives after its
  query already timed out, each resolve the *correct* pending promise
  instead of the first matching reply satisfying whichever `requestScope()`
  call happened to still be listening.
- `rung` / `scopeSource` — which ladder rung the reply came from and where
  that rung's text came from (e.g. a `.las-agent.json` field name or a
  `.vxia-scope.<N>.md` filename), so the requester can ask for "a bit more"
  next time and expect the *next* rung, not the same one again.
- Neither message is retained — this is a request/reply exchange, not a
  mailbox. `VortexiaClient.requestScope()` rejects with a timeout error if
  the target never replies (e.g. it has no `onScopeQuery` handler
  registered).
- `scanScopes(dir)` (in `src/scope.js`, CLI: `vortexia scope scan <dir>`)
  reads a directory's `.vxia-scope.<N>.md` files plus README into an
  ordered ladder — this is the file-based half of the ladder; combining it
  with a consumer's own pre-existing fields (like local-agent-society's
  `.las-agent.json`) to answer an `onScopeQuery` call is that consumer's own
  business, not vortexia's.
