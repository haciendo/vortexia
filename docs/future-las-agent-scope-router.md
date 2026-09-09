# Future work: scope-ladder-aware routing in vortexia

**Status: not started. No agent is assigned to vortexia yet.** This file is
notes-to-future-self/whoever picks this up, written from `local-agent-society`
while designing the scope ladder these ideas depend on. Nothing here is
implemented; nothing here should block vortexia's current scope (see this
repo's own README: "just the broker, a JS client, a Python client, and a
CLI... no routing logic. It will grow later." This is the "later.")

## Prerequisite: the `.vxia-scope` ladder protocol

The file-based ladder mechanics (the `.vxia-scope.<N>.md` naming, the
Fibonacci length rule, how a README slots in, how collisions resolve) are
specified in **`vxia-scope-ladder.md`**, right next to this file — that's
the vortexia-owned protocol, independent of any one consumer. Read it first.

`local-agent-society` is the first adopter: it feeds its own pre-existing
`.las-agent.json` fields into the bottom of that same ladder (see its
`docs/adr/0001-las-agent-scope-ladder.md`):

```
name (34 chars, mandatory, .las-agent.json)
  -> short_description (55, .las-agent.json)
  -> long_description (89, .las-agent.json)
  -> .vxia-scope.144.md
  -> .vxia-scope.233.md
  -> .vxia-scope.377.md
  -> ... (next Fibonacci number each rung)
  -> README.md (wherever its real length lands it on the ladder)
```

That LAS-specific mapping (which JSON fields feed which rung) is
`local-agent-society`'s own business; everything about the `.vxia-scope.*`
files themselves and the rules around them is vortexia's protocol, so any
other project can adopt it the same way without depending on LAS at all.

Everything below is future vortexia work that *consumes* this ladder.

## 1. A query protocol for "give me a summary of size X"

The core idea: a message on vortexia asking an agent (or asking vortexia
*about* an agent, if vortexia caches the ladder) for a description at a
given level of detail — "chico", "un poco más", "detallado", or a target
size — should walk the ladder and return the smallest rung that satisfies
the request, not force the asker to know exact Fibonacci numbers.

Sketch of an inbox message kind (naming/shape not final):

```json
{
  "kind": "scope-query",
  "from": "SomeRouter",
  "to": "TargetAgent",
  "detail": "short" // "short" | "more" | "full" | {"maxChars": N}
}
```

and a reply carrying whichever rung's text matched, plus which rung it was
(so the asker can request "a bit more" next and get the *next* rung, not the
same one again).

This is naturally incremental: "a bit more" than rung N is just rung N+1.
That conversational pattern (ask small, escalate on demand) is the whole
point of the ladder being ordered and Fibonacci-spaced rather than a single
blob.

### 1b. Routing a message with no named recipient

The longer-term point of all this: a message shouldn't have to name a
specific agent to reach the right one. Someone (a person or another agent)
publishes "necesito ayuda con X" with no `to`, and the router compares X
against every known agent's scope ladder using the same progressive-depth
matching described in section 2 — going one rung deeper wherever the match
is still ambiguous, until it either finds a confident match or exhausts the
ladder — and only then delivers to whichever agent's scope actually matched
at whatever depth ("grado N," in rung terms) was needed to be sure. This is
what makes the ladder worth building at all, not just a docs nicety: it's
the thing that lets a message find its destination without either side
knowing the other's exact name in advance, and without landing on the wrong
agent because two agents looked alike at a shallow rung.

## 2. Duplicate / overlapping scope detection via embeddings

Once agents have real scope text (not just names), it becomes possible to
embed each agent's scope ladder and compare agents pairwise by embedding
distance — but not at one fixed rung. Use **progressive-depth matching**
(see `vxia-scope-ladder.md`'s "Future: scanScopes() and routing" section):
start shallow (rung 1, cheap to embed); if two agents match closely there,
go one rung deeper on both sides and compare again, repeating until either
a rung fails to match closely enough (not a duplicate — they only look
similar at a shallow summary level) or both run out of rungs to compare
(a genuine duplicate). The depth reached is itself a confidence signal, not
just a yes/no.

Two agents that turn out to be near-duplicates are either:

- genuinely redundant (same job, should probably be merged or one retired), or
- legitimately overlapping (e.g. two agents that both touch "billing" from
  different angles) and worth flagging so a human notices, not necessarily
  merging.

This is a nice-to-have diagnostic (`las agents check-overlap` or similar,
someday), not a blocker for the query protocol in section 1. It needs real
scope text populated across several agents before it's worth building —
right now every agent's `short_description`/`long_description` is empty.

## 3. Ports as another identity facet

An agent's claimed ports (from `local-agent-society`'s `/ports` registry)
are, in a sense, part of "what this agent is" — a queryable fact about it,
same category as its scope ladder. Idea raised but not designed: could a
vortexia query ask "who holds port 5173" and get routed to that agent via
the same mechanism as a scope query, rather than only via the HTTP registry?
Worth exploring once section 1's query mechanism exists, since it'd reuse
the same request/reply shape — but ports are already served by
`local-agent-society`'s backend (`GET /ports`), so this is about exposing an
*additional* path to the same fact over vortexia, not a new source of
truth. Don't duplicate the registry; decide whether vortexia should proxy it
or leave it to the HTTP API.

## 4. Cross-machine queue federation (star topology)

Longer-term: connect this machine's vortexia broker to another machine's,
so agents on different machines can message each other — a star topology
(each machine's local broker peers with a hub, or with each other directly)
rather than one giant shared broker. Not designed yet beyond the shape of
the idea. This is squarely vortexia's own scope (per its README, "not yet
wired into local-agent-society's backend... that integration is a later
phase") — the scope-ladder query protocol in section 1 should probably be
designed to work transparently across that federation once it exists (i.e.
a scope-query shouldn't care whether the target agent is local or on another
machine's broker), but federation itself is a separate, larger piece of work
than anything else in this file.

## Suggested order, whenever someone picks this up

1. Get a handful of real agents to actually populate `short_description`/
   `long_description` (rungs 1–2) — there's no point designing a query
   protocol against fields that are all empty strings today.
2. Build the query protocol (section 1) against just those two rungs first;
   add `.vxia-scope.<N>.md` rung support (see `vxia-scope-ladder.md`) once
   something is actually using it.
3. Revisit sections 2–4 once 1 is real and in use.
