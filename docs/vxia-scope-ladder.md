# The `.vxia-scope` ladder protocol

**Status: specified, not implemented.** No scanning code exists yet — this
is the canonical definition for whoever builds it (see
`future-las-agent-scope-router.md` in this same folder for the router work
that consumes it). This doc is intentionally independent of any one
consumer's internals — it does not assume `local-agent-society`,
`.las-agent.json`, or any particular project layout. Any project that wants
its agent/service discoverable and routable by vortexia can adopt it.

## Why this lives in vortexia, not in a consumer project

The first adopter of this convention is `local-agent-society` (see its
`docs/adr/0001-las-agent-scope-ladder.md`), but the convention itself —
the file names, the Fibonacci length rule, how a README slots in, how
collisions resolve — is vortexia's protocol: vortexia is the thing that
will eventually scan for these files, embed them, and route messages based
on them. A naming convention owned by the router belongs with the router,
not with whichever project happened to ask for it first. Consumers (like
`local-agent-society`) adopt this doc; they don't each define their own
variant of it.

## The ladder

Any directory that wants a discoverable, progressively-detailed
self-description publishes some subset of:

```
.vxia-scope.<N>.md   — one file per rung, N = a Fibonacci number
README.md            — optional; never renamed, see below
```

`<N>` is both the rung's identity and its soft length-cap target, in
characters — `.vxia-scope.144.md`, `.vxia-scope.233.md`, `.vxia-scope.377.md`,
and so on, each cap being the next number in the Fibonacci sequence. There
is no fixed top rung; a directory publishes as many or as few as it wants.
All caps are **soft targets** (a writing goal, never a hard truncation
limit) — a consumer reading these files should never cut content to fit.

A consumer project may also feed its OWN small, pre-existing identity
fields (a name, a one-line description, whatever it already has) into the
bottom of this same ladder, at whatever rung its length matches — see
`local-agent-society/docs/adr/0001-las-agent-scope-ladder.md` for how LAS
does this with `.las-agent.json`'s `name`/`short_description`/
`long_description`. That mapping is the consumer's own business; this doc
only defines the file-based rungs and the rules below, which apply
regardless of what (if anything) a consumer stacks under them.

## README.md is never renamed

`README.md` (or `README`, case-insensitively) keeps its own name — renaming
it would break every other tool that expects it there (GitHub, npm,
editors, humans). Instead, its rung is *computed*: measure its actual
character count and place it at the smallest Fibonacci rung ≥ that count. A
short README lands on an early rung; a long one lands on a later rung.
There's no dedicated "README rung" — it slots into the ladder wherever its
real size puts it, small or large.

## Rung collisions

A collision is two different files mapping to the same rung number — e.g.
an explicit `.vxia-scope.144.md` and a README whose real length also rounds
up to 144. Resolution:

1. **Warn.** Whatever scans the ladder prints a warning naming the rung and
   the competing files.
2. **First-in-scan-order wins**, deterministically: any consumer-specific
   inline rungs first (in their own defined order), then `.vxia-scope.*.md`
   files by ascending `<N>`, then `README.md` computed last. An explicit
   `.vxia-scope.<N>.md` always beats a README that happens to land on the
   same rung by coincidence of length.
3. Nothing is deleted or modified. The loser just isn't returned when that
   rung is requested.

## Future: `scanScopes()` and routing

Not implemented. Sketch, for whoever builds it:

- A function (working name `scanScopes(dir)`, exported from vortexia's JS
  client — CLI equivalent `vortexia scope scan <dir>`) that reads a
  directory, finds every `.vxia-scope.<N>.md` plus any README, computes the
  README's rung, applies the collision rule above, and returns an ordered
  list of `{ rung, source, text }` entries — one per resolved rung.
- A vortexia router publishes each known agent's scanned ladder (at least
  its first rung or two) so it can answer "who handles X" queries, and,
  per-agent, answer "give me your rung-N text" / "a bit more" requests by
  walking to the next rung — see `future-las-agent-scope-router.md`,
  section 1, for the message shape.
- **Progressive-depth duplicate/overlap matching**: when comparing two
  agents' ladders for overlap (embedding distance — see
  `future-las-agent-scope-router.md`, section 2), don't only compare a
  single fixed rung. Start shallow (rung 1, cheap to embed) — if the match
  is ambiguous or suspiciously close, go one rung deeper on both sides and
  compare again, repeating until either a rung fails to match closely
  enough (not a duplicate) or both sides run out of rungs to compare
  (likely a genuine duplicate). The rung depth reached before ruling
  in/out is itself useful signal: "matched through rung N" is a confidence
  level, not just a yes/no.
- The same progressive-depth idea applies to **message routing without a
  named recipient**: a message that says "I need help with X" (no target
  agent) gets compared against known agents' ladders at increasing rung
  depth until one agent's scope matches closely enough to route to
  confidently, rather than matching only on a shallow (and thus
  ambiguous) rung-1 summary.
