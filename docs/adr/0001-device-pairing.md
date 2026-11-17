# ADR 0001 — Device pairing ("friendship") and the device role in vortex-relay

- Status: **accepted** (2026-11-14, José via LocalAgentSociety)
- Asked by: LocalAgentSociety, on José's behalf — LAS mobile app (React Native / Expo, iPhone first, then Android)
- Owner: vortexia. LAS consumes the result; it does not implement pairing or device auth.

## Context

The LAS mobile app captures audio offline, queues it, and has it uploaded to and
transcribed on the Mac; it also shows a panel with the Mac's agents. It has to
work on the home LAN and away from home, and the design must not depend on
macOS, because LAS will also run on Windows.

What vortex-relay does today (checked in code, not from memory):

- **Identity / trust** — each environment has a Nostr secp256k1 keypair
  (`VORTEXIA_NOSTR_SECRET_KEY`), and peers are a static env var
  (`VORTEXIA_NOSTR_PEERS=env:pubkey,…`, `src/index.js`). A read only accepts
  events whose author is a known pubkey (`NostrRelay._trustedAuthors`).
  Pairing is done by hand: copy pubkeys between machines and restart.
- **Signed, not encrypted** — `NostrRelay` publishes kinds 7878 (log) and
  30078 (snapshot) with `content: JSON.stringify(message)`
  (`src/vortex-relay/relay.js`). Message text is **plaintext on public relays**
  (damus, nos.lol, nostr.band). Anyone can read it; only forging is prevented.
- **LAN direct has no authentication** — the broker's TCP and WS servers listen
  on every interface (`listenOnce(server, port)`, no host) and aedes has no
  `authenticate` hook. Anyone on the same Wi-Fi can publish into any mailbox.
  LAN discovery is mDNS (`bonjour-service`, works on Windows too).

Both gaps are already there between Macs, and an app on a phone that moves
between networks makes them worse.

## Decision

### 1. One identity per endpoint; a phone is a **device**, not an environment

Every endpoint has one long-lived keypair, and its public key is its identity.
There are two roles:

| Role | Runs a broker | Advertises mDNS | Relays for others | Example |
|---|---|---|---|---|
| `env` | yes | yes | yes | `ba-mac`, `uy-mac`, a Windows PC |
| `device` | no | no | no | `jose-iphone` |

A device is a leaf: it has an `envName` (e.g. `jose-iphone`) so that
`Name@jose-iphone` stays addressable, and it has one or more **home** envs, the
envs it is paired with. A home routes traffic to the device and holds the
device's mailbox while it is offline (the broker mailbox already does this:
500 messages, 30 days). A device never routes for anyone else. It can only reach
agents in its home envs, plus the agents in their directories through the home,
which forwards them as it already does.

**Scope for now (José, 2026-11-14):** the phone is a *viewer of one computer*.
Its panel lists that computer's agents, and selecting one shows its scope, its
friends and its open terminals. That content is LAS's (agents, sessions,
friends). vortexia only carries it, so LAS defines the panel's messages and
vortexia stays out of their semantics. The `device` role is a starting point,
not a ceiling: nothing here prevents a later promotion of a device to a full
`env` with the same keypair and friendships.

The phone does **not** run vortex-relay's Node code. It implements the wire
format defined below, which keeps the app small and makes Android a re-port of
the same spec, not of the same code.

**Key type:** secp256k1 (Nostr's curve), so that one key signs on every
transport. The iOS Secure Enclave only does P-256, so the key **cannot live in
the Secure Enclave**. It goes in the Keychain (`expo-secure-store`,
`WHEN_UNLOCKED_THIS_DEVICE_ONLY`, no iCloud sync), and on Android in the
Keystore-backed EncryptedSharedPreferences. If it is ever needed, a later
version can bind it with an extra Secure Enclave P-256 key that wraps the
secp256k1 key at rest. That change would not alter the protocol.

On an `env`, the key stays in `vortexia.env` (or `data/identity.json`, mode 0600),
with no Keychain/DPAPI dependency, so it is identical on macOS, Linux and Windows.

### 2. Friends store replaces `VORTEXIA_NOSTR_PEERS`

`data/friends.json` (owned by vortexia, written atomically):

```json
{ "friends": [
  { "envName": "jose-iphone", "pubkey": "<hex>", "role": "device",
    "label": "iPhone de José", "addedAt": 1794700000000, "revokedAt": null,
    "lastSeen": { "transport": "lan", "at": 1794700100000 } }
] }
```

- `VORTEXIA_NOSTR_PEERS` is still read as a seed (with `role: env`), so existing
  pairings keep working. New pairings only go to the file.
- The relay and the broker read the store **live** (the same "re-read every
  tick" approach the roster already uses), so pairing and revoking need no
  restart.
- CLI: `vortexia friends list | pair [--device] | revoke <envName> | rename`.

### 3. Pairing protocol (QR, single-use token, mutual)

```
Mac (home env)                                     Phone (device)
──────────────                                     ──────────────
vortexia friends pair --device
  token = 128 random bits, TTL 10 min, single use
  shows QR:
  vxpair:1?env=ba-mac&pk=<hex>&t=<token b64url>
          &lan=192.168.1.20:9014,ba-mac.local:9014
          &ts=100.x.y.z:9014            (optional, Tailscale)
          &r=wss://relay.damus.io,wss://nos.lol
                                                   scan QR
                                                   generate keypair (first run only)
                                                   build PAIR_REQUEST {
                                                     token, deviceEnvName,
                                                     label, devicePubkey }
                                                   encrypt NIP-44 → Mac pk, sign
                                                   send via first reachable of:
                                                     lan, ts, relay
verify signature, decrypt
check token: exists, unexpired, unused → burn it
(optional) "Accept iPhone de José?" on Mac/widget
add friend {role: device}
PAIR_ACK {homeEnvName, homePubkey, endpoints}
  signed + encrypted to device pk  ───────────────▶  verify signer == pk from QR
                                                   store home friend
                                                   friendship is mutual
```

- The QR is the out-of-band channel. It carries the Mac's pubkey, so the phone
  can never be talked into pairing with an impostor, and the token proves to the
  Mac that the requester saw the screen. No SAS code is needed. A **leaked QR**
  only lets someone pair within 10 minutes and only once. The optional
  "Accept?" prompt closes that window too, and is on by default.
- Mac↔Mac pairing uses the same flow with `role: env` (the QR is shown on one
  Mac and scanned with the other's camera, or the `vxpair:` URI is pasted). It
  replaces copying pubkeys by hand.
- **Revocation** is local and immediate: `revokedAt` is set, the broker
  disconnects any live session with that key, and the relay ignores its
  events. A best-effort signed `UNPAIR` is sent so that the other side forgets
  the friendship too. Revoking never depends on the other side cooperating.
  Either side can revoke.

### 4. Every message is signed **and** end-to-end encrypted (NIP-44 v2)

One envelope for every transport. It is a Nostr event:

```
kind  7879 (device/peer message, regular, stored)
tags  [["p", <recipient pubkey>], ["d", <recipient envName>]]
content  nip44.encrypt(conversationKey(mySk, theirPk), JSON(message))
sig   schnorr over the event id
```

- On the **relay** it is published as-is. Relays and anyone else can see who
  talks to whom and when, but not what is said. (NIP-59 gift wrap would also
  hide the metadata. That is deferred, since it isn't needed yet.)
- On **LAN / Tailscale** the same event is published over MQTT-over-WebSocket to
  the home broker. The signed event is the authentication for every message, so
  plain `ws://` is acceptable: E2E encryption covers confidentiality and the
  signature covers integrity.
- **The Mac↔Mac traffic switches to this envelope as well**, which fixes the
  current plaintext-on-public-relays problem. Kinds 7878/30078 are read for one
  release for backward compatibility and are then dropped.
- `nostr-tools` already ships `nip44` (the existing dependency). On React Native
  the same library works with `react-native-get-random-values`.

### 5. Broker authentication (needed for both LAN and devices)

- Add an aedes `authenticate` hook (`src/auth.js`). A non-loopback connection
  must send `username = <pubkey hex>` and `password = <signed auth event>`
  (NIP-42-style: kind 22242, `created_at` within ±120 s, a `challenge` tag set
  to the broker's `envName`). The pubkey must belong to an active friend of
  role **env**, and each auth event is accepted only once: plain TCP can be
  sniffed, so the event can't be replayed within the window. The LAN direct
  transport signs a fresh event on every connect.
- **Devices do not use MQTT.** Their direct path is the HTTP gateway (§9), so
  a device key gets rejected on the broker.
- `vortexia/*` topics (control, paths) are loopback-only for publishing and
  subscribing.
- Loopback (local agents, LAS backend, widget) stays unauthenticated, as it is
  today.
- Bind policy: keep listening on all interfaces (LAN direct needs it).
- **Rollout without a flag day:** `VORTEXIA_BROKER_AUTH=warn` (the default)
  logs what would be rejected and lets it in. Once every peer runs a vortexia
  that signs in, set `enforce`. `off` disables the check.

### 6. Audio does not travel through the relay — only the notice

Public Nostr relays cap events at tens to a few hundred KB, and the limit
differs per relay. Minutes of m4a is ~0.5–1 MB/min. On top of that, a
store-and-forward log on volunteer relays is the wrong place for bulk data.

- The **phone queues clips locally** (the core of the app). A clip is uploaded
  **only over a direct path to a home**: LAN, or the Tailscale address if the
  user has it.
- The upload goes to vortexia's device gateway:
  `PUT /device/blob/<sha256>` on the home's WS port, with the same signed auth
  event as a header (`Authorization: Nostr <base64 event>`, NIP-98-style, with
  the `sha256` included in the signed tags). The body is the m4a, encrypted with
  a random key, and the key travels in a 7879 message: `{type: "blob",
  sha256, key, mime, durationMs, capturedAt}`. That message can go over the
  relay, so the Mac knows the clip exists even before the bytes arrive.
- vortexia writes the decrypted file to `data/device-inbox/<envName>/` and
  delivers a notice to the system mailbox `las-device-audio` (a
  deterministic consumer, not an agent session; see "What LAS has to do"):
  `{type: "device-audio", from: "jose-iphone", path, sha256, durationMs, capturedAt}`.
  LAS transcribes it from the local path.
- **Away from home without Tailscale** (amended 2026-11-17, see §9.1): a
  clip can travel through the relay after all, inline in the `blob` message
  (`data` = base64 of the encrypted body) and split into `part` events. This
  is meant for short dictations (seconds to a couple of minutes). Big clips
  should still wait for a direct path, because relays rate-limit bursts. The
  phone decides: direct path if there is one, relay otherwise.

### 7. Transports for a device, in cost order

1. **LAN** — the `lan=` endpoints from the QR, refreshed on every `PAIR_ACK`
   and kept current through the directory. Discovery is mDNS when the phone can
   do it (Expo needs `react-native-zeroconf`, or the cached IP is enough).
2. **Tailscale** — just another direct endpoint (`ts=` in the QR). It is **not**
   an identity or a dependency: the keys and the auth are ours, and Tailscale
   only provides reachability. That keeps it optional, and keeps it working on
   Windows.
3. **Nostr relay** — always available. It carries the whole device
   conversation (§9.1): sends, panel reads, replies, the outbox and short
   clips.

The phone keeps no live connection in the background (iOS doesn't allow it
anyway). It syncs on foreground and on background fetch. If push is wanted
later, it would be a separate decision (APNs/FCM through a home).

### 8. Panel data and sends: the device contract

The app's first screen is a panel with **every** agent on the home computer.
Tapping one opens that agent's panel (scope, friends, subordinates, sessions
and titles), and the same structure repeats at every level (fractal). Every
agent has its own mic; **there is no general mic**. On the phone each agent can
do what it can do on the computer: dictate, type, and see its sessions and
their titles. To do that the device needs three things, and each one goes over
the transport that already owns it:

| Need | Goes through | Owner of the semantics |
|---|---|---|
| **Read** the roster (agents, hierarchy, sessions, titles, colors, scope) | vortexia **authenticated proxy** to LAS `/device/*` | LAS |
| **Send** a typed message to an agent or one session | vortexia's **native mailboxes** (7879 envelope → normal delivery) | vortexia |
| **Dictate** to an agent or one session | blob gateway (§6), then LAS transcribes and delivers | LAS (transcription), vortexia (delivery) |

**Proxy (reads).** The device sends `{type: "http", id, method: "GET",
path: "/device/society/tree", ifNoneMatch?}` in a 7879 envelope. On LAN or
Tailscale it can also send it as a plain HTTP request to the home's WS port,
with the NIP-98 auth header. The home verifies the friend, and then:

- only proxies paths under **`/device/`** to `http://127.0.0.1:8700`. LAS
  decides what it exposes by what it mounts under that prefix, and vortexia
  never keeps a route list;
- removes any client-supplied identity headers and adds
  `X-Vortexia-Device: <envName>` and `X-Vortexia-Device-Label: <label>`.
  LAS can trust them because only vortexia reaches loopback with them;
- returns `{type: "http-response", id, status, headers: {etag, content-type},
  body}` encrypted to the device. Over the relay, a body over 48 KB
  (gzipped) is answered `413`: the device then waits for a direct path. LAS
  helps by keeping `/device/society/tree` small, and by supporting `ETag`,
  so that a `304` costs nothing.
- Only `GET` is proxied at first. Writes stay with the dedicated paths below.
  LAS can add more `/device/*` methods later, and vortexia only has to stop
  rejecting them.

**Sends.** A 7879 message `{type: "send", to: "<Agent>" | "<Agent>/<sid|runtime>",
text, kind: "message"|"command"}` is delivered by the home exactly like
`las agent send --to … --session …`. It arrives with `from: "<envName>"` (the
device), `origin: "human"` and a new attribute, `via: "device"`. A device can
only address agents of its home envs (and their `Name@env` friends through
the home, as usual).

**Dictation.** The blob message from §6 carries the target:
`{type: "blob", sha256, key, mime, durationMs, capturedAt, target: {agent,
session?}}`. The `device-audio` notice (system mailbox `las-device-audio`) includes
`target`. LAS transcribes the clip and delivers it to that agent or session as
a mic dictation (`origin: "human"`), so it renders the same as one from the
widget mic. There is no target-less clip, because there is no general mic.

**Why not everything through the proxy:** sends and session addressing are
vortexia's core, already cover offline queueing, and already work across envs.
Proxying them to LAS would just route back into vortexia. **Why not reads
through the mailboxes:** the roster is LAS's data model. A proxy keeps it in
one place (`/society/tree`, which LAS also wants for a future full view on the
Mac), and vortexia doesn't need to know its shape.

### 9. The device gateway's HTTP interface (direct paths)

On a direct path (LAN or Tailscale) the app speaks **plain HTTP** to the
home's vortexia WS port, which serves these routes beside MQTT-over-WS.
That is simpler in React Native than an MQTT client, and the same events can
go over Nostr when there is no direct path.

- **Base URL:** `http://<host>:<port>`, taken from the QR's `lan=` / `ts=`
  endpoints. No TLS: every request is signed, and every body that carries
  content is encrypted (see below).
- **Auth header, on every request:** `Authorization: Nostr <base64(JSON of a
  signed event)>`, NIP-98 style. The event has `kind 27235`, `created_at`
  within ±60 s of the home's clock, and these tags:
  `["u", "<absolute URL, query included>"]`, `["method", "GET"|"PUT"|"POST"]`,
  and, when there is a body, `["payload", "<hex sha256 of the exact body bytes>"]`.
  Add `["nonce", "<random hex>"]` as well. Each auth event is accepted only
  once, so without a nonce two identical requests in the same second would be
  the same event, and the second would get a `401 replayed`.
  It is signed with the device key, whose pubkey must be an active friend. A
  missing or invalid header gets `401`, and a revoked friend gets `403`.
- **`POST /device/events`** — the body is one signed **7879 event** (§4) as JSON.
  It is the direct-path equivalent of publishing to Nostr: `send`, `blob`,
  `http`, `PAIR_REQUEST` (this last one goes without the auth header, since
  the token is the proof). Answers: `202`, or `200` with
  `{"events": [ <7879 events for the device> ]}`, which carries replies
  such as `http-response` and `PAIR_ACK`.
- **`GET /device/pair/<sessionId>`** (no auth: the reply is encrypted to
  the requester) — pairing status after a `PAIR_REQUEST`. It answers `202`
  `{state: "pending"}` while the human decides, `200 {state: "paired",
  event: <PAIR_ACK 7879>}` once accepted, and `410` when the session ends up
  `rejected`, `expired` or `cancelled` (with `event: <PAIR_REJECT>` if it
  was rejected). `POST /device/events` with a `PAIR_REQUEST` answers
  `202 {sessionId}`, or `409` for a used or unknown token.
- **`GET /device/events?ack=<cursor>`** — the device's **outbox**: everything
  agents sent to `<deviceEnvName>` (e.g. `device-audio-result`), as 7879
  events `{type: "message", seq, id, from, text, ts, kind, source}`. The
  answer is `{cursor, events}`. Pass the last `cursor` as `ack` on the next
  call to drop what was already received. The home keeps the device's
  mailbox, so messages wait while the phone is away (cap 500).
- **`GET /device/<path>`** — proxied to LAS at `127.0.0.1:8700/device/<path>`
  (§8). `If-None-Match` is passed through, and `ETag` / `304` come back. The
  response body is **NIP-44 encrypted to the device**
  (`Content-Type: application/vnd.vortexia.nip44`; the plaintext is LAS's
  body). The original type is in `X-Vortexia-Content-Type`.
- **`PUT /device/blob/<sha256>`** — `<sha256>` is the hex sha256 of the
  **plaintext** clip: it is the clip's id and is used to deduplicate.
  `Content-Type: application/octet-stream`. The body is
  `nonce(12) ‖ AES-256-GCM ciphertext ‖ tag(16)`, with a random 32-byte key
  per clip, and the `payload` tag is the sha256 of these body bytes. The key
  travels only inside the `blob` 7879 event (NIP-44), which can arrive
  before or after the PUT. The home only decrypts and delivers once it has
  both. Answers: `201`, or `200` if it already had that clip, which makes a
  re-upload harmless.
- **Dev mode:** `VORTEXIA_DEVICE_DEV=1` on the home accepts requests without
  the auth header, and answers `/device/*` GETs in plaintext. It is for a dev
  build only: the home logs a warning on every request, and it only accepts
  requests from loopback or private-range source addresses (RFC 1918,
  Tailscale's 100.64/10).

### 9.1 The same conversation over a Nostr relay (away from home)

When the phone has no direct path, it talks to its home through the relays
from the QR / `PAIR_ACK` (`r=`). This is implemented in
`src/vortex-relay/deviceRelay.js` and is on by default
(`VORTEXIA_DEVICE_RELAY=0` turns it off).

- **Events.** Each one is kind 7879, NIP-44 encrypted to the recipient,
  `["p", <recipient pubkey>]`.
  - The phone publishes to the home pubkey and subscribes to
    `{kinds:[7879], "#p":[devicePk], authors:[homePk], since}`.
  - Home subscribes to `{kinds:[7879], "#p":[homePk]}`. It only answers
    active device friends, plus `PAIR_REQUEST` for a waiting token.
    Strangers get silence.
- **Payloads, phone → home.** These are the same as `POST /device/events`:
  - `send`, `http`, `PAIR_REQUEST`;
  - `blob`, which may carry `data`, base64 of the AES-GCM body (§6);
  - `{type:"sync", ack?}`, which drops outbox items with seq ≤ ack and
    resends the rest.
- **Payloads, home → phone.** Every incoming event gets exactly one answer,
  carrying `ref` = the event id, or the `msgId` of a split payload:
  - `{type:"ack", ref, ok, …}` for `send`, `{…, delivered}` for `blob`;
  - `{type:"http-response", ref, id, status, headers, body}`;
  - `{type:"error", ref, status, error}`;
  - `{type:"pair-pending", ref, sessionId}`, followed later by the
    `PAIR_ACK` / `PAIR_REJECT` event itself;
  - `{type:"synced", ref, cursor}`.

  On top of that, **every outbox message is pushed live** as
  `{type:"message", seq, id, from, text, ts, kind, source}`. That is the same
  payload as `GET /device/events`, so the phone dedupes by `seq` across both
  paths.
- **Splitting.** A payload whose JSON is over 32 000 bytes travels as
  `{type:"part", msgId, index, total, data}`, where `data` is a slice of the
  base64 of the UTF-8 JSON. The cap is 400 parts. This works both ways: the
  receiver joins the parts and handles the result as one payload. The phone
  must also join parts, because a large panel read comes back split.
- **Reliability.** Home:
  - remembers every handled event id (`data/device-relay-state.json`), so a
    relay resending, a catch-up query or a restart never processes anything
    twice;
  - reconnects and resumes the subscription;
  - runs a catch-up query every minute;
  - retries a publish no relay accepted (2 s, 10 s, 1 min, 5 min).

  Events older than 7 days are ignored. The phone should do the same: dedupe
  by event id, resend if no `ref` answer arrives, and `sync` on foreground.

### 10. Pairing and friends control over MQTT (for the LAS UI)

The LAS UI drives pairing over the **local** broker. It never runs the CLI.
All of this follows the same request/response pattern as
`vortexia/control/mailbox/drop`: the request carries a caller-chosen `id`,
and the answer goes on `<topic>/result` with the same `id`, QoS 1, not
retained. Only **loopback** clients may publish to `vortexia/control/*`;
the broker enforces this once §5's auth hook lands.

| Request topic | Payload | Answer on `…/result` |
|---|---|---|
| `vortexia/control/pair/start` | `{id, role: "device"\|"env", label?, ttlMs?}` (ttl default 600000, max 900000) | `{id, ok, sessionId, uri: "vxpair:1?…", expiresAt}` or `{id, ok: false, error}` |
| `vortexia/control/pair/cancel` | `{id, sessionId}` | `{id, ok}`. The token is burned. |
| `vortexia/control/pair/decide` | `{id, sessionId, accept: true\|false}` | `{id, ok, envName?}` or `{id, ok: false, error}` (`"expired"`, `"unknown session"`) |
| `vortexia/control/friends/revoke` | `{id, envName}` | `{id, ok, envName}` or `{id, ok: false, error: "unknown friend"}` |
| `vortexia/control/friends/rename` | `{id, envName, label}` | `{id, ok, envName, label}` |

**Pending requests, retained** on `vortexia/pair/pending`. A UI that opens
late still sees them, and the topic is republished on every change (empty
list = nothing pending):

```json
{ "updatedAt": 1794700000000,
  "pending": [{ "sessionId": "…", "role": "device", "envName": "jose-iphone",
                "label": "iPhone de José", "fingerprint": "3F9A·C21E·77B0·4D18",
                "requestedAt": 1794700000000, "expiresAt": 1794700600000 }] }
```

- `fingerprint` is the first 8 bytes of the requester's pubkey, in hex,
  grouped in 4s. The app shows the same string for its own key, so the human
  can compare the two before accepting.
- A pairing session goes through `waiting` (QR shown) → `pending` (a valid
  `PAIR_REQUEST` arrived) → `paired` | `rejected` | `expired` | `cancelled`.
  It only appears in `pending` while it is in that state. A pending request
  not decided before `expiresAt` expires, and the device gets a rejection.
- **Outcome events** (not retained) on `vortexia/pair/events`:
  `{type: "paired"|"rejected"|"expired"|"cancelled", sessionId, envName?}`.
  The UI can close the QR on these. The friends list itself is still
  `vortex-relay/friends` (retained), which updates on its own.
- `vortexia friends add/revoke/rename` remain as the CLI over the same store.

## What LAS has to do

**Backend (`:8700`)**
1. **Bind to `127.0.0.1`.** Today it listens on `0.0.0.0` with no auth, which
   means anyone on the Wi-Fi can make agents speak, send messages, or close
   sessions. The phone never talks to `:8700`. It talks to vortexia, which
   authenticates it.
2. Consume `device-audio` notices with a **deterministic consumer** (the
   widget app, which runs Whisper), never with an agent's LLM session. They go
   to the system mailbox `las-device-audio` (topic
   `las/agent/las-device-audio/inbox`; consumer client id
   `las-agent-las-device-audio`, clean=false, QoS 1). It is a mailbox so that
   it gets queueing before the first connect, the cap, the TTL and the restart
   snapshot. It is not in the LAS registry, so it is not an agent. The
   message text is exactly
   `{"type":"device-audio","from","label","path","sha256","mime","durationMs","capturedAt","target":{"agent","session"?}}`.
   The consumer transcribes the clip and delivers it to `target` as a mic
   dictation (`origin: "human"`). If `session` is missing, it goes to the
   agent's default session. An unknown `target.agent` gets an error back to
   `from`; it is never silently dropped.
   **The file is the source of truth:** `path` (decrypted, under
   `data/device-inbox/<env>/<sha256>.<ext>`) is deleted only after delivery.
   On start, the consumer processes whatever is left in that folder,
   deduplicated by sha256. That way a crash mid-transcription never loses a
   clip, even if the MQTT ack was already sent.
   **Sidecar:** next to every clip the gateway writes `<sha256>.json`, holding
   the exact notice JSON (so the target survives without the message). Write
   order: the clip, then the sidecar (both tmp+rename), then the notice. The
   startup scan looks for `deviceInboxDir/<env>/*.json`; a clip without a
   sidecar is logged and ignored. After delivery, LAS deletes both files.
   A **rejected** clip (unknown agent, no speech, sha mismatch) is never
   deleted, since a dictation may be irreplaceable: LAS moves both files to
   `<env>/rejected/` with a `<sha256>.reason.txt`.
   **Result to the device:** LAS always answers `from` with
   `{"type":"device-audio-result","sha256","ok":true,"chars"}` or
   `{…,"ok":false,"error","target"}`. The phone keeps the clip in its queue
   until it gets a result, and on either answer it marks it done; a rejected
   clip is safe on the home.
3. Mount the device-facing reads under `/device/`, starting with
   `GET /device/society/tree` (the full tree, with `ETag`). Trust
   `X-Vortexia-Device` only on loopback.
4. Read the friends list (vortexia will publish it retained on
   `vortex-relay/friends`: no keys, only `envName/role/label/lastSeen`) to show
   "paired devices" with a revoke button. The button calls
   `vortexia friends revoke`, or an MQTT control topic that vortexia will
   define.
5. Pairing UI: a "pair phone" button that asks vortexia for a pairing session
   (control topic `vortex-relay/pair/start` → retained reply with the
   `vxpair:` URI) and renders the QR in the widget, plus the "Accept
   <label>?" prompt when the request arrives.

**App**
1. Key generation and storage as in §1. `nostr-tools` (secp256k1 schnorr +
   nip44) and `react-native-get-random-values`.
2. QR scan (`expo-camera`), and the `PAIR_REQUEST` / `PAIR_ACK` exchange (§3).
3. An offline clip queue keyed by sha256, an upload worker that only runs on a
   direct path (§6), and retry with backoff.
4. Panel: subscribe through a home to its retained `vortex-relay/directory`
   (it already includes agent colors), and send messages as 7879 envelopes.

## What vortexia will do (this repo)

1. `friends.json` store, live reload, CLI, and seeding from `VORTEXIA_NOSTR_PEERS`.
2. The 7879 NIP-44 envelope on `NostrRelay` (Mac↔Mac first, with a one-release
   back-compat read of 7878/30078).
3. aedes `authenticate` / `authorize*` for non-loopback clients.
4. The pairing session (token store, `vxpair:` URI, control topics, accept prompt).
5. The device gateway: `PUT /device/blob/<sha256>` on the WS server and
   `device-audio` delivery (with `target`), the `/device/*` GET proxy (§8),
   and `send` envelopes delivered with `origin: human, via: device`.
6. A spec section in `PROTOCOL.md` for the device wire format, so that the app
   (and the later Android port) build against a document and not against Node
   code.

Order: 1 → 2 → 3 give a secure Mac↔Mac link with nothing visible to LAS.
4 → 5 unblock the app.

## Rejected alternatives

- **Tailscale as the identity / pairing layer.** It would make every user need a
  tailnet account and would tie the app to a third party. It is kept as an
  optional transport only.
- **Secure Enclave P-256 identity.** It would not sign Nostr events, so we would
  need two identity systems. It stays possible later as at-rest wrapping.
- **The phone as a full `env` running a broker.** iOS can't keep the broker
  alive in the background, and the phone doesn't need to route for others.
- **Audio over the relay, chunked.** Relay size limits, retention policies, and
  bulk data on volunteer infrastructure. It is rejected for blobs; the relay
  carries control messages only.
