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

- Add an aedes `authenticate` hook. A non-loopback connection must send
  `username = <pubkey hex>` and `password = <signed auth event>` (NIP-42-style:
  kind 22242, `created_at` within ±60 s, a `challenge` tag set to the broker's
  `envName`). The pubkey must be a non-revoked friend.
- `authorizePublish` / `authorizeSubscribe`: a **device** may only subscribe to
  its own mailbox topics and publish 7879 envelopes addressed to this env. An
  **env** keeps today's relay topics.
- Loopback (local agents, LAS backend, widget) stays unauthenticated, as it is
  today.
- Bind policy: keep listening on all interfaces (LAN direct needs it), but
  reject unauthenticated non-loopback clients from day one.

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
- **Away from home without Tailscale** the clip waits in the phone's queue, and
  only the notice ("3 clips pending") reaches the Mac through the relay. If
  uploading away from home without Tailscale becomes a requirement, a later
  step can add an encrypted Blossom (Nostr blob server) upload. The encryption
  is already end-to-end, so that would only add a transport, not a redesign.

### 7. Transports for a device, in cost order

1. **LAN** — the `lan=` endpoints from the QR, refreshed on every `PAIR_ACK`
   and kept current through the directory. Discovery is mDNS when the phone can
   do it (Expo needs `react-native-zeroconf`, or the cached IP is enough).
2. **Tailscale** — just another direct endpoint (`ts=` in the QR). It is **not**
   an identity or a dependency: the keys and the auth are ours, and Tailscale
   only provides reachability. That keeps it optional, and keeps it working on
   Windows.
3. **Nostr relay** — always available, for control messages and notices
   (panel, agents directory, "clip pending", replies). Never for blobs.

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
  It is signed with the device key, whose pubkey must be an active friend. A
  missing or invalid header gets `401`, and a revoked friend gets `403`.
- **`POST /device/events`** — the body is one signed **7879 event** (§4) as JSON.
  It is the direct-path equivalent of publishing to Nostr: `send`, `blob`,
  `http`, `PAIR_REQUEST` (this last one goes without the auth header, since
  the token is the proof). Answers: `202`, or `200` with
  `{"events": [ <7879 events for the device> ]}`, which carries replies
  such as `http-response` and `PAIR_ACK`.
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
  build only: the home logs a warning on every request, and the setting is
  refused unless the broker also binds to a private address.

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
