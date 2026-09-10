# say — plan

A messaging service that unifies **Signal**, **Telegram**, and an **AI assistant**
behind one API, consumed by a mobile app and desktop/TUI clients.

Greenfield. `../chatty` is reference only — no code is carried over.

**Target:** thousands of users, real-time push for everyone, no hibernation.
That target is what drives every decision below.

---

## 1. What we're building

Three things behind one API:

1. **Real conversations** — read and send Signal + Telegram messages.
2. **An AI assistant** — standalone chat threads, native to say.
3. **AI over real conversations** — summarize a thread, draft a reply, search
   across everything. This is the differentiator; without it say is just
   another chat client.

Multi-user, but deliberately *not* multi-tenant.

---

## 2. The constraint: one daemon per user account

Signal and Telegram sessions are **stateful, per-account, and long-lived**.
`signal-cli` keeps a data directory and a long-running process; TDLib keeps an
encrypted database and a native client. They are not stateless workers.

### One shared daemon per platform does not work

Both APIs advertise multi-account support — `signal-cli daemon` without `-a`
serves every local account, and one TDLib `ClientManager` is documented as
handling "thousands of clients". Both claims are true about the *interface* and
misleading about *operations*. Every failure mode is shared-fate:

| Evidence | What happens |
|---|---|
| [signal-cli #1585](https://github.com/AsamK/signal-cli/issues/1585) | One account's `account.db` grew to 17 MB (vs 2 MB healthy) and pinned the daemon at 100% CPU for ~3 months. Sends took 5–30 minutes — **for every account in the process**. |
| [signal-cli #1974](https://github.com/AsamK/signal-cli/issues/1974) | Daemon hangs on HikariPool thread starvation; connections time out after 9 minutes. The pool is **2 connections, shared by all accounts**. |
| [signal-cli #2087](https://github.com/AsamK/signal-cli/pull/2087) | ~47 MB/hour linear growth in daemon mode. Now capped, but at ~10 MB per account per store × 2 stores — additive per account by design. |
| [TDLib #3671](https://github.com/tdlib/td/issues/3671) | "Continuous native memory growth with multiple concurrent clients in a single process" → memory exhaustion. Upstream remedy: periodically close and recreate clients, and swap in jemalloc. |

Beyond the bugs, a shared process means every restart, upgrade, and OOM takes
all users down at once, and there is no way to apply a per-user memory limit.

chatty already reached this conclusion — `chatty-api/TELEGRAM_PLAN.md` is marked
*locked* with "Support Telegram chat via TDLib in **BYO mode**".

So: **one daemon container per linked account.** Blast radius is one user, and
resource limits become enforceable.

---

## 3. The economics force BYO-first

Always-on is not a choice: **neither Signal nor Telegram offers a push relay
for linked devices.** To know a message arrived, something of ours must hold a
live connection. With no hibernation, every linked account needs a running
daemon 24/7.

Order of magnitude, to be replaced by real measurements in phases 2–3:

| | Rough |
|---|---|
| signal-cli native process, one account | measure in phase 2 |
| TDLib client, one account | ~100–200 MB RSS |
| **Fully-linked user** | measured Signal + Telegram p95 |
| 1,000 users | linear in the measured per-account p95 |
| 10,000 users | 10× the 1,000-user requirement |

The official native signal-cli build avoids paying for a JVM per account, but
the process remains stateful and always connected. An exe.dev plan is a pooled
CPU/memory allowance across VMs — it can host the control plane comfortably,
not an unmeasured number of thousands of daemons.

**Conclusion: we cannot host the daemons for thousands of users, so we don't.**
The daemon runs on hardware the user already owns. What we host is the
coordination layer, which is cheap.

### Three hosting modes, one worker binary

| Mode | Who runs the daemon | Always on? | For |
|---|---|---|---|
| **Desktop-embedded** | the say desktop app | while desktop is running | trial/local mode; not a real-time service |
| **Self-hosted** | `docker run` on a NAS, homelab, or cheap VPS | yes | default real-time mode |
| **Hosted** | us, capped and enforced at link time | yes | paid convenience tier |

All three run **the same worker binary with the same protocol**. Hosted is
literally "we run your worker for you". There is no separate code path, which
is the main thing that keeps this from becoming chatty.

Desktop-embedded makes trial onboarding easy: install the desktop app, link an
account, and the user's machine is the daemon host — no Docker, token, or VPS.
But it does **not** meet the product requirement of real-time push when that
machine sleeps. It is therefore a clearly labelled local/trial mode, not the
default production mode. Real-time users must choose self-hosted or hosted.

---

## 4. Workers dial out (the key simplification)

chatty's BYO required the *user* to expose a public HTTPS endpoint, prove
ownership via a `.well-known` file, and let the gateway call inbound. That means
DNS, certificates, a reverse proxy, and NAT traversal — for every user. It is
the highest-friction part of chatty and most of its BYO complexity.

**Reverse the direction: the worker dials out to say and holds the connection.**

```
worker  ──── WSS, outbound ────►  say-api
        ◄─── commands ──────────
        ──── events ───────────►
```

What this deletes outright:

- No public endpoint, DNS, or TLS certificate for the user.
- No `.well-known` ownership proof — holding a valid pairing token *is* the proof.
- No NAT traversal; works behind home internet unchanged.
- No routing/placement table for reachability. The gateway knows an account by
  *which connection it arrived on*.
- No encrypted-endpoint-credential storage.

Onboarding becomes a pairing token. For self-hosted:

```
docker run -d -v say-signal:/data ghcr.io/<org>/say-signal --token=SAY-...
```

For desktop-embedded, the app mints and stores the token itself and the user
never sees it.

**This also inverts the cost curve in our favour.** The expensive part (daemons)
now runs on user hardware; the part we host is thousands of idle WebSockets,
which for a Go process is a few hundred megabytes and entirely unremarkable. A
single VM genuinely serves thousands of users this way.

One consequence to respect: a worker connection is pinned to the gateway
instance that accepted it. One gateway instance is fine well past our target,
so **stay single-instance** until it isn't, then add Redis pub/sub for
cross-instance command routing. Don't build that now.

---

## 5. Why chatty got complicated, and what say does instead

| chatty | say | Why |
|---|---|---|
| BYO via user-hosted public HTTPS + `.well-known` proof | Worker dials out with a pairing token | §4. Deletes DNS, TLS, NAT, and proofs. |
| gRPC + protobuf between gateway and modules | JSON envelopes over the worker WebSocket | Kills two codegen pipelines and the HMAC metadata signer. |
| `tenant_id` on every row and in every JWT | Users only | Nobody asked for orgs. Adding tenancy later is a migration; carrying it now taxes every query. |
| Refresh-token families with reuse detection, Redis throttle, verify lockout | Magic link, one signing key, access + refresh, rate limit in Postgres | Keep the passwordless UX, drop the machinery until a threat model needs it. |
| File **and** Postgres storage backends | Postgres only | Compose makes local Postgres a non-issue. |
| 4 providers (Signal, Telegram, Slack, Teams) | 2 + AI | Scope. |
| Every client hand-writes its own copy of the API types | One OpenAPI spec, generated Go and TS clients | chatty's CLI and web duplicated these by hand and drifted. |
| No message storage at all | Bounded cache (§7) | Passthrough can't do offline, unread counts, or push. |
| Shared daemon per platform | One daemon per account | §2. |

Net: one Go binary, two worker images, one Postgres. No supervisor for the
common case, no service mesh, no scheduler.

---

## 6. Architecture

```
   mobile · desktop · TUI
            │ REST + WS + SSE (user JWT)
     ┌──────▼──────────────────────────┐
     │  say-api (Go)                   │
     │  auth · ingest · AI · push      │──► Postgres
     │  worker hub                     │
     └──────▲──────────────────────────┘
            │ WSS, worker-initiated, pairing token
     ┌──────┴───────┬───────────────┬──────────────┐
     │              │               │              │
 desktop app    docker run      docker run    say-supervisor
 (embedded)     on a NAS        on a VPS      (hosted tier only)
                                                   │
                                            capped # of local
                                            worker containers
```

### Repo layout

```
say/
  say-api/          Go: gateway, auth, worker hub, ingest, AI, push
    openapi.yaml    single source of truth for all clients
  say-worker/       shared Go: dial-out, envelope protocol, pairing
  say-signal/       Go: say-worker + signal-cli JSON-RPC, one account
  say-telegram/     Node: TDLib, one account (speaks the same protocol)
  say-supervisor/   Go: hosted tier only — spawn/limit/reap local containers
  say-desktop/      embeds say-signal + say-telegram
  say-tui/          Go + Bubble Tea
  say-mobile/       Expo, templated from ../sessh
  deploy/           compose.yaml, Caddyfile, .env.example
```

Go everywhere except Telegram, which realistically wants the Node TDLib
bindings. `say-worker` is the shared dial-out client so both images and the
desktop app behave identically.

`say-supervisor` now serves only the small hosted tier and is **not** on the
critical path — it can be deferred to phase 9 without blocking anything.

---

## 7. Worker protocol

One WSS connection per account, worker-initiated. Envelopes in both directions,
correlated by `id`:

```
worker → hub   { "type": "hello",  "payload": { token, platform, version } }
hub    → worker{ "type": "ready",  "payload": { account_id } }

hub    → worker{ "id", "type": "send",         "payload": { conversation_id, body } }
hub    → worker{ "id", "type": "conversations","payload": { cursor } }
hub    → worker{ "id", "type": "messages",     "payload": { conversation_id, cursor } }
hub    → worker{ "id", "type": "link.start" | "link.finish" }
worker → hub   { "id", "status": "ok" | "error", "payload": {...} }

worker → hub   { "type": "message.new",   "payload": {...} }
worker → hub   { "type": "account.status","payload": { state } }
```

Deliberately the same envelope shape as the client-facing WebSocket, so there
is one protocol idea in the codebase rather than two.

Linking runs over this connection too: `link.start` returns the Signal QR URI
or triggers the Telegram OTP, `link.finish` completes it. So a freshly started
worker connects with a pairing token, gets told to link, and the user completes
it from whichever client they're holding.

---

## 8. Persistence: cache, not archive

Messages are cached for **speed, unread counts, push, and AI context** — not as
a permanent archive. A retention job prunes to the last N days or last K
messages per conversation, whichever is larger.

Telegram can usually refetch older history through TDLib. Signal cannot:
signal-cli receives queued/new events but exposes no general history API.
Pruned Signal messages therefore disappear from say permanently. The retention
setting must make that tradeoff explicit to users.

This matters more in a BYO world: it bounds our disk, and it keeps say from
quietly becoming the permanent store of everyone's Signal history when the whole
premise is that users hold their own data.

```
users              id, email, created_at
auth_logins        id, email, code_hash, expires_at, consumed_at
refresh_tokens     id, user_id, token_hash, expires_at, revoked_at
push_tokens        id, user_id, platform, token

accounts           id, user_id, platform, external_id, display_name,
                   status, hosting_mode, last_seen_at, linked_at
pairing_tokens     id, account_id, token_hash, issued_at, redeemed_at, revoked_at
conversations      id, account_id, external_id, title, kind,
                   last_message_at, unread_count
messages           id, conversation_id, external_id, author_id, author_name,
                   body, sent_at, direction, attachments jsonb    -- pruned

ai_threads         id, user_id, title, created_at                 -- kept
ai_messages        id, thread_id, role, content, created_at       -- kept
```

At thousands of users the `messages` table is the only thing that grows without
bound, so the retention job is load-bearing infrastructure, not a nicety. Size
it on real data in phase 5. Full-text index for search; if search needs to get
good later, that's when to reach for Typesense — `../cavet` has the pattern.

**Content-free push.** Notifications carry only "you have a new message" and the
client fetches over an authenticated connection. This is what Signal itself
does. It makes the privacy story defensible for BYO users and avoids putting
message bodies through APNs and FCM.

---

## 9. Public API

Two streaming mechanisms, deliberately. Forcing AI token deltas through a
uniform provider abstraction is the kind of thing that made chatty complicated.

**Real conversations — WebSocket, whole events.**

```
POST   /auth/start · /auth/verify · /auth/refresh
GET    /accounts
POST   /accounts            → create account + mint pairing token
DELETE /accounts/{id}
POST   /accounts/{id}/link  → start/finish link via the worker
GET    /conversations?account_id=&cursor=
GET    /conversations/{id}/messages?cursor=
POST   /conversations/{id}/messages
POST   /push/tokens
GET    /ws        → message.new · conversation.updated · account.status
```

**AI — SSE, token deltas.**

```
GET    /ai/threads
POST   /ai/threads · /ai/threads/{id}/messages     → SSE
POST   /conversations/{id}/ai/summarize            → SSE
POST   /conversations/{id}/ai/draft                → SSE
GET    /search?q=
```

Worker reachability is visible in the API rather than hidden: every account
reports `status` (`live`, `offline`, `unlinked`), a send to an offline account
returns `409` with that status, and clients show cached history with an
"offline" affordance. For desktop-embedded users this is a normal daily state,
not an error, so it has to be designed in from the start.

AI reads from the cache and, when it needs a wider window, requests it from the
worker over the same connection — so it works identically in all three hosting
modes.

**Drafts are never auto-sent.** `/ai/draft` returns text; the user reviews and
sends through the normal path. An AI that can autonomously send as you on Signal
is a different product with a different risk profile.

---

## 10. Deployment

The control plane is small and fits one VM: Caddy, `say-api`, Postgres. Worker
containers for the hosted tier are created by the supervisor at runtime, not
declared in compose.

exe.dev specifics:

- **The proxy is private by default** and redirects unauthenticated visitors to
  log into exe.dev. Neither the mobile app nor a worker can do that redirect. Run
  `ssh exe.dev share set-public <vm>` and rely on say's own JWT and pairing
  tokens.
- **Only one port can be public** — hence Caddy, with worker WSS and client
  traffic multiplexed by path.
- WebSockets and SSE are supported at the edge. **Test thousands of concurrent
  long-lived WSS through the edge proxy early** — that is the one capacity
  assumption in this design I have no evidence for, and it's load-bearing.
- TLS terminates at the edge; bind `0.0.0.0`, trust `X-Forwarded-*`.

For hosted-tier accounts, each one owns a `signal-data-<id>` or
`tdlib-data-<id>` volume that is **not recreatable** — losing it means that user
re-links from their phone. Same applies to self-hosted users' volumes, so the
docs need to say so plainly.

---

## 11. Phases

| # | Phase | Done when |
|---|---|---|
| 0 | Contract | `openapi.yaml` + worker envelope protocol agreed; Go/TS client generation wired |
| 1 | Gateway + hub | Magic-link auth, Postgres schema, client `/ws`, worker hub accepting dial-out with pairing tokens |
| 2 | Signal worker | Link by QR from a real phone; send and receive end to end; **RSS measured** |
| 3 | Telegram worker | Link by phone + OTP; send and receive; **RSS measured** |
| 4 | Deploy thin slice | Live on exe.dev, public; **edge tested with thousands of idle WSS** |
| 5 | Cache + ingest | Worker events → Postgres; history, unread counts, retention job sized on real data |
| 6 | Desktop app | Embeds both workers; offline status handled as a normal state |
| 7 | AI assistant | Standalone threads with SSE token streaming |
| 8 | AI over conversations | Summarize, draft, search |
| 9 | Hosted tier | Supervisor spawns capped local workers; quota enforced at link time |
| 10 | Mobile | Expo app + content-free APNs/FCM push |
| 11 | TUI | Bubble Tea on the generated Go client |

Phases 2 and 3 are independent and can run in parallel.

Phase 4 is early on purpose. Two assumptions can invalidate the architecture —
per-account RSS (phases 2–3) and edge WSS capacity (phase 4) — so both get
tested before anything is built on top of them.

Phase 6 provides the low-friction trial path. Mobile users who require the
promised real-time behavior need either a self-hosted always-on worker or the
hosted tier.

---

## 12. Open questions

- **Which AI provider?** `../cavet` uses a pluggable
  `ASSIST_PROVIDER`/`DRAFT_PROVIDER` with a `stub` default — good pattern to
  copy. Anthropic, OpenAI, or local Ollama?
- **Desktop-offline UX.** The client should say "worker offline — showing
  cached messages" and show the last-seen time. It must not imply that push is
  active. Offer self-hosted and hosted setup from that state.
- **Attachments.** Images and voice notes are most of real Signal traffic. Proxy
  through say, or fetch from the worker on demand and never store? Affects disk
  and the retention job.
- **Worker distribution.** Docker image, or also a Homebrew/systemd package for
  people who won't run Docker? Affects how wide "self-hosted" can realistically go.
- **Registration policy.** BYO removes most of the trust burden, but the hosted
  tier still means holding session keys. Invite-only for hosted.

---

## 13. Reliability semantics

Messaging needs explicit delivery semantics. A WebSocket is a transport, not a
queue, and treating it as one will lose or duplicate messages during ordinary
reconnects.

### One authoritative worker

An account has exactly one active worker connection. Each account has a
monotonically increasing `generation`:

- A normal duplicate connection is rejected with `already_connected`.
- An explicit takeover increments `generation` and closes the old connection.
- Events and acknowledgements include the generation; stale connections cannot
  write after replacement.

This prevents two copies of one Signal or Telegram session from processing the
same account concurrently.

### Outbound sends

1. Client sends a message with a stable `idempotency_key`.
2. `say-api` verifies ownership and inserts a `commands` row in the same
   transaction as the optimistic local message.
3. The hub dispatches the command when the worker is live.
4. Worker sends through the provider and acknowledges with the provider
   message ID.
5. API marks the command complete and updates the cached message.

Signal and Telegram do not provide an idempotency key for sends, so exactly-once
delivery is impossible around a process or network failure. say deduplicates
client retries before dispatch. Once a command is marked `dispatched`, it is
never sent automatically again. If the connection dies after provider send but
before acknowledgement, the message is marked `unknown` for user
reconciliation rather than risk sending it twice.

For MVP, sending while the worker is offline returns `409 worker_offline`.
Queueing offline sends can be added later, but only with visible cancellation
and expiry; silently sending hours later is unsafe.

### Inbound events

- Worker emits one ordered stream per account; no global ordering is promised.
- API acknowledges an event only after the event and its outbox entry commit
  to Postgres.
- Deduplicate messages on `(conversation_id, provider_message_id)`; provider
  event IDs can extend this for non-message event types.
- A reconnect resends every locally unacknowledged envelope with the same event
  ID. Telegram can additionally reconcile recent history. Signal cannot, so
  the Signal worker keeps a durable local spool and deletes entries only after
  say acknowledges them.
- A transactional outbox drives client WebSocket events and push
  notifications. A database commit cannot be lost merely because the process
  crashes before notifying a phone.

### Connection behavior

- Application heartbeat every 25 seconds; three missed heartbeats marks the
  worker offline.
- Exponential reconnect with full jitter, capped at 60 seconds.
- Bounded send queues. A slow worker or client is disconnected and resumes
  from durable state rather than consuming unbounded memory.
- Server advertises a minimum supported worker protocol version and an
  `upgrade_required` state.

---

## 14. Pairing and worker authentication

The pairing token is an enrollment credential, not a permanent bearer secret.

1. An authenticated user creates an account.
2. API returns a 256-bit, single-use enrollment token that expires in 10
   minutes. Only its SHA-256 hash is stored.
3. Worker presents it in the first WSS message over TLS.
4. API atomically redeems it and returns a `worker_id` plus a new 256-bit
   worker credential, shown only once.
5. Worker stores that credential in its local data volume or OS keychain.
6. User can revoke or rotate the worker from any client.

Credentials are scoped to one account and cannot call the user REST API. User
JWTs cannot connect to the worker endpoint. Logs contain credential IDs, never
tokens. Query-string tokens are forbidden because proxies and access logs
commonly retain URLs.

The worker credential is high entropy, so a fast hash is appropriate. Login
codes and any future passwords require a password KDF.

---

## 15. Security and privacy model

BYO moves provider session keys off our infrastructure, but it does **not** make
say end-to-end encrypted. The control plane receives plaintext message events,
caches plaintext, sends push, and may send selected text to an AI provider.
The product must state this plainly.

### Trust boundaries

| Data | Where it exists |
|---|---|
| Signal/Telegram session keys | worker volume only; hosted tier is the exception |
| Cached message text | worker, say Postgres, authenticated clients |
| Attachments | provider/worker initially; §19 defines later handling |
| AI prompt window | say-api and the selected AI provider |
| Worker credential | worker secret store; hash in say Postgres |
| User refresh token | client secure storage; hash in say Postgres |

### Required controls before public signup

- Strict ownership checks on every account, conversation, message, and AI
  thread query. Never trust IDs merely because they are UUIDs.
- Per-user and per-IP rate limits for auth, pairing, sends, search, and AI.
- Short-lived access JWTs; opaque, hashed, rotating refresh tokens.
- CORS allowlist and origin checks on browser WebSockets.
- Request/body limits and WebSocket frame limits.
- Encryption at rest for database volumes and backups.
- Secret redaction in structured logs; message bodies never logged.
- Pairing, revocation, worker takeover, account unlink, and AI access recorded
  in a content-free audit log.
- Dependency and container image scanning in CI.

Deleting an account revokes its workers immediately and schedules cached
content for hard deletion. Backups need a documented expiry window so deletion
claims are honest.

### AI data boundary

AI is opt-in per operation, not an always-on reader. The API sends only the
selected conversation window, never the whole user archive. The UI shows which
provider receives data. Provider request retention is disabled where supported.

Local AI can be added through the same interface, but it is not a reason to
abstract every model feature up front. Start with one hosted provider plus a
deterministic test provider.

---

## 16. Control-plane scaling

Thousands of mostly idle worker and client WebSockets are plausible in Go, but
not assumed. Phase 4 proves the actual limits through the exe.dev edge.

### Single-instance first

One `say-api` instance owns all live connections. Postgres owns durable state.
Tune and measure:

- file descriptor limits;
- memory per idle and active connection;
- edge and application idle timeouts;
- reconnect rate after an API restart;
- heartbeat write volume;
- database pool saturation;
- push and AI concurrency.

Reconnect jitter is mandatory: after a restart, thousands of workers must not
reconnect in the same second.

### Scale-out trigger

Add another gateway only when one measured resource is exhausted. At that
point:

- a load balancer accepts any client or worker connection;
- Redis stores ephemeral `account_id → gateway_id` presence with TTL;
- Redis Streams or NATS routes commands to the gateway holding the worker;
- Postgres and the outbox remain authoritative;
- no sticky session is required for clients.

Do not introduce Redis, NATS, or Kubernetes before this trigger. They solve a
measured second-instance problem, not an MVP problem.

### Hosted worker capacity

Hosted workers run on separate worker VMs, never on the control-plane VM.
Each VM advertises available memory and accepts accounts until a conservative
RSS cap is reached. The supervisor enforces per-container memory/CPU limits and
drains a host before upgrades.

Signal and Telegram capacity are calculated separately from observed p95 RSS,
not averages. A hosted user linking both consumes two independently limited
worker slots.

---

## 17. Observability and service objectives

Use structured JSON logs, Prometheus metrics, and OpenTelemetry traces from the
start. Useful labels are platform, operation, status, worker version, and
hosting mode. Do not label metrics by user or account ID.

Initial objectives, measured only while the worker is online:

- 99.9% monthly control-plane availability.
- p95 worker event committed to Postgres in under 1 second.
- p95 committed event to client WebSocket or push enqueue in under 1 second.
- 99.99% of accepted inbound events survive an API restart.
- No automatic resend when a provider send outcome is unknown.

Provider delivery latency is reported separately because Signal and Telegram
are dependencies outside our SLO.

Alert on worker disconnect rate, reconnect storms, pending command age, outbox
backlog age, duplicate-event rate, database health, push rejection rate, and AI
error/latency/spend.

---

## 18. Testing strategy

### Every pull request

- Unit tests for auth, ownership, protocol parsing, deduplication, retention,
  and AI prompt construction.
- Contract tests generated from OpenAPI and the worker envelope JSON Schema.
- Integration tests with Postgres and a deterministic fake worker.
- Reconnect tests that drop the socket before and after event/command
  acknowledgements.
- Migration tests from an empty database and from the previous release.
- Static analysis, dependency scanning, and container builds.

### Gated live tests

Signal and Telegram tests require real test accounts and should not be required
PR checks. Run a scheduled smoke suite that uses pre-provisioned accounts,
sends both directions, verifies deduplication, and removes message content.

### Before public beta

- 10,000 idle worker sockets plus representative client sockets through the
  exe.dev edge for 24 hours.
- API restart with all simulated workers reconnecting using jitter.
- Active-message load while retention and outbox workers run.
- Postgres restore drill.
- Worker credential theft/revocation exercise.
- Hosted worker OOM test proving one account does not affect another.

---

## 19. Attachments

Text-only is acceptable for the first end-to-end slice, not for public beta.
Large binary payloads should not be base64 frames on the long-lived worker
WebSocket.

Proposed inbound flow:

1. Client asks API for an attachment transfer.
2. API issues a short-lived transfer ID to the worker.
3. Worker streams the provider attachment to an authenticated HTTP upload.
4. API writes it to S3-compatible object storage with a short retention TTL.
5. Client downloads through a short-lived signed URL.

Uploads are size-limited, content-type checked, and encrypted at rest. Push
remains content-free. Default retention should be hours, not the message
cache's full retention window.

For outbound attachments, reverse the transfer: client uploads first, worker
downloads through a one-time URL, then sends to the provider. Object storage is
introduced only when this phase starts.

---

## 20. Concrete build increments

### Increment A — executable contract

- Initialize Go module and service layout.
- Write OpenAPI for auth, accounts, conversations, messages, and AI stubs.
- Write JSON Schema for worker/client envelopes.
- Generate Go server types and TypeScript client types in CI.
- Build a fake worker CLI that connects, pairs, emits messages, and answers
  commands.

**Exit:** two fake users cannot access each other's resources; disconnect and
reconnect tests pass.

### Increment B — real Signal vertical slice

- Worker enrollment and credential rotation.
- Signal QR link flow in a single-account worker container.
- List conversations, fetch text messages, send text, receive events.
- Durable event ingest, command idempotency, and client WebSocket fan-out.
- Deploy to exe.dev and measure RSS and reconnect behavior.

**Exit:** a message can travel phone → Signal → worker → say → test client and
back without duplicates across an API or worker restart.

### Increment C — Telegram parity

- Phone, OTP, and optional 2FA password authorization states.
- Same conversation/message/event contract as Signal.
- Provider-specific errors mapped to stable say error codes.

**Exit:** the same black-box suite passes for Signal and Telegram, except
explicit capability differences.

### Increment D — usable clients

- TUI first: login, account status, conversation list, history, send, reconnect.
- Desktop packaging for workers and explicit trial/offline behavior.
- Mobile: secure tokens, cached history, account status, content-free push.

**Exit:** a self-hosted worker plus mobile client provides uninterrupted
real-time notification without a desktop process.

### Increment E — AI

- Deterministic fake provider and one real hosted provider.
- Standalone AI threads.
- Explicit summarize and draft actions over bounded cached context.
- Token/cost limits per user and SSE cancellation.

**Exit:** no background AI access, no automatic sends, and every external AI
request is attributable in the content-free audit log.

### Increment F — public beta

- Attachments.
- Retention/deletion jobs and restore drill.
- Worker installers and upgrade path.
- Abuse controls, quotas, status page, and operator runbooks.
- Hosted tier only after measured unit economics exist.

---

## 21. MVP boundary

The first useful MVP is narrower than the complete phase table.

**Included**

- invite-only users;
- one Signal account and one Telegram account per user;
- self-hosted Docker workers that dial out;
- text conversations and real-time events;
- bounded cached history;
- TUI client;
- standalone AI plus manual summarize/draft;
- single `say-api` instance and Postgres on exe.dev.

**Explicitly excluded**

- open registration;
- hosted workers;
- organizations/tenants;
- worker hibernation;
- Slack, Teams, or other providers;
- calls, stories, reactions, edits, and group administration;
- autonomous AI sending;
- multi-region or active-active gateway;
- permanent message archive.

Attachments are required for public beta but do not block proving the core
architecture.

---

## 22. Decisions now locked

1. One Signal or Telegram daemon per linked account.
2. Workers initiate outbound WSS connections; users expose no public endpoint.
3. Self-hosted always-on workers are the default real-time mode.
4. Desktop-embedded is a limited trial mode, not a real-time guarantee.
5. Hosted workers are a later paid tier on separate worker VMs.
6. One control-plane instance until load testing proves a need to scale out.
7. Postgres is the only durable control-plane store.
8. Message storage is a bounded cache; AI threads are durable.
9. AI drafts require user review and cannot send autonomously.
10. Signal and Telegram ship before any additional chat provider.
