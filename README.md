# say

Signal, Telegram, and AI conversations behind one API.

The current gateway has Postgres-backed accounts, worker credentials,
magic-code login, signed access JWTs, and rotating refresh tokens. It proves
worker enrollment, worker reconnects, account isolation, and the shared
protocol. Single-account Signal and Telegram text workers are implemented;
attachments are not. Both still require gated smoke tests against real provider
accounts.

Signal events are persisted into a bounded Postgres cache before they are
published to authenticated client WebSockets. Conversation discovery and
idempotent text sends are exposed through the public API.

See [PLAN.md](./PLAN.md) for the architecture and scope.

## Deploy

On a persistent Docker host:

```sh
make deploy
```

The first run generates `.env.production`, builds the API, starts Postgres, and
waits for the stack to become healthy on `127.0.0.1:8000`. See
[docs/deployment.md](./docs/deployment.md) for exe.dev proxy setup, production
login delivery, and optional per-account worker profiles.

## Requirements

- Go 1.26+
- Node.js 24+ and npm (contract generation and the Telegram worker)
- Docker with Compose (for local Postgres)

## Run the contract slice

Start the API:

```sh
make run
```

Start a development login:

```sh
curl -sS http://localhost:8080/v1/auth/start \
  -H 'Content-Type: application/json' \
  -d '{"email":"alice@example.com"}'
```

Use the returned `login_id` and development-only `debug_code`:

```sh
curl -sS http://localhost:8080/v1/auth/verify \
  -H 'Content-Type: application/json' \
  -d '{"login_id":"login_...","code":"123456"}'
```

Use the returned access token to create a Signal account:

```sh
curl -sS http://localhost:8080/v1/accounts \
  -H 'Authorization: Bearer <access_token>' \
  -H 'Content-Type: application/json' \
  -d '{"platform":"signal","display_name":"Alice Signal"}'
```

Copy that response's single-use enrollment token and start the fake worker:

```sh
go run ./cmd/fake-worker \
  -token 'say_enroll_...' \
  -emit-every 10s
```

The worker exchanges the enrollment token for a durable credential in
`.fake-worker-credential`, reconnects with jitter, sends heartbeats, emits fake
message events, and acknowledges commands.

`make run` explicitly enables returning the login code. Production forbids
debug login codes and development bearer identities, and requires a login-code
delivery webhook.

## Contracts

- `api/openapi.yaml` — public REST and SSE API.
- `protocol/worker-envelope.schema.json` — worker WebSocket protocol.
- `protocol/client-envelope.schema.json` — client real-time protocol.
- `internal/contract/openapi.gen.go` — generated Go API types.
- `clients/typescript/src/schema.d.ts` — generated TypeScript API types.

Regenerate and verify:

```sh
npm --prefix clients/typescript install
make check
```

## Current commands

```text
cmd/say-api       development gateway and worker hub
cmd/fake-worker   reconnecting protocol test worker
cmd/say-signal    single-account Signal worker
workers/telegram  single-account TDLib worker
```

See [docs/signal-worker.md](./docs/signal-worker.md) for Signal setup and
limitations, and [docs/telegram-worker.md](./docs/telegram-worker.md) for
Telegram.
