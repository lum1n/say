# Signal worker

`say-signal` runs exactly one isolated signal-cli data directory and initiates
an outbound WebSocket connection to `say-api`. The user does not expose a port.

Signal has no history API for linked clients. The worker receives new messages
from the moment it is linked; cached messages removed by say's retention policy
cannot later be fetched from Signal.

## Run with Docker

The image is based on the official
`ghcr.io/asamk/signal-cli:0.14.7-native` image. The native build avoids a JVM
per account. Actual RSS still needs measuring with representative accounts.

Create an account through `POST /v1/accounts` and copy its one-time enrollment
token, then:

```sh
cd deploy
export SAY_WORKER_URL=wss://your-say-host.example/v1/workers/connect
export SAY_ENROLLMENT_TOKEN=say_enroll_...
docker compose -f signal.compose.yaml up -d --build
```

The enrollment token is redeemed once. Its replacement worker credential is
written to the `signal-data` volume, so subsequent starts do not need the
enrollment token.

Start linking:

```sh
curl -sS https://your-say-host.example/v1/accounts/ACCOUNT_ID/link \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{"action":"start","device_name":"say"}'
```

Render the returned `qr_uri` as a QR code and scan it in Signal under
**Settings → Linked devices**. Then complete linking:

```sh
curl -sS https://your-say-host.example/v1/accounts/ACCOUNT_ID/link \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{"action":"finish","device_name":"say"}'
```

`finish` may wait while Signal completes provisioning.

## Data and upgrades

The `signal-data` volume contains Signal session keys, the say worker
credential, and unacknowledged inbound events. An event is removed from the
local spool only after `say-api` has committed it and acknowledged its event
ID. Losing the volume requires relinking and can also lose events that had not
yet reached the API. Back it up encrypted and never copy one live volume to two
simultaneously running workers.

The signal-cli version is deliberately pinned. Upgrade it explicitly, test
link/send/receive with a disposable account, then roll workers gradually.

## Current text-only boundary

Implemented:

- QR device linking;
- direct and group conversation discovery;
- direct and group text sends;
- incoming and synced outgoing text events.

Deferred:

- attachments;
- reactions, edits, typing, receipts, stories, and calls;
- historical message fetch, which signal-cli cannot provide.
