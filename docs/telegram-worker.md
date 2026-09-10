# Telegram worker

`say-telegram` runs one TDLib client for one Telegram account and connects
outbound to `say-api`. Telegram session databases, the worker credential, and
unacknowledged events all remain in its `/data` volume.

## Run with Docker

Create Telegram application credentials at <https://my.telegram.org/apps>.
Create a Telegram account in say with `POST /v1/accounts`, then run:

```sh
cd deploy
export SAY_WORKER_URL=wss://your-say-host.example/v1/workers/connect
export SAY_ENROLLMENT_TOKEN=say_enroll_...
export TELEGRAM_API_ID=...
export TELEGRAM_API_HASH=...
docker compose -f telegram.compose.yaml up -d --build
```

The TDLib source revision is pinned in `Dockerfile.telegram`. Building it is
CPU-intensive; publish the resulting image rather than rebuilding it on every
worker machine.

## Link an account

Request Telegram's verification code:

```sh
curl -sS https://your-say-host.example/v1/accounts/ACCOUNT_ID/link \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{"action":"start","phone_number":"+4712345678"}'
```

Submit the code:

```sh
curl -sS https://your-say-host.example/v1/accounts/ACCOUNT_ID/link \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{"action":"submit_code","code":"12345"}'
```

If the response is `password_required`, submit the account's 2FA password:

```sh
curl -sS https://your-say-host.example/v1/accounts/ACCOUNT_ID/link \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{"action":"submit_password","password":"..."}'
```

Codes and passwords traverse the authenticated TLS connection but are never
stored by say. TDLib persists the resulting session in `/data/telegram`.

## Current boundary

Implemented in the worker:

- phone, verification-code, and optional 2FA authorization states;
- direct and group conversation discovery;
- text history, sends, and incoming events;
- durable event replay using the common worker acknowledgement protocol.

Attachments, reactions, edits, deletes, topics, secret chats, and complete
cursor pagination remain deferred. A real Telegram account smoke test and RSS
measurement are required before calling this production-ready.
