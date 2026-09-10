# Deployment

The production stack is designed for a persistent Linux VM such as exe.dev.
After cloning the repository, the control plane starts with one command:

```sh
make deploy
```

On its first run this:

1. creates `.env.production` with independent random database and signing keys;
2. builds `Dockerfile.api`;
3. starts Postgres and `say-api`;
4. waits for both services to become healthy; and
5. binds the API to `127.0.0.1:8000`.

On an exe.dev VM, direct the managed HTTPS proxy at that port:

```sh
ssh exe.dev share port VM_NAME 8000
ssh exe.dev share set-public VM_NAME
```

The API is then available at `https://VM_NAME.exe.xyz`. WebSocket upgrades use
the same endpoint.

## Required production login delivery

The generated deployment is secure-by-default and never returns login codes in
HTTP responses. Point say at email-service, then rerun `make deploy`:

```dotenv
SAY_AUTH_WEBHOOK_URL=https://email.example/emails/send
SAY_AUTH_WEBHOOK_TOKEN=...
SAY_AUTH_EMAIL_FROM=Say <noreply@example.com>
```

`SAY_AUTH_WEBHOOK_TOKEN` is sent as `X-Internal-Api-Token`. say posts the
email-service `/emails/send` body with subject/text/html containing the code.

Without these values the stack is healthy, but users cannot receive login codes.

## Add provider workers

Workers are optional Compose profiles because each one needs an account-specific
enrollment token. First create the Signal and/or Telegram account through the
API. Then update `.env.production`:

```dotenv
SAY_WORKER_BASE_URL=wss://VM_NAME.exe.xyz
COMPOSE_PROFILES=signal,telegram
SAY_SIGNAL_ENROLLMENT_TOKEN=say_enroll_...
SAY_TELEGRAM_ENROLLMENT_TOKEN=say_enroll_...
TELEGRAM_API_ID=...
TELEGRAM_API_HASH=...
```

Run the same command again:

```sh
make deploy
```

This starts one isolated Signal worker and one isolated Telegram worker for the
two configured accounts. Their session state and unacknowledged events use
separate persistent Docker volumes. More users require additional worker
services or the planned supervisor; the supplied profiles intentionally model
one account each.

## Operations

```sh
make deploy-status
make deploy-logs
make deploy-down
```

`make deploy-down` preserves volumes. Do not use `docker compose down -v` unless
you intend to destroy Postgres and force provider relinking.

Back up `.env.production` securely and take encrypted, off-VM Postgres backups.
Re-running `make deploy` rebuilds changed images and reconciles the running
stack without regenerating secrets.
