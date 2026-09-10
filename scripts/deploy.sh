#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
env_file=${SAY_DEPLOY_ENV_FILE:-"$repo_root/.env.production"}
compose_file="$repo_root/deploy/production.compose.yaml"

if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose is required" >&2
  exit 1
fi

random_hex() {
  od -An -N 32 -tx1 /dev/urandom | tr -d ' \n'
}

if [ ! -f "$env_file" ]; then
  vm_name=$(hostname -s)
  worker_base_url=${SAY_WORKER_BASE_URL:-"wss://${vm_name}.exe.xyz"}
  umask 077
  cat >"$env_file" <<EOF
POSTGRES_PASSWORD=$(random_hex)
SAY_JWT_SIGNING_KEY=$(random_hex)
SAY_LOGIN_HASH_KEY=$(random_hex)
SAY_PUBLIC_PORT=8000
SAY_WORKER_BASE_URL=$worker_base_url
SAY_JWT_ISSUER=say
SAY_MESSAGE_RETENTION_DAYS=30
SAY_MESSAGE_KEEP_PER_CONVERSATION=500
SAY_AUTH_WEBHOOK_URL=
SAY_AUTH_WEBHOOK_TOKEN=
SAY_AUTH_EMAIL_FROM=
COMPOSE_PROFILES=
SAY_SIGNAL_ENROLLMENT_TOKEN=
SAY_TELEGRAM_ENROLLMENT_TOKEN=
SIGNAL_LINK_NAME=say
TELEGRAM_API_ID=
TELEGRAM_API_HASH=
TELEGRAM_DB_ENCRYPTION_KEY=
EOF
  echo "Created $env_file with new deployment secrets."
fi
chmod 600 "$env_file"

env_value() {
  key=$1
  value=$(awk -F= -v key="$key" '$1 == key { sub(/^[^=]*=/, ""); print; exit }' "$env_file")
  override=$(printenv "$key" 2>/dev/null || true)
  if [ -n "$override" ]; then
    printf '%s' "$override"
  else
    printf '%s' "$value"
  fi
}

profiles=$(env_value COMPOSE_PROFILES)
worker_base_url=$(env_value SAY_WORKER_BASE_URL)
case ",$profiles," in
  *,signal,*)
    if [ -z "$worker_base_url" ] || [ -z "$(env_value SAY_SIGNAL_ENROLLMENT_TOKEN)" ]; then
      echo "The signal profile requires SAY_WORKER_BASE_URL and SAY_SIGNAL_ENROLLMENT_TOKEN." >&2
      exit 1
    fi
    ;;
esac
case ",$profiles," in
  *,telegram,*)
    if [ -z "$worker_base_url" ] ||
      [ -z "$(env_value SAY_TELEGRAM_ENROLLMENT_TOKEN)" ] ||
      [ -z "$(env_value TELEGRAM_API_ID)" ] ||
      [ -z "$(env_value TELEGRAM_API_HASH)" ]; then
      echo "The telegram profile requires its enrollment token and Telegram API credentials." >&2
      exit 1
    fi
    ;;
esac

COMPOSE_PROFILES=$profiles docker compose \
  --env-file "$env_file" \
  -f "$compose_file" \
  config --quiet

if ! COMPOSE_PROFILES=$profiles docker compose \
  --env-file "$env_file" \
  -f "$compose_file" \
  up -d --build --remove-orphans --wait --wait-timeout 180; then
  echo "Deployment failed; current service state and startup logs follow." >&2
  COMPOSE_PROFILES=$profiles docker compose \
    --env-file "$env_file" \
    -f "$compose_file" \
    ps -a >&2 || true
  COMPOSE_PROFILES=$profiles docker compose \
    --env-file "$env_file" \
    -f "$compose_file" \
    logs --tail=100 api postgres >&2 || true
  exit 1
fi

echo
COMPOSE_PROFILES=$profiles docker compose \
  --env-file "$env_file" \
  -f "$compose_file" \
  ps

if [ -z "$(env_value SAY_AUTH_WEBHOOK_URL)" ]; then
  echo
  echo "WARNING: SAY_AUTH_WEBHOOK_URL is empty; production login codes will not be delivered." >&2
  echo "Set it in $env_file and rerun 'make deploy'." >&2
fi

echo
echo "say is healthy on http://127.0.0.1:$(env_value SAY_PUBLIC_PORT)/healthz"
