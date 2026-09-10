CREATE TABLE users (
    id text PRIMARY KEY,
    email text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (email = lower(email))
);

CREATE TABLE auth_logins (
    id text PRIMARY KEY,
    email text NOT NULL,
    code_hash bytea NOT NULL,
    expires_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    consumed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX auth_logins_email_created_idx
    ON auth_logins (email, created_at DESC);

CREATE TABLE refresh_tokens (
    id text PRIMARY KEY,
    user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash bytea NOT NULL UNIQUE,
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    replaced_by text REFERENCES refresh_tokens(id),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX refresh_tokens_user_idx ON refresh_tokens (user_id);

CREATE TABLE accounts (
    id text PRIMARY KEY,
    user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    platform text NOT NULL CHECK (platform IN ('signal', 'telegram')),
    external_id text,
    display_name text NOT NULL DEFAULT '',
    status text NOT NULL DEFAULT 'unlinked'
        CHECK (status IN ('unlinked', 'linking', 'live', 'offline', 'upgrade_required')),
    hosting_mode text NOT NULL DEFAULT 'self_hosted'
        CHECK (hosting_mode IN ('desktop', 'self_hosted', 'hosted')),
    worker_id text,
    generation bigint NOT NULL DEFAULT 0 CHECK (generation >= 0),
    last_seen_at timestamptz,
    linked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX accounts_user_idx ON accounts (user_id, created_at);

CREATE TABLE pairing_tokens (
    id text PRIMARY KEY,
    account_id text NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    token_hash bytea NOT NULL UNIQUE,
    expires_at timestamptz NOT NULL,
    redeemed_at timestamptz,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE worker_credentials (
    id text PRIMARY KEY,
    account_id text NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    token_hash bytea NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz
);
CREATE UNIQUE INDEX worker_credentials_one_active_idx
    ON worker_credentials (account_id)
    WHERE revoked_at IS NULL;

CREATE TABLE conversations (
    id text PRIMARY KEY,
    account_id text NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    provider_id text NOT NULL,
    title text NOT NULL DEFAULT '',
    kind text NOT NULL DEFAULT 'direct',
    unread_count integer NOT NULL DEFAULT 0 CHECK (unread_count >= 0),
    last_message_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (account_id, provider_id)
);
CREATE INDEX conversations_account_activity_idx
    ON conversations (account_id, last_message_at DESC NULLS LAST);

CREATE TABLE messages (
    id text PRIMARY KEY,
    conversation_id text NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    provider_id text,
    author_id text NOT NULL DEFAULT '',
    author_name text NOT NULL DEFAULT '',
    body text NOT NULL DEFAULT '',
    sent_at timestamptz NOT NULL,
    direction text NOT NULL CHECK (direction IN ('incoming', 'outgoing')),
    status text NOT NULL DEFAULT 'sent'
        CHECK (status IN ('pending', 'sent', 'failed', 'unknown')),
    attachments jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX messages_provider_id_idx
    ON messages (conversation_id, provider_id)
    WHERE provider_id IS NOT NULL;
CREATE INDEX messages_conversation_time_idx
    ON messages (conversation_id, sent_at DESC);
CREATE INDEX messages_body_search_idx
    ON messages USING gin (to_tsvector('simple', body));

CREATE TABLE commands (
    id text PRIMARY KEY,
    account_id text NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    message_id text REFERENCES messages(id) ON DELETE SET NULL,
    idempotency_key text NOT NULL,
    command_type text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'dispatched', 'completed', 'failed', 'unknown', 'expired')),
    provider_result jsonb,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (account_id, idempotency_key)
);
CREATE INDEX commands_pending_idx
    ON commands (created_at)
    WHERE status IN ('pending', 'dispatched');

CREATE TABLE outbox (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    delivered_at timestamptz
);
CREATE INDEX outbox_pending_idx ON outbox (id) WHERE delivered_at IS NULL;

CREATE TABLE push_tokens (
    id text PRIMARY KEY,
    user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    platform text NOT NULL CHECK (platform IN ('apns', 'fcm', 'expo')),
    token text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ai_threads (
    id text PRIMARY KEY,
    user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ai_messages (
    id text PRIMARY KEY,
    thread_id text NOT NULL REFERENCES ai_threads(id) ON DELETE CASCADE,
    role text NOT NULL CHECK (role IN ('system', 'user', 'assistant')),
    content text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ai_messages_thread_idx ON ai_messages (thread_id, created_at);

CREATE TABLE audit_log (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id text REFERENCES users(id) ON DELETE SET NULL,
    action text NOT NULL,
    resource_type text NOT NULL,
    resource_id text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX audit_log_user_time_idx ON audit_log (user_id, created_at DESC);
