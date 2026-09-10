package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vegard/say/internal/gateway"
	"github.com/vegard/say/internal/protocol"
	"github.com/vegard/say/internal/random"
)

type Conversation struct {
	ID            string     `json:"id"`
	AccountID     string     `json:"account_id"`
	ProviderID    string     `json:"provider_id"`
	Title         string     `json:"title"`
	Kind          string     `json:"kind"`
	UnreadCount   int        `json:"unread_count"`
	LastMessageAt *time.Time `json:"last_message_at,omitempty"`
}

type Message struct {
	ID             string    `json:"id"`
	ConversationID string    `json:"conversation_id"`
	ProviderID     string    `json:"provider_id,omitempty"`
	AuthorName     string    `json:"author_name"`
	Body           string    `json:"body"`
	SentAt         time.Time `json:"sent_at"`
	Direction      string    `json:"direction"`
	Status         string    `json:"status"`
}

type SendPreparation struct {
	AccountID              string
	ProviderConversationID string
	CommandID              string
	CommandStatus          string
	Message                Message
	Created                bool
}

type ConversationTarget struct {
	AccountID  string
	ProviderID string
	Platform   string
}

type MessageStore struct {
	pool *pgxpool.Pool
}

type OutboxEvent struct {
	ID      int64
	UserID  string
	Type    string
	Payload json.RawMessage
}

func NewMessageStore(pool *pgxpool.Pool) *MessageStore {
	return &MessageStore{pool: pool}
}

func (s *MessageStore) IngestMessage(
	ctx context.Context,
	account gateway.Account,
	message protocol.Message,
) (Message, bool, error) {
	return s.ingestMessage(ctx, account, message, true)
}

func (s *MessageStore) CacheMessage(
	ctx context.Context,
	account gateway.Account,
	message protocol.Message,
) (Message, bool, error) {
	return s.ingestMessage(ctx, account, message, false)
}

func (s *MessageStore) ingestMessage(
	ctx context.Context,
	account gateway.Account,
	message protocol.Message,
	live bool,
) (Message, bool, error) {
	if message.ProviderMessageID == "" || message.ConversationID == "" || message.SentAt.IsZero() {
		return Message{}, false, errors.New("provider message id, conversation id, and sent time are required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, false, fmt.Errorf("begin message ingest: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	conversationID, err := random.Value("con_", 16)
	if err != nil {
		return Message{}, false, err
	}
	title := message.AuthorName
	kind := "direct"
	if strings.HasPrefix(message.ConversationID, "group:") ||
		(account.Platform == "telegram" && strings.HasPrefix(message.ConversationID, "-")) {
		title = strings.ToUpper(account.Platform[:1]) + account.Platform[1:] + " group"
		kind = "group"
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO conversations (
			id, account_id, provider_id, title, kind, last_message_at
		)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (account_id, provider_id) DO UPDATE
		SET last_message_at = CASE
		        WHEN conversations.last_message_at IS NULL
		          OR EXCLUDED.last_message_at > conversations.last_message_at
		        THEN EXCLUDED.last_message_at
		        ELSE conversations.last_message_at
		    END,
		    updated_at = now()
		RETURNING id
	`, conversationID, account.ID, message.ConversationID, title, kind, message.SentAt).Scan(&conversationID)
	if err != nil {
		return Message{}, false, fmt.Errorf("upsert conversation: %w", err)
	}

	cached := Message{
		ConversationID: conversationID,
		ProviderID:     message.ProviderMessageID,
		AuthorName:     message.AuthorName,
		Body:           message.Body,
		SentAt:         message.SentAt,
		Direction:      message.Direction,
		Status:         "sent",
	}
	cached.ID, err = random.Value("msg_", 16)
	if err != nil {
		return Message{}, false, err
	}
	inserted := true
	err = tx.QueryRow(ctx, `
		INSERT INTO messages (
			id, conversation_id, provider_id, author_id, author_name,
			body, sent_at, direction, status
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'sent')
		ON CONFLICT (conversation_id, provider_id)
			WHERE provider_id IS NOT NULL
			DO NOTHING
		RETURNING id
	`, cached.ID, conversationID, cached.ProviderID, message.AuthorID, cached.AuthorName,
		cached.Body, cached.SentAt, cached.Direction).Scan(&cached.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		inserted = false
		err = scanMessage(tx.QueryRow(ctx, `
			SELECT id, conversation_id, COALESCE(provider_id, ''), author_name,
			       body, sent_at, direction, status
			FROM messages
			WHERE conversation_id = $1 AND provider_id = $2
		`, conversationID, cached.ProviderID), &cached)
	}
	if err != nil {
		return Message{}, false, fmt.Errorf("insert message: %w", err)
	}
	if !inserted {
		if err := tx.Commit(ctx); err != nil {
			return Message{}, false, fmt.Errorf("commit duplicate ingest: %w", err)
		}
		return cached, false, nil
	}

	if live && cached.Direction == "incoming" {
		if _, err := tx.Exec(ctx, `
			UPDATE conversations SET unread_count = unread_count + 1 WHERE id = $1
		`, conversationID); err != nil {
			return Message{}, false, fmt.Errorf("increment unread count: %w", err)
		}
	}
	if live {
		if err := insertOutbox(ctx, tx, account.UserID, protocol.TypeMessageNew, cached); err != nil {
			return Message{}, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Message{}, false, fmt.Errorf("commit message ingest: %w", err)
	}
	return cached, true, nil
}

func (s *MessageStore) UpsertConversations(
	ctx context.Context,
	userID, accountID string,
	conversations []protocol.Conversation,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin conversation sync: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var ownsAccount bool
	if err := tx.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM accounts WHERE id = $1 AND user_id = $2)",
		accountID, userID,
	).Scan(&ownsAccount); err != nil {
		return fmt.Errorf("check account ownership: %w", err)
	}
	if !ownsAccount {
		return gateway.ErrNotFound
	}
	for _, conversation := range conversations {
		if conversation.ProviderID == "" {
			continue
		}
		id, err := random.Value("con_", 16)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO conversations (id, account_id, provider_id, title, kind)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (account_id, provider_id) DO UPDATE
			SET title = EXCLUDED.title, kind = EXCLUDED.kind, updated_at = now()
		`, id, accountID, conversation.ProviderID, conversation.Title, conversation.Kind); err != nil {
			return fmt.Errorf("upsert synced conversation: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit conversation sync: %w", err)
	}
	return nil
}

func (s *MessageStore) ListConversations(
	ctx context.Context,
	userID, accountID string,
) ([]Conversation, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT c.id, c.account_id, c.provider_id, c.title, c.kind,
		       c.unread_count, c.last_message_at
		FROM conversations c
		JOIN accounts a ON a.id = c.account_id
		WHERE c.account_id = $1 AND a.user_id = $2
		ORDER BY c.last_message_at DESC NULLS LAST, c.title, c.id
		LIMIT 100
	`, accountID, userID)
	if err != nil {
		return nil, fmt.Errorf("list conversations: %w", err)
	}
	defer rows.Close()
	result := make([]Conversation, 0)
	for rows.Next() {
		var conversation Conversation
		if err := rows.Scan(
			&conversation.ID,
			&conversation.AccountID,
			&conversation.ProviderID,
			&conversation.Title,
			&conversation.Kind,
			&conversation.UnreadCount,
			&conversation.LastMessageAt,
		); err != nil {
			return nil, fmt.Errorf("scan conversation: %w", err)
		}
		result = append(result, conversation)
	}
	return result, rows.Err()
}

func (s *MessageStore) ListMessages(
	ctx context.Context,
	userID, conversationID string,
) ([]Message, error) {
	var ownsConversation bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM conversations c
			JOIN accounts a ON a.id = c.account_id
			WHERE c.id = $1 AND a.user_id = $2
		)
	`, conversationID, userID).Scan(&ownsConversation); err != nil {
		return nil, fmt.Errorf("check conversation ownership: %w", err)
	}
	if !ownsConversation {
		return nil, gateway.ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `
		SELECT m.id, m.conversation_id, COALESCE(m.provider_id, ''), m.author_name,
		       m.body, m.sent_at, m.direction, m.status
		FROM messages m
		JOIN conversations c ON c.id = m.conversation_id
		JOIN accounts a ON a.id = c.account_id
		WHERE m.conversation_id = $1 AND a.user_id = $2
		ORDER BY m.sent_at DESC, m.id DESC
		LIMIT 100
	`, conversationID, userID)
	if err != nil {
		return nil, fmt.Errorf("list messages: %w", err)
	}
	defer rows.Close()
	result := make([]Message, 0)
	for rows.Next() {
		var message Message
		if err := scanMessage(rows, &message); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		result = append(result, message)
	}
	return result, rows.Err()
}

func (s *MessageStore) GetConversationTarget(
	ctx context.Context,
	userID, conversationID string,
) (ConversationTarget, error) {
	var target ConversationTarget
	err := s.pool.QueryRow(ctx, `
		SELECT c.account_id, c.provider_id, a.platform
		FROM conversations c
		JOIN accounts a ON a.id = c.account_id
		WHERE c.id = $1 AND a.user_id = $2
	`, conversationID, userID).Scan(
		&target.AccountID,
		&target.ProviderID,
		&target.Platform,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ConversationTarget{}, gateway.ErrNotFound
	}
	if err != nil {
		return ConversationTarget{}, fmt.Errorf("get conversation target: %w", err)
	}
	return target, nil
}

func (s *MessageStore) PrepareSend(
	ctx context.Context,
	userID, conversationID, body, idempotencyKey string,
) (SendPreparation, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SendPreparation{}, fmt.Errorf("begin send: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var accountID, providerConversationID string
	err = tx.QueryRow(ctx, `
		SELECT c.account_id, c.provider_id
		FROM conversations c
		JOIN accounts a ON a.id = c.account_id
		WHERE c.id = $1 AND a.user_id = $2
	`, conversationID, userID).Scan(&accountID, &providerConversationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return SendPreparation{}, gateway.ErrNotFound
	}
	if err != nil {
		return SendPreparation{}, fmt.Errorf("find send account: %w", err)
	}

	commandID, err := random.Value("cmd_", 16)
	if err != nil {
		return SendPreparation{}, err
	}
	messageID, err := random.Value("msg_", 16)
	if err != nil {
		return SendPreparation{}, err
	}
	payload, err := json.Marshal(protocol.SendCommand{
		ConversationID: providerConversationID,
		Body:           body,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return SendPreparation{}, err
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO commands (
			id, account_id, idempotency_key, command_type, payload, expires_at
		)
		VALUES ($1, $2, $3, 'send', $4::jsonb, now() + interval '5 minutes')
		ON CONFLICT (account_id, idempotency_key) DO NOTHING
	`, commandID, accountID, idempotencyKey, string(payload))
	if err != nil {
		return SendPreparation{}, fmt.Errorf("insert send command: %w", err)
	}
	if tag.RowsAffected() == 0 {
		_ = tx.Rollback(ctx)
		return s.existingSend(ctx, accountID, idempotencyKey)
	}

	message := Message{
		ID:             messageID,
		ConversationID: conversationID,
		AuthorName:     "You",
		Body:           body,
		SentAt:         time.Now().UTC(),
		Direction:      "outgoing",
		Status:         "pending",
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO messages (
			id, conversation_id, author_id, author_name, body,
			sent_at, direction, status
		)
		VALUES ($1, $2, 'self', 'You', $3, $4, 'outgoing', 'pending')
	`, message.ID, message.ConversationID, message.Body, message.SentAt); err != nil {
		return SendPreparation{}, fmt.Errorf("insert pending message: %w", err)
	}
	if _, err := tx.Exec(ctx,
		"UPDATE commands SET message_id = $2 WHERE id = $1",
		commandID, message.ID,
	); err != nil {
		return SendPreparation{}, fmt.Errorf("link command message: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE conversations
		SET last_message_at = $2, updated_at = now()
		WHERE id = $1
	`, conversationID, message.SentAt); err != nil {
		return SendPreparation{}, fmt.Errorf("update conversation activity: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return SendPreparation{}, fmt.Errorf("commit send: %w", err)
	}
	return SendPreparation{
		AccountID:              accountID,
		ProviderConversationID: providerConversationID,
		CommandID:              commandID,
		CommandStatus:          "pending",
		Message:                message,
		Created:                true,
	}, nil
}

func (s *MessageStore) MarkCommandDispatched(ctx context.Context, commandID string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE commands
		SET status = 'dispatched', updated_at = now()
		WHERE id = $1 AND status = 'pending' AND expires_at > now()
	`, commandID)
	if err != nil {
		return false, fmt.Errorf("mark command dispatched: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *MessageStore) CompleteSend(
	ctx context.Context,
	userID, commandID, providerMessageID string,
	sentAt time.Time,
) (Message, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, fmt.Errorf("begin complete send: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var messageID, conversationID string
	err = tx.QueryRow(ctx, `
		SELECT cmd.message_id, m.conversation_id
		FROM commands cmd
		JOIN accounts a ON a.id = cmd.account_id
		JOIN messages m ON m.id = cmd.message_id
		WHERE cmd.id = $1 AND a.user_id = $2
		FOR UPDATE OF cmd, m
	`, commandID, userID).Scan(&messageID, &conversationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Message{}, gateway.ErrNotFound
	}
	if err != nil {
		return Message{}, fmt.Errorf("read send command: %w", err)
	}
	if providerMessageID != "" {
		if _, err := tx.Exec(ctx, `
			DELETE FROM messages
			WHERE conversation_id = $1 AND provider_id = $2 AND id <> $3
		`, conversationID, providerMessageID, messageID); err != nil {
			return Message{}, fmt.Errorf("reconcile sent message: %w", err)
		}
	}
	var message Message
	err = scanMessage(tx.QueryRow(ctx, `
		UPDATE messages
		SET provider_id = NULLIF($2, ''), sent_at = $3, status = 'sent'
		WHERE id = $1
		RETURNING id, conversation_id, COALESCE(provider_id, ''), author_name,
		          body, sent_at, direction, status
	`, messageID, providerMessageID, sentAt), &message)
	if err != nil {
		return Message{}, fmt.Errorf("complete message: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE commands
		SET status = 'completed',
		    provider_result = jsonb_build_object('provider_message_id', $2),
		    updated_at = now()
		WHERE id = $1
	`, commandID, providerMessageID); err != nil {
		return Message{}, fmt.Errorf("complete command: %w", err)
	}
	if err := insertOutbox(ctx, tx, userID, "message.updated", message); err != nil {
		return Message{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Message{}, fmt.Errorf("commit completed send: %w", err)
	}
	return message, nil
}

func (s *MessageStore) FailSend(
	ctx context.Context,
	userID, commandID, code string,
) (Message, error) {
	status := "failed"
	if code == "worker_timeout" || code == "signal_send_unknown" {
		status = "unknown"
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, fmt.Errorf("begin fail send: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var messageID string
	err = tx.QueryRow(ctx, `
		SELECT cmd.message_id
		FROM commands cmd
		JOIN accounts a ON a.id = cmd.account_id
		WHERE cmd.id = $1 AND a.user_id = $2
		FOR UPDATE OF cmd
	`, commandID, userID).Scan(&messageID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Message{}, gateway.ErrNotFound
	}
	if err != nil {
		return Message{}, fmt.Errorf("read failed command: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE commands
		SET status = $2,
		    provider_result = jsonb_build_object('error_code', $3::text),
		    updated_at = now()
		WHERE id = $1
	`, commandID, status, code); err != nil {
		return Message{}, fmt.Errorf("fail command: %w", err)
	}
	var message Message
	err = scanMessage(tx.QueryRow(ctx, `
		UPDATE messages SET status = $2 WHERE id = $1
		RETURNING id, conversation_id, COALESCE(provider_id, ''), author_name,
		          body, sent_at, direction, status
	`, messageID, status), &message)
	if err != nil {
		return Message{}, fmt.Errorf("fail message: %w", err)
	}
	if err := insertOutbox(ctx, tx, userID, "message.updated", message); err != nil {
		return Message{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Message{}, fmt.Errorf("commit failed send: %w", err)
	}
	return message, nil
}

func (s *MessageStore) existingSend(
	ctx context.Context,
	accountID, idempotencyKey string,
) (SendPreparation, error) {
	var result SendPreparation
	result.AccountID = accountID
	err := scanMessageAndCommand(s.pool.QueryRow(ctx, `
		SELECT cmd.id, cmd.status, c.provider_id, m.id, m.conversation_id, COALESCE(m.provider_id, ''),
		       m.author_name, m.body, m.sent_at, m.direction, m.status
		FROM commands cmd
		JOIN messages m ON m.id = cmd.message_id
		JOIN conversations c ON c.id = m.conversation_id
		WHERE cmd.account_id = $1 AND cmd.idempotency_key = $2
	`, accountID, idempotencyKey), &result)
	if err != nil {
		return SendPreparation{}, fmt.Errorf("read idempotent send: %w", err)
	}
	result.Created = result.CommandStatus == "pending"
	return result, nil
}

func (s *MessageStore) PendingOutbox(ctx context.Context, limit int) ([]OutboxEvent, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, user_id, event_type, payload
		FROM outbox
		WHERE delivered_at IS NULL
		ORDER BY id
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list pending outbox: %w", err)
	}
	defer rows.Close()
	events := make([]OutboxEvent, 0)
	for rows.Next() {
		var event OutboxEvent
		if err := rows.Scan(&event.ID, &event.UserID, &event.Type, &event.Payload); err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *MessageStore) MarkOutboxDelivered(ctx context.Context, id int64) error {
	if _, err := s.pool.Exec(ctx,
		"UPDATE outbox SET delivered_at = now() WHERE id = $1 AND delivered_at IS NULL",
		id,
	); err != nil {
		return fmt.Errorf("mark outbox delivered: %w", err)
	}
	return nil
}

func (s *MessageStore) Prune(
	ctx context.Context,
	retention time.Duration,
	keepPerConversation int,
) (int64, error) {
	if retention <= 0 || keepPerConversation < 0 {
		return 0, errors.New("retention must be positive and keep count cannot be negative")
	}
	tag, err := s.pool.Exec(ctx, `
		WITH ranked AS (
			SELECT id, sent_at,
			       row_number() OVER (
			           PARTITION BY conversation_id
			           ORDER BY sent_at DESC, id DESC
			       ) AS position
			FROM messages
			WHERE status <> 'pending'
		)
		DELETE FROM messages m
		USING ranked r
		WHERE m.id = r.id
		  AND r.sent_at < $1
		  AND r.position > $2
	`, time.Now().UTC().Add(-retention), keepPerConversation)
	if err != nil {
		return 0, fmt.Errorf("prune messages: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `
		DELETE FROM outbox
		WHERE delivered_at IS NOT NULL
		  AND delivered_at < now() - interval '7 days'
	`); err != nil {
		return 0, fmt.Errorf("prune outbox: %w", err)
	}
	return tag.RowsAffected(), nil
}

func insertOutbox(
	ctx context.Context,
	tx pgx.Tx,
	userID, eventType string,
	payload any,
) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode outbox event: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox (user_id, event_type, payload)
		VALUES ($1, $2, $3::jsonb)
	`, userID, eventType, string(encoded)); err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

func scanMessage(row scanner, message *Message) error {
	return row.Scan(
		&message.ID,
		&message.ConversationID,
		&message.ProviderID,
		&message.AuthorName,
		&message.Body,
		&message.SentAt,
		&message.Direction,
		&message.Status,
	)
}

func scanMessageAndCommand(row scanner, result *SendPreparation) error {
	return row.Scan(
		&result.CommandID,
		&result.CommandStatus,
		&result.ProviderConversationID,
		&result.Message.ID,
		&result.Message.ConversationID,
		&result.Message.ProviderID,
		&result.Message.AuthorName,
		&result.Message.Body,
		&result.Message.SentAt,
		&result.Message.Direction,
		&result.Message.Status,
	)
}
