package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/vegard/say/internal/postgres"
	"github.com/vegard/say/internal/protocol"
)

func TestMessageIngestAndIdempotentSend(t *testing.T) {
	databaseURL := os.Getenv("SAY_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SAY_TEST_DATABASE_URL is not set")
	}
	ctx := t.Context()
	database, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(database.Close)
	lock, err := database.Pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire test lock: %v", err)
	}
	if _, err := lock.Exec(ctx, "SELECT pg_advisory_lock(7295921002)"); err != nil {
		t.Fatalf("lock test database: %v", err)
	}
	t.Cleanup(func() {
		_, _ = lock.Exec(context.Background(), "SELECT pg_advisory_unlock(7295921002)")
		lock.Release()
	})
	if _, err := database.Pool.Exec(ctx, "TRUNCATE auth_logins, users CASCADE"); err != nil {
		t.Fatalf("clean database: %v", err)
	}
	if _, err := database.Pool.Exec(ctx,
		"INSERT INTO users (id, email) VALUES ('alice', 'alice@example.com')",
	); err != nil {
		t.Fatalf("insert user: %v", err)
	}

	accountStore := postgres.NewAccountStore(database.Pool)
	account, _, err := accountStore.CreateAccount(ctx, "alice", "signal", "Alice Signal")
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	messageStore := postgres.NewMessageStore(database.Pool)
	if err := messageStore.UpsertConversations(ctx, "alice", account.ID, []protocol.Conversation{{
		ProviderID: "user:+15551234567",
		Title:      "Bob",
		Kind:       "direct",
	}}); err != nil {
		t.Fatalf("sync conversations: %v", err)
	}
	conversations, err := messageStore.ListConversations(ctx, "alice", account.ID)
	if err != nil || len(conversations) != 1 {
		t.Fatalf("list conversations = %#v, err=%v", conversations, err)
	}

	incoming := protocol.Message{
		ProviderMessageID: "1720000000000",
		ConversationID:    "user:+15551234567",
		AuthorID:          "+15551234567",
		AuthorName:        "Bob",
		Body:              "hello",
		SentAt:            time.UnixMilli(1720000000000).UTC(),
		Direction:         "incoming",
	}
	if _, inserted, err := messageStore.IngestMessage(ctx, account, incoming); err != nil || !inserted {
		t.Fatalf("first ingest inserted=%v err=%v", inserted, err)
	}
	if _, inserted, err := messageStore.IngestMessage(ctx, account, incoming); err != nil || inserted {
		t.Fatalf("duplicate ingest inserted=%v err=%v", inserted, err)
	}

	prepared, err := messageStore.PrepareSend(
		ctx,
		"alice",
		conversations[0].ID,
		"reply",
		"idempotency-key-0001",
	)
	if err != nil || !prepared.Created {
		t.Fatalf("prepare send = %#v, err=%v", prepared, err)
	}
	if dispatched, err := messageStore.MarkCommandDispatched(ctx, prepared.CommandID); err != nil || !dispatched {
		t.Fatalf("mark dispatched = %v, err=%v", dispatched, err)
	}
	repeated, err := messageStore.PrepareSend(
		ctx,
		"alice",
		conversations[0].ID,
		"reply",
		"idempotency-key-0001",
	)
	if err != nil || repeated.Created || repeated.CommandID != prepared.CommandID {
		t.Fatalf("repeated send = %#v, err=%v", repeated, err)
	}
	if _, err := messageStore.CompleteSend(
		ctx,
		"alice",
		prepared.CommandID,
		"1720000000001",
		time.UnixMilli(1720000000001).UTC(),
	); err != nil {
		t.Fatalf("complete send: %v", err)
	}
	messages, err := messageStore.ListMessages(ctx, "alice", conversations[0].ID)
	if err != nil || len(messages) != 2 {
		t.Fatalf("list messages = %#v, err=%v", messages, err)
	}

	var unread int
	if err := database.Pool.QueryRow(ctx,
		"SELECT unread_count FROM conversations WHERE id = $1",
		conversations[0].ID,
	).Scan(&unread); err != nil || unread != 1 {
		t.Fatalf("unread count = %d, err=%v", unread, err)
	}
}
