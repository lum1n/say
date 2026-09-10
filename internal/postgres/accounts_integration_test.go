package postgres_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/vegard/say/internal/gateway"
	"github.com/vegard/say/internal/postgres"
)

func TestPersistentAccountEnrollmentAndIsolation(t *testing.T) {
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
	if _, err := database.Pool.Exec(ctx, `
		INSERT INTO users (id, email)
		VALUES ('alice', 'alice@example.com'), ('bob', 'bob@example.com')
	`); err != nil {
		t.Fatalf("insert users: %v", err)
	}

	store := postgres.NewAccountStore(database.Pool)
	account, enrollment, err := store.CreateAccount(ctx, "alice", "signal", "Alice Signal")
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	if _, err := store.GetAccount(ctx, "bob", account.ID); !errors.Is(err, gateway.ErrNotFound) {
		t.Fatalf("cross-user read error = %v", err)
	}
	if _, err := store.AuthenticateWorker(ctx, enrollment, "telegram"); !errors.Is(err, gateway.ErrPlatformMismatch) {
		t.Fatalf("platform mismatch error = %v", err)
	}

	enrolled, err := store.AuthenticateWorker(ctx, enrollment, "signal")
	if err != nil {
		t.Fatalf("enroll worker: %v", err)
	}
	if enrolled.Credential == "" || enrolled.Account.Generation != 1 {
		t.Fatalf("enrolled worker = %#v", enrolled)
	}
	if _, err := store.AuthenticateWorker(ctx, enrollment, "signal"); !errors.Is(err, gateway.ErrInvalidToken) {
		t.Fatalf("reused enrollment error = %v", err)
	}

	reconnected, err := store.AuthenticateWorker(ctx, enrolled.Credential, "signal")
	if err != nil {
		t.Fatalf("reconnect worker: %v", err)
	}
	if reconnected.Credential != "" || reconnected.Account.Generation != enrolled.Account.Generation {
		t.Fatalf("reconnected worker = %#v", reconnected)
	}
	if err := store.SetWorkerStatus(ctx, account.ID, 1, "live"); err != nil {
		t.Fatalf("set live status: %v", err)
	}
	live, err := store.GetAccount(ctx, "alice", account.ID)
	if err != nil {
		t.Fatalf("get live account: %v", err)
	}
	if live.Status != "live" || live.LastSeenAt == nil {
		t.Fatalf("live account = %#v", live)
	}
	if err := store.DeleteAccount(ctx, "bob", account.ID); !errors.Is(err, gateway.ErrNotFound) {
		t.Fatalf("cross-user delete error = %v", err)
	}
	if err := store.DeleteAccount(ctx, "alice", account.ID); err != nil {
		t.Fatalf("delete own account: %v", err)
	}
}
