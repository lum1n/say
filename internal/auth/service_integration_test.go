package auth_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/vegard/say/internal/auth"
	"github.com/vegard/say/internal/postgres"
)

func TestMagicLoginAndRefreshRotation(t *testing.T) {
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

	service, err := auth.NewService(database.Pool, auth.Config{
		Issuer:          "say-test",
		JWTSigningKey:   []byte("test-jwt-signing-key-with-32-bytes-minimum"),
		LoginHashKey:    []byte("test-login-hash-key-with-32-bytes-minimum"),
		DebugReturnCode: true,
		AccessLifetime:  time.Minute,
	}, nil)
	if err != nil {
		t.Fatalf("new auth service: %v", err)
	}

	if _, err := service.Start(ctx, "not-an-email"); !errors.Is(err, auth.ErrInvalidEmail) {
		t.Fatalf("invalid email error = %v", err)
	}
	login, err := service.Start(ctx, "Alice@example.com")
	if err != nil {
		t.Fatalf("start login: %v", err)
	}
	if login.DebugCode == "" {
		t.Fatal("development login did not return code")
	}
	if _, err := service.Verify(ctx, login.LoginID, "wrong"); !errors.Is(err, auth.ErrInvalidLogin) {
		t.Fatalf("wrong-code error = %v", err)
	}

	pair, err := service.Verify(ctx, login.LoginID, login.DebugCode)
	if err != nil {
		t.Fatalf("verify login: %v", err)
	}
	userID, err := service.ValidateAccessToken(pair.AccessToken)
	if err != nil || userID == "" {
		t.Fatalf("validate access token: user=%q err=%v", userID, err)
	}
	if _, err := service.Verify(ctx, login.LoginID, login.DebugCode); !errors.Is(err, auth.ErrInvalidLogin) {
		t.Fatalf("replayed-login error = %v", err)
	}

	rotated, err := service.Refresh(ctx, pair.RefreshToken)
	if err != nil {
		t.Fatalf("rotate refresh token: %v", err)
	}
	rotatedUserID, err := service.ValidateAccessToken(rotated.AccessToken)
	if err != nil || rotatedUserID != userID {
		t.Fatalf("rotated access token: user=%q err=%v", rotatedUserID, err)
	}
	if _, err := service.Refresh(context.Background(), pair.RefreshToken); !errors.Is(err, auth.ErrInvalidRefresh) {
		t.Fatalf("reused-refresh error = %v", err)
	}
}
