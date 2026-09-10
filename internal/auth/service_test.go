package auth_test

import (
	"errors"
	"testing"

	"github.com/vegard/say/internal/auth"
)

func TestServiceStartsWithoutDeliveryButLoginDoesNot(t *testing.T) {
	service, err := auth.NewService(nil, auth.Config{
		JWTSigningKey: []byte("test-jwt-signing-key-with-32-bytes-minimum"),
		LoginHashKey:  []byte("test-login-hash-key-with-32-bytes-minimum"),
	}, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	if _, err := service.Start(t.Context(), "alice@example.com"); !errors.Is(err, auth.ErrDeliveryUnavailable) {
		t.Fatalf("start login error = %v, want delivery unavailable", err)
	}
}
