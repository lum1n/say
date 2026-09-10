package random

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

func Value(prefix string, bytes int) (string, error) {
	value := make([]byte, bytes)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("read random bytes: %w", err)
	}
	return prefix + hex.EncodeToString(value), nil
}
