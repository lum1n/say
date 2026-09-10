package signalcli_test

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/vegard/say/internal/signalcli"
)

func TestClientCallsAndReceivesNotifications(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { _ = serverConn.Close() })
	client := signalcli.NewClient(clientConn)
	t.Cleanup(func() { _ = client.Close() })

	go func() {
		decoder := json.NewDecoder(serverConn)
		encoder := json.NewEncoder(serverConn)
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := decoder.Decode(&request); err != nil {
			return
		}
		_ = encoder.Encode(map[string]any{
			"jsonrpc": "2.0",
			"method":  "receive",
			"params": map[string]any{
				"envelope": map[string]any{"timestamp": 123},
			},
		})
		_ = encoder.Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      request.ID,
			"result":  map[string]any{"timestamp": 456},
		})
	}()

	var result struct {
		Timestamp int64 `json:"timestamp"`
	}
	if err := client.Call(t.Context(), "send", map[string]any{
		"recipient": []string{"+15551234567"},
		"message":   "hello",
	}, &result); err != nil {
		t.Fatalf("call: %v", err)
	}
	if result.Timestamp != 456 {
		t.Fatalf("timestamp = %d, want 456", result.Timestamp)
	}
	select {
	case notification := <-client.Notifications():
		if notification.Method != "receive" {
			t.Fatalf("notification method = %q", notification.Method)
		}
	case <-time.After(time.Second):
		t.Fatal("notification was not delivered")
	}
}

func TestClientReturnsJSONRPCError(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { _ = serverConn.Close() })
	client := signalcli.NewClient(clientConn)
	t.Cleanup(func() { _ = client.Close() })

	go func() {
		decoder := json.NewDecoder(serverConn)
		encoder := json.NewEncoder(serverConn)
		var request struct {
			ID json.RawMessage `json:"id"`
		}
		if err := decoder.Decode(&request); err != nil {
			return
		}
		_ = encoder.Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      request.ID,
			"error": map[string]any{
				"code":    -1,
				"message": "send failed",
			},
		})
	}()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	err := client.Call(ctx, "send", map[string]any{}, nil)
	if err == nil {
		t.Fatal("call unexpectedly succeeded")
	}
}
