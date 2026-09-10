package worker_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vegard/say/internal/protocol"
	"github.com/vegard/say/internal/worker"
)

type testHandler struct{}

func (testHandler) HandleCommand(
	_ context.Context,
	envelope protocol.Envelope,
) (any, *protocol.Error) {
	return map[string]string{"handled": envelope.Type}, nil
}

func TestClientReplaysUnacknowledgedEventAfterReconnect(t *testing.T) {
	var connections atomic.Int32
	firstEventID := make(chan string, 1)
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var hello protocol.Envelope
		if err := conn.ReadJSON(&hello); err != nil {
			return
		}
		ready, _ := protocol.NewEnvelope(protocol.TypeReady, protocol.Ready{
			AccountID: "acc_test", WorkerID: "wrk_test", Generation: 1,
		})
		ready.Generation = 1
		if err := conn.WriteJSON(ready); err != nil {
			return
		}
		var event protocol.Envelope
		if err := conn.ReadJSON(&event); err != nil {
			return
		}
		if connections.Add(1) == 1 {
			firstEventID <- event.ID
			return
		}
		if event.ID != <-firstEventID {
			t.Errorf("replayed event has a different id: %q", event.ID)
			return
		}
		if err := conn.WriteJSON(protocol.Envelope{
			Version: protocol.Version, ID: event.ID, Type: protocol.TypeEventAck,
			Generation: 1, Status: "ok",
		}); err != nil {
			return
		}
		close(done)
	}))
	defer server.Close()

	event, _ := protocol.NewEnvelope(
		protocol.TypeAccountStatus,
		protocol.AccountStatus{State: "live"},
	)
	events := make(chan protocol.Envelope, 1)
	events <- event
	dataDir := t.TempDir()
	client, err := worker.New(worker.Config{
		Server:         "ws" + strings.TrimPrefix(server.URL, "http"),
		Platform:       "signal",
		Version:        "test/1",
		Token:          "say_enroll_test_token_long_enough",
		CredentialFile: filepath.Join(dataDir, "credential"),
		EventSpoolDir:  filepath.Join(dataDir, "events"),
		Events:         events,
		Handler:        testHandler{},
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- client.Run(ctx) }()
	select {
	case <-done:
		cancel()
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("event was not replayed")
	}
	select {
	case <-result:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop")
	}
}

func TestClientEnrollsHandlesCommandsAndPublishesEvents(t *testing.T) {
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		var hello protocol.Envelope
		if err := conn.ReadJSON(&hello); err != nil {
			return
		}
		ready, _ := protocol.NewEnvelope(protocol.TypeReady, protocol.Ready{
			AccountID:  "acc_test",
			WorkerID:   "wrk_test",
			Credential: "say_worker_persistent_test_credential",
			Generation: 1,
		})
		ready.Generation = 1
		if err := conn.WriteJSON(ready); err != nil {
			return
		}
		command, _ := protocol.NewEnvelope(protocol.TypeConversations, protocol.ConversationsCommand{})
		command.ID = "cmd_test"
		command.Generation = 1
		if err := conn.WriteJSON(command); err != nil {
			return
		}

		gotResponse := false
		gotEvent := false
		for !gotResponse || !gotEvent {
			var envelope protocol.Envelope
			if err := conn.ReadJSON(&envelope); err != nil {
				return
			}
			if envelope.ID == "cmd_test" && envelope.Status == "ok" {
				gotResponse = true
			}
			if envelope.Type == protocol.TypeAccountStatus && envelope.ID != "" {
				gotEvent = true
				if err := conn.WriteJSON(protocol.Envelope{
					Version:    protocol.Version,
					ID:         envelope.ID,
					Type:       protocol.TypeEventAck,
					Generation: 1,
					Status:     "ok",
				}); err != nil {
					return
				}
			}
		}
		close(done)
	}))
	defer server.Close()

	event, _ := protocol.NewEnvelope(protocol.TypeAccountStatus, protocol.AccountStatus{State: "live"})
	events := make(chan protocol.Envelope, 1)
	events <- event
	dataDir := t.TempDir()
	credentialFile := filepath.Join(dataDir, "worker-credential")
	spoolDir := filepath.Join(dataDir, "events")
	client, err := worker.New(worker.Config{
		Server:         "ws" + strings.TrimPrefix(server.URL, "http"),
		Platform:       "signal",
		Version:        "test/1",
		Token:          "say_enroll_test_token_long_enough",
		CredentialFile: credentialFile,
		EventSpoolDir:  spoolDir,
		Events:         events,
		Handler:        testHandler{},
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- client.Run(ctx) }()

	select {
	case <-done:
		deadline := time.Now().Add(time.Second)
		for {
			entries, readErr := os.ReadDir(spoolDir)
			if readErr == nil && len(entries) == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("event was not acknowledged from spool: entries=%d err=%v", len(entries), readErr)
			}
			time.Sleep(10 * time.Millisecond)
		}
		cancel()
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("worker exchange timed out")
	}
	select {
	case <-result:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop")
	}
	stored, err := os.ReadFile(credentialFile)
	if err != nil {
		t.Fatalf("read credential: %v", err)
	}
	if strings.TrimSpace(string(stored)) != "say_worker_persistent_test_credential" {
		t.Fatalf("stored credential = %q", stored)
	}
}
