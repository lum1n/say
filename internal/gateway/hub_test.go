package gateway_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vegard/say/internal/gateway"
	"github.com/vegard/say/internal/protocol"
)

func TestWorkerEventIsAcknowledgedAfterHandlerCompletes(t *testing.T) {
	store := gateway.NewStore()
	_, token, err := store.CreateAccount(t.Context(), "alice", "signal", "Signal")
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	handled := make(chan string, 1)
	hub := gateway.NewHub(
		store,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		func(_ context.Context, _ gateway.Account, event protocol.Envelope) error {
			handled <- event.ID
			return nil
		},
	)
	server := httptest.NewServer(http.HandlerFunc(hub.ServeWorker))
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(server.URL, "http"),
		nil,
	)
	if err != nil {
		t.Fatalf("dial worker: %v", err)
	}
	defer conn.Close()
	hello, _ := protocol.NewEnvelope(protocol.TypeHello, protocol.Hello{
		Token: token, Platform: "signal", Version: "test",
	})
	if err := conn.WriteJSON(hello); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	var ready protocol.Envelope
	if err := conn.ReadJSON(&ready); err != nil {
		t.Fatalf("read ready: %v", err)
	}
	event, _ := protocol.NewEnvelope(
		protocol.TypeAccountStatus,
		protocol.AccountStatus{State: "live"},
	)
	event.ID = "evt_test"
	event.Generation = ready.Generation
	if err := conn.WriteJSON(event); err != nil {
		t.Fatalf("write event: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var ack protocol.Envelope
	if err := conn.ReadJSON(&ack); err != nil {
		t.Fatalf("read event ack: %v", err)
	}
	if handledID := <-handled; handledID != event.ID {
		t.Fatalf("handled event = %q", handledID)
	}
	if ack.Type != protocol.TypeEventAck || ack.ID != event.ID || ack.Status != "ok" {
		t.Fatalf("ack = %#v", ack)
	}
}
