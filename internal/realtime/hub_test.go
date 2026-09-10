package realtime_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vegard/say/internal/protocol"
	"github.com/vegard/say/internal/realtime"
)

func TestPublishIsScopedToUser(t *testing.T) {
	hub := realtime.NewHub()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hub.Serve(w, r, r.URL.Query().Get("user"))
	}))
	defer server.Close()
	baseURL := "ws" + strings.TrimPrefix(server.URL, "http")

	alice, _, err := websocket.DefaultDialer.Dial(baseURL+"?user=alice", nil)
	if err != nil {
		t.Fatalf("dial alice: %v", err)
	}
	defer alice.Close()
	bob, _, err := websocket.DefaultDialer.Dial(baseURL+"?user=bob", nil)
	if err != nil {
		t.Fatalf("dial bob: %v", err)
	}
	defer bob.Close()
	subscribe(t, alice)
	subscribe(t, bob)

	if err := hub.Publish("alice", protocol.TypeMessageNew, map[string]string{"body": "hello"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	_ = alice.SetReadDeadline(time.Now().Add(time.Second))
	var event protocol.Envelope
	if err := alice.ReadJSON(&event); err != nil {
		t.Fatalf("read alice event: %v", err)
	}
	if event.Type != protocol.TypeMessageNew {
		t.Fatalf("event type = %q", event.Type)
	}

	_ = bob.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	if err := bob.ReadJSON(&event); err == nil {
		t.Fatal("bob received alice's event")
	}
}

func subscribe(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	envelope, _ := protocol.NewEnvelope("subscribe", map[string]any{})
	envelope.ID = "subscribe-test"
	if err := conn.WriteJSON(envelope); err != nil {
		t.Fatalf("write subscribe: %v", err)
	}
	var ack protocol.Envelope
	if err := conn.ReadJSON(&ack); err != nil {
		t.Fatalf("read subscribe ack: %v", err)
	}
}
