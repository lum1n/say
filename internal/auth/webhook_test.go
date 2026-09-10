package auth_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vegard/say/internal/auth"
)

func TestEmailServiceSenderPostsLoginCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("content-type = %q", r.Header.Get("Content-Type"))
		}
		if r.Header.Get("X-Internal-Api-Token") != "secret" {
			t.Fatalf("token = %q", r.Header.Get("X-Internal-Api-Token"))
		}
		if r.Header.Get("X-Internal-Actor") != "say" {
			t.Fatalf("actor = %q", r.Header.Get("X-Internal-Actor"))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if payload["from"] != "Say <noreply@example.com>" {
			t.Fatalf("from = %#v", payload["from"])
		}
		if payload["to"] != "alice@example.com" {
			t.Fatalf("to = %#v", payload["to"])
		}
		if payload["subject"] != "Your say login code" {
			t.Fatalf("subject = %#v", payload["subject"])
		}
		text, _ := payload["text"].(string)
		if !strings.Contains(text, "123456") {
			t.Fatalf("text = %#v", payload["text"])
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(server.Close)

	sender, err := auth.NewEmailServiceSender(server.URL+"/emails/send", "Say <noreply@example.com>", "secret")
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	sender.Client = server.Client()
	if err := sender.SendLoginCode(t.Context(), "alice@example.com", "123456"); err != nil {
		t.Fatalf("send login code: %v", err)
	}
}
