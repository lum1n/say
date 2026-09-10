package httpapi_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vegard/say/internal/gateway"
	"github.com/vegard/say/internal/httpapi"
	"github.com/vegard/say/internal/protocol"
)

type createAccountResponse struct {
	Account    gateway.Account `json:"account"`
	Enrollment struct {
		Token string `json:"token"`
	} `json:"enrollment"`
}

func TestAccountsAreScopedToUser(t *testing.T) {
	server, _, _ := newTestServer(t)

	created := createAccount(t, server.URL, "alice", "signal")

	response := request(t, http.MethodGet, server.URL+"/v1/accounts/"+created.Account.ID, nil, "bob")
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("bob reading alice's account: got %d, want 404", response.StatusCode)
	}

	response = request(t, http.MethodGet, server.URL+"/v1/accounts", nil, "bob")
	defer response.Body.Close()
	var listed struct {
		Accounts []gateway.Account `json:"accounts"`
	}
	decodeResponse(t, response, &listed)
	if len(listed.Accounts) != 0 {
		t.Fatalf("bob listed %d of alice's accounts", len(listed.Accounts))
	}

	response = request(t, http.MethodGet, server.URL+"/v1/accounts/"+created.Account.ID, nil, "alice")
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("alice reading own account: got %d, want 200", response.StatusCode)
	}
}

func TestWorkerEnrollsRejectsDuplicateAndReconnects(t *testing.T) {
	server, store, _ := newTestServer(t)
	created := createAccount(t, server.URL, "alice", "signal")
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/workers/connect"

	first := dialWorker(t, wsURL, created.Enrollment.Token, "signal")
	defer first.conn.Close()
	if first.ready.Credential == "" {
		t.Fatal("first enrollment did not return a durable worker credential")
	}

	duplicateConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial duplicate worker: %v", err)
	}
	duplicateHello, _ := protocol.NewEnvelope(protocol.TypeHello, protocol.Hello{
		Token:    first.ready.Credential,
		Platform: "signal",
		Version:  "test/1",
	})
	if err := duplicateConn.WriteJSON(duplicateHello); err != nil {
		t.Fatalf("write duplicate hello: %v", err)
	}
	var duplicateResponse protocol.Envelope
	if err := duplicateConn.ReadJSON(&duplicateResponse); err != nil {
		t.Fatalf("read duplicate response: %v", err)
	}
	_ = duplicateConn.Close()
	if duplicateResponse.Error == nil || duplicateResponse.Error.Code != "already_connected" {
		t.Fatalf("duplicate response = %#v, want already_connected", duplicateResponse)
	}

	_ = first.conn.Close()
	waitFor(t, time.Second, func() bool {
		account, err := store.GetAccount(t.Context(), "alice", created.Account.ID)
		return err == nil && account.Status == "offline"
	})

	second := dialWorker(t, wsURL, first.ready.Credential, "signal")
	defer second.conn.Close()
	if second.ready.Credential != "" {
		t.Fatal("reconnect unexpectedly rotated worker credential")
	}
	if second.ready.Generation != first.ready.Generation {
		t.Fatalf("reconnect generation = %d, want %d", second.ready.Generation, first.ready.Generation)
	}
}

func TestLinkCommandIsOwnerScopedAndRoutedToWorker(t *testing.T) {
	server, _, _ := newTestServer(t)
	created := createAccount(t, server.URL, "alice", "signal")
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/workers/connect"
	worker := dialWorker(t, wsURL, created.Enrollment.Token, "signal")
	defer worker.conn.Close()

	workerResult := make(chan error, 1)
	go func() {
		var command protocol.Envelope
		if err := worker.conn.ReadJSON(&command); err != nil {
			workerResult <- err
			return
		}
		payload, err := json.Marshal(protocol.LinkResult{
			State: "qr_required",
			QRURI: "sgnl://linkdevice?uuid=test",
		})
		if err != nil {
			workerResult <- err
			return
		}
		workerResult <- worker.conn.WriteJSON(protocol.Envelope{
			Version:    protocol.Version,
			ID:         command.ID,
			Type:       command.Type,
			Generation: worker.ready.Generation,
			Status:     "ok",
			Payload:    payload,
		})
	}()

	body := []byte(`{"action":"start","device_name":"say test"}`)
	response := request(t, http.MethodPost, server.URL+"/v1/accounts/"+created.Account.ID+"/link", body, "bob")
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("bob linking alice's account: got %d, want 404", response.StatusCode)
	}

	response = request(t, http.MethodPost, server.URL+"/v1/accounts/"+created.Account.ID+"/link", body, "alice")
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("alice linking own account: got %d, want 200", response.StatusCode)
	}
	var linked protocol.LinkResult
	decodeResponse(t, response, &linked)
	if linked.State != "qr_required" || linked.QRURI == "" {
		t.Fatalf("link result = %#v", linked)
	}
	if err := <-workerResult; err != nil {
		t.Fatalf("worker command loop: %v", err)
	}
}

type workerSession struct {
	conn  *websocket.Conn
	ready protocol.Ready
}

func dialWorker(t *testing.T, url, token, platform string) workerSession {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial worker: %v", err)
	}
	hello, _ := protocol.NewEnvelope(protocol.TypeHello, protocol.Hello{
		Token:    token,
		Platform: platform,
		Version:  "test/1",
	})
	if err := conn.WriteJSON(hello); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	var envelope protocol.Envelope
	if err := conn.ReadJSON(&envelope); err != nil {
		t.Fatalf("read ready: %v", err)
	}
	if envelope.Error != nil {
		t.Fatalf("worker rejected: %s: %s", envelope.Error.Code, envelope.Error.Message)
	}
	ready, err := protocol.DecodePayload[protocol.Ready](envelope)
	if err != nil {
		t.Fatalf("decode ready: %v", err)
	}
	return workerSession{conn: conn, ready: ready}
}

func newTestServer(t *testing.T) (*httptest.Server, *gateway.Store, *gateway.Hub) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := gateway.NewStore()
	hub := gateway.NewHub(store, logger, nil)
	server := httptest.NewServer(httpapi.New(
		store,
		hub,
		logger,
		httpapi.Config{DevAuth: true},
	))
	t.Cleanup(server.Close)
	return server, store, hub
}

func createAccount(t *testing.T, baseURL, userID, platform string) createAccountResponse {
	t.Helper()
	body := []byte(`{"platform":"` + platform + `","display_name":"test"}`)
	response := request(t, http.MethodPost, baseURL+"/v1/accounts", body, userID)
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create account: got %d, want 201", response.StatusCode)
	}
	var created createAccountResponse
	decodeResponse(t, response, &created)
	return created
}

func request(t *testing.T, method, url string, body []byte, userID string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer dev:"+userID)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	return response
}

func decodeResponse(t *testing.T, response *http.Response, target any) {
	t.Helper()
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
