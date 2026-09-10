package signalworker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"testing"

	"github.com/vegard/say/internal/protocol"
	"github.com/vegard/say/internal/signalcli"
)

func TestParseIncomingReceive(t *testing.T) {
	raw := json.RawMessage(`{
		"envelope": {
			"sourceNumber": "+15551234567",
			"sourceName": "Alice",
			"timestamp": 1720000000000,
			"dataMessage": {
				"timestamp": 1720000000000,
				"message": "hello"
			}
		}
	}`)
	message, ok := parseReceive(raw)
	if !ok {
		t.Fatal("incoming message was not parsed")
	}
	if message.ConversationID != "user:+15551234567" ||
		message.AuthorName != "Alice" ||
		message.Direction != "incoming" ||
		message.Body != "hello" {
		t.Fatalf("incoming message = %#v", message)
	}
}

func TestParseOutgoingSyncReceiveUsesDestination(t *testing.T) {
	raw := json.RawMessage(`{
		"envelope": {
			"sourceNumber": "+15550000000",
			"timestamp": 1720000000000,
			"syncMessage": {
				"sentMessage": {
					"destination": "+15551234567",
					"timestamp": 1720000000001,
					"message": "outgoing"
				}
			}
		}
	}`)
	message, ok := parseReceive(raw)
	if !ok {
		t.Fatal("outgoing message was not parsed")
	}
	if message.ConversationID != "user:+15551234567" ||
		message.AuthorID != "self" ||
		message.Direction != "outgoing" {
		t.Fatalf("outgoing message = %#v", message)
	}
}

func TestParseGroupReceive(t *testing.T) {
	raw := json.RawMessage(`{
		"envelope": {
			"sourceNumber": "+15551234567",
			"timestamp": 1720000000000,
			"dataMessage": {
				"message": "group hello",
				"groupInfo": {"groupId": "base64-group-id"}
			}
		}
	}`)
	message, ok := parseReceive(raw)
	if !ok {
		t.Fatal("group message was not parsed")
	}
	if message.ConversationID != "group:base64-group-id" {
		t.Fatalf("conversation = %q", message.ConversationID)
	}
}

func TestParseReceiveIgnoresNonMessageEnvelope(t *testing.T) {
	if _, ok := parseReceive(json.RawMessage(`{"envelope":{"timestamp":123}}`)); ok {
		t.Fatal("non-message envelope was accepted")
	}
}

func TestServiceListsConversationsAndSends(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { _ = serverConn.Close() })
	rpc := signalcli.NewClient(clientConn)
	t.Cleanup(func() { _ = rpc.Close() })

	requests := make(chan rpcRequest, 4)
	go serveRPC(serverConn, requests)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	service := New(ctx, rpc, "say test", slog.New(slog.NewTextHandler(io.Discard, nil)))

	account, err := service.DetectAccount(ctx)
	if err != nil || account != "+15550000000" {
		t.Fatalf("detect account = %q, err=%v", account, err)
	}
	result, commandErr := service.HandleCommand(ctx, protocol.Envelope{
		Version: protocol.Version,
		Type:    protocol.TypeConversations,
		Payload: json.RawMessage(`{}`),
	})
	if commandErr != nil {
		t.Fatalf("list conversations: %v", commandErr)
	}
	conversations := result.(protocol.ConversationsResult)
	if len(conversations.Conversations) != 2 {
		t.Fatalf("conversations = %#v", conversations)
	}

	sendPayload, _ := json.Marshal(protocol.SendCommand{
		ConversationID: "user:+15551234567",
		Body:           "hello",
		IdempotencyKey: "test-idempotency",
	})
	result, commandErr = service.HandleCommand(ctx, protocol.Envelope{
		Version: protocol.Version,
		Type:    protocol.TypeSend,
		Payload: sendPayload,
	})
	if commandErr != nil {
		t.Fatalf("send: %v", commandErr)
	}
	if result.(protocol.SendResult).ProviderMessageID != "1720000000000" {
		t.Fatalf("send result = %#v", result)
	}

	expectedMethods := []string{"listAccounts", "listGroups", "listContacts", "send"}
	for _, expected := range expectedMethods {
		request := <-requests
		if request.Method != expected {
			t.Fatalf("RPC method = %q, want %q", request.Method, expected)
		}
	}
}

func TestServiceLinksWithSameLongLivedRPCProcess(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { _ = serverConn.Close() })
	rpc := signalcli.NewClient(clientConn)
	t.Cleanup(func() { _ = rpc.Close() })

	go func() {
		decoder := json.NewDecoder(serverConn)
		encoder := json.NewEncoder(serverConn)
		listCalls := 0
		for {
			var request rpcRequest
			if err := decoder.Decode(&request); err != nil {
				return
			}
			var result any
			switch request.Method {
			case "listAccounts":
				listCalls++
				if listCalls == 1 {
					result = []any{}
				} else {
					result = []map[string]string{{"number": "+15550000000"}}
				}
			case "startLink":
				result = map[string]string{
					"deviceLinkUri": "sgnl://linkdevice?uuid=test&pub_key=test",
				}
			case "finishLink":
				result = map[string]string{
					"deviceLinkUri": "sgnl://linkdevice?uuid=test&pub_key=test",
				}
			}
			_ = encoder.Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      request.ID,
				"result":  result,
			})
		}
	}()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	service := New(ctx, rpc, "say test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if account, err := service.DetectAccount(ctx); err != nil || account != "" {
		t.Fatalf("initial account = %q, err=%v", account, err)
	}

	started, commandErr := service.HandleCommand(ctx, protocol.Envelope{
		Version: protocol.Version,
		Type:    protocol.TypeLinkStart,
		Payload: json.RawMessage(`{}`),
	})
	if commandErr != nil {
		t.Fatalf("start link: %v", commandErr)
	}
	if started.(protocol.LinkResult).State != "qr_required" {
		t.Fatalf("start result = %#v", started)
	}

	finishPayload, _ := json.Marshal(protocol.LinkFinishCommand{DeviceName: "say test"})
	finished, commandErr := service.HandleCommand(ctx, protocol.Envelope{
		Version: protocol.Version,
		Type:    protocol.TypeLinkFinish,
		Payload: finishPayload,
	})
	if commandErr != nil {
		t.Fatalf("finish link: %v", commandErr)
	}
	if finished.(protocol.LinkResult).ExternalID != "+15550000000" {
		t.Fatalf("finish result = %#v", finished)
	}
}

type rpcRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

func serveRPC(conn net.Conn, requests chan<- rpcRequest) {
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	for {
		var request rpcRequest
		if err := decoder.Decode(&request); err != nil {
			return
		}
		requests <- request
		var result any
		switch request.Method {
		case "listAccounts":
			result = []map[string]string{{"number": "+15550000000"}}
		case "listGroups":
			result = []map[string]string{{"id": "group-id", "name": "Friends"}}
		case "listContacts":
			result = []map[string]string{{"number": "+15551234567", "name": "Alice"}}
		case "send":
			result = map[string]int64{"timestamp": 1720000000000}
		default:
			result = map[string]any{}
		}
		_ = encoder.Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      request.ID,
			"result":  result,
		})
	}
}
