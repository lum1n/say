package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
)

const Version = 1

const (
	TypeHello         = "hello"
	TypeReady         = "ready"
	TypeHeartbeat     = "heartbeat"
	TypeSend          = "send"
	TypeConversations = "conversations"
	TypeMessages      = "messages"
	TypeLinkStart     = "link.start"
	TypeLinkFinish    = "link.finish"
	TypeMessageNew    = "message.new"
	TypeAccountStatus = "account.status"
	TypeEventAck      = "event.ack"
)

type Envelope struct {
	Version    int             `json:"version"`
	ID         string          `json:"id,omitempty"`
	Type       string          `json:"type"`
	Generation uint64          `json:"generation,omitempty"`
	Status     string          `json:"status,omitempty"`
	Error      *Error          `json:"error,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Hello struct {
	Token    string `json:"token"`
	Platform string `json:"platform"`
	Version  string `json:"worker_version"`
}

type Ready struct {
	AccountID  string `json:"account_id"`
	WorkerID   string `json:"worker_id"`
	Credential string `json:"credential,omitempty"`
	Generation uint64 `json:"generation"`
}

func NewEnvelope(kind string, payload any) (Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal %s payload: %w", kind, err)
	}
	return Envelope{Version: Version, Type: kind, Payload: raw}, nil
}

func ErrorEnvelope(code, message string) Envelope {
	return Envelope{
		Version: Version,
		Type:    "error",
		Status:  "error",
		Error:   &Error{Code: code, Message: message},
	}
}

func DecodePayload[T any](envelope Envelope) (T, error) {
	var value T
	if len(envelope.Payload) == 0 {
		return value, errors.New("payload is required")
	}
	if err := json.Unmarshal(envelope.Payload, &value); err != nil {
		return value, fmt.Errorf("decode %s payload: %w", envelope.Type, err)
	}
	return value, nil
}

func (e Envelope) Validate() error {
	if e.Version != Version {
		return fmt.Errorf("unsupported protocol version %d", e.Version)
	}
	if e.Type == "" {
		return errors.New("type is required")
	}
	if e.Status != "" && e.Status != "ok" && e.Status != "error" {
		return fmt.Errorf("invalid status %q", e.Status)
	}
	return nil
}
