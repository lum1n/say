package protocol

import "time"

type SendCommand struct {
	ConversationID string `json:"conversation_id"`
	Body           string `json:"body"`
	IdempotencyKey string `json:"idempotency_key"`
}

type SendResult struct {
	ProviderMessageID string    `json:"provider_message_id"`
	SentAt            time.Time `json:"sent_at"`
}

type ConversationsCommand struct {
	Cursor string `json:"cursor,omitempty"`
}

type Conversation struct {
	ProviderID string `json:"provider_id"`
	Title      string `json:"title"`
	Kind       string `json:"kind"`
}

type ConversationsResult struct {
	Conversations []Conversation `json:"conversations"`
	NextCursor    string         `json:"next_cursor,omitempty"`
}

type MessagesCommand struct {
	ConversationID string `json:"conversation_id"`
	Cursor         string `json:"cursor,omitempty"`
}

type MessagesResult struct {
	Messages   []Message `json:"messages"`
	NextCursor string    `json:"next_cursor,omitempty"`
}

type Message struct {
	ProviderMessageID string    `json:"provider_message_id"`
	ConversationID    string    `json:"conversation_id"`
	AuthorID          string    `json:"author_id"`
	AuthorName        string    `json:"author_name"`
	Body              string    `json:"body"`
	SentAt            time.Time `json:"sent_at"`
	Direction         string    `json:"direction"`
}

type LinkStartCommand struct {
	DeviceName  string `json:"device_name,omitempty"`
	PhoneNumber string `json:"phone_number,omitempty"`
}

type LinkFinishCommand struct {
	DeviceName string `json:"device_name,omitempty"`
	Code       string `json:"code,omitempty"`
	Password   string `json:"password,omitempty"`
}

type LinkResult struct {
	State      string `json:"state"`
	QRURI      string `json:"qr_uri,omitempty"`
	ExternalID string `json:"external_id,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

type AccountStatus struct {
	State      string `json:"state"`
	ExternalID string `json:"external_id,omitempty"`
	Detail     string `json:"detail,omitempty"`
}
