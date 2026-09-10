package signalworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/vegard/say/internal/protocol"
	"github.com/vegard/say/internal/signalcli"
)

type Service struct {
	rpc    *signalcli.Client
	log    *slog.Logger
	events chan protocol.Envelope

	mu       sync.RWMutex
	account  string
	linkURI  string
	linkName string
}

func New(
	ctx context.Context,
	rpc *signalcli.Client,
	linkName string,
	logger *slog.Logger,
) *Service {
	if linkName == "" {
		linkName = "say"
	}
	if logger == nil {
		logger = slog.Default()
	}
	service := &Service{
		rpc:      rpc,
		log:      logger,
		events:   make(chan protocol.Envelope, 1024),
		linkName: linkName,
	}
	go service.consumeNotifications(ctx)
	return service
}

func (s *Service) Events() <-chan protocol.Envelope {
	return s.events
}

func (s *Service) PublishStatus(ctx context.Context, state, externalID string) {
	event, err := protocol.NewEnvelope(protocol.TypeAccountStatus, protocol.AccountStatus{
		State:      state,
		ExternalID: externalID,
	})
	if err != nil {
		return
	}
	select {
	case s.events <- event:
	case <-ctx.Done():
	}
}

func (s *Service) DetectAccount(ctx context.Context) (string, error) {
	var result json.RawMessage
	if err := s.rpc.Call(ctx, "listAccounts", map[string]any{}, &result); err != nil {
		return "", err
	}
	account := parseAccount(result)
	s.mu.Lock()
	s.account = account
	s.mu.Unlock()
	return account, nil
}

func (s *Service) HandleCommand(
	ctx context.Context,
	envelope protocol.Envelope,
) (any, *protocol.Error) {
	switch envelope.Type {
	case protocol.TypeLinkStart:
		return s.startLink(ctx)
	case protocol.TypeLinkFinish:
		return s.finishLink(ctx, envelope)
	case protocol.TypeSend:
		return s.send(ctx, envelope)
	case protocol.TypeConversations:
		return s.conversations(ctx)
	case protocol.TypeMessages:
		return protocol.MessagesResult{Messages: []protocol.Message{}}, nil
	default:
		return nil, commandError("unsupported_command", "Signal worker does not support this command")
	}
}

func (s *Service) startLink(ctx context.Context) (any, *protocol.Error) {
	if account := s.currentAccount(); account != "" {
		return protocol.LinkResult{State: "linked", ExternalID: account}, nil
	}
	var result struct {
		DeviceLinkURI string `json:"deviceLinkUri"`
	}
	if err := s.rpc.Call(ctx, "startLink", map[string]any{}, &result); err != nil {
		s.log.Error("start Signal link", "error", err)
		return nil, commandError("signal_link_failed", "Signal failed to start device linking")
	}
	if !strings.HasPrefix(result.DeviceLinkURI, "sgnl://") &&
		!strings.HasPrefix(result.DeviceLinkURI, "tsdevice:") {
		return nil, commandError("signal_link_failed", "Signal returned an invalid link URI")
	}
	s.mu.Lock()
	s.linkURI = result.DeviceLinkURI
	s.mu.Unlock()
	return protocol.LinkResult{
		State:  "qr_required",
		QRURI:  result.DeviceLinkURI,
		Detail: "Scan this QR code from Signal settings under Linked devices",
	}, nil
}

func (s *Service) finishLink(
	ctx context.Context,
	envelope protocol.Envelope,
) (any, *protocol.Error) {
	input, err := protocol.DecodePayload[protocol.LinkFinishCommand](envelope)
	if err != nil {
		return nil, commandError("invalid_command", "Invalid Signal finish-link payload")
	}
	s.mu.RLock()
	linkURI := s.linkURI
	s.mu.RUnlock()
	if linkURI == "" {
		return nil, commandError("link_not_started", "Start Signal linking before finishing it")
	}
	deviceName := strings.TrimSpace(input.DeviceName)
	if deviceName == "" {
		deviceName = s.linkName
	}
	var result json.RawMessage
	if err := s.rpc.Call(ctx, "finishLink", map[string]any{
		"deviceLinkUri": linkURI,
		"deviceName":    deviceName,
	}, &result); err != nil {
		s.log.Error("finish Signal link", "error", err)
		return nil, commandError("signal_link_failed", "Signal failed to finish device linking")
	}
	account, err := s.DetectAccount(ctx)
	if err != nil || account == "" {
		s.log.Error("detect linked Signal account", "error", err)
		return nil, commandError("signal_link_failed", "Signal linked but the account could not be detected")
	}
	s.mu.Lock()
	s.linkURI = ""
	s.mu.Unlock()
	s.PublishStatus(ctx, "live", account)
	return protocol.LinkResult{State: "linked", ExternalID: account}, nil
}

func (s *Service) send(
	ctx context.Context,
	envelope protocol.Envelope,
) (any, *protocol.Error) {
	input, err := protocol.DecodePayload[protocol.SendCommand](envelope)
	if err != nil || strings.TrimSpace(input.Body) == "" {
		return nil, commandError("invalid_command", "Conversation and non-empty body are required")
	}
	account := s.currentAccount()
	if account == "" {
		return nil, commandError("signal_unlinked", "Signal account is not linked")
	}
	params := map[string]any{
		"account": account,
		"message": input.Body,
	}
	switch {
	case strings.HasPrefix(input.ConversationID, "user:"):
		recipient := strings.TrimPrefix(input.ConversationID, "user:")
		if recipient == "" {
			return nil, commandError("invalid_conversation", "Signal recipient is empty")
		}
		params["recipient"] = []string{recipient}
	case strings.HasPrefix(input.ConversationID, "group:"):
		groupID := strings.TrimPrefix(input.ConversationID, "group:")
		if groupID == "" {
			return nil, commandError("invalid_conversation", "Signal group is empty")
		}
		params["groupId"] = groupID
	default:
		return nil, commandError("invalid_conversation", "Unknown Signal conversation")
	}
	var result struct {
		Timestamp int64 `json:"timestamp"`
	}
	if err := s.rpc.Call(ctx, "send", params, &result); err != nil {
		s.log.Error("send Signal message", "error", err)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, commandError("signal_send_unknown", "Signal send outcome is unknown")
		}
		return nil, commandError("signal_send_failed", "Signal failed to send the message")
	}
	sentAt := time.UnixMilli(result.Timestamp).UTC()
	if result.Timestamp == 0 {
		sentAt = time.Now().UTC()
	}
	return protocol.SendResult{
		ProviderMessageID: fmt.Sprintf("%d", result.Timestamp),
		SentAt:            sentAt,
	}, nil
}

func (s *Service) conversations(ctx context.Context) (any, *protocol.Error) {
	account := s.currentAccount()
	if account == "" {
		return nil, commandError("signal_unlinked", "Signal account is not linked")
	}
	var groups []struct {
		ID      string `json:"id"`
		GroupID string `json:"groupId"`
		Name    string `json:"name"`
	}
	if err := s.rpc.Call(ctx, "listGroups", map[string]any{
		"account":  account,
		"groupIds": []string{},
	}, &groups); err != nil {
		s.log.Error("list Signal groups", "error", err)
		return nil, commandError("signal_list_failed", "Signal failed to list groups")
	}
	var contacts []struct {
		Number      string `json:"number"`
		Name        string `json:"name"`
		ProfileName string `json:"profileName"`
	}
	if err := s.rpc.Call(ctx, "listContacts", map[string]any{
		"account":       account,
		"recipients":    []string{},
		"allRecipients": false,
		"detailed":      true,
		"internal":      false,
	}, &contacts); err != nil {
		s.log.Error("list Signal contacts", "error", err)
		return nil, commandError("signal_list_failed", "Signal failed to list contacts")
	}

	conversations := make([]protocol.Conversation, 0, len(groups)+len(contacts))
	for _, group := range groups {
		id := group.ID
		if id == "" {
			id = group.GroupID
		}
		if id == "" {
			continue
		}
		title := group.Name
		if title == "" {
			title = "Signal group"
		}
		conversations = append(conversations, protocol.Conversation{
			ProviderID: "group:" + id,
			Title:      title,
			Kind:       "group",
		})
	}
	for _, contact := range contacts {
		if contact.Number == "" {
			continue
		}
		title := contact.Name
		if title == "" {
			title = contact.ProfileName
		}
		if title == "" {
			title = contact.Number
		}
		conversations = append(conversations, protocol.Conversation{
			ProviderID: "user:" + contact.Number,
			Title:      title,
			Kind:       "direct",
		})
	}
	return protocol.ConversationsResult{Conversations: conversations}, nil
}

func (s *Service) consumeNotifications(ctx context.Context) {
	defer close(s.events)
	for {
		select {
		case <-ctx.Done():
			return
		case notification, ok := <-s.rpc.Notifications():
			if !ok {
				return
			}
			if notification.Method != "receive" {
				continue
			}
			message, ok := parseReceive(notification.Params)
			if !ok {
				continue
			}
			event, err := protocol.NewEnvelope(protocol.TypeMessageNew, message)
			if err != nil {
				continue
			}
			select {
			case s.events <- event:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (s *Service) currentAccount() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.account
}

func commandError(code, message string) *protocol.Error {
	return &protocol.Error{Code: code, Message: message}
}

func parseAccount(raw json.RawMessage) string {
	var accounts []struct {
		Number  string `json:"number"`
		Account string `json:"account"`
	}
	if err := json.Unmarshal(raw, &accounts); err == nil {
		for _, account := range accounts {
			if account.Number != "" {
				return account.Number
			}
			if account.Account != "" {
				return account.Account
			}
		}
	}
	var stringsResult []string
	if err := json.Unmarshal(raw, &stringsResult); err == nil && len(stringsResult) > 0 {
		return stringsResult[0]
	}
	return ""
}

func parseReceive(raw json.RawMessage) (protocol.Message, bool) {
	var notification struct {
		Envelope struct {
			Source       string `json:"source"`
			SourceNumber string `json:"sourceNumber"`
			SourceUUID   string `json:"sourceUuid"`
			SourceName   string `json:"sourceName"`
			Timestamp    int64  `json:"timestamp"`
			DataMessage  *struct {
				Timestamp int64  `json:"timestamp"`
				Message   string `json:"message"`
				GroupInfo *struct {
					GroupID string `json:"groupId"`
				} `json:"groupInfo"`
			} `json:"dataMessage"`
			SyncMessage *struct {
				SentMessage *struct {
					Destination string `json:"destination"`
					Timestamp   int64  `json:"timestamp"`
					Message     string `json:"message"`
					GroupInfo   *struct {
						GroupID string `json:"groupId"`
					} `json:"groupInfo"`
				} `json:"sentMessage"`
			} `json:"syncMessage"`
		} `json:"envelope"`
	}
	if err := json.Unmarshal(raw, &notification); err != nil {
		return protocol.Message{}, false
	}
	envelope := notification.Envelope
	message := protocol.Message{Direction: "incoming"}
	var groupID string
	var directPeer string
	if envelope.DataMessage != nil {
		message.Body = envelope.DataMessage.Message
		if envelope.DataMessage.Timestamp != 0 {
			envelope.Timestamp = envelope.DataMessage.Timestamp
		}
		if envelope.DataMessage.GroupInfo != nil {
			groupID = envelope.DataMessage.GroupInfo.GroupID
		}
		message.AuthorID = firstNonEmpty(envelope.SourceNumber, envelope.Source, envelope.SourceUUID)
		message.AuthorName = firstNonEmpty(envelope.SourceName, message.AuthorID)
		directPeer = firstNonEmpty(envelope.SourceNumber, envelope.Source, envelope.SourceUUID)
	} else if envelope.SyncMessage != nil && envelope.SyncMessage.SentMessage != nil {
		sent := envelope.SyncMessage.SentMessage
		message.Direction = "outgoing"
		message.Body = sent.Message
		if sent.Timestamp != 0 {
			envelope.Timestamp = sent.Timestamp
		}
		if sent.GroupInfo != nil {
			groupID = sent.GroupInfo.GroupID
		}
		message.AuthorID = "self"
		message.AuthorName = "You"
		directPeer = sent.Destination
	} else {
		return protocol.Message{}, false
	}
	if message.Body == "" || envelope.Timestamp == 0 {
		return protocol.Message{}, false
	}
	if groupID != "" {
		message.ConversationID = "group:" + groupID
	} else {
		if directPeer == "" {
			return protocol.Message{}, false
		}
		message.ConversationID = "user:" + directPeer
	}
	message.ProviderMessageID = fmt.Sprintf("%d", envelope.Timestamp)
	message.SentAt = time.UnixMilli(envelope.Timestamp).UTC()
	return message, true
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
