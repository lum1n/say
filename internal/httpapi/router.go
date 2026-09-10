package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/vegard/say/internal/auth"
	"github.com/vegard/say/internal/gateway"
	"github.com/vegard/say/internal/postgres"
	"github.com/vegard/say/internal/protocol"
	"github.com/vegard/say/internal/realtime"
)

type contextKey string

const userIDKey contextKey = "user_id"

type Config struct {
	DevAuth  bool
	Auth     *auth.Service
	Messages *postgres.MessageStore
	Realtime *realtime.Hub
}

type API struct {
	store gateway.AccountStore
	hub   *gateway.Hub
	log   *slog.Logger
	cfg   Config
}

func New(store gateway.AccountStore, hub *gateway.Hub, logger *slog.Logger, cfg Config) http.Handler {
	api := &API{store: store, hub: hub, log: logger, cfg: cfg}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", api.health)
	mux.HandleFunc("GET /v1/workers/connect", hub.ServeWorker)
	mux.HandleFunc("POST /v1/auth/start", api.startAuth)
	mux.HandleFunc("POST /v1/auth/verify", api.verifyAuth)
	mux.HandleFunc("POST /v1/auth/refresh", api.refreshAuth)
	mux.Handle("GET /v1/accounts", api.requireUser(http.HandlerFunc(api.listAccounts)))
	mux.Handle("POST /v1/accounts", api.requireUser(http.HandlerFunc(api.createAccount)))
	mux.Handle("GET /v1/accounts/{accountID}", api.requireUser(http.HandlerFunc(api.getAccount)))
	mux.Handle("DELETE /v1/accounts/{accountID}", api.requireUser(http.HandlerFunc(api.deleteAccount)))
	mux.Handle("POST /v1/accounts/{accountID}/link", api.requireUser(http.HandlerFunc(api.linkAccount)))
	mux.Handle("GET /v1/conversations", api.requireUser(http.HandlerFunc(api.listConversations)))
	mux.Handle("GET /v1/conversations/{conversationID}/messages", api.requireUser(http.HandlerFunc(api.listMessages)))
	mux.Handle("POST /v1/conversations/{conversationID}/messages", api.requireUser(http.HandlerFunc(api.sendMessage)))
	mux.Handle("GET /v1/ws", api.requireUser(http.HandlerFunc(api.clientWebSocket)))
	return api.recover(mux)
}

func (a *API) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) startAuth(w http.ResponseWriter, r *http.Request) {
	if a.cfg.Auth == nil {
		writeError(w, http.StatusServiceUnavailable, "auth_not_configured", "authentication is not configured")
		return
	}
	var input struct {
		Email string `json:"email"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	login, err := a.cfg.Auth.Start(r.Context(), input.Email)
	if errors.Is(err, auth.ErrInvalidEmail) {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid email address")
		return
	}
	if err != nil {
		a.log.Error("start login", "error", err)
		writeError(w, http.StatusServiceUnavailable, "delivery_failed", "failed to deliver login code")
		return
	}
	writeJSON(w, http.StatusAccepted, login)
}

func (a *API) verifyAuth(w http.ResponseWriter, r *http.Request) {
	if a.cfg.Auth == nil {
		writeError(w, http.StatusServiceUnavailable, "auth_not_configured", "authentication is not configured")
		return
	}
	var input struct {
		LoginID string `json:"login_id"`
		Code    string `json:"code"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	pair, err := a.cfg.Auth.Verify(r.Context(), input.LoginID, input.Code)
	if errors.Is(err, auth.ErrInvalidLogin) {
		writeError(w, http.StatusUnauthorized, "invalid_login", "invalid or expired login")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to verify login")
		return
	}
	writeJSON(w, http.StatusOK, pair)
}

func (a *API) refreshAuth(w http.ResponseWriter, r *http.Request) {
	if a.cfg.Auth == nil {
		writeError(w, http.StatusServiceUnavailable, "auth_not_configured", "authentication is not configured")
		return
	}
	var input struct {
		RefreshToken string `json:"refresh_token"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	pair, err := a.cfg.Auth.Refresh(r.Context(), input.RefreshToken)
	if errors.Is(err, auth.ErrInvalidRefresh) {
		writeError(w, http.StatusUnauthorized, "invalid_refresh", "invalid or expired refresh token")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to refresh login")
		return
	}
	writeJSON(w, http.StatusOK, pair)
}

func (a *API) listAccounts(w http.ResponseWriter, r *http.Request) {
	accounts, err := a.store.ListAccounts(r.Context(), userID(r.Context()))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to list accounts")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accounts": accounts,
	})
}

func (a *API) createAccount(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Platform    string `json:"platform"`
		DisplayName string `json:"display_name"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	account, enrollment, err := a.store.CreateAccount(
		r.Context(),
		userID(r.Context()),
		input.Platform,
		input.DisplayName,
	)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"account": account,
		"enrollment": map[string]any{
			"token":      enrollment,
			"expires_at": time.Now().UTC().Add(10 * time.Minute),
		},
	})
}

func (a *API) getAccount(w http.ResponseWriter, r *http.Request) {
	account, err := a.store.GetAccount(r.Context(), userID(r.Context()), r.PathValue("accountID"))
	if errors.Is(err, gateway.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "account not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to read account")
		return
	}
	writeJSON(w, http.StatusOK, account)
}

func (a *API) deleteAccount(w http.ResponseWriter, r *http.Request) {
	accountID := r.PathValue("accountID")
	if err := a.store.DeleteAccount(r.Context(), userID(r.Context()), accountID); errors.Is(err, gateway.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "account not found")
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to delete account")
		return
	}
	a.hub.Disconnect(accountID)
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) linkAccount(w http.ResponseWriter, r *http.Request) {
	accountID := r.PathValue("accountID")
	account, err := a.store.GetAccount(r.Context(), userID(r.Context()), accountID)
	if errors.Is(err, gateway.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "account not found")
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to read account")
		return
	}

	var input struct {
		Action      string `json:"action"`
		DeviceName  string `json:"device_name"`
		PhoneNumber string `json:"phone_number"`
		Code        string `json:"code"`
		Password    string `json:"password"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	var commandType string
	var payload any
	var timeout time.Duration
	switch input.Action {
	case "start":
		commandType = protocol.TypeLinkStart
		payload = protocol.LinkStartCommand{
			DeviceName:  input.DeviceName,
			PhoneNumber: input.PhoneNumber,
		}
		timeout = 30 * time.Second
	case "finish":
		if account.Platform != "signal" {
			writeError(w, http.StatusBadRequest, "invalid_request", "finish is only used for Signal linking")
			return
		}
		commandType = protocol.TypeLinkFinish
		payload = protocol.LinkFinishCommand{DeviceName: input.DeviceName}
		timeout = 2 * time.Minute
	case "submit_code":
		if account.Platform != "telegram" || strings.TrimSpace(input.Code) == "" {
			writeError(w, http.StatusBadRequest, "invalid_request", "a Telegram verification code is required")
			return
		}
		commandType = protocol.TypeLinkFinish
		payload = protocol.LinkFinishCommand{Code: input.Code}
		timeout = 30 * time.Second
	case "submit_password":
		if account.Platform != "telegram" || input.Password == "" {
			writeError(w, http.StatusBadRequest, "invalid_request", "a Telegram 2FA password is required")
			return
		}
		commandType = protocol.TypeLinkFinish
		payload = protocol.LinkFinishCommand{Password: input.Password}
		timeout = 2 * time.Minute
	default:
		writeError(w, http.StatusBadRequest, "invalid_request", "unsupported link action")
		return
	}
	if account.Platform == "telegram" && input.Action == "start" && strings.TrimSpace(input.PhoneNumber) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "phone_number is required to start Telegram linking")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	result, err := a.hub.Request(ctx, accountID, commandType, payload)
	if errors.Is(err, gateway.ErrWorkerOffline) {
		writeError(w, http.StatusConflict, "worker_offline", "worker is offline")
		return
	}
	var commandError *gateway.WorkerCommandError
	if errors.As(err, &commandError) {
		writeError(w, http.StatusConflict, commandError.Code, commandError.Message)
		return
	}
	if err != nil {
		writeError(w, http.StatusGatewayTimeout, "worker_timeout", "worker did not complete link command")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *API) listConversations(w http.ResponseWriter, r *http.Request) {
	if a.cfg.Messages == nil {
		writeError(w, http.StatusServiceUnavailable, "messages_not_configured", "message storage is not configured")
		return
	}
	accountID := r.URL.Query().Get("account_id")
	if accountID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "account_id is required")
		return
	}
	currentUserID := userID(r.Context())
	if _, err := a.store.GetAccount(r.Context(), currentUserID, accountID); errors.Is(err, gateway.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "account not found")
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to read account")
		return
	}
	conversations, err := a.cfg.Messages.ListConversations(r.Context(), currentUserID, accountID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to list conversations")
		return
	}
	if len(conversations) == 0 || r.URL.Query().Get("refresh") == "true" {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		result, requestErr := a.hub.Request(ctx, accountID, protocol.TypeConversations, protocol.ConversationsCommand{})
		cancel()
		if requestErr == nil {
			var synced protocol.ConversationsResult
			if err := json.Unmarshal(result, &synced); err == nil {
				if err := a.cfg.Messages.UpsertConversations(
					r.Context(),
					currentUserID,
					accountID,
					synced.Conversations,
				); err != nil {
					writeError(w, http.StatusInternalServerError, "internal_error", "failed to cache conversations")
					return
				}
				conversations, err = a.cfg.Messages.ListConversations(r.Context(), currentUserID, accountID)
				if err != nil {
					writeError(w, http.StatusInternalServerError, "internal_error", "failed to list conversations")
					return
				}
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"conversations": conversations,
		"next_cursor":   "",
	})
}

func (a *API) listMessages(w http.ResponseWriter, r *http.Request) {
	if a.cfg.Messages == nil {
		writeError(w, http.StatusServiceUnavailable, "messages_not_configured", "message storage is not configured")
		return
	}
	currentUserID := userID(r.Context())
	conversationID := r.PathValue("conversationID")
	target, err := a.cfg.Messages.GetConversationTarget(r.Context(), currentUserID, conversationID)
	if errors.Is(err, gateway.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "conversation not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to read conversation")
		return
	}
	messages, err := a.cfg.Messages.ListMessages(r.Context(), currentUserID, conversationID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to list messages")
		return
	}
	if target.Platform == "telegram" &&
		(len(messages) == 0 || r.URL.Query().Get("refresh") == "true") {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		result, requestErr := a.hub.Request(ctx, target.AccountID, protocol.TypeMessages, protocol.MessagesCommand{
			ConversationID: target.ProviderID,
		})
		cancel()
		if requestErr == nil {
			var history protocol.MessagesResult
			if err := json.Unmarshal(result, &history); err == nil {
				account, err := a.store.GetAccount(r.Context(), currentUserID, target.AccountID)
				if err != nil {
					writeError(w, http.StatusInternalServerError, "internal_error", "failed to read account")
					return
				}
				for _, historical := range history.Messages {
					if _, _, err := a.cfg.Messages.CacheMessage(r.Context(), account, historical); err != nil {
						writeError(w, http.StatusInternalServerError, "internal_error", "failed to cache message history")
						return
					}
				}
				messages, err = a.cfg.Messages.ListMessages(r.Context(), currentUserID, conversationID)
				if err != nil {
					writeError(w, http.StatusInternalServerError, "internal_error", "failed to list messages")
					return
				}
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"messages":    messages,
		"next_cursor": "",
	})
}

func (a *API) sendMessage(w http.ResponseWriter, r *http.Request) {
	if a.cfg.Messages == nil {
		writeError(w, http.StatusServiceUnavailable, "messages_not_configured", "message storage is not configured")
		return
	}
	var input struct {
		Body           string `json:"body"`
		IdempotencyKey string `json:"idempotency_key"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if strings.TrimSpace(input.Body) == "" ||
		len(input.Body) > 65536 ||
		len(input.IdempotencyKey) < 16 ||
		len(input.IdempotencyKey) > 128 {
		writeError(w, http.StatusBadRequest, "invalid_request", "body and a 16-128 character idempotency_key are required")
		return
	}
	currentUserID := userID(r.Context())
	prepared, err := a.cfg.Messages.PrepareSend(
		r.Context(),
		currentUserID,
		r.PathValue("conversationID"),
		input.Body,
		input.IdempotencyKey,
	)
	if errors.Is(err, gateway.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "conversation not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to prepare message")
		return
	}
	if !prepared.Created {
		writeJSON(w, http.StatusAccepted, map[string]any{
			"command_id": prepared.CommandID,
			"message":    prepared.Message,
		})
		return
	}
	dispatched, err := a.cfg.Messages.MarkCommandDispatched(r.Context(), prepared.CommandID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to dispatch message")
		return
	}
	if !dispatched {
		writeJSON(w, http.StatusAccepted, map[string]any{
			"command_id": prepared.CommandID,
			"message":    prepared.Message,
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	rawResult, requestErr := a.hub.Request(ctx, prepared.AccountID, protocol.TypeSend, protocol.SendCommand{
		ConversationID: prepared.ProviderConversationID,
		Body:           input.Body,
		IdempotencyKey: input.IdempotencyKey,
	})
	cancel()
	if requestErr != nil {
		persistCtx, persistCancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
		defer persistCancel()
		_, _ = a.cfg.Messages.FailSend(
			persistCtx,
			currentUserID,
			prepared.CommandID,
			workerErrorCode(requestErr),
		)
		if errors.Is(requestErr, gateway.ErrWorkerOffline) {
			writeError(w, http.StatusConflict, "worker_offline", "worker is offline")
			return
		}
		var commandError *gateway.WorkerCommandError
		if errors.As(requestErr, &commandError) {
			writeError(w, http.StatusBadGateway, commandError.Code, commandError.Message)
			return
		}
		writeError(w, http.StatusGatewayTimeout, "worker_timeout", "worker did not complete send command")
		return
	}
	var sendResult protocol.SendResult
	if err := json.Unmarshal(rawResult, &sendResult); err != nil ||
		sendResult.ProviderMessageID == "" ||
		sendResult.SentAt.IsZero() {
		persistCtx, persistCancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
		defer persistCancel()
		_, _ = a.cfg.Messages.FailSend(persistCtx, currentUserID, prepared.CommandID, "invalid_worker_response")
		writeError(w, http.StatusBadGateway, "invalid_worker_response", "worker returned an invalid send result")
		return
	}
	persistCtx, persistCancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
	defer persistCancel()
	message, err := a.cfg.Messages.CompleteSend(
		persistCtx,
		currentUserID,
		prepared.CommandID,
		sendResult.ProviderMessageID,
		sendResult.SentAt,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to complete message")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"command_id": prepared.CommandID,
		"message":    message,
	})
}

func (a *API) clientWebSocket(w http.ResponseWriter, r *http.Request) {
	if a.cfg.Realtime == nil {
		writeError(w, http.StatusServiceUnavailable, "realtime_not_configured", "realtime is not configured")
		return
	}
	a.cfg.Realtime.Serve(w, r, userID(r.Context()))
}

func workerErrorCode(err error) string {
	if errors.Is(err, gateway.ErrWorkerOffline) {
		return "worker_offline"
	}
	var commandError *gateway.WorkerCommandError
	if errors.As(err, &commandError) {
		return commandError.Code
	}
	return "worker_timeout"
}

func (a *API) requireUser(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		const bearerPrefix = "Bearer "
		if !strings.HasPrefix(header, bearerPrefix) {
			writeError(w, http.StatusUnauthorized, "unauthorized", "bearer token required")
			return
		}
		token := strings.TrimSpace(strings.TrimPrefix(header, bearerPrefix))
		var authenticatedUserID string
		if a.cfg.DevAuth && strings.HasPrefix(token, "dev:") {
			authenticatedUserID = strings.TrimSpace(strings.TrimPrefix(token, "dev:"))
		} else if a.cfg.Auth != nil {
			var err error
			authenticatedUserID, err = a.cfg.Auth.ValidateAccessToken(token)
			if err != nil {
				writeError(w, http.StatusUnauthorized, "unauthorized", "invalid access token")
				return
			}
		}
		if authenticatedUserID == "" || len(authenticatedUserID) > 128 {
			writeError(w, http.StatusUnauthorized, "unauthorized", "invalid authenticated user")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userIDKey, authenticatedUserID)))
	})
}

func (a *API) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if value := recover(); value != nil {
				a.log.Error("request panic", "value", value)
				writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func userID(ctx context.Context) string {
	value, _ := ctx.Value(userIDKey).(string)
	return value
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}
