package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/vegard/say/internal/auth"
	"github.com/vegard/say/internal/gateway"
	"github.com/vegard/say/internal/httpapi"
	"github.com/vegard/say/internal/postgres"
	"github.com/vegard/say/internal/protocol"
	"github.com/vegard/say/internal/realtime"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	addr := envOr("SAY_ADDR", ":8080")
	environment := envOr("SAY_ENV", "development")
	devAuth := envBool("SAY_DEV_AUTH")
	debugCode := envBool("SAY_AUTH_DEBUG_RETURN_CODE")
	if environment == "production" && (devAuth || debugCode) {
		logger.Error("development authentication options are forbidden in production")
		os.Exit(1)
	}

	startupCtx, startupCancel := context.WithTimeout(ctx, 20*time.Second)
	defer startupCancel()
	database, err := postgres.Open(startupCtx, mustEnv(logger, "SAY_DATABASE_URL"))
	if err != nil {
		logger.Error("database startup failed", "error", err)
		os.Exit(1)
	}
	defer database.Close()

	var codeSender auth.CodeSender
	if webhookURL := os.Getenv("SAY_AUTH_WEBHOOK_URL"); webhookURL != "" {
		if environment == "production" && !strings.HasPrefix(webhookURL, "https://") {
			logger.Error("SAY_AUTH_WEBHOOK_URL must use HTTPS in production")
			os.Exit(1)
		}
		sender, err := auth.NewEmailServiceSender(
			webhookURL,
			mustEnv(logger, "SAY_AUTH_EMAIL_FROM"),
			os.Getenv("SAY_AUTH_WEBHOOK_TOKEN"),
		)
		if err != nil {
			logger.Error("email delivery configuration failed", "error", err)
			os.Exit(1)
		}
		codeSender = sender
	}
	authService, err := auth.NewService(database.Pool, auth.Config{
		Issuer:          envOr("SAY_JWT_ISSUER", "say"),
		JWTSigningKey:   []byte(mustEnv(logger, "SAY_JWT_SIGNING_KEY")),
		LoginHashKey:    []byte(mustEnv(logger, "SAY_LOGIN_HASH_KEY")),
		DebugReturnCode: debugCode,
	}, codeSender)
	if err != nil {
		logger.Error("authentication startup failed", "error", err)
		os.Exit(1)
	}

	store := postgres.NewAccountStore(database.Pool)
	if err := store.MarkWorkersOffline(startupCtx); err != nil {
		logger.Error("worker presence reset failed", "error", err)
		os.Exit(1)
	}
	messageStore := postgres.NewMessageStore(database.Pool)
	clientHub := realtime.NewHub()
	hub := gateway.NewHub(store, logger, func(
		ctx context.Context,
		account gateway.Account,
		envelope protocol.Envelope,
	) error {
		switch envelope.Type {
		case protocol.TypeMessageNew:
			message, err := protocol.DecodePayload[protocol.Message](envelope)
			if err != nil {
				return err
			}
			_, _, err = messageStore.IngestMessage(ctx, account, message)
			return err
		case protocol.TypeAccountStatus:
			status, err := protocol.DecodePayload[protocol.AccountStatus](envelope)
			if err != nil {
				return err
			}
			return clientHub.Publish(account.UserID, protocol.TypeAccountStatus, map[string]any{
				"account_id":  account.ID,
				"state":       status.State,
				"external_id": status.ExternalID,
			})
		default:
			return fmt.Errorf("unsupported worker event type %q", envelope.Type)
		}
	})
	handler := httpapi.New(store, hub, logger, httpapi.Config{
		DevAuth:  devAuth,
		Auth:     authService,
		Messages: messageStore,
		Realtime: clientHub,
	})
	go runOutbox(ctx, messageStore, clientHub, logger)
	go runRetention(
		ctx,
		messageStore,
		logger,
		time.Duration(envInt("SAY_MESSAGE_RETENTION_DAYS", 30))*24*time.Hour,
		envInt("SAY_MESSAGE_KEEP_PER_CONVERSATION", 500),
	)

	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	go func() {
		logger.Info("say-api listening", "addr", addr, "dev_auth", devAuth)
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			logger.Error("say-api stopped", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
}

func runOutbox(
	ctx context.Context,
	store *postgres.MessageStore,
	hub *realtime.Hub,
	logger *slog.Logger,
) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := drainOutbox(ctx, store, hub); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("drain realtime outbox", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func drainOutbox(
	ctx context.Context,
	store *postgres.MessageStore,
	hub *realtime.Hub,
) error {
	events, err := store.PendingOutbox(ctx, 100)
	if err != nil {
		return err
	}
	for _, event := range events {
		if err := hub.Publish(event.UserID, event.Type, event.Payload); err != nil {
			return err
		}
		if err := store.MarkOutboxDelivered(ctx, event.ID); err != nil {
			return err
		}
	}
	return nil
}

func runRetention(
	ctx context.Context,
	store *postgres.MessageStore,
	logger *slog.Logger,
	retention time.Duration,
	keepPerConversation int,
) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		pruneCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		deleted, err := store.Prune(pruneCtx, retention, keepPerConversation)
		cancel()
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("prune message cache", "error", err)
		} else if deleted > 0 {
			logger.Info("pruned message cache", "messages", deleted)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envBool(key string) bool {
	value, err := strconv.ParseBool(os.Getenv(key))
	return err == nil && value
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func mustEnv(logger *slog.Logger, key string) string {
	value := os.Getenv(key)
	if value == "" {
		logger.Error("required environment variable is missing", "key", key)
		os.Exit(1)
	}
	return value
}
