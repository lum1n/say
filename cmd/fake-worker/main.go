package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vegard/say/internal/protocol"
)

type config struct {
	server         string
	platform       string
	token          string
	credentialFile string
	emitEvery      time.Duration
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.server, "server", "ws://localhost:8080/v1/workers/connect", "worker WebSocket URL")
	flag.StringVar(&cfg.platform, "platform", "signal", "signal or telegram")
	flag.StringVar(&cfg.token, "token", "", "single-use enrollment token")
	flag.StringVar(&cfg.credentialFile, "credential-file", ".fake-worker-credential", "worker credential file")
	flag.DurationVar(&cfg.emitEvery, "emit-every", 0, "emit a fake message at this interval")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	credential := readCredential(cfg.credentialFile)
	if credential == "" {
		credential = cfg.token
	}
	if credential == "" {
		logger.Error("token is required on first run")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	backoff := time.Second
	for ctx.Err() == nil {
		nextCredential, err := run(ctx, cfg, credential, logger)
		if nextCredential != "" {
			credential = nextCredential
		}
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		logger.Warn("worker disconnected", "error", err, "retry_in", backoff)
		delay := backoff + time.Duration(rand.Int64N(int64(backoff)))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func run(
	ctx context.Context,
	cfg config,
	credential string,
	logger *slog.Logger,
) (string, error) {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, cfg.server, nil)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	hello, err := protocol.NewEnvelope(protocol.TypeHello, protocol.Hello{
		Token:    credential,
		Platform: cfg.platform,
		Version:  "fake-worker/0.1.0",
	})
	if err != nil {
		return "", err
	}
	if err := writeEnvelope(conn, hello); err != nil {
		return "", err
	}

	var readyEnvelope protocol.Envelope
	if err := conn.ReadJSON(&readyEnvelope); err != nil {
		return "", err
	}
	if readyEnvelope.Error != nil {
		return "", fmt.Errorf("%s: %s", readyEnvelope.Error.Code, readyEnvelope.Error.Message)
	}
	if err := readyEnvelope.Validate(); err != nil || readyEnvelope.Type != protocol.TypeReady {
		return "", errors.New("server did not return a valid ready envelope")
	}
	ready, err := protocol.DecodePayload[protocol.Ready](readyEnvelope)
	if err != nil {
		return "", err
	}
	if ready.Credential != "" {
		if err := os.WriteFile(cfg.credentialFile, []byte(ready.Credential+"\n"), 0o600); err != nil {
			return "", fmt.Errorf("save worker credential: %w", err)
		}
		credential = ready.Credential
	}
	logger.Info("worker ready",
		"account_id", ready.AccountID,
		"generation", ready.Generation,
		"platform", cfg.platform,
	)

	incoming := make(chan protocol.Envelope)
	readErrors := make(chan error, 1)
	go func() {
		defer close(incoming)
		for {
			var envelope protocol.Envelope
			if err := conn.ReadJSON(&envelope); err != nil {
				readErrors <- err
				return
			}
			incoming <- envelope
		}
	}()

	heartbeat := time.NewTicker(25 * time.Second)
	defer heartbeat.Stop()
	var emitted <-chan time.Time
	var emitter *time.Ticker
	if cfg.emitEvery > 0 {
		emitter = time.NewTicker(cfg.emitEvery)
		emitted = emitter.C
		defer emitter.Stop()
	}

	status, _ := protocol.NewEnvelope(protocol.TypeAccountStatus, map[string]string{"state": "live"})
	status.Generation = ready.Generation
	if err := writeEnvelope(conn, status); err != nil {
		return credential, err
	}

	for {
		select {
		case <-ctx.Done():
			return credential, ctx.Err()
		case err := <-readErrors:
			return credential, err
		case envelope, ok := <-incoming:
			if !ok {
				return credential, errors.New("connection closed")
			}
			response := protocol.Envelope{
				Version:    protocol.Version,
				ID:         envelope.ID,
				Type:       envelope.Type,
				Generation: ready.Generation,
				Status:     "ok",
				Payload:    json.RawMessage(`{"fake":true}`),
			}
			if err := writeEnvelope(conn, response); err != nil {
				return credential, err
			}
		case <-heartbeat.C:
			envelope := protocol.Envelope{
				Version:    protocol.Version,
				Type:       protocol.TypeHeartbeat,
				Generation: ready.Generation,
			}
			if err := writeEnvelope(conn, envelope); err != nil {
				return credential, err
			}
		case at := <-emitted:
			envelope, err := protocol.NewEnvelope(protocol.TypeMessageNew, map[string]any{
				"provider_message_id": fmt.Sprintf("fake-%d", at.UnixNano()),
				"conversation_id":     "fake-conversation",
				"author":              "fake-sender",
				"body":                "hello from the fake worker",
				"sent_at":             at.UTC(),
			})
			if err != nil {
				return credential, err
			}
			envelope.Generation = ready.Generation
			if err := writeEnvelope(conn, envelope); err != nil {
				return credential, err
			}
		}
	}
}

func writeEnvelope(conn *websocket.Conn, envelope protocol.Envelope) error {
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	return conn.WriteJSON(envelope)
}

func readCredential(path string) string {
	value, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(value))
}
