package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/vegard/say/internal/signalcli"
	"github.com/vegard/say/internal/signalworker"
	"github.com/vegard/say/internal/worker"
)

var version = "dev"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	daemon, err := signalcli.StartDaemon(ctx, signalcli.DaemonConfig{
		Path:       envOr("SIGNAL_CLI_PATH", "signal-cli"),
		DataDir:    envOr("SIGNAL_DATA_DIR", "/data/signal"),
		SocketPath: envOr("SIGNAL_SOCKET_PATH", "/run/say/signal-cli.sock"),
		Logger:     logger,
	})
	if err != nil {
		logger.Error("start signal-cli daemon", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := daemon.Stop(); err != nil && ctx.Err() == nil {
			logger.Error("stop signal-cli daemon", "error", err)
		}
	}()

	service := signalworker.New(ctx, daemon.Client(), envOr("SIGNAL_LINK_NAME", "say"), logger)
	detectCtx, cancelDetect := context.WithTimeout(ctx, 10*time.Second)
	account, detectErr := service.DetectAccount(detectCtx)
	cancelDetect()
	if detectErr != nil {
		logger.Warn("detect existing Signal account", "error", detectErr)
	}
	if account == "" {
		service.PublishStatus(ctx, "unlinked", "")
	} else {
		service.PublishStatus(ctx, "live", account)
	}

	credentialFile := envOr("SAY_WORKER_CREDENTIAL_FILE", "/data/worker-credential")
	client, err := worker.New(worker.Config{
		Server:         os.Getenv("SAY_WORKER_URL"),
		Platform:       "signal",
		Version:        "say-signal/" + version,
		Token:          os.Getenv("SAY_ENROLLMENT_TOKEN"),
		CredentialFile: credentialFile,
		EventSpoolDir:  envOr("SAY_EVENT_SPOOL_DIR", filepath.Join(filepath.Dir(credentialFile), "events")),
		Events:         service.Events(),
		Handler:        service,
		Logger:         logger,
	})
	if err != nil {
		logger.Error("configure say worker", "error", err)
		os.Exit(1)
	}

	clientResult := make(chan error, 1)
	go func() { clientResult <- client.Run(ctx) }()

	select {
	case <-ctx.Done():
	case err := <-daemon.Done():
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("signal-cli daemon exited", "error", err)
		}
		stop()
	case err := <-clientResult:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("worker client stopped", "error", err)
		}
		stop()
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
