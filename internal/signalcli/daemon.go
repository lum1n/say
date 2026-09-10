package signalcli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type DaemonConfig struct {
	Path       string
	DataDir    string
	SocketPath string
	Logger     *slog.Logger
}

type Daemon struct {
	client *Client
	cmd    *exec.Cmd
	cancel context.CancelFunc
	exited chan error
	log    *slog.Logger
}

func StartDaemon(ctx context.Context, cfg DaemonConfig) (*Daemon, error) {
	if cfg.Path == "" {
		cfg.Path = "signal-cli"
	}
	if cfg.DataDir == "" || cfg.SocketPath == "" {
		return nil, errors.New("signal-cli data directory and socket path are required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create signal data directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(cfg.SocketPath), 0o700); err != nil {
		return nil, fmt.Errorf("create signal socket directory: %w", err)
	}
	if err := os.Remove(cfg.SocketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale signal socket: %w", err)
	}

	processCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(
		processCtx,
		cfg.Path,
		"-c", cfg.DataDir,
		"daemon",
		"--socket", cfg.SocketPath,
		"--receive-mode", "on-start",
		"--no-receive-stdout",
	)
	// signal-cli logs may include message metadata. Keep its streams out of the
	// control-plane logs; operators can opt into its own log file separately.
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start signal-cli: %w", err)
	}

	daemon := &Daemon{
		cmd:    cmd,
		cancel: cancel,
		exited: make(chan error, 1),
		log:    cfg.Logger,
	}
	go func() {
		daemon.exited <- cmd.Wait()
		close(daemon.exited)
	}()

	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = daemon.Stop()
			return nil, ctx.Err()
		case err := <-daemon.exited:
			cancel()
			return nil, fmt.Errorf("signal-cli exited before socket was ready: %w", err)
		case <-deadline.C:
			_ = daemon.Stop()
			return nil, errors.New("signal-cli socket was not ready within 30 seconds")
		case <-ticker.C:
			conn, err := net.DialTimeout("unix", cfg.SocketPath, time.Second)
			if err == nil {
				daemon.client = NewClient(conn)
				cfg.Logger.Info("signal-cli daemon ready", "socket", cfg.SocketPath)
				return daemon, nil
			}
		}
	}
}

func (d *Daemon) Client() *Client {
	return d.client
}

func (d *Daemon) Done() <-chan error {
	return d.exited
}

func (d *Daemon) Stop() error {
	if d.client != nil {
		_ = d.client.Close()
	}
	d.cancel()
	select {
	case err := <-d.exited:
		if err != nil && d.cmd.ProcessState != nil && !d.cmd.ProcessState.Success() {
			return err
		}
		return nil
	case <-time.After(10 * time.Second):
		if d.cmd.Process != nil {
			return d.cmd.Process.Kill()
		}
		return nil
	}
}
