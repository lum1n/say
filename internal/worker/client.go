package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vegard/say/internal/protocol"
)

type CommandHandler interface {
	HandleCommand(context.Context, protocol.Envelope) (any, *protocol.Error)
}

type Config struct {
	Server         string
	Platform       string
	Version        string
	Token          string
	CredentialFile string
	EventSpoolDir  string
	Events         <-chan protocol.Envelope
	Handler        CommandHandler
	Logger         *slog.Logger
}

type Client struct {
	cfg    Config
	spool  *eventSpool
	notify chan struct{}
}

func New(cfg Config) (*Client, error) {
	if cfg.Server == "" || cfg.Platform == "" || cfg.Version == "" {
		return nil, errors.New("server, platform, and version are required")
	}
	if cfg.Handler == nil {
		return nil, errors.New("command handler is required")
	}
	serverURL, err := url.Parse(cfg.Server)
	if err != nil || (serverURL.Scheme != "ws" && serverURL.Scheme != "wss") || serverURL.Host == "" {
		return nil, errors.New("server must be a valid ws or wss URL")
	}
	if serverURL.Scheme != "wss" && !isLoopbackHost(serverURL.Hostname()) {
		return nil, errors.New("worker credentials require wss outside localhost")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if credential := readCredential(cfg.CredentialFile); credential != "" {
		cfg.Token = credential
	}
	if cfg.Token == "" {
		return nil, errors.New("enrollment token or stored worker credential is required")
	}
	var spool *eventSpool
	if cfg.Events != nil {
		if cfg.EventSpoolDir == "" {
			cfg.EventSpoolDir = cfg.CredentialFile + "-events"
		}
		spool, err = newEventSpool(cfg.EventSpoolDir)
		if err != nil {
			return nil, err
		}
	}
	return &Client{cfg: cfg, spool: spool, notify: make(chan struct{}, 1)}, nil
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (c *Client) Run(ctx context.Context) error {
	if c.spool != nil {
		go c.collectEvents(ctx)
	}
	backoff := time.Second
	for ctx.Err() == nil {
		startedAt := time.Now()
		credential, err := c.connect(ctx)
		if credential != "" {
			c.cfg.Token = credential
		}
		if err == nil || errors.Is(err, context.Canceled) {
			return err
		}
		if time.Since(startedAt) >= time.Minute {
			backoff = time.Second
		}
		c.cfg.Logger.Warn("worker connection lost", "error", err, "retry_in", backoff)
		delay := backoff + time.Duration(rand.Int64N(int64(backoff)))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
	return ctx.Err()
}

func (c *Client) connect(ctx context.Context) (string, error) {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.cfg.Server, nil)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	hello, err := protocol.NewEnvelope(protocol.TypeHello, protocol.Hello{
		Token:    c.cfg.Token,
		Platform: c.cfg.Platform,
		Version:  c.cfg.Version,
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
		if err := writeCredential(c.cfg.CredentialFile, ready.Credential); err != nil {
			return "", err
		}
		c.cfg.Token = ready.Credential
	}
	c.cfg.Logger.Info("worker connected",
		"account_id", ready.AccountID,
		"generation", ready.Generation,
	)

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	incoming := make(chan protocol.Envelope)
	readErrors := make(chan error, 1)
	go readLoop(sessionCtx, conn, incoming, readErrors)

	type commandResult struct {
		envelope protocol.Envelope
	}
	results := make(chan commandResult, 16)
	concurrency := make(chan struct{}, 4)
	heartbeat := time.NewTicker(25 * time.Second)
	defer heartbeat.Stop()
	sentEvents := make(map[string]struct{})
	if err := c.sendPendingEvents(ctx, conn, ready.Generation, sentEvents); err != nil {
		return c.cfg.Token, err
	}

	for {
		select {
		case <-ctx.Done():
			return c.cfg.Token, ctx.Err()
		case err := <-readErrors:
			return c.cfg.Token, err
		case envelope, ok := <-incoming:
			if !ok {
				return c.cfg.Token, errors.New("worker connection closed")
			}
			if envelope.Type == protocol.TypeEventAck && envelope.ID != "" {
				if envelope.Status == "ok" {
					if err := c.spool.Ack(sessionCtx, envelope.ID); err != nil {
						return c.cfg.Token, err
					}
					delete(sentEvents, envelope.ID)
				}
				continue
			}
			if envelope.ID == "" {
				continue
			}
			select {
			case concurrency <- struct{}{}:
			default:
				results <- commandResult{envelope: protocol.Envelope{
					Version:    protocol.Version,
					ID:         envelope.ID,
					Type:       envelope.Type,
					Generation: ready.Generation,
					Status:     "error",
					Error: &protocol.Error{
						Code:    "worker_busy",
						Message: "worker is already handling its command limit",
					},
				}}
				continue
			}
			go func(command protocol.Envelope) {
				defer func() { <-concurrency }()
				payload, commandError := c.cfg.Handler.HandleCommand(sessionCtx, command)
				result := protocol.Envelope{
					Version:    protocol.Version,
					ID:         command.ID,
					Type:       command.Type,
					Generation: ready.Generation,
				}
				if commandError != nil {
					result.Status = "error"
					result.Error = commandError
				} else {
					result.Status = "ok"
					success, marshalErr := protocol.NewEnvelope(command.Type, payload)
					if marshalErr != nil {
						result.Status = "error"
						result.Error = &protocol.Error{
							Code:    "encode_failed",
							Message: "worker failed to encode command result",
						}
					} else {
						result.Payload = success.Payload
					}
				}
				select {
				case results <- commandResult{envelope: result}:
				case <-sessionCtx.Done():
				}
			}(envelope)
		case result := <-results:
			if err := writeEnvelope(conn, result.envelope); err != nil {
				return c.cfg.Token, err
			}
		case <-c.notify:
			if err := c.sendPendingEvents(sessionCtx, conn, ready.Generation, sentEvents); err != nil {
				return c.cfg.Token, err
			}
		case <-heartbeat.C:
			envelope := protocol.Envelope{
				Version:    protocol.Version,
				Type:       protocol.TypeHeartbeat,
				Generation: ready.Generation,
			}
			if err := writeEnvelope(conn, envelope); err != nil {
				return c.cfg.Token, err
			}
		}
	}
}

func (c *Client) collectEvents(ctx context.Context) {
	events := c.cfg.Events
	for events != nil {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			for {
				err := c.spool.Append(ctx, event)
				if err == nil {
					select {
					case c.notify <- struct{}{}:
					default:
					}
					break
				}
				if errors.Is(err, context.Canceled) {
					return
				}
				c.cfg.Logger.Error("spool worker event", "error", err)
				timer := time.NewTimer(time.Second)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}
	}
}

func (c *Client) sendPendingEvents(
	ctx context.Context,
	conn *websocket.Conn,
	generation uint64,
	sent map[string]struct{},
) error {
	if c.spool == nil {
		return nil
	}
	events, err := c.spool.Pending(ctx)
	if err != nil {
		return err
	}
	for _, event := range events {
		if _, alreadySent := sent[event.ID]; alreadySent {
			continue
		}
		event.Generation = generation
		if err := writeEnvelope(conn, event); err != nil {
			return err
		}
		sent[event.ID] = struct{}{}
	}
	return nil
}

func readLoop(
	ctx context.Context,
	conn *websocket.Conn,
	incoming chan<- protocol.Envelope,
	readErrors chan<- error,
) {
	defer close(incoming)
	for {
		var envelope protocol.Envelope
		if err := conn.ReadJSON(&envelope); err != nil {
			select {
			case readErrors <- err:
			case <-ctx.Done():
			}
			return
		}
		select {
		case incoming <- envelope:
		case <-ctx.Done():
			return
		}
	}
}

func writeEnvelope(conn *websocket.Conn, envelope protocol.Envelope) error {
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	return conn.WriteJSON(envelope)
}

func readCredential(path string) string {
	if path == "" {
		return ""
	}
	value, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(value))
}

func writeCredential(path, credential string) error {
	if path == "" {
		return errors.New("credential file is required after enrollment")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create credential directory: %w", err)
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, []byte(credential+"\n"), 0o600); err != nil {
		return fmt.Errorf("write worker credential: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		return fmt.Errorf("replace worker credential: %w", err)
	}
	return nil
}
