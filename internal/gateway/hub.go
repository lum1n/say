package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vegard/say/internal/protocol"
	"github.com/vegard/say/internal/random"
)

const (
	handshakeTimeout = 10 * time.Second
	heartbeatTimeout = 90 * time.Second
	maxWorkerFrame   = 1 << 20
)

var ErrWorkerOffline = errors.New("worker is offline")

type WorkerCommandError struct {
	Code    string
	Message string
}

func (e *WorkerCommandError) Error() string {
	return e.Code + ": " + e.Message
}

type WorkerEventHandler func(context.Context, Account, protocol.Envelope) error

type workerConnection struct {
	accountID  string
	generation uint64
	conn       *websocket.Conn
	writeMu    sync.Mutex
	pendingMu  sync.Mutex
	pending    map[string]chan protocol.Envelope
	closed     chan struct{}
}

type Hub struct {
	store   AccountStore
	log     *slog.Logger
	onEvent WorkerEventHandler

	mu      sync.Mutex
	workers map[string]*workerConnection
}

func NewHub(store AccountStore, logger *slog.Logger, onEvent WorkerEventHandler) *Hub {
	if logger == nil {
		logger = slog.Default()
	}
	return &Hub{
		store:   store,
		log:     logger,
		onEvent: onEvent,
		workers: make(map[string]*workerConnection),
	}
}

func (h *Hub) ServeWorker(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		HandshakeTimeout: handshakeTimeout,
		CheckOrigin: func(r *http.Request) bool {
			return r.Header.Get("Origin") == ""
		},
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.SetReadLimit(maxWorkerFrame)

	if err := conn.SetReadDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return
	}
	var helloEnvelope protocol.Envelope
	if err := conn.ReadJSON(&helloEnvelope); err != nil {
		h.writeAndClose(conn, "invalid_hello", "failed to read worker hello")
		return
	}
	if err := helloEnvelope.Validate(); err != nil || helloEnvelope.Type != protocol.TypeHello {
		h.writeAndClose(conn, "invalid_hello", "first message must be a valid hello envelope")
		return
	}
	hello, err := protocol.DecodePayload[protocol.Hello](helloEnvelope)
	if err != nil || hello.Token == "" || hello.Platform == "" || hello.Version == "" {
		h.writeAndClose(conn, "invalid_hello", "token, platform, and worker_version are required")
		return
	}

	auth, err := h.store.AuthenticateWorker(r.Context(), hello.Token, hello.Platform)
	if err != nil {
		code := "invalid_token"
		if errors.Is(err, ErrExpiredToken) {
			code = "expired_token"
		} else if errors.Is(err, ErrPlatformMismatch) {
			code = "platform_mismatch"
		}
		h.writeAndClose(conn, code, err.Error())
		return
	}

	worker := &workerConnection{
		accountID:  auth.Account.ID,
		generation: auth.Account.Generation,
		conn:       conn,
		pending:    make(map[string]chan protocol.Envelope),
		closed:     make(chan struct{}),
	}
	if !h.register(worker) {
		h.writeAndClose(conn, "already_connected", "account already has a live worker")
		return
	}
	defer h.unregister(worker)

	ready, err := protocol.NewEnvelope(protocol.TypeReady, protocol.Ready{
		AccountID:  auth.Account.ID,
		WorkerID:   auth.Account.WorkerID,
		Credential: auth.Credential,
		Generation: auth.Account.Generation,
	})
	if err != nil {
		return
	}
	ready.Generation = auth.Account.Generation
	if err := worker.writeJSON(ready); err != nil {
		return
	}

	if err := h.store.TouchWorker(r.Context(), worker.accountID, worker.generation); err != nil {
		h.log.Error("touch connected worker", "account_id", worker.accountID, "error", err)
		return
	}
	h.log.Info("worker connected",
		"account_id", worker.accountID,
		"platform", auth.Account.Platform,
		"worker_version", hello.Version,
		"generation", worker.generation,
	)

	if err := conn.SetReadDeadline(time.Now().Add(heartbeatTimeout)); err != nil {
		return
	}
	for {
		var envelope protocol.Envelope
		if err := conn.ReadJSON(&envelope); err != nil {
			return
		}
		if err := conn.SetReadDeadline(time.Now().Add(heartbeatTimeout)); err != nil {
			return
		}
		if err := envelope.Validate(); err != nil {
			h.log.Warn("invalid worker envelope", "account_id", worker.accountID, "error", err)
			continue
		}
		if envelope.Generation != 0 && envelope.Generation != worker.generation {
			h.log.Warn("stale worker envelope", "account_id", worker.accountID)
			continue
		}
		if envelope.Type == protocol.TypeHeartbeat {
			if err := h.store.TouchWorker(r.Context(), worker.accountID, worker.generation); err != nil {
				h.log.Error("refresh worker heartbeat", "account_id", worker.accountID, "error", err)
				return
			}
			continue
		}
		if envelope.ID != "" && (envelope.Status != "" || envelope.Error != nil) {
			worker.deliver(envelope)
			continue
		}
		envelope.Generation = worker.generation
		if envelope.Type == protocol.TypeAccountStatus {
			status, err := protocol.DecodePayload[protocol.AccountStatus](envelope)
			if err != nil {
				h.log.Warn("invalid account status", "account_id", worker.accountID, "error", err)
				continue
			}
			if err := h.store.SetWorkerIdentity(
				r.Context(),
				worker.accountID,
				worker.generation,
				status.State,
				status.ExternalID,
			); err != nil {
				h.log.Error("persist account status", "account_id", worker.accountID, "error", err)
				return
			}
		}
		if h.onEvent != nil {
			if err := h.onEvent(r.Context(), auth.Account, envelope); err != nil {
				h.log.Error("worker event rejected", "account_id", worker.accountID, "error", err)
				return
			}
		}
		if envelope.ID != "" {
			ack := protocol.Envelope{
				Version:    protocol.Version,
				ID:         envelope.ID,
				Type:       protocol.TypeEventAck,
				Generation: worker.generation,
				Status:     "ok",
			}
			if err := worker.writeJSON(ack); err != nil {
				return
			}
		}
	}
}

func (h *Hub) register(worker *workerConnection) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.workers[worker.accountID]; exists {
		return false
	}
	h.workers[worker.accountID] = worker
	return true
}

func (h *Hub) unregister(worker *workerConnection) {
	h.mu.Lock()
	current, exists := h.workers[worker.accountID]
	if exists && current == worker {
		delete(h.workers, worker.accountID)
		close(worker.closed)
	}
	h.mu.Unlock()
	if exists && current == worker {
		if err := h.store.SetWorkerStatus(context.Background(), worker.accountID, worker.generation, "offline"); err != nil {
			h.log.Error("set worker offline status", "account_id", worker.accountID, "error", err)
		}
		h.log.Info("worker disconnected", "account_id", worker.accountID)
	}
}

func (h *Hub) Request(
	ctx context.Context,
	accountID, commandType string,
	payload any,
) (json.RawMessage, error) {
	h.mu.Lock()
	worker := h.workers[accountID]
	h.mu.Unlock()
	if worker == nil {
		return nil, ErrWorkerOffline
	}
	commandID, err := random.Value("cmd_", 16)
	if err != nil {
		return nil, err
	}
	envelope, err := protocol.NewEnvelope(commandType, payload)
	if err != nil {
		return nil, err
	}
	envelope.ID = commandID
	envelope.Generation = worker.generation

	response := make(chan protocol.Envelope, 1)
	worker.pendingMu.Lock()
	worker.pending[commandID] = response
	worker.pendingMu.Unlock()
	defer func() {
		worker.pendingMu.Lock()
		delete(worker.pending, commandID)
		worker.pendingMu.Unlock()
	}()

	if err := worker.writeJSON(envelope); err != nil {
		return nil, ErrWorkerOffline
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-worker.closed:
		return nil, ErrWorkerOffline
	case result := <-response:
		if result.Error != nil {
			return nil, &WorkerCommandError{
				Code:    result.Error.Code,
				Message: result.Error.Message,
			}
		}
		if result.Status != "ok" {
			return nil, &WorkerCommandError{
				Code:    "invalid_worker_response",
				Message: "worker response did not report success",
			}
		}
		return result.Payload, nil
	}
}

func (h *Hub) Disconnect(accountID string) {
	h.mu.Lock()
	worker := h.workers[accountID]
	h.mu.Unlock()
	if worker != nil {
		_ = worker.conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "account removed"),
			time.Now().Add(time.Second),
		)
		_ = worker.conn.Close()
	}
}

func (w *workerConnection) writeJSON(envelope protocol.Envelope) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	if err := w.conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	return w.conn.WriteJSON(envelope)
}

func (w *workerConnection) deliver(envelope protocol.Envelope) {
	w.pendingMu.Lock()
	response := w.pending[envelope.ID]
	w.pendingMu.Unlock()
	if response == nil {
		return
	}
	select {
	case response <- envelope:
	default:
	}
}

func (h *Hub) writeAndClose(conn *websocket.Conn, code, message string) {
	envelope := protocol.ErrorEnvelope(code, message)
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_ = conn.WriteJSON(envelope)
	payload, _ := json.Marshal(envelope.Error)
	_ = conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.ClosePolicyViolation, string(payload)),
		time.Now().Add(time.Second),
	)
}
