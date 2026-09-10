package realtime

import (
	"encoding/json"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vegard/say/internal/protocol"
)

type subscriber struct {
	events chan protocol.Envelope
	done   chan struct{}
	once   sync.Once
	active atomic.Bool
}

func (s *subscriber) close() {
	s.once.Do(func() { close(s.done) })
}

type Hub struct {
	mu          sync.RWMutex
	subscribers map[string]map[*subscriber]struct{}
}

func NewHub() *Hub {
	return &Hub{subscribers: make(map[string]map[*subscriber]struct{})}
}

func (h *Hub) Publish(userID, eventType string, payload any) error {
	envelope, err := protocol.NewEnvelope(eventType, payload)
	if err != nil {
		return err
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for subscriber := range h.subscribers[userID] {
		if !subscriber.active.Load() {
			continue
		}
		select {
		case subscriber.events <- envelope:
		default:
			subscriber.close()
		}
	}
	return nil
}

func (h *Hub) Serve(w http.ResponseWriter, r *http.Request, userID string) {
	upgrader := websocket.Upgrader{
		HandshakeTimeout: 10 * time.Second,
		CheckOrigin:      sameOriginOrNative,
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.SetReadLimit(64 << 10)

	sub := &subscriber{
		events: make(chan protocol.Envelope, 64),
		done:   make(chan struct{}),
	}
	sub.active.Store(true)
	h.add(userID, sub)
	defer h.remove(userID, sub)

	incoming := make(chan protocol.Envelope)
	readError := make(chan struct{})
	go func() {
		defer close(readError)
		for {
			var envelope protocol.Envelope
			if err := conn.ReadJSON(&envelope); err != nil {
				return
			}
			select {
			case incoming <- envelope:
			case <-sub.done:
				return
			}
		}
	}()

	ping := time.NewTicker(25 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-sub.done:
			return
		case <-readError:
			return
		case event := <-sub.events:
			if !sub.active.Load() {
				continue
			}
			if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
				return
			}
			if err := conn.WriteJSON(event); err != nil {
				return
			}
		case request := <-incoming:
			if request.Type != "subscribe" && request.Type != "unsubscribe" {
				continue
			}
			state := "subscribed"
			if request.Type == "unsubscribe" {
				state = "unsubscribed"
				sub.active.Store(false)
			} else {
				sub.active.Store(true)
			}
			ackPayload, _ := json.Marshal(map[string]string{"state": state})
			ack := protocol.Envelope{
				Version: protocol.Version,
				ID:      request.ID,
				Type:    state,
				Status:  "ok",
				Payload: ackPayload,
			}
			if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
				return
			}
			if err := conn.WriteJSON(ack); err != nil {
				return
			}
		case <-ping.C:
			if err := conn.WriteControl(
				websocket.PingMessage,
				nil,
				time.Now().Add(10*time.Second),
			); err != nil {
				return
			}
		}
	}
}

func (h *Hub) add(userID string, sub *subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subscribers[userID] == nil {
		h.subscribers[userID] = make(map[*subscriber]struct{})
	}
	h.subscribers[userID][sub] = struct{}{}
}

func (h *Hub) remove(userID string, sub *subscriber) {
	sub.close()
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.subscribers[userID], sub)
	if len(h.subscribers[userID]) == 0 {
		delete(h.subscribers, userID)
	}
}

func sameOriginOrNative(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	parsed, err := url.Parse(origin)
	return err == nil && parsed.Host == r.Host
}
