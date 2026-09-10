package signalcli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("signal-cli JSON-RPC %d: %s", e.Code, e.Message)
}

type Notification struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type wireMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

type Client struct {
	conn io.ReadWriteCloser

	nextID  atomic.Uint64
	writeMu sync.Mutex
	mu      sync.Mutex
	pending map[string]chan wireMessage

	notifications chan Notification
	done          chan struct{}
	closeOnce     sync.Once
	errMu         sync.Mutex
	readErr       error
}

func NewClient(conn io.ReadWriteCloser) *Client {
	client := &Client{
		conn:          conn,
		pending:       make(map[string]chan wireMessage),
		notifications: make(chan Notification, 64),
		done:          make(chan struct{}),
	}
	go client.readLoop()
	return client
}

func (c *Client) Call(ctx context.Context, method string, params any, result any) error {
	id := fmt.Sprintf("%d", c.nextID.Add(1))
	rawParams, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("marshal %s params: %w", method, err)
	}
	requestID, _ := json.Marshal(id)
	request := wireMessage{
		JSONRPC: "2.0",
		ID:      requestID,
		Method:  method,
		Params:  rawParams,
	}
	response := make(chan wireMessage, 1)
	c.mu.Lock()
	c.pending[id] = response
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	c.writeMu.Lock()
	err = json.NewEncoder(c.conn).Encode(request)
	c.writeMu.Unlock()
	if err != nil {
		return fmt.Errorf("write %s request: %w", method, err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.connectionError()
	case message := <-response:
		if message.Error != nil {
			return message.Error
		}
		if result == nil || len(message.Result) == 0 || string(message.Result) == "null" {
			return nil
		}
		if err := json.Unmarshal(message.Result, result); err != nil {
			return fmt.Errorf("decode %s result: %w", method, err)
		}
		return nil
	}
}

func (c *Client) Notifications() <-chan Notification {
	return c.notifications
}

func (c *Client) Done() <-chan struct{} {
	return c.done
}

func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		err = c.conn.Close()
	})
	return err
}

func (c *Client) readLoop() {
	defer close(c.done)
	defer close(c.notifications)
	decoder := json.NewDecoder(c.conn)
	for {
		var message wireMessage
		if err := decoder.Decode(&message); err != nil {
			if !errors.Is(err, io.EOF) {
				c.errMu.Lock()
				c.readErr = err
				c.errMu.Unlock()
			}
			return
		}
		if message.Method != "" && len(message.ID) == 0 {
			notification := Notification{Method: message.Method, Params: message.Params}
			select {
			case c.notifications <- notification:
			default:
				c.errMu.Lock()
				c.readErr = errors.New("signal-cli notification buffer overflow")
				c.errMu.Unlock()
				_ = c.Close()
				return
			}
			continue
		}
		var id string
		if err := json.Unmarshal(message.ID, &id); err != nil {
			continue
		}
		c.mu.Lock()
		pending := c.pending[id]
		c.mu.Unlock()
		if pending != nil {
			select {
			case pending <- message:
			default:
			}
		}
	}
}

func (c *Client) connectionError() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	if c.readErr != nil {
		return c.readErr
	}
	return errors.New("signal-cli connection closed")
}
