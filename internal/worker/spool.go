package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/vegard/say/internal/protocol"
	"github.com/vegard/say/internal/random"
)

type eventSpool struct {
	dir string
	mu  sync.Mutex
}

func newEventSpool(dir string) (*eventSpool, error) {
	if dir == "" {
		return nil, errors.New("event spool directory is required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create event spool: %w", err)
	}
	return &eventSpool{dir: dir}, nil
}

func (s *eventSpool) Append(ctx context.Context, event protocol.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if event.ID == "" {
		sequence, err := s.nextSequenceLocked()
		if err != nil {
			return err
		}
		suffix, err := random.Value("", 12)
		if err != nil {
			return err
		}
		event.ID = fmt.Sprintf("evt_%020d_%s", sequence, suffix)
	}
	if filepath.Base(event.ID) != event.ID {
		return errors.New("invalid event id")
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	path := filepath.Join(s.dir, event.ID+".json")
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect event spool: %w", err)
	}
	temporary := path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create spooled event: %w", err)
	}
	if _, err := file.Write(encoded); err != nil {
		_ = file.Close()
		return fmt.Errorf("write spooled event: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync spooled event: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close spooled event: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		return fmt.Errorf("commit spooled event: %w", err)
	}
	return syncDirectory(s.dir)
}

func (s *eventSpool) nextSequenceLocked() (uint64, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return 0, fmt.Errorf("read event spool sequence: %w", err)
	}
	var maximum uint64
	for _, entry := range entries {
		name := entry.Name()
		if len(name) < len("evt_")+20 || !strings.HasPrefix(name, "evt_") {
			continue
		}
		value, err := strconv.ParseUint(name[len("evt_"):len("evt_")+20], 10, 64)
		if err == nil && value > maximum {
			maximum = value
		}
	}
	if maximum == ^uint64(0) {
		return 0, errors.New("event spool sequence exhausted")
	}
	return maximum + 1, nil
}

func (s *eventSpool) Pending(ctx context.Context) ([]protocol.Envelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("read event spool: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	events := make([]protocol.Envelope, 0, len(names))
	for _, name := range names {
		encoded, err := os.ReadFile(filepath.Join(s.dir, name))
		if err != nil {
			return nil, fmt.Errorf("read spooled event: %w", err)
		}
		var event protocol.Envelope
		if err := json.Unmarshal(encoded, &event); err != nil {
			return nil, fmt.Errorf("decode spooled event %s: %w", name, err)
		}
		if event.ID == "" || event.ID+".json" != name {
			return nil, fmt.Errorf("spooled event %s has an invalid id", name)
		}
		events = append(events, event)
	}
	return events, nil
}

func (s *eventSpool) Ack(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == "" || filepath.Base(id) != id {
		return errors.New("invalid event id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(filepath.Join(s.dir, id+".json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("remove acknowledged event: %w", err)
	}
	return syncDirectory(s.dir)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open spool directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync spool directory: %w", err)
	}
	return nil
}
