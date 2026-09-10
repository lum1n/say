package worker

import (
	"testing"

	"github.com/vegard/say/internal/protocol"
)

func TestEventSpoolSurvivesRestartUntilAcknowledged(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	first, err := newEventSpool(dir)
	if err != nil {
		t.Fatalf("create first spool: %v", err)
	}
	event, _ := protocol.NewEnvelope(
		protocol.TypeAccountStatus,
		protocol.AccountStatus{State: "live"},
	)
	if err := first.Append(ctx, event); err != nil {
		t.Fatalf("append event: %v", err)
	}

	restarted, err := newEventSpool(dir)
	if err != nil {
		t.Fatalf("reopen spool: %v", err)
	}
	pending, err := restarted.Pending(ctx)
	if err != nil || len(pending) != 1 || pending[0].ID == "" {
		t.Fatalf("pending events = %#v, err=%v", pending, err)
	}
	if err := restarted.Ack(ctx, pending[0].ID); err != nil {
		t.Fatalf("ack event: %v", err)
	}
	pending, err = restarted.Pending(ctx)
	if err != nil || len(pending) != 0 {
		t.Fatalf("events after ack = %#v, err=%v", pending, err)
	}
}
