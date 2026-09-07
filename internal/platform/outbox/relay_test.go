package outbox

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dwijamahardika/ledgerd/internal/domain"
	"github.com/dwijamahardika/ledgerd/internal/platform/backoff"
)

type memSource struct {
	pending []Message
	sent    []int64
	dead    []int64
	retried map[int64]time.Duration
}

func (m *memSource) ProcessBatch(ctx context.Context, limit int, fn func(context.Context, []Message) []Outcome) (int, error) {
	n := min(limit, len(m.pending))
	batch := m.pending[:n]
	outs := fn(ctx, batch)
	var keep []Message
	for i, o := range outs {
		msg := batch[i]
		switch {
		case o.Err == nil:
			m.sent = append(m.sent, msg.ID)
		case o.Dead:
			m.dead = append(m.dead, msg.ID)
		default:
			msg.Attempts++
			m.retried[msg.ID] = o.RetryIn
			keep = append(keep, msg)
		}
	}
	m.pending = append(keep, m.pending[n:]...)
	return n, nil
}

type flakyPub struct{ failIDs map[uuid.UUID]int }

func (f *flakyPub) Publish(_ context.Context, e domain.Event) error {
	if n := f.failIDs[e.ID]; n > 0 {
		f.failIDs[e.ID] = n - 1
		return errors.New("receiver down")
	}
	return nil
}

func TestRelay_SuccessRetryDead(t *testing.T) {
	good := domain.Event{ID: uuid.New(), Type: "x"}
	flaky := domain.Event{ID: uuid.New(), Type: "x"}
	broken := domain.Event{ID: uuid.New(), Type: "x"}
	src := &memSource{retried: map[int64]time.Duration{}, pending: []Message{{ID: 1, Event: good}, {ID: 2, Event: flaky}, {ID: 3, Event: broken}}}
	pub := &flakyPub{failIDs: map[uuid.UUID]int{flaky.ID: 1, broken.ID: 100}}
	r := NewRelay(src, pub, Config{BatchSize: 10, MaxAttempts: 3, Backoff: backoff.New(time.Millisecond, time.Millisecond).WithSeed(1)}, slog.Default())

	// pass 1: good sent, flaky+broken retried
	if n, err := r.RunOnce(context.Background()); err != nil || n != 3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if len(src.sent) != 1 || len(src.pending) != 2 {
		t.Fatalf("after pass1 sent=%v pending=%d", src.sent, len(src.pending))
	}
	// pass 2: flaky recovers, broken retried (attempt 2)
	_, _ = r.RunOnce(context.Background())
	if len(src.sent) != 2 || len(src.pending) != 1 {
		t.Fatalf("after pass2 sent=%v pending=%d", src.sent, len(src.pending))
	}
	// pass 3: broken hits MaxAttempts -> dead
	_, _ = r.RunOnce(context.Background())
	if len(src.dead) != 1 || src.dead[0] != 3 || len(src.pending) != 0 {
		t.Fatalf("after pass3 dead=%v pending=%d", src.dead, len(src.pending))
	}
}

func TestRelay_RunStopsOnCancel(t *testing.T) {
	src := &memSource{retried: map[int64]time.Duration{}}
	r := NewRelay(src, &flakyPub{}, Config{PollInterval: time.Millisecond}, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := r.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
}
