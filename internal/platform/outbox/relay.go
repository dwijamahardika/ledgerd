// Package outbox implements the relay half of the transactional outbox pattern.
// Writers insert events in the same transaction as their state change; this
// relay polls for due rows, publishes them, and records the outcome. At-least-
// once delivery is the contract: consumers must dedupe on event ID.
package outbox

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/dwijamahardika/ledgerd/internal/domain"
	"github.com/dwijamahardika/ledgerd/internal/platform/backoff"
)

type Message struct {
	ID       int64
	Event    domain.Event
	Attempts int
}

type Outcome struct {
	Err     error
	RetryIn time.Duration
	Dead    bool
}

type Source interface {
	// ProcessBatch must claim up to limit due messages under a lock, invoke fn,
	// persist the returned outcomes atomically, and report how many were claimed.
	ProcessBatch(ctx context.Context, limit int, fn func(ctx context.Context, msgs []Message) []Outcome) (int, error)
}

type Publisher interface {
	Publish(ctx context.Context, e domain.Event) error
}

// ErrRetryable-wrapped errors are retried; anything else after MaxAttempts is
// parked as dead for operator inspection.
type Metrics interface {
	Published(eventType string)
	Failed(eventType string, dead bool)
}

type noopMetrics struct{}

func (noopMetrics) Published(string)    {}
func (noopMetrics) Failed(string, bool) {}

type Config struct {
	PollInterval time.Duration
	BatchSize    int
	MaxAttempts  int
	Backoff      backoff.Policy
	Metrics      Metrics
}

type Relay struct {
	src Source
	pub Publisher
	cfg Config
	log *slog.Logger
}

func NewRelay(src Source, pub Publisher, cfg Config, log *slog.Logger) *Relay {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 10
	}
	if cfg.Backoff.Base == 0 {
		cfg.Backoff = backoff.New(500*time.Millisecond, 5*time.Minute)
	}
	if cfg.Metrics == nil {
		cfg.Metrics = noopMetrics{}
	}
	return &Relay{src: src, pub: pub, cfg: cfg, log: log}
}

// Run blocks until ctx is cancelled. It drains greedily: as long as a full
// batch was claimed it polls again immediately, otherwise waits PollInterval.
func (r *Relay) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		n, err := r.RunOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			r.log.Error("outbox batch failed", "err", err)
		}
		if n == r.cfg.BatchSize {
			timer.Reset(0)
		} else {
			timer.Reset(r.cfg.PollInterval)
		}
	}
}

// RunOnce processes a single batch; exposed for tests and one-shot CLI use.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	return r.src.ProcessBatch(ctx, r.cfg.BatchSize, r.deliver)
}

func (r *Relay) deliver(ctx context.Context, msgs []Message) []Outcome {
	outcomes := make([]Outcome, len(msgs))
	for i, m := range msgs {
		err := r.pub.Publish(ctx, m.Event)
		if err == nil {
			r.cfg.Metrics.Published(m.Event.Type)
			continue
		}
		attempt := m.Attempts + 1
		dead := attempt >= r.cfg.MaxAttempts
		outcomes[i] = Outcome{Err: err, Dead: dead, RetryIn: r.cfg.Backoff.Next(attempt)}
		r.cfg.Metrics.Failed(m.Event.Type, dead)
		r.log.Warn("outbox publish failed", "event_id", m.Event.ID, "type", m.Event.Type, "attempt", attempt, "dead", dead, "err", err)
	}
	return outcomes
}
