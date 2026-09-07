// Package circuitbreaker protects a downstream from being hammered while it is
// failing. Classic three-state machine:
//
//	closed    -> normal operation; consecutive failures are counted
//	open      -> calls fail fast with ErrOpen until OpenTimeout elapses
//	half-open -> a limited number of probe calls pass through; success closes
//	             the breaker, failure re-opens it
package circuitbreaker

import (
	"errors"
	"sync"
	"time"

	"github.com/dwijamahardika/ledgerd/internal/platform/clock"
)

var ErrOpen = errors.New("circuit breaker is open")

type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	default:
		return "half-open"
	}
}

type Config struct {
	FailureThreshold int           // consecutive failures that trip the breaker
	OpenTimeout      time.Duration // how long to stay open before probing
	HalfOpenMaxCalls int           // concurrent probes allowed while half-open
	OnStateChange    func(from, to State)
}

type Breaker struct {
	cfg   Config
	clock clock.Clock

	mu           sync.Mutex
	state        State
	failures     int
	openedAt     time.Time
	halfOpenBusy int
}

func New(cfg Config, clk clock.Clock) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = 30 * time.Second
	}
	if cfg.HalfOpenMaxCalls <= 0 {
		cfg.HalfOpenMaxCalls = 1
	}
	if clk == nil {
		clk = clock.System{}
	}
	return &Breaker{cfg: cfg, clock: clk}
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeTransitionToHalfOpen()
	return b.state
}

// Execute runs fn if the breaker permits it and records the outcome.
func (b *Breaker) Execute(fn func() error) error {
	if err := b.acquire(); err != nil {
		return err
	}
	err := fn()
	b.record(err)
	return err
}

func (b *Breaker) acquire() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeTransitionToHalfOpen()
	switch b.state {
	case Open:
		return ErrOpen
	case HalfOpen:
		if b.halfOpenBusy >= b.cfg.HalfOpenMaxCalls {
			return ErrOpen
		}
		b.halfOpenBusy++
	}
	return nil
}

func (b *Breaker) record(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == HalfOpen {
		b.halfOpenBusy--
	}
	if err == nil {
		b.failures = 0
		if b.state == HalfOpen {
			b.setState(Closed)
		}
		return
	}
	b.failures++
	if b.state == HalfOpen || b.failures >= b.cfg.FailureThreshold {
		b.openedAt = b.clock.Now()
		b.setState(Open)
	}
}

// caller must hold mu
func (b *Breaker) maybeTransitionToHalfOpen() {
	if b.state == Open && b.clock.Now().Sub(b.openedAt) >= b.cfg.OpenTimeout {
		b.halfOpenBusy = 0
		b.setState(HalfOpen)
	}
}

// caller must hold mu
func (b *Breaker) setState(s State) {
	if b.state == s {
		return
	}
	from := b.state
	b.state = s
	if b.cfg.OnStateChange != nil {
		b.cfg.OnStateChange(from, s)
	}
}
