package circuitbreaker

import (
	"errors"
	"testing"
	"time"

	"github.com/dwijamahardika/ledgerd/internal/platform/clock"
)

var errBoom = errors.New("boom")

func TestBreaker_StateMachine(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	var transitions []string
	b := New(Config{FailureThreshold: 3, OpenTimeout: 10 * time.Second, HalfOpenMaxCalls: 1,
		OnStateChange: func(from, to State) { transitions = append(transitions, from.String()+">"+to.String()) }}, clk)

	fail := func() error { return errBoom }
	ok := func() error { return nil }

	// 2 failures + 1 success resets the consecutive counter.
	_ = b.Execute(fail)
	_ = b.Execute(fail)
	_ = b.Execute(ok)
	if b.State() != Closed {
		t.Fatal("success should reset counter")
	}

	for i := 0; i < 3; i++ {
		_ = b.Execute(fail)
	}
	if b.State() != Open {
		t.Fatalf("state=%s want open", b.State())
	}
	if err := b.Execute(ok); !errors.Is(err, ErrOpen) {
		t.Fatalf("open breaker must fail fast, got %v", err)
	}

	clk.Advance(10 * time.Second)
	if b.State() != HalfOpen {
		t.Fatalf("state=%s want half-open", b.State())
	}
	// Probe fails -> straight back to open.
	if err := b.Execute(fail); !errors.Is(err, errBoom) {
		t.Fatalf("probe should run, got %v", err)
	}
	if b.State() != Open {
		t.Fatal("failed probe must reopen")
	}

	clk.Advance(10 * time.Second)
	if err := b.Execute(ok); err != nil {
		t.Fatal(err)
	}
	if b.State() != Closed {
		t.Fatal("successful probe must close")
	}
	want := []string{"closed>open", "open>half-open", "half-open>open", "open>half-open", "half-open>closed"}
	if len(transitions) != len(want) {
		t.Fatalf("transitions %v", transitions)
	}
	for i := range want {
		if transitions[i] != want[i] {
			t.Fatalf("transition %d = %s want %s", i, transitions[i], want[i])
		}
	}
}

func TestBreaker_HalfOpenLimitsConcurrentProbes(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	b := New(Config{FailureThreshold: 1, OpenTimeout: time.Second, HalfOpenMaxCalls: 1}, clk)
	_ = b.Execute(func() error { return errBoom })
	clk.Advance(time.Second)

	release := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- b.Execute(func() error { <-release; return nil }) }()
	// Wait until the probe is in flight.
	deadline := time.Now().Add(time.Second)
	for {
		b.mu.Lock()
		busy := b.halfOpenBusy
		b.mu.Unlock()
		if busy == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err := b.Execute(func() error { return nil }); !errors.Is(err, ErrOpen) {
		t.Fatalf("second probe should be rejected, got %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if b.State() != Closed {
		t.Fatal("should close after probe success")
	}
}
