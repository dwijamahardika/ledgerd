package ratelimit

import (
	"sync"
	"testing"
	"time"

	"github.com/dwijamahardika/ledgerd/internal/platform/clock"
)

func TestLimiter_BurstThenRefill(t *testing.T) {
	clk := clock.NewFake(time.Unix(1_700_000_000, 0))
	l := New(2, 3, clk) // 2 tokens/s, burst 3

	for i := 0; i < 3; i++ {
		if d := l.Allow("k"); !d.Allowed {
			t.Fatalf("request %d should pass burst", i)
		}
	}
	d := l.Allow("k")
	if d.Allowed {
		t.Fatal("4th request should be limited")
	}
	if d.RetryAfter <= 0 || d.RetryAfter > 500*time.Millisecond {
		t.Fatalf("RetryAfter=%v, want ~500ms", d.RetryAfter)
	}

	clk.Advance(500 * time.Millisecond) // +1 token
	if !l.Allow("k").Allowed {
		t.Fatal("should have refilled one token")
	}
	if l.Allow("k").Allowed {
		t.Fatal("should be empty again")
	}

	clk.Advance(time.Hour) // capped at burst, not unbounded
	for i := 0; i < 3; i++ {
		if !l.Allow("k").Allowed {
			t.Fatalf("burst refill %d", i)
		}
	}
	if l.Allow("k").Allowed {
		t.Fatal("refill must be capped at burst")
	}
}

func TestLimiter_KeysAreIndependent(t *testing.T) {
	l := New(1, 1, clock.NewFake(time.Now()))
	if !l.Allow("a").Allowed || l.Allow("a").Allowed {
		t.Fatal("a")
	}
	if !l.Allow("b").Allowed {
		t.Fatal("b must have its own bucket")
	}
}

func TestLimiter_Sweep(t *testing.T) {
	clk := clock.NewFake(time.Now())
	l := New(1, 1, clk)
	l.Allow("old")
	clk.Advance(time.Hour)
	l.Allow("fresh")
	if n := l.Sweep(30 * time.Minute); n != 1 {
		t.Fatalf("evicted %d, want 1", n)
	}
	// Swept key starts with a full bucket again.
	if !l.Allow("old").Allowed {
		t.Fatal("re-created bucket should allow")
	}
}

func TestLimiter_RaceFree(t *testing.T) {
	l := New(1000, 1000, clock.System{})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				l.Allow(string(rune('a' + i%26)))
			}
		}(i)
	}
	wg.Wait()
}
