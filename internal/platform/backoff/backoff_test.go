package backoff

import (
	"testing"
	"time"
)

func TestBackoff_BoundsAndCap(t *testing.T) {
	p := New(100*time.Millisecond, 2*time.Second).WithSeed(42)
	for attempt := 0; attempt < 40; attempt++ {
		ceiling := time.Duration(float64(100*time.Millisecond) * float64(uint64(1)<<uint(min(attempt, 30))))
		if ceiling > 2*time.Second {
			ceiling = 2 * time.Second
		}
		for i := 0; i < 50; i++ {
			d := p.Next(attempt)
			if d < 0 || d > ceiling {
				t.Fatalf("attempt %d: %v outside [0,%v]", attempt, d, ceiling)
			}
		}
	}
}

func TestBackoff_Deterministic(t *testing.T) {
	a := New(time.Second, time.Minute).WithSeed(7)
	b := New(time.Second, time.Minute).WithSeed(7)
	for i := 0; i < 10; i++ {
		if a.Next(i) != b.Next(i) {
			t.Fatal("seeded policies diverged")
		}
	}
}

func TestBackoff_NegativeAttempt(t *testing.T) {
	p := New(time.Second, time.Minute).WithSeed(1)
	if d := p.Next(-5); d > time.Second {
		t.Fatalf("negative attempt should behave like 0, got %v", d)
	}
}
