// Package clock abstracts time so rate limiters, breakers and TTLs are testable
// without sleeping.
package clock

import (
	"sync"
	"time"
)

type Clock interface{ Now() time.Time }

type System struct{}

func (System) Now() time.Time { return time.Now() }

// Fake is a manually advanced, goroutine-safe clock for tests.
type Fake struct {
	mu sync.Mutex
	t  time.Time
}

func NewFake(t time.Time) *Fake { return &Fake{t: t} }

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
}
