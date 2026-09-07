// Package backoff implements capped exponential backoff with "full jitter"
// (AWS Architecture Blog, 2015), which spreads retries evenly across the window
// and avoids thundering herds far better than equal or no jitter.
package backoff

import (
	"math"
	"math/rand/v2"
	"time"
)

type Policy struct {
	Base time.Duration
	Max  time.Duration
	rand *rand.Rand // nil => global source
}

func New(base, max time.Duration) Policy { return Policy{Base: base, Max: max} }

// WithSeed returns a deterministic policy for tests.
func (p Policy) WithSeed(seed uint64) Policy {
	p.rand = rand.New(rand.NewPCG(seed, seed))
	return p
}

// Next returns the delay before retry number attempt (0-based).
func (p Policy) Next(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	// Clamp the exponent so 1<<attempt never overflows.
	exp := math.Min(float64(attempt), 30)
	ceiling := float64(p.Base) * math.Pow(2, exp)
	if ceiling > float64(p.Max) {
		ceiling = float64(p.Max)
	}
	var r float64
	if p.rand != nil {
		r = p.rand.Float64()
	} else {
		r = rand.Float64()
	}
	return time.Duration(r * ceiling)
}
