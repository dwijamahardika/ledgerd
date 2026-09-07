// Package ratelimit implements a sharded, lazily-refilled token bucket keyed by
// caller identity. Refill is computed on demand from elapsed time, so there is
// no background ticker per key; a single janitor evicts idle buckets.
package ratelimit

import (
	"hash/fnv"
	"math"
	"sync"
	"time"

	"github.com/dwijamahardika/ledgerd/internal/platform/clock"
)

const shardCount = 64

type bucket struct {
	tokens   float64
	lastSeen time.Time
}

type shard struct {
	mu      sync.Mutex
	buckets map[string]*bucket
}

type Limiter struct {
	rate   float64 // tokens per second
	burst  float64 // bucket capacity
	clock  clock.Clock
	shards [shardCount]*shard
}

type Decision struct {
	Allowed    bool
	Remaining  int
	RetryAfter time.Duration // zero when allowed
}

func New(ratePerSec float64, burst int, clk clock.Clock) *Limiter {
	if clk == nil {
		clk = clock.System{}
	}
	l := &Limiter{rate: ratePerSec, burst: float64(burst), clock: clk}
	for i := range l.shards {
		l.shards[i] = &shard{buckets: make(map[string]*bucket)}
	}
	return l
}

func (l *Limiter) shardFor(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return l.shards[h.Sum32()%shardCount]
}

// Allow consumes one token for key if available.
func (l *Limiter) Allow(key string) Decision {
	now := l.clock.Now()
	s := l.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.buckets[key]
	if !ok {
		b = &bucket{tokens: l.burst, lastSeen: now}
		s.buckets[key] = b
	} else {
		elapsed := now.Sub(b.lastSeen).Seconds()
		if elapsed > 0 {
			b.tokens = math.Min(l.burst, b.tokens+elapsed*l.rate)
		}
		b.lastSeen = now
	}

	if b.tokens >= 1 {
		b.tokens--
		return Decision{Allowed: true, Remaining: int(b.tokens)}
	}
	deficit := 1 - b.tokens
	wait := time.Duration(math.Ceil(deficit / l.rate * float64(time.Second)))
	return Decision{Allowed: false, Remaining: 0, RetryAfter: wait}
}

// Sweep drops buckets idle for longer than idle. Returns the number evicted.
func (l *Limiter) Sweep(idle time.Duration) int {
	cutoff := l.clock.Now().Add(-idle)
	evicted := 0
	for _, s := range l.shards {
		s.mu.Lock()
		for k, b := range s.buckets {
			if b.lastSeen.Before(cutoff) {
				delete(s.buckets, k)
				evicted++
			}
		}
		s.mu.Unlock()
	}
	return evicted
}

func (l *Limiter) Burst() int { return int(l.burst) }
