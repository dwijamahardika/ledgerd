package httpapi

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics groups the RED (rate, errors, duration) signals for the API plus
// domain counters. Registered once against an injectable registry so tests
// never hit "duplicate metrics collector registration".
type Metrics struct {
	requests    *prometheus.CounterVec
	latency     *prometheus.HistogramVec
	rateLimited *prometheus.CounterVec
	transfers   *prometheus.CounterVec
	outboxPub   *prometheus.CounterVec
	outboxFail  *prometheus.CounterVec
	breaker     *prometheus.GaugeVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "ledgerd", Subsystem: "http", Name: "requests_total"},
			[]string{"route", "method", "status"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "ledgerd", Subsystem: "http", Name: "request_duration_seconds",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5}}, []string{"route", "method"}),
		rateLimited: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "ledgerd", Subsystem: "http", Name: "rate_limited_total"}, []string{"principal"}),
		transfers:   prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "ledgerd", Name: "transfers_total"}, []string{"result"}),
		outboxPub:   prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "ledgerd", Subsystem: "outbox", Name: "published_total"}, []string{"type"}),
		outboxFail:  prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "ledgerd", Subsystem: "outbox", Name: "failed_total"}, []string{"type", "dead"}),
		breaker:     prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "ledgerd", Name: "circuit_breaker_state"}, []string{"name"}),
	}
	reg.MustRegister(m.requests, m.latency, m.rateLimited, m.transfers, m.outboxPub, m.outboxFail, m.breaker)
	return m
}

func (m *Metrics) Observe(route, method string, status int, d time.Duration) {
	m.requests.WithLabelValues(route, method, strconv.Itoa(status)).Inc()
	m.latency.WithLabelValues(route, method).Observe(d.Seconds())
}

func (m *Metrics) RateLimited(principal string) { m.rateLimited.WithLabelValues(principal).Inc() }
func (m *Metrics) Transfer(result string)       { m.transfers.WithLabelValues(result).Inc() }

// Outbox metrics satisfy outbox.Metrics.
func (m *Metrics) Published(t string) { m.outboxPub.WithLabelValues(t).Inc() }
func (m *Metrics) Failed(t string, dead bool) {
	m.outboxFail.WithLabelValues(t, strconv.FormatBool(dead)).Inc()
}

// BreakerState: 0 closed, 1 open, 2 half-open.
func (m *Metrics) BreakerState(name string, state int) {
	m.breaker.WithLabelValues(name).Set(float64(state))
}
