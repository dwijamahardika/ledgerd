// Package httpapi is the inbound HTTP adapter. It uses only net/http (Go 1.22+
// method/path patterns) - no router framework - to keep the dependency surface
// small and the middleware model explicit.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dwijamahardika/ledgerd/internal/adapters/httpapi/idempotency"
	"github.com/dwijamahardika/ledgerd/internal/app"
	"github.com/dwijamahardika/ledgerd/internal/platform/ratelimit"
)

type Deps struct {
	Service     *app.Service
	Idempotency idempotency.Store
	Limiter     *ratelimit.Limiter
	Metrics     *Metrics
	Registry    prometheus.Gatherer
	Logger      *slog.Logger
	APIKeys     map[string]string // key -> principal name
	Ready       func(ctx context.Context) error
	IdemTTL     time.Duration
	EnablePprof bool
}

func NewHandler(d Deps) http.Handler {
	h := &handlers{svc: d.Service, metrics: d.Metrics}
	mux := http.NewServeMux()

	// ---- operational endpoints: no auth, no rate limit ----
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := d.Ready(ctx); err != nil {
			writeProblem(w, r, http.StatusServiceUnavailable, "Not ready", err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(d.Registry, promhttp.HandlerOpts{}))
	if d.EnablePprof {
		mux.HandleFunc("GET /debug/pprof/", pprof.Index)
		mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("GET /debug/pprof/heap", pprof.Handler("heap").ServeHTTP)
		mux.HandleFunc("GET /debug/pprof/goroutine", pprof.Handler("goroutine").ServeHTTP)
	}

	// ---- business API ----
	idem := idempotency.New(d.Idempotency, func(r *http.Request) string { return PrincipalFrom(r.Context()) },
		d.IdemTTL, writeProblem, d.Logger, true)
	protected := func(fn http.HandlerFunc) http.Handler {
		return chain(fn, apiKeyAuth(d.APIKeys), rateLimit(d.Limiter, d.Metrics), maxBody(1<<20))
	}
	mutating := func(fn http.HandlerFunc) http.Handler {
		return chain(fn, apiKeyAuth(d.APIKeys), rateLimit(d.Limiter, d.Metrics), maxBody(1<<20), idem.Wrap)
	}

	mux.Handle("POST /v1/accounts", mutating(h.createAccount))
	mux.Handle("GET /v1/accounts/{id}", protected(h.getAccount))
	mux.Handle("GET /v1/accounts/{id}/entries", protected(h.listEntries))
	mux.Handle("POST /v1/accounts/{id}/deposits", mutating(h.deposit))
	mux.Handle("POST /v1/accounts/{id}/withdrawals", mutating(h.withdraw))
	mux.Handle("POST /v1/transfers", mutating(h.createTransfer))
	mux.Handle("GET /v1/transfers/{id}", protected(h.getTransfer))

	return chain(mux, recoverer, requestID, logging(d.Logger, d.Metrics))
}

// NewServer applies the timeouts every production server needs and most
// tutorials forget; without them a slow client can hold a connection forever.
func NewServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}
}
