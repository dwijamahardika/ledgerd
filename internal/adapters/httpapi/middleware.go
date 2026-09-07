package httpapi

import (
	"context"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/dwijamahardika/ledgerd/internal/platform/ratelimit"
)

type ctxKey int

const (
	ctxRequestID ctxKey = iota
	ctxLogger
	ctxPrincipal
)

const (
	HeaderRequestID = "X-Request-ID"
	HeaderAPIKey    = "X-API-Key"
)

func RequestIDFrom(ctx context.Context) string {
	v, _ := ctx.Value(ctxRequestID).(string)
	return v
}

func LoggerFrom(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(ctxLogger).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}

func PrincipalFrom(ctx context.Context) string {
	v, _ := ctx.Value(ctxPrincipal).(string)
	return v
}

// Middleware is the classic decorator shape; chain applies them so the first
// listed runs outermost.
type Middleware func(http.Handler) http.Handler

func chain(h http.Handler, mws ...Middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// requestID honours an inbound ID (from a gateway) or mints one, and echoes
// it back so clients can quote it in support tickets.
func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(HeaderRequestID)
		if id == "" || len(id) > 64 {
			id = uuid.NewString()
		}
		w.Header().Set(HeaderRequestID, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxRequestID, id)))
	})
}

func recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if p := recover(); p != nil {
				if p == http.ErrAbortHandler { //nolint:errorlint // sentinel compared by identity per net/http docs
					panic(p)
				}
				LoggerFrom(r.Context()).Error("panic recovered", "panic", p, "stack", string(debug.Stack()))
				writeProblem(w, r, http.StatusInternalServerError, "Internal error", "Unexpected failure.")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (s *statusWriter) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	n, err := s.ResponseWriter.Write(b)
	s.bytes += n
	return n, err
}

func (s *statusWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func logging(base *slog.Logger, m *Metrics) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			log := base.With("request_id", RequestIDFrom(r.Context()))
			ctx := context.WithValue(r.Context(), ctxLogger, log)
			sw := &statusWriter{ResponseWriter: w}
			next.ServeHTTP(sw, r.WithContext(ctx))
			if sw.status == 0 {
				sw.status = http.StatusOK
			}
			dur := time.Since(start)
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			m.Observe(route, r.Method, sw.status, dur)
			lvl := slog.LevelInfo
			if sw.status >= 500 {
				lvl = slog.LevelError
			}
			log.Log(ctx, lvl, "http request", "method", r.Method, "route", route, "path", r.URL.Path,
				"status", sw.status, "bytes", sw.bytes, "duration_ms", dur.Milliseconds(), "principal", PrincipalFrom(ctx))
		})
	}
}

// apiKeyAuth is deliberately simple (static keys from config) but does the
// important thing right: constant-time comparison to avoid timing oracles.
func apiKeyAuth(keys map[string]string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			presented := r.Header.Get(HeaderAPIKey)
			var principal string
			for key, name := range keys {
				if subtle.ConstantTimeCompare([]byte(key), []byte(presented)) == 1 {
					principal = name
				}
			}
			if principal == "" {
				w.Header().Set("WWW-Authenticate", `ApiKey realm="ledgerd"`)
				writeProblem(w, r, http.StatusUnauthorized, "Unauthorized", "Missing or invalid X-API-Key.")
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxPrincipal, principal)))
		})
	}
}

func rateLimit(l *ratelimit.Limiter, m *Metrics) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := PrincipalFrom(r.Context())
			if key == "" {
				key = r.RemoteAddr
			}
			d := l.Allow(key)
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(l.Burst()))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(d.Remaining))
			if !d.Allowed {
				secs := int(d.RetryAfter.Seconds())
				if secs < 1 {
					secs = 1
				}
				w.Header().Set("Retry-After", strconv.Itoa(secs))
				m.RateLimited(key)
				writeProblem(w, r, http.StatusTooManyRequests, "Rate limit exceeded", "Slow down and retry after the indicated delay.")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func maxBody(n int64) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, n)
			next.ServeHTTP(w, r)
		})
	}
}
