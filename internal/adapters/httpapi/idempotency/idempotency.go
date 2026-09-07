// Package idempotency makes unsafe HTTP methods safely retryable. Clients send
// an Idempotency-Key header; the first request executes and its response is
// stored, subsequent requests with the same key replay the stored response.
//
// Semantics (modelled on Stripe's implementation):
//   - same key + same request fingerprint, completed  -> replay, 200-ish
//   - same key + same fingerprint, still in progress  -> 409 (client raced itself)
//   - same key + different fingerprint                -> 422 (key reuse bug)
//   - handler crashed / returned 5xx                  -> key released so the
//     client can retry and actually get a fresh attempt
package idempotency

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"
)

const (
	HeaderKey    = "Idempotency-Key"
	HeaderReplay = "Idempotent-Replay"
	maxKeyLen    = 255
	maxBodyBytes = 1 << 20
)

var ErrRetry = errors.New("idempotency: transient race, retry acquire")

type Record struct {
	Fingerprint string
	Completed   bool
	Status      int
	Headers     map[string]string
	Body        []byte
}

type Store interface {
	// Acquire claims (scope,key). acquired=true means the caller owns execution.
	// Otherwise rec describes the existing record.
	Acquire(ctx context.Context, scope, key, fingerprint string, ttl time.Duration) (rec *Record, acquired bool, err error)
	Complete(ctx context.Context, scope, key string, status int, headers map[string]string, body []byte) error
	Release(ctx context.Context, scope, key string) error
}

// ScopeFunc derives the namespace for a key - typically the authenticated
// principal - so two tenants can never collide on the same key string.
type ScopeFunc func(r *http.Request) string

type ProblemWriter func(w http.ResponseWriter, r *http.Request, status int, title, detail string)

type Middleware struct {
	store   Store
	scope   ScopeFunc
	ttl     time.Duration
	problem ProblemWriter
	log     *slog.Logger
	require bool
}

func New(store Store, scope ScopeFunc, ttl time.Duration, problem ProblemWriter, log *slog.Logger, require bool) *Middleware {
	return &Middleware{store: store, scope: scope, ttl: ttl, problem: problem, log: log, require: require}
}

func (m *Middleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get(HeaderKey)
		if key == "" {
			if m.require {
				m.problem(w, r, http.StatusBadRequest, "Missing Idempotency-Key", "This endpoint requires an Idempotency-Key header.")
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		if len(key) > maxKeyLen {
			m.problem(w, r, http.StatusBadRequest, "Invalid Idempotency-Key", "Key must be at most 255 characters.")
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
		if err != nil || len(body) > maxBodyBytes {
			m.problem(w, r, http.StatusRequestEntityTooLarge, "Body too large", "Request body exceeds 1 MiB.")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		fp := fingerprint(r.Method, r.URL.Path, body)
		scope := m.scope(r) + "|" + r.Method + " " + r.URL.Path

		var (
			rec      *Record
			acquired bool
		)
		for attempt := 0; attempt < 3; attempt++ {
			rec, acquired, err = m.store.Acquire(r.Context(), scope, key, fp, m.ttl)
			if !errors.Is(err, ErrRetry) {
				break
			}
		}
		if err != nil {
			m.log.Error("idempotency acquire failed", "err", err)
			m.problem(w, r, http.StatusServiceUnavailable, "Idempotency store unavailable", "Please retry.")
			return
		}

		if !acquired {
			switch {
			case rec.Fingerprint != fp:
				m.problem(w, r, http.StatusUnprocessableEntity, "Idempotency-Key reused",
					"This key was already used with a different request payload.")
			case !rec.Completed:
				w.Header().Set("Retry-After", "1")
				m.problem(w, r, http.StatusConflict, "Request in progress",
					"A request with this Idempotency-Key is still being processed.")
			default:
				for k, v := range rec.Headers {
					w.Header().Set(k, v)
				}
				w.Header().Set(HeaderReplay, "true")
				w.WriteHeader(rec.Status)
				_, _ = w.Write(rec.Body)
			}
			return
		}

		rec2 := &recorder{ResponseWriter: w, status: http.StatusOK}
		completed := false
		defer func() {
			if completed {
				return
			}
			// Panic or early exit: give the key back.
			_ = m.store.Release(context.WithoutCancel(r.Context()), scope, key)
		}()

		next.ServeHTTP(rec2, r)

		// Detached context: the client may have gone away, but the outcome must
		// still be recorded or the key would be released on a successful write.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
		defer cancel()
		if rec2.status >= 500 {
			_ = m.store.Release(ctx, scope, key)
			completed = true
			return
		}
		if err := m.store.Complete(ctx, scope, key, rec2.status, headersToStore(rec2.Header()), rec2.buf.Bytes()); err != nil {
			m.log.Error("idempotency complete failed", "err", err)
			return // deferred Release runs
		}
		completed = true
	})
}

func fingerprint(method, path string, body []byte) string {
	h := sha256.New()
	h.Write([]byte(method))
	h.Write([]byte{0})
	h.Write([]byte(path))
	h.Write([]byte{0})
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

func headersToStore(h http.Header) map[string]string {
	out := map[string]string{}
	for _, k := range []string{"Content-Type", "Location"} {
		if v := h.Get(k); v != "" {
			out[k] = v
		}
	}
	return out
}

// recorder tees the response so it can be both sent and stored.
type recorder struct {
	http.ResponseWriter
	status      int
	buf         bytes.Buffer
	wroteHeader bool
}

func (r *recorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.wroteHeader = true
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	r.buf.Write(b)
	return r.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
