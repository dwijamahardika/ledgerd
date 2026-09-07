package idempotency

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// memStore is a reference implementation of Store used to test the
// middleware's state machine in isolation from Postgres.
type memStore struct {
	mu   sync.Mutex
	recs map[string]*Record
}

func newMem() *memStore { return &memStore{recs: map[string]*Record{}} }

func (m *memStore) Acquire(_ context.Context, scope, key, fp string, _ time.Duration) (*Record, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := scope + "/" + key
	if r, ok := m.recs[k]; ok {
		cp := *r
		return &cp, false, nil
	}
	m.recs[k] = &Record{Fingerprint: fp}
	return nil, true, nil
}

func (m *memStore) Complete(_ context.Context, scope, key string, status int, headers map[string]string, body []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.recs[scope+"/"+key]
	r.Completed, r.Status, r.Headers, r.Body = true, status, headers, append([]byte(nil), body...)
	return nil
}

func (m *memStore) Release(_ context.Context, scope, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.recs, scope+"/"+key)
	return nil
}

func problem(w http.ResponseWriter, _ *http.Request, status int, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"title": title, "detail": detail, "status": status})
}

func newMW(store Store, require bool) *Middleware {
	return New(store, func(r *http.Request) string { return r.Header.Get("X-Principal") }, time.Hour, problem, slog.Default(), require)
}

func do(h http.Handler, key, principal, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/things", strings.NewReader(body))
	if key != "" {
		req.Header.Set(HeaderKey, key)
	}
	req.Header.Set("X-Principal", principal)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestReplayReturnsStoredResponseWithoutReexecuting(t *testing.T) {
	var calls atomic.Int32
	h := newMW(newMem(), true).Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Location", "/v1/things/1")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":1}`))
	}))

	first := do(h, "k1", "alice", `{"a":1}`)
	second := do(h, "k1", "alice", `{"a":1}`)

	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", calls.Load())
	}
	if second.Code != http.StatusCreated || second.Body.String() != `{"id":1}` {
		t.Fatalf("replay mismatch: %d %s", second.Code, second.Body.String())
	}
	if second.Header().Get(HeaderReplay) != "true" || first.Header().Get(HeaderReplay) != "" {
		t.Fatal("replay header wrong")
	}
	if second.Header().Get("Location") != "/v1/things/1" {
		t.Fatal("stored headers not replayed")
	}
}

func TestDifferentPayloadSameKeyIs422(t *testing.T) {
	h := newMW(newMem(), true).Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	do(h, "k", "p", `{"a":1}`)
	if rec := do(h, "k", "p", `{"a":2}`); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("got %d", rec.Code)
	}
}

func TestKeysAreScopedPerPrincipal(t *testing.T) {
	var calls atomic.Int32
	h := newMW(newMem(), true).Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); w.WriteHeader(200) }))
	do(h, "k", "alice", `{}`)
	do(h, "k", "bob", `{}`)
	if calls.Load() != 2 {
		t.Fatalf("calls=%d; principals must not share keys", calls.Load())
	}
}

func TestInFlightIs409(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	h := newMW(newMem(), true).Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(200)
	}))
	go do(h, "k", "p", `{}`)
	<-entered
	rec := do(h, "k", "p", `{}`)
	close(release)
	if rec.Code != http.StatusConflict || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("got %d", rec.Code)
	}
}

func TestServerErrorReleasesKey(t *testing.T) {
	var calls atomic.Int32
	h := newMW(newMem(), true).Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	if rec := do(h, "k", "p", `{}`); rec.Code != http.StatusBadGateway {
		t.Fatal("first should fail")
	}
	if rec := do(h, "k", "p", `{}`); rec.Code != http.StatusOK || rec.Header().Get(HeaderReplay) != "" {
		t.Fatalf("retry after 5xx must re-execute, got %d replay=%q", rec.Code, rec.Header().Get(HeaderReplay))
	}
}

func TestPanicReleasesKey(t *testing.T) {
	var calls atomic.Int32
	h := newMW(newMem(), true).Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			panic("kaboom")
		}
		w.WriteHeader(200)
	}))
	func() {
		defer func() { _ = recover() }()
		do(h, "k", "p", `{}`)
	}()
	if rec := do(h, "k", "p", `{}`); rec.Code != 200 {
		t.Fatalf("got %d", rec.Code)
	}
}

func TestClientErrorsAreCachedToo(t *testing.T) {
	// A 4xx is a deterministic outcome for that payload; replaying it is correct
	// and protects the backend from validation-storms.
	var calls atomic.Int32
	h := newMW(newMem(), true).Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); w.WriteHeader(422) }))
	do(h, "k", "p", `{}`)
	rec := do(h, "k", "p", `{}`)
	if calls.Load() != 1 || rec.Code != 422 {
		t.Fatalf("calls=%d code=%d", calls.Load(), rec.Code)
	}
}

func TestMissingKey(t *testing.T) {
	h := newMW(newMem(), true).Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	if rec := do(h, "", "p", `{}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("required mode: got %d", rec.Code)
	}
	h2 := newMW(newMem(), false).Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	if rec := do(h2, "", "p", `{}`); rec.Code != 200 {
		t.Fatalf("optional mode: got %d", rec.Code)
	}
}

func TestBodyIsStillReadableByHandler(t *testing.T) {
	var got string
	h := newMW(newMem(), true).Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = string(b)
		w.WriteHeader(200)
	}))
	do(h, "k", "p", `{"x":"y"}`)
	if got != `{"x":"y"}` {
		t.Fatalf("handler saw %q", got)
	}
}
