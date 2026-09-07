package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dwijamahardika/ledgerd/internal/adapters/httpapi"
	"github.com/dwijamahardika/ledgerd/internal/adapters/httpapi/idempotency"
	"github.com/dwijamahardika/ledgerd/internal/app"
	"github.com/dwijamahardika/ledgerd/internal/domain"
	"github.com/dwijamahardika/ledgerd/internal/platform/clock"
	"github.com/dwijamahardika/ledgerd/internal/platform/ratelimit"
)

// ---- minimal in-memory store (transactions serialised by a mutex) ----

type store struct {
	mu   sync.Mutex
	accs map[uuid.UUID]*domain.Account
	xf   map[uuid.UUID]*domain.Transfer
	ents []domain.Entry
	evs  []domain.Event
}

func (s *store) Accounts() app.AccountRepository { return s }
func (s *store) Ledger() app.LedgerRepository    { return s }
func (s *store) Outbox() app.OutboxRepository    { return s }
func (s *store) WithinTx(ctx context.Context, fn func(context.Context, app.Store) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(ctx, s)
}
func (s *store) Create(_ context.Context, a *domain.Account) error { s.accs[a.ID] = a; return nil }
func (s *store) Get(_ context.Context, id uuid.UUID) (*domain.Account, error) {
	if a, ok := s.accs[id]; ok {
		cp := *a
		return &cp, nil
	}
	return nil, domain.ErrNotFound
}
func (s *store) EnsureTreasury(_ context.Context, c domain.Currency) (*domain.Account, error) {
	for _, a := range s.accs {
		if a.Kind == domain.KindTreasury && a.Currency == c {
			return a, nil
		}
	}
	t, _ := domain.NewTreasuryAccount(c, time.Now())
	s.accs[t.ID] = t
	return t, nil
}
func (s *store) LockMany(_ context.Context, ids []uuid.UUID) (map[uuid.UUID]*domain.Account, error) {
	out := map[uuid.UUID]*domain.Account{}
	for _, id := range ids {
		if a, ok := s.accs[id]; ok {
			cp := *a
			out[id] = &cp
		}
	}
	return out, nil
}
func (s *store) Save(_ context.Context, a *domain.Account, v int64) error {
	if s.accs[a.ID].Version != v {
		return domain.ErrVersionConflict
	}
	cp := *a
	s.accs[a.ID] = &cp
	return nil
}
func (s *store) CreateTransfer(_ context.Context, t *domain.Transfer) error {
	s.xf[t.ID] = t
	return nil
}
func (s *store) GetTransfer(_ context.Context, id uuid.UUID) (*domain.Transfer, error) {
	if t, ok := s.xf[id]; ok {
		return t, nil
	}
	return nil, domain.ErrNotFound
}
func (s *store) AppendEntries(_ context.Context, e []domain.Entry) error {
	s.ents = append(s.ents, e...)
	return nil
}
func (s *store) ListEntries(_ context.Context, id uuid.UUID, _ *app.EntryCursor, limit int) ([]domain.Entry, error) {
	var out []domain.Entry
	for _, e := range s.ents {
		if e.AccountID == id && len(out) < limit {
			out = append(out, e)
		}
	}
	return out, nil
}
func (s *store) Enqueue(_ context.Context, e domain.Event) error {
	s.evs = append(s.evs, e)
	return nil
}

type idemMem struct {
	mu sync.Mutex
	m  map[string]*idempotency.Record
}

func (i *idemMem) Acquire(_ context.Context, scope, key, fp string, _ time.Duration) (*idempotency.Record, bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if r, ok := i.m[scope+key]; ok {
		return r, false, nil
	}
	i.m[scope+key] = &idempotency.Record{Fingerprint: fp}
	return nil, true, nil
}
func (i *idemMem) Complete(_ context.Context, scope, key string, st int, h map[string]string, b []byte) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	r := i.m[scope+key]
	r.Completed, r.Status, r.Headers, r.Body = true, st, h, b
	return nil
}
func (i *idemMem) Release(_ context.Context, scope, key string) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.m, scope+key)
	return nil
}

// ---- harness ----

type harness struct {
	srv *httptest.Server
	clk *clock.Fake
}

func newHarness(t *testing.T, rps float64, burst int) *harness {
	t.Helper()
	st := &store{accs: map[uuid.UUID]*domain.Account{}, xf: map[uuid.UUID]*domain.Transfer{}}
	clk := clock.NewFake(time.Now())
	reg := prometheus.NewRegistry()
	h := httpapi.NewHandler(httpapi.Deps{
		Service:     app.NewService(st, clk),
		Idempotency: &idemMem{m: map[string]*idempotency.Record{}},
		Limiter:     ratelimit.New(rps, burst, clk),
		Metrics:     httpapi.NewMetrics(reg),
		Registry:    reg,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		APIKeys:     map[string]string{"test-key": "tester"},
		Ready:       func(context.Context) error { return nil },
		IdemTTL:     time.Hour,
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &harness{srv: srv, clk: clk}
}

func (h *harness) do(t *testing.T, method, path, body string, hdr map[string]string) (int, map[string]any, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(method, h.srv.URL+path, strings.NewReader(body))
	req.Header.Set("X-API-Key", "test-key")
	req.Header.Set("Content-Type", "application/json")
	if method == http.MethodPost && hdr["Idempotency-Key"] == "" {
		req.Header.Set("Idempotency-Key", uuid.NewString())
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	raw, _ := io.ReadAll(resp.Body)
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out)
	}
	return resp.StatusCode, out, resp.Header
}

func (h *harness) account(t *testing.T, cur string) string {
	code, body, _ := h.do(t, "POST", "/v1/accounts", fmt.Sprintf(`{"owner":"x","currency":%q}`, cur), nil)
	if code != 201 {
		t.Fatalf("create account: %d %v", code, body)
	}
	return body["id"].(string)
}

// ---- tests ----

func TestAuthRequired(t *testing.T) {
	h := newHarness(t, 100, 100)
	req, _ := http.NewRequest("GET", h.srv.URL+"/v1/accounts/"+uuid.NewString(), nil)
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != 401 || resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatalf("got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "application/problem+json" {
		t.Fatal("errors must be problem+json")
	}
}

func TestEndToEndFlow(t *testing.T) {
	h := newHarness(t, 100, 100)
	a := h.account(t, "USD")
	b := h.account(t, "USD")

	code, body, _ := h.do(t, "POST", "/v1/accounts/"+a+"/deposits", `{"amount":1000,"reference":"topup"}`, nil)
	if code != 201 {
		t.Fatalf("deposit %d %v", code, body)
	}
	code, body, hdr := h.do(t, "POST", "/v1/transfers", fmt.Sprintf(`{"from_account_id":%q,"to_account_id":%q,"amount":400,"currency":"USD"}`, a, b), nil)
	if code != 201 || hdr.Get("Location") == "" {
		t.Fatalf("transfer %d %v", code, body)
	}
	if len(body["entries"].([]any)) != 2 {
		t.Fatal("expected 2 entries in response")
	}
	code, body, _ = h.do(t, "GET", "/v1/accounts/"+b, "", nil)
	if code != 200 || body["balance"].(float64) != 400 {
		t.Fatalf("balance %v", body)
	}
	code, body, _ = h.do(t, "GET", "/v1/accounts/"+a+"/entries?limit=10", "", nil)
	if code != 200 || len(body["data"].([]any)) != 2 {
		t.Fatalf("entries %d %v", code, body)
	}
	code, body, _ = h.do(t, "POST", "/v1/transfers", fmt.Sprintf(`{"from_account_id":%q,"to_account_id":%q,"amount":100000,"currency":"USD"}`, a, b), nil)
	if code != 422 || body["title"] != "Insufficient funds" {
		t.Fatalf("overdraft %d %v", code, body)
	}
}

func TestValidationProblems(t *testing.T) {
	h := newHarness(t, 100, 100)
	sameID := uuid.New()
	cases := []struct {
		name string
		path string
		body string
		code int
		fld  string
	}{
		{"unknown field", "/v1/accounts", `{"owner":"x","currency":"USD","extra":1}`, 400, ""},
		{"bad currency", "/v1/accounts", `{"owner":"x","currency":"usd"}`, 400, "currency"},
		{"empty owner", "/v1/accounts", `{"owner":"  ","currency":"USD"}`, 400, "owner"},
		{"bad uuid", "/v1/accounts/nope/deposits", `{"amount":1}`, 400, "id"},
		{"same account", "/v1/transfers", fmt.Sprintf(`{"from_account_id":%q,"to_account_id":%q,"amount":1,"currency":"USD"}`, sameID, sameID), 400, "to_account_id"},
		{"unknown account", "/v1/transfers", fmt.Sprintf(`{"from_account_id":%q,"to_account_id":%q,"amount":1,"currency":"USD"}`, uuid.New(), uuid.New()), 422, "from_account_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, body, _ := h.do(t, "POST", tc.path, tc.body, nil)
			if code != tc.code {
				t.Fatalf("code=%d want %d body=%v", code, tc.code, body)
			}
			if tc.fld != "" && body["field"] != tc.fld {
				t.Fatalf("field=%v want %s", body["field"], tc.fld)
			}
			if body["instance"] == "" {
				t.Fatal("problem must carry request id")
			}
		})
	}
}

func TestIdempotentTransfer(t *testing.T) {
	h := newHarness(t, 100, 100)
	a := h.account(t, "USD")
	b := h.account(t, "USD")
	h.do(t, "POST", "/v1/accounts/"+a+"/deposits", `{"amount":100}`, nil)

	payload := fmt.Sprintf(`{"from_account_id":%q,"to_account_id":%q,"amount":60,"currency":"USD"}`, a, b)
	key := map[string]string{"Idempotency-Key": "order-42"}
	c1, b1, _ := h.do(t, "POST", "/v1/transfers", payload, key)
	c2, b2, hdr := h.do(t, "POST", "/v1/transfers", payload, key)
	if c1 != 201 || c2 != 201 || b1["id"] != b2["id"] || hdr.Get("Idempotent-Replay") != "true" {
		t.Fatalf("replay broken: %d %d %v %v", c1, c2, b1["id"], b2["id"])
	}
	// Money moved exactly once (100-60=40, a second execution would overdraft).
	_, acc, _ := h.do(t, "GET", "/v1/accounts/"+a, "", nil)
	if acc["balance"].(float64) != 40 {
		t.Fatalf("balance %v", acc["balance"])
	}
	// Missing key on a mutating route is rejected up front.
	code, _, _ := h.do(t, "POST", "/v1/transfers", payload, map[string]string{"Idempotency-Key": ""})
	if code != 400 {
		t.Fatalf("missing key: %d", code)
	}
}

func TestRateLimit(t *testing.T) {
	h := newHarness(t, 1, 2)
	id := uuid.NewString()
	codes := []int{}
	for i := 0; i < 3; i++ {
		c, _, _ := h.do(t, "GET", "/v1/accounts/"+id, "", nil)
		codes = append(codes, c)
	}
	if codes[0] != 404 || codes[1] != 404 || codes[2] != 429 {
		t.Fatalf("codes %v", codes)
	}
	h.clk.Advance(time.Second)
	if c, _, hdr := h.do(t, "GET", "/v1/accounts/"+id, "", nil); c != 404 || hdr.Get("X-RateLimit-Limit") != "2" {
		t.Fatalf("after refill: %d", c)
	}
}

func TestOpsEndpoints(t *testing.T) {
	h := newHarness(t, 1, 1)
	for _, p := range []string{"/healthz", "/readyz"} {
		resp, _ := http.Get(h.srv.URL + p)
		if resp.StatusCode != 204 {
			t.Fatalf("%s: %d", p, resp.StatusCode)
		}
	}
	resp, _ := http.Get(h.srv.URL + "/metrics")
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(string(body), "ledgerd_http_requests_total") {
		t.Fatal("metrics not exposed")
	}
}

func TestRequestIDPropagation(t *testing.T) {
	h := newHarness(t, 10, 10)
	_, _, hdr := h.do(t, "GET", "/healthz", "", map[string]string{"X-Request-ID": "trace-123"})
	if hdr.Get("X-Request-ID") != "trace-123" {
		t.Fatalf("got %q", hdr.Get("X-Request-ID"))
	}
}
