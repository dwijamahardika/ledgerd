package webhook

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dwijamahardika/ledgerd/internal/domain"
	"github.com/dwijamahardika/ledgerd/internal/platform/circuitbreaker"
	"github.com/dwijamahardika/ledgerd/internal/platform/clock"
)

func TestSignVerify(t *testing.T) {
	secret := []byte("s3cret")
	body := []byte(`{"hello":"world"}`)
	now := time.Unix(1_700_000_000, 0)
	ts := "1700000000"
	sig := Sign(secret, ts, body)

	if err := Verify(secret, ts, sig, body, 5*time.Minute, now); err != nil {
		t.Fatal(err)
	}
	if err := Verify(secret, ts, sig, []byte(`{"hello":"mars"}`), 5*time.Minute, now); err == nil {
		t.Fatal("tampered body must fail")
	}
	if err := Verify([]byte("other"), ts, sig, body, 5*time.Minute, now); err == nil {
		t.Fatal("wrong secret must fail")
	}
	if err := Verify(secret, ts, sig, body, 5*time.Minute, now.Add(time.Hour)); err == nil {
		t.Fatal("stale timestamp must fail (replay protection)")
	}
}

func TestPublisher_SignsAndBreaks(t *testing.T) {
	var hits atomic.Int32
	var mode atomic.Int32 // 0 ok, 1 fail
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		body, _ := io.ReadAll(r.Body)
		if err := Verify([]byte("k"), r.Header.Get(HeaderTimestamp), r.Header.Get(HeaderSignature), body, time.Minute, time.Now()); err != nil {
			t.Errorf("bad signature: %v", err)
		}
		if mode.Load() == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	clk := clock.NewFake(time.Now())
	br := circuitbreaker.New(circuitbreaker.Config{FailureThreshold: 2, OpenTimeout: time.Minute}, clk)
	p := New(srv.URL, "k", br, time.Second)
	ev := domain.Event{ID: uuid.New(), Type: "t", AggregateID: uuid.New(), Payload: []byte(`{}`), OccurredAt: time.Now()}

	if err := p.Publish(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	mode.Store(1)
	_ = p.Publish(context.Background(), ev)
	_ = p.Publish(context.Background(), ev)
	before := hits.Load()
	if err := p.Publish(context.Background(), ev); !errors.Is(err, circuitbreaker.ErrOpen) {
		t.Fatalf("breaker should be open, got %v", err)
	}
	if hits.Load() != before {
		t.Fatal("open breaker must not hit the receiver")
	}
	mode.Store(0)
	clk.Advance(time.Minute)
	if err := p.Publish(context.Background(), ev); err != nil {
		t.Fatalf("probe should succeed: %v", err)
	}
}
