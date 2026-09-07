// Package webhook delivers outbox events to an HTTP endpoint. Each request is
// signed with HMAC-SHA256 over the raw body so receivers can verify origin and
// integrity without TLS client certs. The circuit breaker turns a dead
// receiver into fast failures instead of a pile of timed-out goroutines.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/dwijamahardika/ledgerd/internal/domain"
	"github.com/dwijamahardika/ledgerd/internal/platform/circuitbreaker"
)

const (
	HeaderSignature = "X-Ledger-Signature"
	HeaderEventID   = "X-Ledger-Event-Id"
	HeaderTimestamp = "X-Ledger-Timestamp"
)

type Publisher struct {
	url     string
	secret  []byte
	client  *http.Client
	breaker *circuitbreaker.Breaker
	now     func() time.Time
}

func New(url, secret string, breaker *circuitbreaker.Breaker, timeout time.Duration) *Publisher {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Publisher{url: url, secret: []byte(secret), client: &http.Client{Timeout: timeout}, breaker: breaker, now: time.Now}
}

type envelope struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	AggregateID string          `json:"aggregate_id"`
	OccurredAt  time.Time       `json:"occurred_at"`
	Data        json.RawMessage `json:"data"`
}

func (p *Publisher) Publish(ctx context.Context, e domain.Event) error {
	body, err := json.Marshal(envelope{ID: e.ID.String(), Type: e.Type, AggregateID: e.AggregateID.String(), OccurredAt: e.OccurredAt, Data: e.Payload})
	if err != nil {
		return err
	}
	return p.breaker.Execute(func() error { return p.send(ctx, e, body) })
}

func (p *Publisher) send(ctx context.Context, e domain.Event, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	ts := strconv.FormatInt(p.now().Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderEventID, e.ID.String())
	req.Header.Set(HeaderTimestamp, ts)
	req.Header.Set(HeaderSignature, Sign(p.secret, ts, body))

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("webhook: receiver returned %d", resp.StatusCode)
}

// Sign computes "v1=<hex hmac>" over "<timestamp>.<body>". Including the
// timestamp lets receivers reject replays older than their tolerance window.
func Sign(secret []byte, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(timestamp))
	mac.Write([]byte{'.'})
	mac.Write(body)
	return "v1=" + hex.EncodeToString(mac.Sum(nil))
}

// Verify is what a receiver runs; shipped here so the contract lives in one place.
func Verify(secret []byte, timestamp, signature string, body []byte, tolerance time.Duration, now time.Time) error {
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return errors.New("bad timestamp")
	}
	if d := now.Sub(time.Unix(ts, 0)); d > tolerance || d < -tolerance {
		return errors.New("timestamp outside tolerance")
	}
	if !hmac.Equal([]byte(Sign(secret, timestamp, body)), []byte(signature)) {
		return errors.New("signature mismatch")
	}
	return nil
}

// LogPublisher is the no-webhook fallback for local development.
type LogPublisher struct {
	Log interface{ Info(string, ...any) }
}

func (l LogPublisher) Publish(_ context.Context, e domain.Event) error {
	l.Log.Info("event published (log sink)", "event_id", e.ID, "type", e.Type, "payload", string(e.Payload))
	return nil
}
