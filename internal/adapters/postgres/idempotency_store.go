package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dwijamahardika/ledgerd/internal/adapters/httpapi/idempotency"
)

// IdempotencyStore persists idempotency records in Postgres so replays work
// across replicas and restarts (an in-process map would silently break the
// guarantee behind a load balancer).
type IdempotencyStore struct{ q querier }

func NewIdempotencyStore(s *Store) *IdempotencyStore { return &IdempotencyStore{q: s.q} }

var _ idempotency.Store = (*IdempotencyStore)(nil)

func (s *IdempotencyStore) Acquire(ctx context.Context, scope, key, fingerprint string, ttl time.Duration) (*idempotency.Record, bool, error) {
	// Expired rows are reclaimed inline so no janitor is strictly required.
	_, _ = s.q.Exec(ctx, `DELETE FROM idempotency_keys WHERE scope = $1 AND key = $2 AND expires_at < now()`, scope, key)

	tag, err := s.q.Exec(ctx, `INSERT INTO idempotency_keys (scope, key, fingerprint, status, expires_at)
		VALUES ($1, $2, $3, 'in_progress', now() + make_interval(secs => $4)) ON CONFLICT (scope, key) DO NOTHING`,
		scope, key, fingerprint, ttl.Seconds())
	if err != nil {
		return nil, false, err
	}
	if tag.RowsAffected() == 1 {
		return nil, true, nil
	}

	var rec idempotency.Record
	var status string
	var respStatus *int
	var headers []byte
	err = s.q.QueryRow(ctx, `SELECT fingerprint, status, response_status, response_headers, response_body FROM idempotency_keys WHERE scope = $1 AND key = $2`, scope, key).
		Scan(&rec.Fingerprint, &status, &respStatus, &headers, &rec.Body)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Lost a race with a concurrent expiry-delete; caller retries.
			return nil, false, idempotency.ErrRetry
		}
		return nil, false, err
	}
	rec.Completed = status == "completed"
	if respStatus != nil {
		rec.Status = *respStatus
	}
	if len(headers) > 0 {
		_ = json.Unmarshal(headers, &rec.Headers)
	}
	return &rec, false, nil
}

func (s *IdempotencyStore) Complete(ctx context.Context, scope, key string, status int, headers map[string]string, body []byte) error {
	h, _ := json.Marshal(headers)
	_, err := s.q.Exec(ctx, `UPDATE idempotency_keys SET status = 'completed', response_status = $3, response_headers = $4, response_body = $5
		WHERE scope = $1 AND key = $2`, scope, key, status, h, body)
	return err
}

func (s *IdempotencyStore) Release(ctx context.Context, scope, key string) error {
	_, err := s.q.Exec(ctx, `DELETE FROM idempotency_keys WHERE scope = $1 AND key = $2 AND status = 'in_progress'`, scope, key)
	return err
}
