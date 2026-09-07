package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dwijamahardika/ledgerd/internal/domain"
	"github.com/dwijamahardika/ledgerd/internal/platform/outbox"
)

type outboxRepo struct{ q querier }

func (r *outboxRepo) Enqueue(ctx context.Context, e domain.Event) error {
	_, err := r.q.Exec(ctx, `INSERT INTO outbox (event_id, event_type, aggregate_id, payload, created_at) VALUES ($1,$2,$3,$4,$5)`,
		e.ID, e.Type, e.AggregateID, []byte(e.Payload), e.OccurredAt)
	return err
}

// OutboxSource adapts the outbox table to the relay's port.
type OutboxSource struct {
	pool interface {
		Begin(context.Context) (pgx.Tx, error)
	}
}

func NewOutboxSource(s *Store) *OutboxSource { return &OutboxSource{pool: s.pool} }

var _ outbox.Source = (*OutboxSource)(nil)

// ProcessBatch claims due messages with FOR UPDATE SKIP LOCKED so any number
// of relay replicas can run concurrently without double-delivery or blocking
// each other, hands them to fn, then persists each outcome in the same tx.
func (s *OutboxSource) ProcessBatch(ctx context.Context, limit int, fn func(ctx context.Context, msgs []outbox.Message) []outbox.Outcome) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	rows, err := tx.Query(ctx, `SELECT id, event_id, event_type, aggregate_id, payload, attempts, created_at
		FROM outbox WHERE status = 'pending' AND next_attempt_at <= now()
		ORDER BY next_attempt_at, id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, err
	}
	var msgs []outbox.Message
	for rows.Next() {
		var m outbox.Message
		var payload []byte
		if err := rows.Scan(&m.ID, &m.Event.ID, &m.Event.Type, &m.Event.AggregateID, &payload, &m.Attempts, &m.Event.OccurredAt); err != nil {
			rows.Close()
			return 0, err
		}
		m.Event.Payload = payload
		msgs = append(msgs, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(msgs) == 0 {
		return 0, tx.Commit(ctx)
	}

	outcomes := fn(ctx, msgs)
	if len(outcomes) != len(msgs) {
		return 0, fmt.Errorf("outbox: fn returned %d outcomes for %d messages", len(outcomes), len(msgs))
	}
	batch := &pgx.Batch{}
	for i, o := range outcomes {
		id := msgs[i].ID
		switch {
		case o.Err == nil:
			batch.Queue(`UPDATE outbox SET status = 'sent', sent_at = now(), last_error = NULL WHERE id = $1`, id)
		case o.Dead:
			batch.Queue(`UPDATE outbox SET status = 'dead', attempts = attempts + 1, last_error = $2 WHERE id = $1`, id, o.Err.Error())
		default:
			batch.Queue(`UPDATE outbox SET attempts = attempts + 1, next_attempt_at = $2, last_error = $3 WHERE id = $1`,
				id, time.Now().Add(o.RetryIn), o.Err.Error())
		}
	}
	res := tx.SendBatch(ctx, batch)
	for range outcomes {
		if _, err := res.Exec(); err != nil {
			res.Close()
			return 0, err
		}
	}
	if err := res.Close(); err != nil {
		return 0, err
	}
	return len(msgs), tx.Commit(ctx)
}
