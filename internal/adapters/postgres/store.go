// Package postgres implements the app.Store port with pgx. One struct serves
// both pool-backed and transaction-backed use: querier abstracts over
// *pgxpool.Pool and pgx.Tx so repositories never know which one they run on.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dwijamahardika/ledgerd/internal/app"
)

type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

type Store struct {
	pool *pgxpool.Pool
	q    querier
	inTx bool
}

var _ app.Store = (*Store)(nil)

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool, q: pool} }

func (s *Store) Accounts() app.AccountRepository { return &accountRepo{q: s.q} }
func (s *Store) Ledger() app.LedgerRepository    { return &ledgerRepo{q: s.q} }
func (s *Store) Outbox() app.OutboxRepository    { return &outboxRepo{q: s.q} }

// WithinTx starts a transaction (or joins the current one - nested calls do
// not open savepoints, they simply share the outer tx) and guarantees rollback
// on error or panic.
func (s *Store) WithinTx(ctx context.Context, fn func(ctx context.Context, tx app.Store) error) (err error) {
	if s.inTx {
		return fn(ctx, s)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		}
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
				err = errors.Join(err, fmt.Errorf("rollback: %w", rbErr))
			}
			return
		}
		if cErr := tx.Commit(ctx); cErr != nil {
			err = fmt.Errorf("commit: %w", cErr)
		}
	}()
	return fn(ctx, &Store{pool: s.pool, q: tx, inTx: true})
}

// Ping is used by the readiness probe.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func isCheckViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23514"
}
