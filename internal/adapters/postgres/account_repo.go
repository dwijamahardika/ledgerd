package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/dwijamahardika/ledgerd/internal/domain"
)

type accountRepo struct{ q querier }

const accountCols = `id, kind, owner, currency, balance, version, created_at`

func scanAccount(row pgx.Row) (*domain.Account, error) {
	var a domain.Account
	var cur string
	if err := row.Scan(&a.ID, &a.Kind, &a.Owner, &cur, &a.Balance, &a.Version, &a.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	a.Currency = domain.Currency(cur)
	return &a, nil
}

func (r *accountRepo) Create(ctx context.Context, a *domain.Account) error {
	_, err := r.q.Exec(ctx, `INSERT INTO accounts (`+accountCols+`) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		a.ID, a.Kind, a.Owner, string(a.Currency), a.Balance, a.Version, a.CreatedAt)
	return err
}

func (r *accountRepo) Get(ctx context.Context, id uuid.UUID) (*domain.Account, error) {
	return scanAccount(r.q.QueryRow(ctx, `SELECT `+accountCols+` FROM accounts WHERE id = $1`, id))
}

func (r *accountRepo) EnsureTreasury(ctx context.Context, currency domain.Currency) (*domain.Account, error) {
	row := r.q.QueryRow(ctx, `SELECT `+accountCols+` FROM accounts WHERE kind = 'treasury' AND currency = $1`, string(currency))
	acc, err := scanAccount(row)
	if err == nil {
		return acc, nil
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return nil, err
	}
	fresh, err := domain.NewTreasuryAccount(currency, nowUTC())
	if err != nil {
		return nil, err
	}
	// ON CONFLICT on the partial unique index: a concurrent creator wins, we re-read.
	_, err = r.q.Exec(ctx, `INSERT INTO accounts (`+accountCols+`) VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (currency) WHERE kind = 'treasury' DO NOTHING`,
		fresh.ID, fresh.Kind, fresh.Owner, string(fresh.Currency), fresh.Balance, fresh.Version, fresh.CreatedAt)
	if err != nil {
		return nil, err
	}
	return scanAccount(r.q.QueryRow(ctx, `SELECT `+accountCols+` FROM accounts WHERE kind = 'treasury' AND currency = $1`, string(currency)))
}

// LockMany: ORDER BY id before FOR UPDATE makes Postgres acquire the row locks
// in a deterministic order, which is what prevents A->B / B->A deadlocks.
func (r *accountRepo) LockMany(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*domain.Account, error) {
	rows, err := r.q.Query(ctx, `SELECT `+accountCols+` FROM accounts WHERE id = ANY($1) ORDER BY id FOR UPDATE`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[uuid.UUID]*domain.Account, len(ids))
	for rows.Next() {
		a, err := scanAccount(rows)
		if err != nil {
			return nil, err
		}
		out[a.ID] = a
	}
	return out, rows.Err()
}

func (r *accountRepo) Save(ctx context.Context, a *domain.Account, expectedVersion int64) error {
	tag, err := r.q.Exec(ctx, `UPDATE accounts SET balance = $1, version = $2 WHERE id = $3 AND version = $4`,
		a.Balance, a.Version, a.ID, expectedVersion)
	if err != nil {
		if isCheckViolation(err) {
			return domain.ErrInsufficientFunds
		}
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("account %s: %w", a.ID, domain.ErrVersionConflict)
	}
	return nil
}
