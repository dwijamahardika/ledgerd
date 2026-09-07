package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/dwijamahardika/ledgerd/internal/app"
	"github.com/dwijamahardika/ledgerd/internal/domain"
)

type ledgerRepo struct{ q querier }

func nowUTC() time.Time { return time.Now().UTC() }

func (r *ledgerRepo) CreateTransfer(ctx context.Context, t *domain.Transfer) error {
	_, err := r.q.Exec(ctx, `INSERT INTO transfers (id, from_account_id, to_account_id, amount, currency, reference, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`, t.ID, t.FromAccountID, t.ToAccountID, t.Amount, string(t.Currency), t.Reference, t.CreatedAt)
	return err
}

func (r *ledgerRepo) GetTransfer(ctx context.Context, id uuid.UUID) (*domain.Transfer, error) {
	var t domain.Transfer
	var cur string
	err := r.q.QueryRow(ctx, `SELECT id, from_account_id, to_account_id, amount, currency, reference, created_at FROM transfers WHERE id = $1`, id).
		Scan(&t.ID, &t.FromAccountID, &t.ToAccountID, &t.Amount, &cur, &t.Reference, &t.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	t.Currency = domain.Currency(cur)
	return &t, nil
}

// AppendEntries uses COPY-style batch insert via pgx.Batch: one round trip for
// all legs regardless of how many accounts a posting touches.
func (r *ledgerRepo) AppendEntries(ctx context.Context, entries []domain.Entry) error {
	if err := domain.CheckBalanced(entries); err != nil {
		return err
	}
	batch := &pgx.Batch{}
	for _, e := range entries {
		batch.Queue(`INSERT INTO ledger_entries (id, transfer_id, account_id, amount, balance_after, created_at) VALUES ($1,$2,$3,$4,$5,$6)`,
			e.ID, e.TransferID, e.AccountID, e.Amount, e.BalanceAfter, e.CreatedAt)
	}
	res := r.q.SendBatch(ctx, batch)
	defer res.Close()
	for range entries {
		if _, err := res.Exec(); err != nil {
			return err
		}
	}
	return nil
}

func (r *ledgerRepo) ListEntries(ctx context.Context, accountID uuid.UUID, after *app.EntryCursor, limit int) ([]domain.Entry, error) {
	const base = `SELECT id, transfer_id, account_id, amount, balance_after, created_at FROM ledger_entries WHERE account_id = $1`
	var (
		rows pgx.Rows
		err  error
	)
	if after == nil {
		rows, err = r.q.Query(ctx, base+` ORDER BY created_at DESC, id DESC LIMIT $2`, accountID, limit)
	} else {
		// Row-value comparison maps directly onto the composite index.
		rows, err = r.q.Query(ctx, base+` AND (created_at, id) < ($2, $3) ORDER BY created_at DESC, id DESC LIMIT $4`,
			accountID, after.CreatedAt, after.ID, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.Entry, 0, limit)
	for rows.Next() {
		var e domain.Entry
		if err := rows.Scan(&e.ID, &e.TransferID, &e.AccountID, &e.Amount, &e.BalanceAfter, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
