package app

import (
	"context"

	"github.com/google/uuid"

	"github.com/dwijamahardika/ledgerd/internal/domain"
)

// Store is the persistence port. The same interface is implemented by a
// pool-backed store and by a transaction-backed store, so use cases run the
// identical code path inside or outside a transaction.
type Store interface {
	Accounts() AccountRepository
	Ledger() LedgerRepository
	Outbox() OutboxRepository
	// WithinTx runs fn atomically. Any error (or panic) rolls back.
	WithinTx(ctx context.Context, fn func(ctx context.Context, tx Store) error) error
}

type AccountRepository interface {
	Create(ctx context.Context, a *domain.Account) error
	Get(ctx context.Context, id uuid.UUID) (*domain.Account, error)
	// EnsureTreasury returns the singleton treasury account for a currency,
	// creating it on first use (race-safe via a partial unique index).
	EnsureTreasury(ctx context.Context, currency domain.Currency) (*domain.Account, error)
	// LockMany acquires row locks in ascending ID order to avoid deadlocks between
	// concurrent transfers touching the same pair in opposite directions.
	LockMany(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*domain.Account, error)
	// Save persists balance/version and fails with ErrVersionConflict if the row's
	// version no longer equals expectedVersion.
	Save(ctx context.Context, a *domain.Account, expectedVersion int64) error
}

type LedgerRepository interface {
	CreateTransfer(ctx context.Context, t *domain.Transfer) error
	GetTransfer(ctx context.Context, id uuid.UUID) (*domain.Transfer, error)
	AppendEntries(ctx context.Context, entries []domain.Entry) error
	// ListEntries returns up to limit entries newest-first, strictly after cursor.
	ListEntries(ctx context.Context, accountID uuid.UUID, after *EntryCursor, limit int) ([]domain.Entry, error)
}

type OutboxRepository interface {
	Enqueue(ctx context.Context, e domain.Event) error
}
