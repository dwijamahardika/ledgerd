// Package app holds the use cases. It depends only on domain and on ports it
// defines itself; adapters (Postgres, HTTP, webhooks) depend on it, never the
// other way round.
package app

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"

	"github.com/dwijamahardika/ledgerd/internal/domain"
	"github.com/dwijamahardika/ledgerd/internal/platform/clock"
)

const (
	DefaultPageSize = 50
	MaxPageSize     = 200
)

type Service struct {
	store Store
	clock clock.Clock
}

func NewService(store Store, clk clock.Clock) *Service {
	if clk == nil {
		clk = clock.System{}
	}
	return &Service{store: store, clock: clk}
}

type CreateAccountInput struct {
	Owner    string
	Currency string
}

func (s *Service) CreateAccount(ctx context.Context, in CreateAccountInput) (*domain.Account, error) {
	acc, err := domain.NewAccount(in.Owner, domain.Currency(in.Currency), s.clock.Now())
	if err != nil {
		return nil, err
	}
	if err := s.store.Accounts().Create(ctx, acc); err != nil {
		return nil, fmt.Errorf("create account: %w", err)
	}
	return acc, nil
}

func (s *Service) GetAccount(ctx context.Context, id uuid.UUID) (*domain.Account, error) {
	return s.store.Accounts().Get(ctx, id)
}

type TransferInput struct {
	FromAccountID uuid.UUID
	ToAccountID   uuid.UUID
	Amount        int64
	Currency      string
	Reference     string
}

type TransferResult struct {
	Transfer *domain.Transfer
	Entries  []domain.Entry
	From     *domain.Account
	To       *domain.Account
}

// Transfer is the core use case. Consistency strategy, in layers:
//
//  1. Row locks (SELECT ... FOR UPDATE) on both accounts, taken in ascending ID
//     order so A->B and B->A running concurrently cannot deadlock.
//  2. Optimistic version check on Save as a belt-and-braces guard against any
//     code path that forgets step 1.
//  3. A CHECK (balance >= 0) constraint in the schema as the last line of defence.
//  4. The domain event is written to the outbox in the same transaction, so a
//     transfer is never published without being committed, nor committed without
//     being (eventually) published.
func (s *Service) Transfer(ctx context.Context, in TransferInput) (*TransferResult, error) {
	t, err := domain.NewTransfer(in.FromAccountID, in.ToAccountID, in.Amount, domain.Currency(in.Currency), in.Reference, s.clock.Now())
	if err != nil {
		return nil, err
	}

	var res *TransferResult
	err = s.store.WithinTx(ctx, func(ctx context.Context, tx Store) error {
		ids := []uuid.UUID{t.FromAccountID, t.ToAccountID}
		sort.Slice(ids, func(i, j int) bool { return lessUUID(ids[i], ids[j]) })

		locked, err := tx.Accounts().LockMany(ctx, ids)
		if err != nil {
			return fmt.Errorf("lock accounts: %w", err)
		}
		from, ok := locked[t.FromAccountID]
		if !ok {
			return &domain.ValidationError{Field: "from_account_id", Err: domain.ErrNotFound}
		}
		to, ok := locked[t.ToAccountID]
		if !ok {
			return &domain.ValidationError{Field: "to_account_id", Err: domain.ErrNotFound}
		}
		fromVersion, toVersion := from.Version, to.Version

		entries, err := domain.PostTransfer(t, from, to)
		if err != nil {
			return err
		}

		if err := tx.Accounts().Save(ctx, from, fromVersion); err != nil {
			return fmt.Errorf("save source: %w", err)
		}
		if err := tx.Accounts().Save(ctx, to, toVersion); err != nil {
			return fmt.Errorf("save destination: %w", err)
		}
		if err := tx.Ledger().CreateTransfer(ctx, t); err != nil {
			return fmt.Errorf("create transfer: %w", err)
		}
		if err := tx.Ledger().AppendEntries(ctx, entries); err != nil {
			return fmt.Errorf("append entries: %w", err)
		}
		ev, err := domain.NewTransferPostedEvent(t)
		if err != nil {
			return err
		}
		if err := tx.Outbox().Enqueue(ctx, ev); err != nil {
			return fmt.Errorf("enqueue event: %w", err)
		}
		res = &TransferResult{Transfer: t, Entries: entries, From: from, To: to}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

type FundingInput struct {
	AccountID uuid.UUID
	Amount    int64
	Reference string
}

// Deposit credits a customer account from the treasury (money entering the
// system, e.g. a settled card payment). Withdraw is the mirror image. Both
// reuse Transfer so they inherit its locking, idempotency and outbox semantics.
func (s *Service) Deposit(ctx context.Context, in FundingInput) (*TransferResult, error) {
	return s.fund(ctx, in, true)
}

func (s *Service) Withdraw(ctx context.Context, in FundingInput) (*TransferResult, error) {
	return s.fund(ctx, in, false)
}

func (s *Service) fund(ctx context.Context, in FundingInput, deposit bool) (*TransferResult, error) {
	acc, err := s.store.Accounts().Get(ctx, in.AccountID)
	if err != nil {
		return nil, err
	}
	if acc.Kind != domain.KindUser {
		return nil, &domain.ValidationError{Field: "account_id", Err: domain.ErrNotFound}
	}
	treasury, err := s.store.Accounts().EnsureTreasury(ctx, acc.Currency)
	if err != nil {
		return nil, fmt.Errorf("ensure treasury: %w", err)
	}
	from, to := treasury.ID, acc.ID
	if !deposit {
		from, to = acc.ID, treasury.ID
	}
	return s.Transfer(ctx, TransferInput{FromAccountID: from, ToAccountID: to, Amount: in.Amount, Currency: string(acc.Currency), Reference: in.Reference})
}

func (s *Service) GetTransfer(ctx context.Context, id uuid.UUID) (*domain.Transfer, error) {
	return s.store.Ledger().GetTransfer(ctx, id)
}

type EntryPage struct {
	Entries    []domain.Entry
	NextCursor string // empty when exhausted
}

func (s *Service) ListEntries(ctx context.Context, accountID uuid.UUID, cursor string, limit int) (*EntryPage, error) {
	if limit <= 0 {
		limit = DefaultPageSize
	}
	if limit > MaxPageSize {
		limit = MaxPageSize
	}
	after, err := DecodeCursor(cursor)
	if err != nil {
		return nil, &domain.ValidationError{Field: "cursor", Err: err}
	}
	if _, err := s.store.Accounts().Get(ctx, accountID); err != nil {
		return nil, err
	}
	// Fetch one extra row to learn whether another page exists without a COUNT.
	rows, err := s.store.Ledger().ListEntries(ctx, accountID, after, limit+1)
	if err != nil {
		return nil, err
	}
	page := &EntryPage{Entries: rows}
	if len(rows) > limit {
		page.Entries = rows[:limit]
		last := page.Entries[limit-1]
		page.NextCursor = EntryCursor{CreatedAt: last.CreatedAt, ID: last.ID}.Encode()
	}
	return page, nil
}

func lessUUID(a, b uuid.UUID) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// IsClientError reports whether err stems from bad input rather than a fault.
func IsClientError(err error) bool {
	var ve *domain.ValidationError
	return errors.As(err, &ve) ||
		errors.Is(err, domain.ErrNotFound) ||
		errors.Is(err, domain.ErrInsufficientFunds) ||
		errors.Is(err, domain.ErrCurrencyMismatch)
}
