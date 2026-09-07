package domain

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Transfer moves Amount from one account to another. It is immutable once posted.
type Transfer struct {
	ID            uuid.UUID
	FromAccountID uuid.UUID
	ToAccountID   uuid.UUID
	Amount        int64
	Currency      Currency
	Reference     string
	CreatedAt     time.Time
}

// Entry is one leg of a double-entry posting. Amount is signed: negative for the
// debited account, positive for the credited one. BalanceAfter is a snapshot so
// statements can be rendered without replaying history.
type Entry struct {
	ID           uuid.UUID
	TransferID   uuid.UUID
	AccountID    uuid.UUID
	Amount       int64
	BalanceAfter int64
	CreatedAt    time.Time
}

func NewTransfer(from, to uuid.UUID, amount int64, currency Currency, reference string, now time.Time) (*Transfer, error) {
	switch {
	case from == uuid.Nil:
		return nil, invalid("from_account_id", ErrNotFound)
	case to == uuid.Nil:
		return nil, invalid("to_account_id", ErrNotFound)
	case from == to:
		return nil, invalid("to_account_id", ErrSameAccount)
	case !ValidAmount(amount):
		return nil, invalid("amount", ErrInvalidAmount)
	case !currency.Valid():
		return nil, invalid("currency", ErrInvalidCurrency)
	case len(reference) > 200:
		return nil, invalid("reference", ErrInvalidRef)
	}
	return &Transfer{
		ID:            uuid.New(),
		FromAccountID: from,
		ToAccountID:   to,
		Amount:        amount,
		Currency:      currency,
		Reference:     strings.TrimSpace(reference),
		CreatedAt:     now.UTC(),
	}, nil
}

// PostTransfer is a pure function: it mutates the in-memory accounts and returns
// the balanced entries. Persistence and locking are the application layer's job.
func PostTransfer(t *Transfer, from, to *Account) ([]Entry, error) {
	if from.ID != t.FromAccountID || to.ID != t.ToAccountID {
		return nil, errors.New("accounts do not match transfer")
	}
	if from.Currency != t.Currency || to.Currency != t.Currency {
		return nil, ErrCurrencyMismatch
	}
	if err := from.debit(t.Amount); err != nil {
		return nil, err
	}
	if err := to.credit(t.Amount); err != nil {
		return nil, err
	}
	entries := []Entry{
		{ID: uuid.New(), TransferID: t.ID, AccountID: from.ID, Amount: -t.Amount, BalanceAfter: from.Balance, CreatedAt: t.CreatedAt},
		{ID: uuid.New(), TransferID: t.ID, AccountID: to.ID, Amount: t.Amount, BalanceAfter: to.Balance, CreatedAt: t.CreatedAt},
	}
	if err := CheckBalanced(entries); err != nil {
		return nil, err
	}
	return entries, nil
}

// CheckBalanced enforces the fundamental double-entry invariant.
func CheckBalanced(entries []Entry) error {
	var sum int64
	for _, e := range entries {
		sum += e.Amount
	}
	if sum != 0 {
		return ErrLedgerImbalance
	}
	return nil
}
