package domain

import (
	"strings"
	"time"

	"github.com/google/uuid"
)

// AccountKind distinguishes customer wallets from the per-currency treasury
// account that represents money entering or leaving the system. Deposits and
// withdrawals are ordinary transfers against the treasury, which is the only
// account allowed to hold a negative balance. That keeps every movement
// double-entry and reconcilable: sum(all balances) == 0 at all times.
type AccountKind string

const (
	KindUser     AccountKind = "user"
	KindTreasury AccountKind = "treasury"
)

func NewTreasuryAccount(currency Currency, now time.Time) (*Account, error) {
	if !currency.Valid() {
		return nil, invalid("currency", ErrInvalidCurrency)
	}
	return &Account{ID: uuid.New(), Kind: KindTreasury, Owner: "treasury:" + string(currency), Currency: currency, Version: 1, CreatedAt: now.UTC()}, nil
}

// Account is an aggregate root. Balance is denormalised from the ledger for
// O(1) reads; the ledger entries remain the source of truth and the two are
// reconciled inside the same transaction that mutates them.
type Account struct {
	ID        uuid.UUID
	Kind      AccountKind
	Owner     string
	Currency  Currency
	Balance   int64
	Version   int64 // optimistic-lock token, bumped on every balance change
	CreatedAt time.Time
}

func NewAccount(owner string, currency Currency, now time.Time) (*Account, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" || len(owner) > 120 {
		return nil, invalid("owner", ErrInvalidOwner)
	}
	if !currency.Valid() {
		return nil, invalid("currency", ErrInvalidCurrency)
	}
	return &Account{
		ID:        uuid.New(),
		Kind:      KindUser,
		Owner:     owner,
		Currency:  currency,
		Balance:   0,
		Version:   1,
		CreatedAt: now.UTC(),
	}, nil
}

// debit and credit are unexported: balance changes only happen through
// PostTransfer so the double-entry invariant cannot be bypassed.
func (a *Account) debit(amount int64) error {
	if a.Kind != KindTreasury && a.Balance < amount {
		return ErrInsufficientFunds
	}
	a.Balance -= amount
	a.Version++
	return nil
}

func (a *Account) credit(amount int64) error {
	if a.Balance > MaxBalance-amount {
		return ErrBalanceOverflow
	}
	a.Balance += amount
	a.Version++
	return nil
}
