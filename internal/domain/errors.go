package domain

import (
	"errors"
	"fmt"
)

// Sentinel errors are the domain's contract with adapters: the HTTP layer maps
// them to status codes, the storage layer raises them for constraint failures.
var (
	ErrNotFound          = errors.New("not found")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrCurrencyMismatch  = errors.New("currency mismatch")
	ErrSameAccount       = errors.New("source and destination account are identical")
	ErrInvalidAmount     = errors.New("amount must be a positive integer of minor units")
	ErrInvalidCurrency   = errors.New("currency must be a 3-letter ISO 4217 code")
	ErrVersionConflict   = errors.New("concurrent modification detected")
	ErrLedgerImbalance   = errors.New("ledger entries do not sum to zero")
	ErrInvalidOwner      = errors.New("owner is required (max 120 chars)")
	ErrBalanceOverflow   = errors.New("balance would overflow")
	ErrInvalidRef        = errors.New("reference max 200 chars")
)

// ValidationError carries a field name so the API can point at the offending input.
type ValidationError struct {
	Field string
	Err   error
}

func (e *ValidationError) Error() string { return fmt.Sprintf("%s: %v", e.Field, e.Err) }
func (e *ValidationError) Unwrap() error { return e.Err }

func invalid(field string, err error) error { return &ValidationError{Field: field, Err: err} }
