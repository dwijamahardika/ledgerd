package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func mustAccount(t *testing.T, cur Currency, balance int64) *Account {
	t.Helper()
	a, err := NewAccount("owner", cur, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	a.Balance = balance
	return a
}

func TestNewTransfer_Validation(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	long := make([]byte, 201)
	for i := range long {
		long[i] = 'x'
	}
	cases := []struct {
		name    string
		from    uuid.UUID
		to      uuid.UUID
		amount  int64
		cur     Currency
		ref     string
		wantErr error
		field   string
	}{
		{"ok", a, b, 100, "USD", "inv-1", nil, ""},
		{"same account", a, a, 100, "USD", "", ErrSameAccount, "to_account_id"},
		{"zero amount", a, b, 0, "USD", "", ErrInvalidAmount, "amount"},
		{"negative amount", a, b, -5, "USD", "", ErrInvalidAmount, "amount"},
		{"overflow amount", a, b, MaxBalance + 1, "USD", "", ErrInvalidAmount, "amount"},
		{"lowercase currency", a, b, 1, "usd", "", ErrInvalidCurrency, "currency"},
		{"short currency", a, b, 1, "US", "", ErrInvalidCurrency, "currency"},
		{"nil from", uuid.Nil, b, 1, "USD", "", ErrNotFound, "from_account_id"},
		{"long reference", a, b, 1, "USD", string(long), ErrInvalidRef, "reference"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewTransfer(tc.from, tc.to, tc.amount, tc.cur, tc.ref, time.Now())
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("want %v, got %v", tc.wantErr, err)
			}
			var ve *ValidationError
			if !errors.As(err, &ve) || ve.Field != tc.field {
				t.Fatalf("want field %q, got %+v", tc.field, ve)
			}
		})
	}
}

func TestPostTransfer_BalancedAndMutates(t *testing.T) {
	from := mustAccount(t, "JPY", 1000)
	to := mustAccount(t, "JPY", 0)
	tr, _ := NewTransfer(from.ID, to.ID, 300, "JPY", "", time.Now())

	entries, err := PostTransfer(tr, from, to)
	if err != nil {
		t.Fatal(err)
	}
	if from.Balance != 700 || to.Balance != 300 {
		t.Fatalf("balances: from=%d to=%d", from.Balance, to.Balance)
	}
	if from.Version != 2 || to.Version != 2 {
		t.Fatalf("versions not bumped: %d %d", from.Version, to.Version)
	}
	if len(entries) != 2 || entries[0].Amount != -300 || entries[1].Amount != 300 {
		t.Fatalf("entries: %+v", entries)
	}
	if entries[0].BalanceAfter != 700 || entries[1].BalanceAfter != 300 {
		t.Fatalf("balance_after snapshots wrong: %+v", entries)
	}
	if err := CheckBalanced(entries); err != nil {
		t.Fatal(err)
	}
}

func TestPostTransfer_InsufficientFundsLeavesStateUntouched(t *testing.T) {
	from := mustAccount(t, "USD", 50)
	to := mustAccount(t, "USD", 0)
	tr, _ := NewTransfer(from.ID, to.ID, 100, "USD", "", time.Now())
	if _, err := PostTransfer(tr, from, to); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("want ErrInsufficientFunds, got %v", err)
	}
	if from.Balance != 50 || from.Version != 1 || to.Balance != 0 {
		t.Fatal("state mutated on failure")
	}
}

func TestPostTransfer_TreasuryMayGoNegative(t *testing.T) {
	treasury, _ := NewTreasuryAccount("USD", time.Now())
	user := mustAccount(t, "USD", 0)
	tr, _ := NewTransfer(treasury.ID, user.ID, 5000, "USD", "deposit", time.Now())
	if _, err := PostTransfer(tr, treasury, user); err != nil {
		t.Fatal(err)
	}
	if treasury.Balance != -5000 || user.Balance != 5000 {
		t.Fatalf("treasury=%d user=%d", treasury.Balance, user.Balance)
	}
}

func TestPostTransfer_CurrencyMismatch(t *testing.T) {
	from := mustAccount(t, "USD", 100)
	to := mustAccount(t, "EUR", 0)
	tr, _ := NewTransfer(from.ID, to.ID, 10, "USD", "", time.Now())
	if _, err := PostTransfer(tr, from, to); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("want ErrCurrencyMismatch, got %v", err)
	}
}

func TestPostTransfer_Overflow(t *testing.T) {
	from := mustAccount(t, "USD", MaxBalance)
	to := mustAccount(t, "USD", MaxBalance)
	tr, _ := NewTransfer(from.ID, to.ID, 1, "USD", "", time.Now())
	if _, err := PostTransfer(tr, from, to); !errors.Is(err, ErrBalanceOverflow) {
		t.Fatalf("want ErrBalanceOverflow, got %v", err)
	}
}

func TestCheckBalanced(t *testing.T) {
	if err := CheckBalanced([]Entry{{Amount: -1}, {Amount: 2}}); !errors.Is(err, ErrLedgerImbalance) {
		t.Fatalf("want imbalance, got %v", err)
	}
}
