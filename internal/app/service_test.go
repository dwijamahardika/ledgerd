package app_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dwijamahardika/ledgerd/internal/app"
	"github.com/dwijamahardika/ledgerd/internal/domain"
	"github.com/dwijamahardika/ledgerd/internal/platform/clock"
)

func setup(t *testing.T) (*app.Service, *memStore, *clock.Fake) {
	t.Helper()
	store := newMemStore()
	clk := clock.NewFake(time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC))
	return app.NewService(store, clk), store, clk
}

func fundedAccount(t *testing.T, svc *app.Service, cur string, amount int64) *domain.Account {
	t.Helper()
	acc, err := svc.CreateAccount(context.Background(), app.CreateAccountInput{Owner: "o", Currency: cur})
	if err != nil {
		t.Fatal(err)
	}
	if amount > 0 {
		if _, err := svc.Deposit(context.Background(), app.FundingInput{AccountID: acc.ID, Amount: amount}); err != nil {
			t.Fatal(err)
		}
	}
	return acc
}

func TestTransfer_HappyPath(t *testing.T) {
	svc, store, _ := setup(t)
	a := fundedAccount(t, svc, "USD", 1_000)
	b := fundedAccount(t, svc, "USD", 0)

	res, err := svc.Transfer(context.Background(), app.TransferInput{FromAccountID: a.ID, ToAccountID: b.ID, Amount: 250, Currency: "USD", Reference: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	if res.From.Balance != 750 || res.To.Balance != 250 {
		t.Fatalf("balances %d %d", res.From.Balance, res.To.Balance)
	}
	gotA, _ := svc.GetAccount(context.Background(), a.ID)
	if gotA.Balance != 750 || gotA.Version != 3 { // create=1, deposit=2, transfer=3
		t.Fatalf("persisted a: %+v", gotA)
	}
	// Outbox: 2 deposits... no - 1 deposit (b was funded with 0) + 1 transfer
	if len(store.outbox) != 2 {
		t.Fatalf("outbox events = %d, want 2", len(store.outbox))
	}
	last := store.outbox[len(store.outbox)-1]
	if last.Type != domain.EventTransferPosted || last.AggregateID != res.Transfer.ID {
		t.Fatalf("bad event %+v", last)
	}
	// Global invariant: treasury + users == 0
	var sum int64
	for _, acc := range store.accounts {
		sum += acc.Balance
	}
	if sum != 0 {
		t.Fatalf("ledger not balanced, sum=%d", sum)
	}
}

func TestTransfer_InsufficientFunds(t *testing.T) {
	svc, _, _ := setup(t)
	a := fundedAccount(t, svc, "USD", 100)
	b := fundedAccount(t, svc, "USD", 0)
	_, err := svc.Transfer(context.Background(), app.TransferInput{FromAccountID: a.ID, ToAccountID: b.ID, Amount: 101, Currency: "USD"})
	if !errors.Is(err, domain.ErrInsufficientFunds) {
		t.Fatalf("want ErrInsufficientFunds, got %v", err)
	}
	if !app.IsClientError(err) {
		t.Fatal("should be a client error")
	}
}

func TestTransfer_UnknownAccountIsFieldError(t *testing.T) {
	svc, _, _ := setup(t)
	a := fundedAccount(t, svc, "USD", 100)
	_, err := svc.Transfer(context.Background(), app.TransferInput{FromAccountID: a.ID, ToAccountID: uuid.New(), Amount: 1, Currency: "USD"})
	var ve *domain.ValidationError
	if !errors.As(err, &ve) || ve.Field != "to_account_id" || !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestTransfer_CurrencyMismatch(t *testing.T) {
	svc, _, _ := setup(t)
	a := fundedAccount(t, svc, "USD", 100)
	b := fundedAccount(t, svc, "JPY", 0)
	_, err := svc.Transfer(context.Background(), app.TransferInput{FromAccountID: a.ID, ToAccountID: b.ID, Amount: 1, Currency: "USD"})
	if !errors.Is(err, domain.ErrCurrencyMismatch) {
		t.Fatalf("got %v", err)
	}
}

// The critical property: if any write inside the transaction fails, nothing
// is persisted - no half-applied balance, no orphan transfer, no phantom event.
func TestTransfer_AtomicOnFailure(t *testing.T) {
	for _, failAt := range []string{"Save", "CreateTransfer", "AppendEntries", "Enqueue"} {
		t.Run(failAt, func(t *testing.T) {
			svc, store, _ := setup(t)
			a := fundedAccount(t, svc, "USD", 500)
			b := fundedAccount(t, svc, "USD", 0)
			before := store.clone()

			store.failOn, store.failErr = failAt, errors.New("disk on fire")
			_, err := svc.Transfer(context.Background(), app.TransferInput{FromAccountID: a.ID, ToAccountID: b.ID, Amount: 100, Currency: "USD"})
			if err == nil || !errors.Is(err, store.failErr) {
				t.Fatalf("want injected failure, got %v", err)
			}
			if store.accounts[a.ID].Balance != before.accounts[a.ID].Balance || store.accounts[b.ID].Balance != before.accounts[b.ID].Balance {
				t.Fatal("balances changed despite failure")
			}
			if len(store.xfers) != len(before.xfers) || len(store.entries) != len(before.entries) || len(store.outbox) != len(before.outbox) {
				t.Fatal("partial writes leaked")
			}
		})
	}
}

// Concurrency: many goroutines shuffle money in both directions; totals must
// be conserved and no balance may go negative. The fake store serialises
// transactions, so this proves the use case is correct under interleaving,
// while the Postgres integration test proves the locking strategy holds too.
func TestTransfer_ConcurrentConservesMoney(t *testing.T) {
	svc, store, _ := setup(t)
	a := fundedAccount(t, svc, "USD", 10_000)
	b := fundedAccount(t, svc, "USD", 10_000)

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			from, to := a.ID, b.ID
			if i%2 == 0 {
				from, to = b.ID, a.ID
			}
			_, _ = svc.Transfer(context.Background(), app.TransferInput{FromAccountID: from, ToAccountID: to, Amount: 300, Currency: "USD"})
		}(i)
	}
	wg.Wait()

	ga, _ := svc.GetAccount(context.Background(), a.ID)
	gb, _ := svc.GetAccount(context.Background(), b.ID)
	if ga.Balance+gb.Balance != 20_000 {
		t.Fatalf("money created or destroyed: %d + %d", ga.Balance, gb.Balance)
	}
	if ga.Balance < 0 || gb.Balance < 0 {
		t.Fatal("negative balance")
	}
	var sum int64
	for _, e := range store.entries {
		sum += e.Amount
	}
	if sum != 0 {
		t.Fatalf("entries not balanced: %d", sum)
	}
}

func TestWithdraw_CannotOverdraw(t *testing.T) {
	svc, _, _ := setup(t)
	a := fundedAccount(t, svc, "USD", 100)
	if _, err := svc.Withdraw(context.Background(), app.FundingInput{AccountID: a.ID, Amount: 100}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Withdraw(context.Background(), app.FundingInput{AccountID: a.ID, Amount: 1}); !errors.Is(err, domain.ErrInsufficientFunds) {
		t.Fatalf("got %v", err)
	}
}

func TestDeposit_RejectsTreasuryTarget(t *testing.T) {
	svc, store, _ := setup(t)
	fundedAccount(t, svc, "USD", 1)
	var treasuryID uuid.UUID
	for id, acc := range store.accounts {
		if acc.Kind == domain.KindTreasury {
			treasuryID = id
		}
	}
	if _, err := svc.Deposit(context.Background(), app.FundingInput{AccountID: treasuryID, Amount: 1}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestListEntries_KeysetPagination(t *testing.T) {
	svc, _, clk := setup(t)
	a := fundedAccount(t, svc, "USD", 0)
	// 7 deposits, each at a distinct timestamp, plus two sharing one timestamp
	// to exercise the (created_at, id) tiebreak.
	for i := 0; i < 7; i++ {
		clk.Advance(time.Second)
		if _, err := svc.Deposit(context.Background(), app.FundingInput{AccountID: a.ID, Amount: int64(i + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	clk.Advance(time.Second)
	for i := 0; i < 2; i++ {
		if _, err := svc.Deposit(context.Background(), app.FundingInput{AccountID: a.ID, Amount: 100}); err != nil {
			t.Fatal(err)
		}
	}

	seen := map[uuid.UUID]bool{}
	cursor := ""
	pages := 0
	for {
		page, err := svc.ListEntries(context.Background(), a.ID, cursor, 4)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, e := range page.Entries {
			if seen[e.ID] {
				t.Fatalf("entry %s returned twice", e.ID)
			}
			seen[e.ID] = true
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(seen) != 9 || pages != 3 {
		t.Fatalf("seen=%d pages=%d", len(seen), pages)
	}
}

func TestListEntries_BadCursor(t *testing.T) {
	svc, _, _ := setup(t)
	a := fundedAccount(t, svc, "USD", 0)
	if _, err := svc.ListEntries(context.Background(), a.ID, "not-a-cursor", 10); !errors.Is(err, app.ErrBadCursor) {
		t.Fatalf("got %v", err)
	}
}

func TestCursor_RoundTrip(t *testing.T) {
	c := app.EntryCursor{CreatedAt: time.Now().UTC().Truncate(time.Microsecond), ID: uuid.New()}
	got, err := app.DecodeCursor(c.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if !got.CreatedAt.Equal(c.CreatedAt) || got.ID != c.ID {
		t.Fatalf("roundtrip mismatch: %+v vs %+v", got, c)
	}
	if _, err := app.DecodeCursor("eyJ0IjoiIn0"); err == nil { // {"t":""}
		t.Fatal("zero-value cursor should be rejected")
	}
}
