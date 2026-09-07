//go:build integration

// Integration tests run against a real Postgres (TEST_DATABASE_URL). They
// prove the parts unit tests cannot: row-lock ordering under real
// concurrency, SKIP LOCKED batching, CHECK constraints, keyset SQL.
//
//	docker compose up -d db
//	TEST_DATABASE_URL=postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable go test -tags integration ./...
package postgres_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dwijamahardika/ledgerd/internal/adapters/postgres"
	"github.com/dwijamahardika/ledgerd/internal/app"
	"github.com/dwijamahardika/ledgerd/internal/domain"
	"github.com/dwijamahardika/ledgerd/internal/platform/backoff"
	"github.com/dwijamahardika/ledgerd/internal/platform/clock"
	"github.com/dwijamahardika/ledgerd/internal/platform/outbox"
)

func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := postgres.Migrate(ctx, pool, log); err != nil {
		t.Fatal(err)
	}
	// Migrate is idempotent: a second run must be a no-op.
	if err := postgres.Migrate(ctx, pool, log); err != nil {
		t.Fatal(err)
	}
	for _, tbl := range []string{"idempotency_keys", "outbox", "ledger_entries", "transfers", "accounts"} {
		if _, err := pool.Exec(ctx, "DELETE FROM "+tbl); err != nil {
			t.Fatal(err)
		}
	}
	return pool
}

func TestPostgres_ConcurrentTransfersNoDeadlockNoLostUpdate(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	store := postgres.NewStore(pool)
	svc := app.NewService(store, clock.System{})

	a, _ := svc.CreateAccount(ctx, app.CreateAccountInput{Owner: "a", Currency: "USD"})
	b, _ := svc.CreateAccount(ctx, app.CreateAccountInput{Owner: "b", Currency: "USD"})
	for _, acc := range []*domain.Account{a, b} {
		if _, err := svc.Deposit(ctx, app.FundingInput{AccountID: acc.ID, Amount: 5_000}); err != nil {
			t.Fatal(err)
		}
	}

	const workers, perWorker = 16, 25
	var wg sync.WaitGroup
	var okCount, insufficient, other sync.Map
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				from, to := a.ID, b.ID
				if (w+i)%2 == 0 {
					from, to = b.ID, a.ID
				}
				_, err := svc.Transfer(ctx, app.TransferInput{FromAccountID: from, ToAccountID: to, Amount: 700, Currency: "USD"})
				switch {
				case err == nil:
					okCount.Store(uuid.New(), true)
				case errors.Is(err, domain.ErrInsufficientFunds):
					insufficient.Store(uuid.New(), true)
				default:
					other.Store(uuid.New(), err)
				}
			}
		}(w)
	}
	wg.Wait()

	other.Range(func(_, v any) bool { t.Errorf("unexpected error: %v", v); return true })

	ga, _ := svc.GetAccount(ctx, a.ID)
	gb, _ := svc.GetAccount(ctx, b.ID)
	if ga.Balance+gb.Balance != 10_000 || ga.Balance < 0 || gb.Balance < 0 {
		t.Fatalf("conservation violated: %d + %d", ga.Balance, gb.Balance)
	}
	var sumEntries, sumBalances int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(amount),0) FROM ledger_entries`).Scan(&sumEntries); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(balance),0) FROM accounts`).Scan(&sumBalances); err != nil {
		t.Fatal(err)
	}
	if sumEntries != 0 || sumBalances != 0 {
		t.Fatalf("ledger imbalance: entries=%d balances=%d", sumEntries, sumBalances)
	}
	// Reconcile: each account balance must equal the sum of its entries.
	var mismatches int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM accounts a WHERE a.balance <> (SELECT COALESCE(SUM(e.amount),0) FROM ledger_entries e WHERE e.account_id = a.id)`).Scan(&mismatches); err != nil {
		t.Fatal(err)
	}
	if mismatches != 0 {
		t.Fatalf("%d accounts disagree with their ledger", mismatches)
	}
}

func TestPostgres_TreasuryIsSingletonUnderRace(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	store := postgres.NewStore(pool)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.Accounts().EnsureTreasury(ctx, "EUR"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM accounts WHERE kind='treasury' AND currency='EUR'`).Scan(&n)
	if n != 1 {
		t.Fatalf("treasury count = %d", n)
	}
}

type recordingPub struct {
	mu   sync.Mutex
	seen []uuid.UUID
	fail bool
}

func (p *recordingPub) Publish(_ context.Context, e domain.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail {
		return errors.New("down")
	}
	p.seen = append(p.seen, e.ID)
	return nil
}

func TestPostgres_OutboxRelayDeliversAndRetries(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	store := postgres.NewStore(pool)
	svc := app.NewService(store, clock.System{})
	a, _ := svc.CreateAccount(ctx, app.CreateAccountInput{Owner: "a", Currency: "JPY"})
	if _, err := svc.Deposit(ctx, app.FundingInput{AccountID: a.ID, Amount: 100}); err != nil {
		t.Fatal(err)
	}

	pub := &recordingPub{fail: true}
	relay := outbox.NewRelay(postgres.NewOutboxSource(store), pub, outbox.Config{BatchSize: 10, MaxAttempts: 5,
		Backoff: backoff.New(time.Millisecond, time.Millisecond)}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if n, err := relay.RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	var attempts int
	var status string
	_ = pool.QueryRow(ctx, `SELECT attempts, status FROM outbox`).Scan(&attempts, &status)
	if attempts != 1 || status != "pending" {
		t.Fatalf("attempts=%d status=%s", attempts, status)
	}

	pub.mu.Lock()
	pub.fail = false
	pub.mu.Unlock()
	time.Sleep(5 * time.Millisecond) // let next_attempt_at pass
	if _, err := relay.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	_ = pool.QueryRow(ctx, `SELECT attempts, status FROM outbox`).Scan(&attempts, &status)
	if status != "sent" || len(pub.seen) != 1 {
		t.Fatalf("status=%s seen=%d", status, len(pub.seen))
	}
	// Sent rows are never re-delivered.
	if n, _ := relay.RunOnce(ctx); n != 0 {
		t.Fatalf("re-delivered %d", n)
	}
}

func TestPostgres_IdempotencyStore(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	s := postgres.NewIdempotencyStore(postgres.NewStore(pool))

	rec, acquired, err := s.Acquire(ctx, "alice", "k1", "fp", time.Hour)
	if err != nil || !acquired || rec != nil {
		t.Fatalf("first acquire: %v %v %v", rec, acquired, err)
	}
	rec, acquired, err = s.Acquire(ctx, "alice", "k1", "fp", time.Hour)
	if err != nil || acquired || rec.Completed {
		t.Fatalf("second acquire: %+v %v %v", rec, acquired, err)
	}
	if err := s.Complete(ctx, "alice", "k1", 201, map[string]string{"Location": "/x"}, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	rec, _, _ = s.Acquire(ctx, "alice", "k1", "fp", time.Hour)
	if !rec.Completed || rec.Status != 201 || rec.Headers["Location"] != "/x" || string(rec.Body) != `{}` {
		t.Fatalf("stored record: %+v", rec)
	}
	// Expired keys are reclaimable.
	if _, acquired, _ := s.Acquire(ctx, "alice", "k2", "fp", -time.Second); !acquired {
		t.Fatal("k2 first")
	}
	if _, acquired, _ := s.Acquire(ctx, "alice", "k2", "fp", time.Hour); !acquired {
		t.Fatal("expired key must be re-acquirable")
	}
}

func TestPostgres_KeysetPagination(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	svc := app.NewService(postgres.NewStore(pool), clock.System{})
	a, _ := svc.CreateAccount(ctx, app.CreateAccountInput{Owner: "a", Currency: "USD"})
	for i := 0; i < 11; i++ {
		if _, err := svc.Deposit(ctx, app.FundingInput{AccountID: a.ID, Amount: int64(i + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[uuid.UUID]bool{}
	cursor := ""
	for {
		page, err := svc.ListEntries(ctx, a.ID, cursor, 4)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Entries {
			if seen[e.ID] {
				t.Fatal("duplicate across pages")
			}
			seen[e.ID] = true
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(seen) != 11 {
		t.Fatalf("seen %d", len(seen))
	}
}
