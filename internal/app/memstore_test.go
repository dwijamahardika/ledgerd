package app_test

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/dwijamahardika/ledgerd/internal/app"
	"github.com/dwijamahardika/ledgerd/internal/domain"
)

// memStore is an in-memory app.Store with real transaction semantics:
// WithinTx works on a deep copy and swaps it in only on success, so tests can
// assert that a failed use case leaves no partial writes behind. A global
// mutex serialises transactions, mirroring the row locks Postgres would take.
type memStore struct {
	mu       sync.Mutex
	accounts map[uuid.UUID]*domain.Account
	xfers    map[uuid.UUID]*domain.Transfer
	entries  []domain.Entry
	outbox   []domain.Event

	failOn  string // repo method name that should return failErr, for atomicity tests
	failErr error
}

func newMemStore() *memStore {
	return &memStore{accounts: map[uuid.UUID]*domain.Account{}, xfers: map[uuid.UUID]*domain.Transfer{}}
}

func (m *memStore) clone() *memStore {
	c := &memStore{accounts: make(map[uuid.UUID]*domain.Account, len(m.accounts)), xfers: make(map[uuid.UUID]*domain.Transfer, len(m.xfers)),
		failOn: m.failOn, failErr: m.failErr}
	for k, v := range m.accounts {
		cp := *v
		c.accounts[k] = &cp
	}
	for k, v := range m.xfers {
		cp := *v
		c.xfers[k] = &cp
	}
	c.entries = append([]domain.Entry(nil), m.entries...)
	c.outbox = append([]domain.Event(nil), m.outbox...)
	return c
}

func (m *memStore) Accounts() app.AccountRepository { return (*memAccounts)(m) }
func (m *memStore) Ledger() app.LedgerRepository    { return (*memLedger)(m) }
func (m *memStore) Outbox() app.OutboxRepository    { return (*memOutbox)(m) }

func (m *memStore) WithinTx(ctx context.Context, fn func(ctx context.Context, tx app.Store) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	work := m.clone()
	if err := fn(ctx, work); err != nil {
		return err
	}
	m.accounts, m.xfers, m.entries, m.outbox = work.accounts, work.xfers, work.entries, work.outbox
	return nil
}

func (m *memStore) fail(method string) error {
	if m.failOn == method {
		return m.failErr
	}
	return nil
}

type memAccounts memStore

func (r *memAccounts) Create(_ context.Context, a *domain.Account) error {
	if err := (*memStore)(r).fail("Create"); err != nil {
		return err
	}
	cp := *a
	r.accounts[a.ID] = &cp
	return nil
}

func (r *memAccounts) Get(_ context.Context, id uuid.UUID) (*domain.Account, error) {
	a, ok := r.accounts[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	cp := *a
	return &cp, nil
}

func (r *memAccounts) EnsureTreasury(_ context.Context, cur domain.Currency) (*domain.Account, error) {
	for _, a := range r.accounts {
		if a.Kind == domain.KindTreasury && a.Currency == cur {
			cp := *a
			return &cp, nil
		}
	}
	t, err := domain.NewTreasuryAccount(cur, time.Now())
	if err != nil {
		return nil, err
	}
	r.accounts[t.ID] = t
	cp := *t
	return &cp, nil
}

func (r *memAccounts) LockMany(_ context.Context, ids []uuid.UUID) (map[uuid.UUID]*domain.Account, error) {
	if !sort.SliceIsSorted(ids, func(i, j int) bool { return lessUUID(ids[i], ids[j]) }) {
		return nil, errors.New("LockMany called with unsorted ids: deadlock hazard")
	}
	out := map[uuid.UUID]*domain.Account{}
	for _, id := range ids {
		if a, ok := r.accounts[id]; ok {
			cp := *a
			out[id] = &cp
		}
	}
	return out, nil
}

func (r *memAccounts) Save(_ context.Context, a *domain.Account, expected int64) error {
	if err := (*memStore)(r).fail("Save"); err != nil {
		return err
	}
	cur, ok := r.accounts[a.ID]
	if !ok {
		return domain.ErrNotFound
	}
	if cur.Version != expected {
		return domain.ErrVersionConflict
	}
	if a.Kind != domain.KindTreasury && a.Balance < 0 {
		return domain.ErrInsufficientFunds
	}
	cp := *a
	r.accounts[a.ID] = &cp
	return nil
}

type memLedger memStore

func (r *memLedger) CreateTransfer(_ context.Context, t *domain.Transfer) error {
	if err := (*memStore)(r).fail("CreateTransfer"); err != nil {
		return err
	}
	cp := *t
	r.xfers[t.ID] = &cp
	return nil
}

func (r *memLedger) GetTransfer(_ context.Context, id uuid.UUID) (*domain.Transfer, error) {
	t, ok := r.xfers[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	cp := *t
	return &cp, nil
}

func (r *memLedger) AppendEntries(_ context.Context, es []domain.Entry) error {
	if err := (*memStore)(r).fail("AppendEntries"); err != nil {
		return err
	}
	r.entries = append(r.entries, es...)
	return nil
}

func (r *memLedger) ListEntries(_ context.Context, accountID uuid.UUID, after *app.EntryCursor, limit int) ([]domain.Entry, error) {
	var all []domain.Entry
	for _, e := range r.entries {
		if e.AccountID != accountID {
			continue
		}
		if after != nil {
			// strictly "less than" cursor in (created_at DESC, id DESC) order
			if e.CreatedAt.After(after.CreatedAt) || e.CreatedAt.Equal(after.CreatedAt) && !lessUUID(e.ID, after.ID) {
				continue
			}
		}
		all = append(all, e)
	}
	sort.Slice(all, func(i, j int) bool {
		if !all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].CreatedAt.After(all[j].CreatedAt)
		}
		return lessUUID(all[j].ID, all[i].ID)
	})
	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

type memOutbox memStore

func (r *memOutbox) Enqueue(_ context.Context, e domain.Event) error {
	if err := (*memStore)(r).fail("Enqueue"); err != nil {
		return err
	}
	r.outbox = append(r.outbox, e)
	return nil
}

func lessUUID(a, b uuid.UUID) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}
