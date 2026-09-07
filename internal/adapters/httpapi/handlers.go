package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/dwijamahardika/ledgerd/internal/app"
	"github.com/dwijamahardika/ledgerd/internal/domain"
)

// ---- DTOs: the wire contract is decoupled from domain structs on purpose ----

type accountResponse struct {
	ID        uuid.UUID `json:"id"`
	Kind      string    `json:"kind"`
	Owner     string    `json:"owner"`
	Currency  string    `json:"currency"`
	Balance   int64     `json:"balance"`
	Version   int64     `json:"version"`
	CreatedAt time.Time `json:"created_at"`
}

func toAccountResponse(a *domain.Account) accountResponse {
	return accountResponse{ID: a.ID, Kind: string(a.Kind), Owner: a.Owner, Currency: string(a.Currency), Balance: a.Balance, Version: a.Version, CreatedAt: a.CreatedAt}
}

type entryResponse struct {
	ID           uuid.UUID `json:"id"`
	TransferID   uuid.UUID `json:"transfer_id"`
	AccountID    uuid.UUID `json:"account_id"`
	Amount       int64     `json:"amount"`
	BalanceAfter int64     `json:"balance_after"`
	CreatedAt    time.Time `json:"created_at"`
}

type transferResponse struct {
	ID            uuid.UUID       `json:"id"`
	FromAccountID uuid.UUID       `json:"from_account_id"`
	ToAccountID   uuid.UUID       `json:"to_account_id"`
	Amount        int64           `json:"amount"`
	Currency      string          `json:"currency"`
	Reference     string          `json:"reference,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	Entries       []entryResponse `json:"entries,omitempty"`
}

func toTransferResponse(res *app.TransferResult) transferResponse {
	t := res.Transfer
	out := transferResponse{ID: t.ID, FromAccountID: t.FromAccountID, ToAccountID: t.ToAccountID, Amount: t.Amount, Currency: string(t.Currency), Reference: t.Reference, CreatedAt: t.CreatedAt}
	for _, e := range res.Entries {
		out.Entries = append(out.Entries, entryResponse{ID: e.ID, TransferID: e.TransferID, AccountID: e.AccountID, Amount: e.Amount, BalanceAfter: e.BalanceAfter, CreatedAt: e.CreatedAt})
	}
	return out
}

type createAccountRequest struct {
	Owner    string `json:"owner"`
	Currency string `json:"currency"`
}

type transferRequest struct {
	FromAccountID uuid.UUID `json:"from_account_id"`
	ToAccountID   uuid.UUID `json:"to_account_id"`
	Amount        int64     `json:"amount"`
	Currency      string    `json:"currency"`
	Reference     string    `json:"reference"`
}

type fundingRequest struct {
	Amount    int64  `json:"amount"`
	Reference string `json:"reference"`
}

type pageResponse[T any] struct {
	Data       []T    `json:"data"`
	NextCursor string `json:"next_cursor,omitempty"`
}

// ---- handlers ----

type handlers struct {
	svc     *app.Service
	metrics *Metrics
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var mbe *http.MaxBytesError
		switch {
		case errors.As(err, &mbe):
			writeProblem(w, r, http.StatusRequestEntityTooLarge, "Body too large", err.Error())
		case errors.Is(err, io.EOF):
			writeProblem(w, r, http.StatusBadRequest, "Empty body", "A JSON body is required.")
		default:
			writeProblem(w, r, http.StatusBadRequest, "Malformed JSON", err.Error())
		}
		return false
	}
	if dec.More() {
		writeProblem(w, r, http.StatusBadRequest, "Malformed JSON", "Body must contain a single JSON object.")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func pathUUID(w http.ResponseWriter, r *http.Request, name string) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue(name))
	if err != nil {
		writeProblemField(w, r, http.StatusBadRequest, "Invalid identifier", "Path parameter must be a UUID.", name)
		return uuid.Nil, false
	}
	return id, true
}

func (h *handlers) createAccount(w http.ResponseWriter, r *http.Request) {
	var req createAccountRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	acc, err := h.svc.CreateAccount(r.Context(), app.CreateAccountInput{Owner: req.Owner, Currency: req.Currency})
	if err != nil {
		writeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/accounts/"+acc.ID.String())
	writeJSON(w, http.StatusCreated, toAccountResponse(acc))
}

func (h *handlers) getAccount(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	acc, err := h.svc.GetAccount(r.Context(), id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toAccountResponse(acc))
}

func (h *handlers) listEntries(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	page, err := h.svc.ListEntries(r.Context(), id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeError(w, r, err)
		return
	}
	out := pageResponse[entryResponse]{Data: make([]entryResponse, 0, len(page.Entries)), NextCursor: page.NextCursor}
	for _, e := range page.Entries {
		out.Data = append(out.Data, entryResponse{ID: e.ID, TransferID: e.TransferID, AccountID: e.AccountID, Amount: e.Amount, BalanceAfter: e.BalanceAfter, CreatedAt: e.CreatedAt})
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *handlers) createTransfer(w http.ResponseWriter, r *http.Request) {
	var req transferRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	res, err := h.svc.Transfer(r.Context(), app.TransferInput{
		FromAccountID: req.FromAccountID, ToAccountID: req.ToAccountID, Amount: req.Amount, Currency: req.Currency, Reference: req.Reference,
	})
	h.finishTransfer(w, r, res, err)
}

func (h *handlers) deposit(w http.ResponseWriter, r *http.Request) {
	h.funding(w, r, h.svc.Deposit)
}

func (h *handlers) withdraw(w http.ResponseWriter, r *http.Request) {
	h.funding(w, r, h.svc.Withdraw)
}

func (h *handlers) funding(w http.ResponseWriter, r *http.Request, op func(ctx context.Context, in app.FundingInput) (*app.TransferResult, error)) {
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var req fundingRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	res, err := op(r.Context(), app.FundingInput{AccountID: id, Amount: req.Amount, Reference: req.Reference})
	h.finishTransfer(w, r, res, err)
}

func (h *handlers) finishTransfer(w http.ResponseWriter, r *http.Request, res *app.TransferResult, err error) {
	if err != nil {
		if app.IsClientError(err) {
			h.metrics.Transfer("rejected")
		} else {
			h.metrics.Transfer("error")
		}
		writeError(w, r, err)
		return
	}
	h.metrics.Transfer("posted")
	w.Header().Set("Location", "/v1/transfers/"+res.Transfer.ID.String())
	writeJSON(w, http.StatusCreated, toTransferResponse(res))
}

func (h *handlers) getTransfer(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	t, err := h.svc.GetTransfer(r.Context(), id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTransferResponse(&app.TransferResult{Transfer: t}))
}
