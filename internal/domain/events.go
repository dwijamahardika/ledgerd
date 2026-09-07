package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

const EventTransferPosted = "transfer.posted"

// Event is what leaves the service boundary through the outbox. Payload is
// pre-serialised so the relay never needs domain knowledge.
type Event struct {
	ID          uuid.UUID
	Type        string
	AggregateID uuid.UUID
	Payload     json.RawMessage
	OccurredAt  time.Time
}

type transferPostedPayload struct {
	TransferID    uuid.UUID `json:"transfer_id"`
	FromAccountID uuid.UUID `json:"from_account_id"`
	ToAccountID   uuid.UUID `json:"to_account_id"`
	Amount        int64     `json:"amount"`
	Currency      Currency  `json:"currency"`
	Reference     string    `json:"reference,omitempty"`
	PostedAt      time.Time `json:"posted_at"`
}

func NewTransferPostedEvent(t *Transfer) (Event, error) {
	body, err := json.Marshal(transferPostedPayload{
		TransferID: t.ID, FromAccountID: t.FromAccountID, ToAccountID: t.ToAccountID,
		Amount: t.Amount, Currency: t.Currency, Reference: t.Reference, PostedAt: t.CreatedAt,
	})
	if err != nil {
		return Event{}, err
	}
	return Event{ID: uuid.New(), Type: EventTransferPosted, AggregateID: t.ID, Payload: body, OccurredAt: t.CreatedAt}, nil
}
