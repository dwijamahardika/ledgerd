package app

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// EntryCursor implements keyset pagination on (created_at DESC, id DESC).
// Unlike OFFSET it is O(log n) regardless of page depth and stable under
// concurrent inserts. The wire form is opaque base64 so clients cannot depend
// on its structure.
type EntryCursor struct {
	CreatedAt time.Time `json:"t"`
	ID        uuid.UUID `json:"i"`
}

var ErrBadCursor = errors.New("malformed cursor")

func (c EntryCursor) Encode() string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func DecodeCursor(s string) (*EntryCursor, error) {
	if s == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, ErrBadCursor
	}
	var c EntryCursor
	if err := json.Unmarshal(raw, &c); err != nil || c.ID == uuid.Nil || c.CreatedAt.IsZero() {
		return nil, ErrBadCursor
	}
	return &c, nil
}
