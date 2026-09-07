package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/dwijamahardika/ledgerd/internal/app"
	"github.com/dwijamahardika/ledgerd/internal/domain"
)

// Problem is an RFC 9457 "problem details" body. One error shape for every
// failure means clients write one error handler.
type Problem struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail,omitempty"`
	Instance string `json:"instance,omitempty"` // request ID for support correlation
	Field    string `json:"field,omitempty"`
}

func writeProblem(w http.ResponseWriter, r *http.Request, status int, title, detail string) {
	writeProblemField(w, r, status, title, detail, "")
}

func writeProblemField(w http.ResponseWriter, r *http.Request, status int, title, detail, field string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(Problem{
		Type:     "about:blank",
		Title:    title,
		Status:   status,
		Detail:   detail,
		Instance: RequestIDFrom(r.Context()),
		Field:    field,
	})
}

// writeError maps domain/app errors to HTTP. Unknown errors become opaque 500s
// so internal details never leak; the request ID links to the server log.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	var ve *domain.ValidationError
	field := ""
	if errors.As(err, &ve) {
		field = ve.Field
	}
	switch {
	case errors.Is(err, domain.ErrNotFound):
		if field != "" {
			writeProblemField(w, r, http.StatusUnprocessableEntity, "Unknown reference", err.Error(), field)
			return
		}
		writeProblem(w, r, http.StatusNotFound, "Not found", "The requested resource does not exist.")
	case errors.Is(err, domain.ErrInsufficientFunds):
		writeProblem(w, r, http.StatusUnprocessableEntity, "Insufficient funds", "The source account balance is too low.")
	case errors.Is(err, domain.ErrCurrencyMismatch):
		writeProblem(w, r, http.StatusUnprocessableEntity, "Currency mismatch", "All accounts and the transfer must share one currency.")
	case errors.Is(err, domain.ErrVersionConflict):
		w.Header().Set("Retry-After", "0")
		writeProblem(w, r, http.StatusConflict, "Concurrent modification", "The account changed while processing; retry the request.")
	case errors.Is(err, app.ErrBadCursor):
		writeProblemField(w, r, http.StatusBadRequest, "Invalid cursor", "The cursor is malformed.", "cursor")
	case ve != nil:
		writeProblemField(w, r, http.StatusBadRequest, "Validation failed", err.Error(), field)
	default:
		LoggerFrom(r.Context()).Error("unhandled error", "err", err)
		writeProblem(w, r, http.StatusInternalServerError, "Internal error", "Something went wrong. Quote the instance ID when reporting.")
	}
}
