package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/saltindag22/case-notification-system/internal/domain"
	"github.com/saltindag22/case-notification-system/internal/logging"
)

// ErrorBody is the uniform error envelope.
type ErrorBody struct {
	Error         ErrorDetail `json:"error"`
	CorrelationID string      `json:"correlationId,omitempty"`
}

// ErrorDetail describes the problem.
type ErrorDetail struct {
	Code    string              `json:"code"`
	Message string              `json:"message"`
	Fields  []domain.FieldError `json:"fields,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeErrorCode(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	writeJSON(w, status, ErrorBody{
		Error:         ErrorDetail{Code: code, Message: msg},
		CorrelationID: logging.CorrelationID(r.Context()),
	})
}

// writeError maps domain errors to HTTP responses.
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	var (
		ve  *domain.ValidationError
		ise *domain.InvalidStateError
		mbe *http.MaxBytesError
	)
	body := ErrorBody{CorrelationID: logging.CorrelationID(r.Context())}
	status := http.StatusInternalServerError
	switch {
	case errors.As(err, &ve):
		status = http.StatusBadRequest
		body.Error = ErrorDetail{Code: "validation_error", Message: "request validation failed", Fields: ve.Fields}
	case errors.As(err, &mbe):
		status = http.StatusRequestEntityTooLarge
		body.Error = ErrorDetail{Code: "payload_too_large", Message: err.Error()}
	case errors.Is(err, domain.ErrNotFound):
		status = http.StatusNotFound
		body.Error = ErrorDetail{Code: "not_found", Message: "resource not found"}
	case errors.As(err, &ise):
		status = http.StatusConflict
		body.Error = ErrorDetail{Code: "invalid_state", Message: ise.Error()}
	case errors.Is(err, domain.ErrIdempotencyConflict):
		status = http.StatusConflict
		body.Error = ErrorDetail{Code: "idempotency_conflict", Message: err.Error()}
	case errors.Is(err, domain.ErrConflict):
		status = http.StatusConflict
		body.Error = ErrorDetail{Code: "conflict", Message: "a resource with the same unique attributes already exists"}
	case errors.Is(err, errBadRequest):
		status = http.StatusBadRequest
		body.Error = ErrorDetail{Code: "invalid_request", Message: err.Error()}
	default:
		s.log.ErrorContext(r.Context(), "request failed", slog.String("error", err.Error()), slog.String("path", r.URL.Path))
		body.Error = ErrorDetail{Code: "internal_error", Message: "internal server error"}
	}
	writeJSON(w, status, body)
}
