package domain

import (
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors shared across layers. The HTTP layer maps them to status codes.
var (
	ErrNotFound            = errors.New("not found")
	ErrInvalidState        = errors.New("invalid state transition")
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")
	ErrConflict            = errors.New("conflict")
)

// FieldError describes a validation problem with a single field.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationError aggregates field level problems.
type ValidationError struct {
	Fields []FieldError `json:"fields"`
}

func (e *ValidationError) Error() string {
	parts := make([]string, 0, len(e.Fields))
	for _, f := range e.Fields {
		parts = append(parts, f.Field+": "+f.Message)
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

// Add records a field problem.
func (e *ValidationError) Add(field, format string, args ...any) {
	e.Fields = append(e.Fields, FieldError{Field: field, Message: fmt.Sprintf(format, args...)})
}

// Merge appends every problem of other, prefixing field names.
func (e *ValidationError) Merge(prefix string, other *ValidationError) {
	if other == nil {
		return
	}
	for _, f := range other.Fields {
		name := f.Field
		if prefix != "" {
			name = prefix + "." + name
		}
		e.Fields = append(e.Fields, FieldError{Field: name, Message: f.Message})
	}
}

// OrNil returns nil when there are no problems so callers can `return v.OrNil()`.
func (e *ValidationError) OrNil() error {
	if e == nil || len(e.Fields) == 0 {
		return nil
	}
	return e
}

// InvalidStateError explains why a transition was rejected.
type InvalidStateError struct {
	Current Status
	Action  string
}

func (e *InvalidStateError) Error() string {
	return fmt.Sprintf("cannot %s a notification in status %q", e.Action, e.Current)
}

// Unwrap lets errors.Is(err, ErrInvalidState) match.
func (e *InvalidStateError) Unwrap() error { return ErrInvalidState }
