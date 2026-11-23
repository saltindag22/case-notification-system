// Package domain contains the core notification model, its state machine and the
// channel specific validation rules. It has no infrastructure dependencies.
package domain

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Channel is a delivery channel.
type Channel string

// Supported channels.
const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Channels lists every supported channel in a stable order.
var Channels = []Channel{ChannelSMS, ChannelEmail, ChannelPush}

// ParseChannel converts user input into a Channel.
func ParseChannel(s string) (Channel, error) {
	switch c := Channel(strings.ToLower(strings.TrimSpace(s))); c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return c, nil
	default:
		return "", fmt.Errorf("unsupported channel %q (allowed: sms, email, push)", s)
	}
}

// Priority controls processing order. Higher values are processed first.
type Priority int

// Supported priorities. The numeric values are persisted and are also used as
// AMQP message priorities, so they must stay in the 1..3 range.
const (
	PriorityLow    Priority = 1
	PriorityNormal Priority = 2
	PriorityHigh   Priority = 3
)

// ParsePriority converts user input into a Priority. Empty input means normal.
func ParsePriority(s string) (Priority, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "normal":
		return PriorityNormal, nil
	case "high":
		return PriorityHigh, nil
	case "low":
		return PriorityLow, nil
	default:
		return 0, fmt.Errorf("unsupported priority %q (allowed: high, normal, low)", s)
	}
}

// String returns the API representation of the priority.
func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "high"
	case PriorityLow:
		return "low"
	default:
		return "normal"
	}
}

// MarshalJSON encodes the priority as its name.
func (p Priority) MarshalJSON() ([]byte, error) { return json.Marshal(p.String()) }

// UnmarshalJSON decodes a priority name.
func (p *Priority) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("priority must be a string")
	}
	v, err := ParsePriority(s)
	if err != nil {
		return err
	}
	*p = v
	return nil
}

// Status is the lifecycle state of a notification.
//
//	pending ──────┐
//	scheduled ────┼─▶ queued ─▶ processing ─▶ sent
//	retrying ◀────┘                 │
//	   ▲                            ├─▶ retrying (retryable error, attempts left)
//	   └────────────────────────────┤
//	                                └─▶ failed   (permanent error or attempts exhausted)
//
//	pending | scheduled | queued | retrying ─▶ cancelled
//	failed ─▶ pending (manual requeue)
type Status string

// Notification statuses.
const (
	StatusPending    Status = "pending"
	StatusScheduled  Status = "scheduled"
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusRetrying   Status = "retrying"
	StatusSent       Status = "sent"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

// Statuses lists every status in lifecycle order.
var Statuses = []Status{
	StatusPending, StatusScheduled, StatusQueued, StatusProcessing,
	StatusRetrying, StatusSent, StatusFailed, StatusCancelled,
}

// ParseStatus converts user input into a Status.
func ParseStatus(s string) (Status, error) {
	v := Status(strings.ToLower(strings.TrimSpace(s)))
	for _, st := range Statuses {
		if st == v {
			return v, nil
		}
	}
	return "", fmt.Errorf("unsupported status %q", s)
}

// CancellableStatuses are the states from which a notification may be cancelled.
// A notification that is already being handed to the provider (processing) can
// no longer be cancelled safely.
var CancellableStatuses = []Status{StatusPending, StatusScheduled, StatusQueued, StatusRetrying}

// DispatchableStatuses are the states the dispatcher picks up once due.
var DispatchableStatuses = []Status{StatusPending, StatusScheduled, StatusRetrying}

// IsCancellable reports whether a notification in status s can be cancelled.
func (s Status) IsCancellable() bool { return contains(CancellableStatuses, s) }

// IsTerminal reports whether s is a final state.
func (s Status) IsTerminal() bool {
	return s == StatusSent || s == StatusFailed || s == StatusCancelled
}

func contains(list []Status, s Status) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// Notification is a single message to deliver over one channel.
type Notification struct {
	ID                uuid.UUID       `json:"id"`
	BatchID           *uuid.UUID      `json:"batchId,omitempty"`
	IdempotencyKey    *string         `json:"idempotencyKey,omitempty"`
	CorrelationID     string          `json:"correlationId,omitempty"`
	Channel           Channel         `json:"channel"`
	Recipient         string          `json:"recipient"`
	Subject           *string         `json:"subject,omitempty"`
	Content           string          `json:"content"`
	TemplateID        *uuid.UUID      `json:"templateId,omitempty"`
	Metadata          json.RawMessage `json:"metadata,omitempty"`
	Priority          Priority        `json:"priority"`
	Status            Status          `json:"status"`
	Attempts          int             `json:"attempts"`
	MaxAttempts       int             `json:"maxAttempts"`
	ScheduledAt       *time.Time      `json:"scheduledAt,omitempty"`
	NextAttemptAt     *time.Time      `json:"nextAttemptAt,omitempty"`
	ProviderMessageID *string         `json:"providerMessageId,omitempty"`
	LastError         *string         `json:"lastError,omitempty"`
	CreatedAt         time.Time       `json:"createdAt"`
	UpdatedAt         time.Time       `json:"updatedAt"`
	SentAt            *time.Time      `json:"sentAt,omitempty"`
	CancelledAt       *time.Time      `json:"cancelledAt,omitempty"`

	// Internal fields that are persisted but never exposed through the API.
	RequestHash       string   `json:"-"`
	TraceParent       string   `json:"-"`
	EffectivePriority Priority `json:"-"`
}

// AttemptOutcome classifies a delivery attempt.
type AttemptOutcome string

// Attempt outcomes.
const (
	OutcomeSuccess        AttemptOutcome = "success"
	OutcomeRetryableError AttemptOutcome = "retryable_error"
	OutcomePermanentError AttemptOutcome = "permanent_error"
)

// Attempt is the audit record of one delivery attempt.
type Attempt struct {
	ID                int64          `json:"-"`
	NotificationID    uuid.UUID      `json:"-"`
	AttemptNumber     int            `json:"attemptNumber"`
	Outcome           AttemptOutcome `json:"outcome"`
	HTTPStatus        *int           `json:"httpStatus,omitempty"`
	Error             *string        `json:"error,omitempty"`
	ProviderMessageID *string        `json:"providerMessageId,omitempty"`
	DurationMS        int            `json:"durationMs"`
	CreatedAt         time.Time      `json:"createdAt"`
}

// Template is a reusable message body with variable placeholders.
type Template struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Channel   Channel   `json:"channel"`
	Subject   *string   `json:"subject,omitempty"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// Batch groups notifications created in a single request.
type Batch struct {
	ID             uuid.UUID
	IdempotencyKey *string
	RequestHash    string
	Total          int
	CorrelationID  string
	CreatedAt      time.Time
}

// BatchSummary aggregates the state of a batch.
type BatchSummary struct {
	BatchID   uuid.UUID      `json:"batchId"`
	Total     int            `json:"total"`
	Counts    map[Status]int `json:"counts"`
	Completed bool           `json:"completed"`
	CreatedAt time.Time      `json:"createdAt"`
}

// NewID returns a new time ordered (v7) UUID. Time ordering keeps B-tree inserts
// append-only and lets the id double as a stable keyset pagination cursor.
func NewID() uuid.UUID {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.New()
	}
	return id
}
