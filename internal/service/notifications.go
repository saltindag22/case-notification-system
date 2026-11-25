// Package service implements the notification use cases on top of the repository.
package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/saltindag22/case-notification-system/internal/domain"
	"github.com/saltindag22/case-notification-system/internal/events"
	"github.com/saltindag22/case-notification-system/internal/logging"
	"github.com/saltindag22/case-notification-system/internal/repository"
	"github.com/saltindag22/case-notification-system/internal/templating"
	"github.com/saltindag22/case-notification-system/internal/tracing"
)

// MaxBatchSize is the maximum number of notifications per batch request.
const MaxBatchSize = 1000

// Store is the persistence port used by the service.
type Store interface {
	CreateNotification(ctx context.Context, n *domain.Notification) (*domain.Notification, bool, error)
	CreateBatch(ctx context.Context, b *domain.Batch, items []*domain.Notification) (*repository.BatchResult, error)
	GetNotification(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	ListAttempts(ctx context.Context, id uuid.UUID) ([]domain.Attempt, error)
	GetBatchSummary(ctx context.Context, id uuid.UUID) (*domain.BatchSummary, error)
	ListNotifications(ctx context.Context, f repository.ListFilter) ([]*domain.Notification, bool, error)
	Cancel(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	CancelBatch(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error)
	Requeue(ctx context.Context, id uuid.UUID) (*domain.Notification, error)

	CreateTemplate(ctx context.Context, t *domain.Template) (*domain.Template, error)
	GetTemplate(ctx context.Context, id uuid.UUID) (*domain.Template, error)
	ListTemplates(ctx context.Context) ([]*domain.Template, error)
	UpdateTemplate(ctx context.Context, t *domain.Template) (*domain.Template, error)
	DeleteTemplate(ctx context.Context, id uuid.UUID) error
}

// Service implements the use cases.
type Service struct {
	store       Store
	events      events.Publisher
	maxAttempts int
	now         func() time.Time
}

// New creates a Service.
func New(store Store, pub events.Publisher, maxAttempts int) *Service {
	if pub == nil {
		pub = events.Nop{}
	}
	return &Service{store: store, events: pub, maxAttempts: maxAttempts, now: time.Now}
}

// CreateNotificationInput is the API payload for one notification.
type CreateNotificationInput struct {
	Recipient      string          `json:"recipient"`
	Channel        string          `json:"channel"`
	Content        string          `json:"content,omitempty"`
	Subject        *string         `json:"subject,omitempty"`
	Priority       string          `json:"priority,omitempty"`
	TemplateID     *string         `json:"templateId,omitempty"`
	Variables      map[string]any  `json:"variables,omitempty"`
	ScheduledAt    *time.Time      `json:"scheduledAt,omitempty"`
	IdempotencyKey string          `json:"idempotencyKey,omitempty"`
	Metadata       json.RawMessage `json:"metadata,omitempty"`
}

// CreateBatchInput is the API payload for a batch.
type CreateBatchInput struct {
	Notifications []CreateNotificationInput `json:"notifications"`
}

// templateCache avoids loading the same template once per batch item.
type templateCache map[uuid.UUID]*domain.Template

func (s *Service) resolveTemplate(ctx context.Context, cache templateCache, id uuid.UUID) (*domain.Template, error) {
	if t, ok := cache[id]; ok {
		return t, nil
	}
	t, err := s.store.GetTemplate(ctx, id)
	if err != nil {
		return nil, err
	}
	cache[id] = t
	return t, nil
}

// build validates and normalises one input. Field names in the returned error are
// relative (the caller adds a prefix for batch items).
func (s *Service) build(ctx context.Context, in CreateNotificationInput, cache templateCache) (*domain.Notification, error) {
	v := &domain.ValidationError{}
	now := s.now().UTC()

	n := &domain.Notification{
		ID:            domain.NewID(),
		Recipient:     strings.TrimSpace(in.Recipient),
		Content:       in.Content,
		MaxAttempts:   s.maxAttempts,
		CreatedAt:     now,
		CorrelationID: logging.CorrelationID(ctx),
		TraceParent:   tracing.TraceParent(ctx),
	}
	if in.Subject != nil {
		subj := strings.TrimSpace(*in.Subject)
		n.Subject = &subj
	}

	var tpl *domain.Template
	templateFailed := false
	if in.TemplateID != nil {
		id, err := uuid.Parse(strings.TrimSpace(*in.TemplateID))
		switch {
		case err != nil:
			v.Add("templateId", "must be a UUID")
		case in.Content != "":
			v.Add("content", "must be empty when templateId is used")
		default:
			t, err := s.resolveTemplate(ctx, cache, id)
			if errors.Is(err, domain.ErrNotFound) {
				v.Add("templateId", "template not found")
			} else if err != nil {
				return nil, err
			}
			tpl = t
		}
	} else if len(in.Variables) > 0 {
		v.Add("variables", "are only allowed together with templateId")
	}

	channelInput := in.Channel
	if channelInput == "" && tpl != nil {
		channelInput = string(tpl.Channel)
	}
	ch, err := domain.ParseChannel(channelInput)
	// With an unusable templateId the channel cannot be inferred from the template;
	// the templateId error already explains the problem, so do not add a second one.
	if err != nil && (in.TemplateID == nil || tpl != nil || in.Channel != "") {
		v.Add("channel", "%s", err.Error())
	}
	n.Channel = ch

	if tpl != nil {
		n.TemplateID = &tpl.ID
		if ch != "" && tpl.Channel != ch {
			v.Add("templateId", "template is for channel %q, not %q", tpl.Channel, ch)
		}
		content, err := templating.Render(tpl.Name, tpl.Content, in.Variables)
		if err != nil {
			v.Add("variables", "%s", err.Error())
			templateFailed = true
		}
		n.Content = content
		if n.Subject == nil && tpl.Subject != nil {
			subj, err := templating.Render(tpl.Name+".subject", *tpl.Subject, in.Variables)
			if err != nil {
				v.Add("variables", "%s", err.Error())
				templateFailed = true
			}
			subj = strings.TrimSpace(subj)
			n.Subject = &subj
		}
	}

	prio, err := domain.ParsePriority(in.Priority)
	if err != nil {
		v.Add("priority", "%s", err.Error())
	}
	n.Priority, n.EffectivePriority = prio, prio

	if in.TemplateID != nil && tpl == nil {
		templateFailed = true
	}
	// Content rules only make sense once the final content is known.
	if ch != "" && !templateFailed {
		v.Merge("", domain.ValidateContent(ch, n.Recipient, n.Subject, n.Content))
	}
	v.Merge("", domain.ValidateMetadata(in.Metadata))
	v.Merge("", domain.ValidateIdempotencyKey("idempotencyKey", in.IdempotencyKey))

	if in.ScheduledAt != nil {
		at := in.ScheduledAt.UTC()
		v.Merge("", domain.ValidateSchedule(&at, now))
		n.ScheduledAt = &at
		n.Status = domain.StatusScheduled
	} else {
		n.Status = domain.StatusPending
	}
	if m := strings.TrimSpace(string(in.Metadata)); m != "" && m != "null" {
		n.Metadata = json.RawMessage(m)
	}
	if in.IdempotencyKey != "" {
		key := in.IdempotencyKey
		n.IdempotencyKey = &key
	}
	if err := v.OrNil(); err != nil {
		return nil, err
	}
	n.RequestHash = fingerprint(n, in)
	return n, nil
}

// fingerprint hashes the semantic content of a request, so an idempotency key
// replayed with a different payload can be detected (and rejected) instead of
// silently returning an unrelated notification.
func fingerprint(n *domain.Notification, in CreateNotificationInput) string {
	payload := struct {
		Channel     domain.Channel  `json:"channel"`
		Recipient   string          `json:"recipient"`
		Subject     *string         `json:"subject"`
		Content     string          `json:"content"`
		TemplateID  *uuid.UUID      `json:"templateId"`
		Variables   map[string]any  `json:"variables"`
		Priority    domain.Priority `json:"priority"`
		ScheduledAt *time.Time      `json:"scheduledAt"`
		Metadata    json.RawMessage `json:"metadata"`
	}{n.Channel, n.Recipient, n.Subject, n.Content, n.TemplateID, in.Variables, n.Priority, n.ScheduledAt, compactJSON(n.Metadata)}
	b, _ := json.Marshal(payload) // map keys are sorted by encoding/json: deterministic
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func compactJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return raw
	}
	b, _ := json.Marshal(v)
	return b
}

func mergeKey(header, body string) (string, error) {
	switch {
	case header == "":
		return body, nil
	case body == "" || body == header:
		return header, nil
	default:
		return "", &domain.ValidationError{Fields: []domain.FieldError{{
			Field: "idempotencyKey", Message: "differs from the Idempotency-Key header",
		}}}
	}
}

// Create validates and stores a single notification. created=false means an
// idempotent replay of an earlier request.
func (s *Service) Create(ctx context.Context, in CreateNotificationInput, idempotencyHeader string) (*domain.Notification, bool, error) {
	key, err := mergeKey(strings.TrimSpace(idempotencyHeader), in.IdempotencyKey)
	if err != nil {
		return nil, false, err
	}
	in.IdempotencyKey = key
	n, err := s.build(ctx, in, templateCache{})
	if err != nil {
		return nil, false, err
	}
	stored, created, err := s.store.CreateNotification(ctx, n)
	if err != nil {
		return stored, false, err
	}
	if created {
		s.events.Publish(ctx, events.FromNotification(stored))
	}
	return stored, created, nil
}

// BatchItem is the per-item outcome of a batch request.
type BatchItem struct {
	Index        int                  `json:"index"`
	Duplicate    bool                 `json:"duplicate,omitempty"`
	Notification *domain.Notification `json:"notification"`
}

// BatchOutcome is the result of CreateBatch.
type BatchOutcome struct {
	BatchID  uuid.UUID   `json:"batchId"`
	Total    int         `json:"total"`
	Created  int         `json:"created"`
	Replayed bool        `json:"replayed"`
	Items    []BatchItem `json:"notifications"`
}

// CreateBatch validates all items first and stores them atomically (all or nothing).
func (s *Service) CreateBatch(ctx context.Context, in CreateBatchInput, idempotencyHeader string) (*BatchOutcome, error) {
	v := &domain.ValidationError{}
	switch {
	case len(in.Notifications) == 0:
		v.Add("notifications", "must contain at least one notification")
	case len(in.Notifications) > MaxBatchSize:
		v.Add("notifications", "must contain at most %d notifications (got %d)", MaxBatchSize, len(in.Notifications))
	}
	headerKey := strings.TrimSpace(idempotencyHeader)
	v.Merge("", domain.ValidateIdempotencyKey("Idempotency-Key", headerKey))
	if err := v.OrNil(); err != nil {
		return nil, err
	}

	batch := &domain.Batch{
		ID:            domain.NewID(),
		Total:         len(in.Notifications),
		CorrelationID: logging.CorrelationID(ctx),
		CreatedAt:     s.now().UTC(),
	}
	if headerKey != "" {
		batch.IdempotencyKey = &headerKey
	}

	cache := templateCache{}
	items := make([]*domain.Notification, 0, len(in.Notifications))
	seenKeys := map[string]int{}
	hash := sha256.New()
	for i, item := range in.Notifications {
		prefix := fmt.Sprintf("notifications[%d]", i)
		n, err := s.build(ctx, item, cache)
		var ve *domain.ValidationError
		if errors.As(err, &ve) {
			v.Merge(prefix, ve)
			continue
		}
		if err != nil {
			return nil, err
		}
		if n.IdempotencyKey != nil {
			if j, dup := seenKeys[*n.IdempotencyKey]; dup {
				v.Add(prefix+".idempotencyKey", "duplicates notifications[%d].idempotencyKey", j)
				continue
			}
			seenKeys[*n.IdempotencyKey] = i
		}
		n.BatchID = &batch.ID
		hash.Write([]byte(n.RequestHash))
		items = append(items, n)
	}
	if err := v.OrNil(); err != nil {
		return nil, err
	}
	batch.RequestHash = hex.EncodeToString(hash.Sum(nil))

	res, err := s.store.CreateBatch(ctx, batch, items)
	if err != nil {
		return nil, err
	}
	out := &BatchOutcome{BatchID: res.Batch.ID, Total: res.Batch.Total, Replayed: res.Replayed, Items: make([]BatchItem, len(res.Items))}
	for i, it := range res.Items {
		out.Items[i] = BatchItem{Index: i, Duplicate: !res.Replayed && !it.Created, Notification: it.Notification}
		if it.Created {
			out.Created++
			s.events.Publish(ctx, events.FromNotification(it.Notification))
		}
	}
	return out, nil
}

// Get returns a notification and its attempts.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (*domain.Notification, []domain.Attempt, error) {
	n, err := s.store.GetNotification(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	attempts, err := s.store.ListAttempts(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	return n, attempts, nil
}

// GetBatch returns the batch summary.
func (s *Service) GetBatch(ctx context.Context, id uuid.UUID) (*domain.BatchSummary, error) {
	return s.store.GetBatchSummary(ctx, id)
}

// List returns a page of notifications.
func (s *Service) List(ctx context.Context, f repository.ListFilter) ([]*domain.Notification, bool, error) {
	return s.store.ListNotifications(ctx, f)
}

// Cancel cancels a notification that has not started processing.
func (s *Service) Cancel(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	n, err := s.store.Cancel(ctx, id)
	if err != nil {
		return n, err
	}
	s.events.Publish(ctx, events.FromNotification(n))
	return n, nil
}

// CancelBatch cancels every cancellable notification in a batch.
func (s *Service) CancelBatch(ctx context.Context, id uuid.UUID) (int, error) {
	list, err := s.store.CancelBatch(ctx, id)
	if err != nil {
		return 0, err
	}
	for _, n := range list {
		s.events.Publish(ctx, events.FromNotification(n))
	}
	return len(list), nil
}

// Retry requeues a failed notification with a fresh attempt budget.
func (s *Service) Retry(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	n, err := s.store.Requeue(ctx, id)
	if err != nil {
		return n, err
	}
	s.events.Publish(ctx, events.FromNotification(n))
	return n, nil
}
