package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/saltindag22/case-notification-system/internal/domain"
	"github.com/saltindag22/case-notification-system/internal/repository"
	"github.com/saltindag22/case-notification-system/internal/service"
)

var errBadRequest = errors.New("bad request")

const (
	maxSingleBody     = 1 << 20  // 1 MiB
	maxBatchBody      = 32 << 20 // 32 MiB: 1000 items incl. large e-mails
	defaultPageSize   = 50
	maxPageSize       = 200
	headerIdempotency = "Idempotency-Key"
	headerReplayed    = "Idempotent-Replayed"
)

// requestError is a malformed request; its message is shown to the client as is.
type requestError struct{ msg string }

func (e *requestError) Error() string { return e.msg }

// Is lets errors.Is(err, errBadRequest) match.
func (e *requestError) Is(target error) bool { return target == errBadRequest }

func badRequest(format string, args ...any) error {
	return &requestError{msg: fmt.Sprintf(format, args...)}
}

// decode reads exactly one JSON object, rejecting unknown fields.
func decode(w http.ResponseWriter, r *http.Request, limit int64, dst any) error {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(strings.ToLower(ct), "application/json") {
		return badRequest("Content-Type must be application/json")
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var mbe *http.MaxBytesError
		var ute *json.UnmarshalTypeError
		switch {
		case errors.As(err, &mbe):
			return mbe
		case errors.Is(err, io.EOF):
			return badRequest("request body is empty")
		case errors.As(err, &ute):
			return badRequest("field %q must be of type %s", ute.Field, ute.Type)
		default:
			return badRequest("invalid JSON: %s", strings.TrimPrefix(err.Error(), "json: "))
		}
	}
	if dec.More() {
		return badRequest("request body must contain a single JSON object")
	}
	return nil
}

func pathID(r *http.Request) (uuid.UUID, error) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		return uuid.Nil, badRequest("id must be a UUID")
	}
	return id, nil
}

// --- notifications ---

func (s *Server) createNotification(w http.ResponseWriter, r *http.Request) {
	var in service.CreateNotificationInput
	if err := decode(w, r, maxSingleBody, &in); err != nil {
		s.writeError(w, r, err)
		return
	}
	n, created, err := s.svc.Create(r.Context(), in, r.Header.Get(headerIdempotency))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/api/v1/notifications/"+n.ID.String())
	if !created {
		w.Header().Set(headerReplayed, "true")
		writeJSON(w, http.StatusOK, n)
		return
	}
	if s.metrics != nil {
		s.metrics.NotificationsCreated.WithLabelValues(string(n.Channel), n.Priority.String()).Inc()
	}
	writeJSON(w, http.StatusAccepted, n)
}

func (s *Server) createBatch(w http.ResponseWriter, r *http.Request) {
	var in service.CreateBatchInput
	if err := decode(w, r, maxBatchBody, &in); err != nil {
		s.writeError(w, r, err)
		return
	}
	out, err := s.svc.CreateBatch(r.Context(), in, r.Header.Get(headerIdempotency))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/api/v1/batches/"+out.BatchID.String())
	if out.Replayed {
		w.Header().Set(headerReplayed, "true")
		writeJSON(w, http.StatusOK, out)
		return
	}
	if s.metrics != nil {
		for _, it := range out.Items {
			if !it.Duplicate {
				s.metrics.NotificationsCreated.WithLabelValues(string(it.Notification.Channel), it.Notification.Priority.String()).Inc()
			}
		}
	}
	writeJSON(w, http.StatusAccepted, out)
}

type notificationWithAttempts struct {
	*domain.Notification
	Attempts []domain.Attempt `json:"attemptHistory"`
}

func (s *Server) getNotification(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	n, attempts, err := s.svc.Get(r.Context(), id)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, notificationWithAttempts{Notification: n, Attempts: attempts})
}

func (s *Server) cancelNotification(w http.ResponseWriter, r *http.Request) {
	s.transition(w, r, s.svc.Cancel)
}

func (s *Server) retryNotification(w http.ResponseWriter, r *http.Request) {
	s.transition(w, r, s.svc.Retry)
}

func (s *Server) transition(w http.ResponseWriter, r *http.Request, fn func(context.Context, uuid.UUID) (*domain.Notification, error)) {
	id, err := pathID(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	n, err := fn(r.Context(), id)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, n)
}

// ListResponse is a page of notifications.
type ListResponse struct {
	Data       []*domain.Notification `json:"data"`
	Pagination Pagination             `json:"pagination"`
}

// Pagination describes the cursor position.
type Pagination struct {
	Limit      int    `json:"limit"`
	HasMore    bool   `json:"hasMore"`
	NextCursor string `json:"nextCursor,omitempty"`
}

func encodeCursor(id uuid.UUID) string { return base64.RawURLEncoding.EncodeToString(id[:]) }

func decodeCursor(c string) (uuid.UUID, error) {
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil || len(b) != 16 {
		return uuid.Nil, badRequest("cursor is invalid")
	}
	return uuid.FromBytes(b)
}

func parseListFilter(r *http.Request) (repository.ListFilter, error) {
	q := r.URL.Query()
	f := repository.ListFilter{Limit: defaultPageSize}
	v := &domain.ValidationError{}
	if s := q.Get("status"); s != "" {
		st, err := domain.ParseStatus(s)
		if err != nil {
			v.Add("status", "%s", err.Error())
		}
		f.Status = &st
	}
	if c := q.Get("channel"); c != "" {
		ch, err := domain.ParseChannel(c)
		if err != nil {
			v.Add("channel", "%s", err.Error())
		}
		f.Channel = &ch
	}
	if b := q.Get("batchId"); b != "" {
		id, err := uuid.Parse(b)
		if err != nil {
			v.Add("batchId", "must be a UUID")
		}
		f.BatchID = &id
	}
	parseTime := func(name string) *time.Time {
		raw := q.Get(name)
		if raw == "" {
			return nil
		}
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			v.Add(name, "must be an RFC 3339 timestamp, e.g. 2026-01-02T15:04:05Z")
			return nil
		}
		return &t
	}
	f.From, f.To = parseTime("from"), parseTime("to")
	if f.From != nil && f.To != nil && !f.From.Before(*f.To) {
		v.Add("to", "must be after from")
	}
	if l := q.Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 || n > maxPageSize {
			v.Add("limit", "must be an integer between 1 and %d", maxPageSize)
		} else {
			f.Limit = n
		}
	}
	if c := q.Get("cursor"); c != "" {
		id, err := decodeCursor(c)
		if err != nil {
			v.Add("cursor", "is invalid")
		}
		f.Cursor = &id
	}
	return f, v.OrNil()
}

func (s *Server) listNotifications(w http.ResponseWriter, r *http.Request) {
	f, err := parseListFilter(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.writeList(w, r, f)
}

func (s *Server) listBatchNotifications(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	f, err := parseListFilter(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	if _, err := s.svc.GetBatch(r.Context(), id); err != nil {
		s.writeError(w, r, err)
		return
	}
	f.BatchID = &id
	s.writeList(w, r, f)
}

func (s *Server) writeList(w http.ResponseWriter, r *http.Request, f repository.ListFilter) {
	items, hasMore, err := s.svc.List(r.Context(), f)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	resp := ListResponse{Data: items, Pagination: Pagination{Limit: f.Limit, HasMore: hasMore}}
	if resp.Data == nil {
		resp.Data = []*domain.Notification{}
	}
	if hasMore && len(items) > 0 {
		resp.Pagination.NextCursor = encodeCursor(items[len(items)-1].ID)
	}
	writeJSON(w, http.StatusOK, resp)
}

// --- batches ---

func (s *Server) getBatch(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	sum, err := s.svc.GetBatch(r.Context(), id)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, sum)
}

func (s *Server) cancelBatch(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	n, err := s.svc.CancelBatch(r.Context(), id)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"batchId": id, "cancelled": n})
}

// --- templates ---

func (s *Server) createTemplate(w http.ResponseWriter, r *http.Request) {
	var in service.TemplateInput
	if err := decode(w, r, maxSingleBody, &in); err != nil {
		s.writeError(w, r, err)
		return
	}
	t, err := s.svc.CreateTemplate(r.Context(), in)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/api/v1/templates/"+t.ID.String())
	writeJSON(w, http.StatusCreated, t)
}

func (s *Server) updateTemplate(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	var in service.TemplateInput
	if err := decode(w, r, maxSingleBody, &in); err != nil {
		s.writeError(w, r, err)
		return
	}
	t, err := s.svc.UpdateTemplate(r.Context(), id, in)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) getTemplate(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	t, err := s.svc.GetTemplate(r.Context(), id)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) listTemplates(w http.ResponseWriter, r *http.Request) {
	list, err := s.svc.ListTemplates(r.Context())
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": list})
}

func (s *Server) deleteTemplate(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	if err := s.svc.DeleteTemplate(r.Context(), id); err != nil {
		s.writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
