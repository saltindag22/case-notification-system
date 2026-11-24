package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newTestClient(t *testing.T, h http.HandlerFunc, timeout time.Duration) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return NewClient(func(string) string { return srv.URL }, timeout)
}

func TestSendAccepted(t *testing.T) {
	var got map[string]string
	var headers http.Header
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		headers = r.Header.Clone()
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"m-1","status":"accepted","timestamp":"2026-01-01T00:00:00Z"}`))
	}, time.Second)

	res, err := c.Send(context.Background(), Message{ID: "n-1", CorrelationID: "c-1", Channel: "sms", To: "+905551234567", Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if res.MessageID != "m-1" || res.StatusCode != 202 {
		t.Fatalf("unexpected result %+v", res)
	}
	if got["to"] != "+905551234567" || got["channel"] != "sms" || got["content"] != "hi" {
		t.Fatalf("unexpected body %v", got)
	}
	if _, ok := got["subject"]; ok {
		t.Fatal("empty subject must be omitted")
	}
	if headers.Get("Idempotency-Key") != "n-1" || headers.Get("X-Correlation-ID") != "c-1" {
		t.Fatalf("missing propagation headers: %v", headers)
	}
}

func TestSendClassifiesErrors(t *testing.T) {
	tests := []struct {
		status     int
		retryAfter string
		retryable  bool
		wantAfter  time.Duration
	}{
		{500, "", true, 0},
		{503, "7", true, 7 * time.Second},
		{429, "2", true, 2 * time.Second},
		{400, "", false, 0},
		{401, "", false, 0},
		{422, "", false, 0},
	}
	for _, tc := range tests {
		c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if tc.retryAfter != "" {
				w.Header().Set("Retry-After", tc.retryAfter)
			}
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(`{"error":"nope"}`))
		}, time.Second)
		_, err := c.Send(context.Background(), Message{ID: "x", Channel: "sms"})
		var pe *Error
		if !errors.As(err, &pe) {
			t.Fatalf("%d: expected *Error, got %v", tc.status, err)
		}
		if pe.StatusCode != tc.status || pe.Retryable != tc.retryable || pe.RetryAfter != tc.wantAfter {
			t.Errorf("%d: got %+v", tc.status, pe)
		}
	}
}

func TestSendTimeoutIsRetryable(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusAccepted)
	}, 50*time.Millisecond)
	_, err := c.Send(context.Background(), Message{ID: "x", Channel: "sms"})
	var pe *Error
	if !errors.As(err, &pe) || !pe.Retryable || pe.StatusCode != 0 {
		t.Fatalf("timeout must be a retryable transport error, got %v", err)
	}
}

func TestSendAcceptsNonJSONSuccess(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}, time.Second)
	res, err := c.Send(context.Background(), Message{ID: "x", Channel: "email"})
	if err != nil || res.StatusCode != 200 {
		t.Fatalf("2xx must be success, got %+v %v", res, err)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if d := parseRetryAfter("120", now); d != 2*time.Minute {
		t.Fatalf("seconds form: %v", d)
	}
	if d := parseRetryAfter(now.Add(30*time.Second).Format(http.TimeFormat), now); d != 30*time.Second {
		t.Fatalf("date form: %v", d)
	}
	for _, v := range []string{"", "-5", "garbage"} {
		if d := parseRetryAfter(v, now); d != 0 {
			t.Fatalf("%q should be 0, got %v", v, d)
		}
	}
}
