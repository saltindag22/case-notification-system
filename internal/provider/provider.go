// Package provider talks to the external notification provider (webhook.site in
// this assessment, a local mock in docker compose).
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/saltindag22/case-notification-system/internal/logging"
	"github.com/saltindag22/case-notification-system/internal/retry"
)

// Message is what we hand to the provider.
type Message struct {
	ID            string // our notification id, sent as idempotency key
	CorrelationID string
	Channel       string
	To            string
	Subject       string
	Content       string
}

// Result of an accepted message.
type Result struct {
	MessageID  string
	Status     string
	StatusCode int
}

// Error describes a failed send.
type Error struct {
	StatusCode int           // 0 for transport errors
	Retryable  bool          // whether trying again can succeed
	RetryAfter time.Duration // provider supplied hint, if any
	Message    string
}

func (e *Error) Error() string {
	if e.StatusCode == 0 {
		return "provider transport error: " + e.Message
	}
	return fmt.Sprintf("provider returned %d: %s", e.StatusCode, e.Message)
}

// Sender sends a message to a provider.
type Sender interface {
	Send(ctx context.Context, msg Message) (Result, error)
}

// URLResolver returns the endpoint for a channel.
type URLResolver func(channel string) string

// Client is an HTTP Sender.
type Client struct {
	http    *http.Client
	resolve URLResolver
}

// NewClient builds a client with sane transport limits and tracing.
func NewClient(resolve URLResolver, timeout time.Duration) *Client {
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	return &Client{
		http:    &http.Client{Timeout: timeout, Transport: otelhttp.NewTransport(transport)},
		resolve: resolve,
	}
}

type requestBody struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
	Subject string `json:"subject,omitempty"`
}

type responseBody struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

const maxResponseBytes = 64 << 10

// Send posts the message. Any 2xx is success; the documented contract is 202.
func (c *Client) Send(ctx context.Context, msg Message) (Result, error) {
	body, err := json.Marshal(requestBody{To: msg.To, Channel: msg.Channel, Content: msg.Content, Subject: msg.Subject})
	if err != nil {
		return Result{}, &Error{Message: err.Error()}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.resolve(msg.Channel), bytes.NewReader(body))
	if err != nil {
		return Result{}, &Error{Message: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Idempotency-Key", msg.ID)
	req.Header.Set("User-Agent", "notification-system/1.0")
	if msg.CorrelationID != "" {
		req.Header.Set(logging.HeaderCorrelationID, msg.CorrelationID)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return Result{}, &Error{Retryable: true, Message: transportMessage(err)}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return Result{StatusCode: resp.StatusCode}, &Error{
			StatusCode: resp.StatusCode,
			Retryable:  retry.IsRetryableStatus(resp.StatusCode),
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
			Message:    snippet(raw),
		}
	}

	var rb responseBody
	_ = json.Unmarshal(raw, &rb) // a 2xx without the documented body is still an acceptance
	return Result{MessageID: rb.MessageID, Status: rb.Status, StatusCode: resp.StatusCode}, nil
}

func transportMessage(err error) string {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout: " + err.Error()
	}
	return err.Error()
}

// parseRetryAfter supports both delta-seconds and HTTP-date forms.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 300 {
		s = s[:300] + "..."
	}
	if s == "" {
		return "empty response body"
	}
	return s
}
