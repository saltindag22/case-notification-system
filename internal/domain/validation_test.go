package domain

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func strPtr(s string) *string { return &s }

func TestAnalyzeSMS(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		encoding SMSEncoding
		units    int
		segments int
	}{
		{"empty", "", EncodingGSM7, 0, 0},
		{"plain ascii", "Hello world", EncodingGSM7, 11, 1},
		{"exactly 160 gsm", strings.Repeat("a", 160), EncodingGSM7, 160, 1},
		{"161 gsm splits", strings.Repeat("a", 161), EncodingGSM7, 161, 2},
		{"306 gsm is two parts", strings.Repeat("a", 306), EncodingGSM7, 306, 2},
		{"307 gsm is three parts", strings.Repeat("a", 307), EncodingGSM7, 307, 3},
		{"extended chars cost two septets", "€{}", EncodingGSM7, 6, 1},
		{"gsm specials", "Ä Ö Ü ä ö ü Ç", EncodingGSM7, 13, 1},
		{"turkish letter forces ucs2", "Şeker", EncodingUCS2, 5, 1},
		{"dotless i forces ucs2", "ılık", EncodingUCS2, 4, 1},
		{"70 ucs2 single", strings.Repeat("ş", 70), EncodingUCS2, 70, 1},
		{"71 ucs2 splits", strings.Repeat("ş", 71), EncodingUCS2, 71, 2},
		{"emoji is two utf16 units", "😀", EncodingUCS2, 2, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := AnalyzeSMS(tc.content)
			if got.Encoding != tc.encoding || got.Units != tc.units || got.Segments != tc.segments {
				t.Fatalf("AnalyzeSMS(%q) = %+v, want {%s %d %d}", tc.content, got, tc.encoding, tc.units, tc.segments)
			}
		})
	}
}

func TestValidateContent(t *testing.T) {
	tests := []struct {
		name      string
		channel   Channel
		recipient string
		subject   *string
		content   string
		wantField string // empty means valid
	}{
		{"valid sms", ChannelSMS, "+905551234567", nil, "Your code is 1234", ""},
		{"sms bad phone", ChannelSMS, "05551234567", nil, "hi", "recipient"},
		{"sms with subject", ChannelSMS, "+905551234567", strPtr("x"), "hi", "subject"},
		{"sms too long", ChannelSMS, "+905551234567", nil, strings.Repeat("a", 153*MaxSMSSegments+1), "content"},
		{"sms max length ok", ChannelSMS, "+905551234567", nil, strings.Repeat("a", 153*MaxSMSSegments), ""},
		{"sms blank content", ChannelSMS, "+905551234567", nil, "   ", "content"},
		{"valid email", ChannelEmail, "user@example.com", strPtr("Welcome"), "<p>Hi</p>", ""},
		{"email missing subject", ChannelEmail, "user@example.com", nil, "hi", "subject"},
		{"email blank subject", ChannelEmail, "user@example.com", strPtr("  "), "hi", "subject"},
		{"email header injection", ChannelEmail, "user@example.com", strPtr("a\r\nBcc: x@y.z"), "hi", "subject"},
		{"email display name rejected", ChannelEmail, "John <user@example.com>", strPtr("s"), "hi", "recipient"},
		{"email invalid", ChannelEmail, "not-an-email", strPtr("s"), "hi", "recipient"},
		{"email too large", ChannelEmail, "user@example.com", strPtr("s"), strings.Repeat("a", MaxEmailContentBytes+1), "content"},
		{"valid push", ChannelPush, "fcm-token_ABCDEFGHIJ:123456", strPtr("Title"), "Body", ""},
		{"push short token", ChannelPush, "abc", nil, "Body", "recipient"},
		{"push too large", ChannelPush, "fcm-token_ABCDEFGHIJ:123456", nil, strings.Repeat("a", MaxPushContentBytes+1), "content"},
		{"missing recipient", ChannelPush, "", nil, "Body", "recipient"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateContent(tc.channel, tc.recipient, tc.subject, tc.content).OrNil()
			if tc.wantField == "" {
				if err != nil {
					t.Fatalf("expected valid, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error on %q, got nil", tc.wantField)
			}
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("expected *ValidationError, got %T", err)
			}
			found := false
			for _, f := range ve.Fields {
				if f.Field == tc.wantField {
					found = true
				}
			}
			if !found {
				t.Fatalf("expected error on field %q, got %v", tc.wantField, ve)
			}
		})
	}
}

func TestValidateSchedule(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Minute)
	future := now.Add(time.Hour)
	tooFar := now.Add(MaxScheduleAhead + time.Hour)

	if ValidateSchedule(nil, now).OrNil() != nil {
		t.Fatal("nil schedule must be valid")
	}
	if ValidateSchedule(&future, now).OrNil() != nil {
		t.Fatal("future schedule must be valid")
	}
	if ValidateSchedule(&past, now).OrNil() == nil {
		t.Fatal("past schedule must be rejected")
	}
	if ValidateSchedule(&tooFar, now).OrNil() == nil {
		t.Fatal("schedule beyond horizon must be rejected")
	}
}

func TestValidateMetadata(t *testing.T) {
	cases := map[string]bool{
		``:                            true,
		`null`:                        true,
		`{"campaign":"black-friday"}`: true,
		`[1,2]`:                       false,
		`"str"`:                       false,
		`{"x":"` + strings.Repeat("a", MaxMetadataBytes) + `"}`: false,
	}
	for in, ok := range cases {
		err := ValidateMetadata(json.RawMessage(in)).OrNil()
		if (err == nil) != ok {
			t.Errorf("ValidateMetadata(%.30q) error=%v, want ok=%v", in, err, ok)
		}
	}
}

func TestValidateIdempotencyKey(t *testing.T) {
	if ValidateIdempotencyKey("k", "order-123:sms").OrNil() != nil {
		t.Fatal("valid key rejected")
	}
	if ValidateIdempotencyKey("k", "has space").OrNil() == nil {
		t.Fatal("key with space accepted")
	}
	if ValidateIdempotencyKey("k", strings.Repeat("a", MaxIdempotencyKeyLen+1)).OrNil() == nil {
		t.Fatal("too long key accepted")
	}
}

func TestParsers(t *testing.T) {
	if p, err := ParsePriority(""); err != nil || p != PriorityNormal {
		t.Fatalf("empty priority should default to normal, got %v %v", p, err)
	}
	if _, err := ParsePriority("urgent"); err == nil {
		t.Fatal("unknown priority accepted")
	}
	if c, err := ParseChannel(" SMS "); err != nil || c != ChannelSMS {
		t.Fatalf("channel parsing should be case/space insensitive, got %v %v", c, err)
	}
	if _, err := ParseStatus("delivered"); err == nil {
		t.Fatal("unknown status accepted")
	}
}

func TestStatusRules(t *testing.T) {
	cancellable := map[Status]bool{
		StatusPending: true, StatusScheduled: true, StatusQueued: true, StatusRetrying: true,
		StatusProcessing: false, StatusSent: false, StatusFailed: false, StatusCancelled: false,
	}
	for s, want := range cancellable {
		if s.IsCancellable() != want {
			t.Errorf("%s.IsCancellable() = %v, want %v", s, !want, want)
		}
	}
	for _, s := range []Status{StatusSent, StatusFailed, StatusCancelled} {
		if !s.IsTerminal() {
			t.Errorf("%s should be terminal", s)
		}
	}
}

func TestPriorityJSON(t *testing.T) {
	var p Priority
	if err := json.Unmarshal([]byte(`"high"`), &p); err != nil || p != PriorityHigh {
		t.Fatalf("unmarshal high: %v %v", p, err)
	}
	b, _ := json.Marshal(PriorityLow)
	if string(b) != `"low"` {
		t.Fatalf("marshal low = %s", b)
	}
	if err := json.Unmarshal([]byte(`3`), &p); err == nil {
		t.Fatal("numeric priority should be rejected")
	}
}
