package domain

import (
	"bytes"
	"encoding/json"
	"net/mail"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// Content limits. They are deliberately conservative and documented in the README.
const (
	MaxSMSSegments        = 10      // concatenated parts; ~1530 GSM-7 chars or ~670 UCS-2 chars
	MaxEmailSubjectLength = 998     // RFC 5322 line length limit
	MaxEmailContentBytes  = 100_000 // keeps a full batch within the request size limit
	MaxEmailAddressLength = 254     // RFC 5321 path limit
	MaxPushTitleBytes     = 256
	MaxPushContentBytes   = 2048 // leaves room for the envelope within APNs/FCM 4KB payload limit
	MaxRecipientLength    = 4096
	MinDeviceTokenLength  = 16
	MaxMetadataBytes      = 8192
	MaxIdempotencyKeyLen  = 255
	MaxScheduleAhead      = 30 * 24 * time.Hour
)

var (
	e164Pattern        = regexp.MustCompile(`^\+[1-9][0-9]{7,14}$`)
	deviceTokenPattern = regexp.MustCompile(`^[A-Za-z0-9_\-:.]+$`)
)

// ValidateContent applies the channel specific recipient/subject/content rules.
// It expects already trimmed values.
func ValidateContent(ch Channel, recipient string, subject *string, content string) *ValidationError {
	v := &ValidationError{}
	if recipient == "" {
		v.Add("recipient", "is required")
	} else if len(recipient) > MaxRecipientLength {
		v.Add("recipient", "must be at most %d characters", MaxRecipientLength)
	}
	if strings.TrimSpace(content) == "" {
		v.Add("content", "is required")
	}
	if !utf8.ValidString(content) {
		v.Add("content", "must be valid UTF-8")
	}

	switch ch {
	case ChannelSMS:
		if recipient != "" && !e164Pattern.MatchString(recipient) {
			v.Add("recipient", "must be a phone number in E.164 format, e.g. +905551234567")
		}
		if subject != nil && *subject != "" {
			v.Add("subject", "is not supported for sms")
		}
		if content != "" {
			info := AnalyzeSMS(content)
			if info.Segments > MaxSMSSegments {
				v.Add("content", "is too long for sms: %d %s units need %d segments (max %d)",
					info.Units, info.Encoding, info.Segments, MaxSMSSegments)
			}
		}
	case ChannelEmail:
		if recipient != "" {
			if len(recipient) > MaxEmailAddressLength {
				v.Add("recipient", "must be at most %d characters", MaxEmailAddressLength)
			} else if addr, err := mail.ParseAddress(recipient); err != nil || addr.Address != recipient {
				v.Add("recipient", "must be a plain email address, e.g. user@example.com")
			}
		}
		if subject == nil || strings.TrimSpace(*subject) == "" {
			v.Add("subject", "is required for email")
		} else {
			if utf8.RuneCountInString(*subject) > MaxEmailSubjectLength {
				v.Add("subject", "must be at most %d characters", MaxEmailSubjectLength)
			}
			if strings.ContainsAny(*subject, "\r\n") {
				v.Add("subject", "must not contain line breaks")
			}
		}
		if len(content) > MaxEmailContentBytes {
			v.Add("content", "must be at most %d bytes for email", MaxEmailContentBytes)
		}
	case ChannelPush:
		if recipient != "" && (len(recipient) < MinDeviceTokenLength || !deviceTokenPattern.MatchString(recipient)) {
			v.Add("recipient", "must be a device token (16-4096 chars of [A-Za-z0-9_-:.])")
		}
		if subject != nil && len(*subject) > MaxPushTitleBytes {
			v.Add("subject", "must be at most %d bytes for push", MaxPushTitleBytes)
		}
		if len(content) > MaxPushContentBytes {
			v.Add("content", "must be at most %d bytes for push", MaxPushContentBytes)
		}
	default:
		v.Add("channel", "unsupported channel %q", ch)
	}
	return v
}

// ValidateMetadata checks that metadata, when present, is a small JSON object.
func ValidateMetadata(raw json.RawMessage) *ValidationError {
	v := &ValidationError{}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return v
	}
	if len(trimmed) > MaxMetadataBytes {
		v.Add("metadata", "must be at most %d bytes", MaxMetadataBytes)
		return v
	}
	if trimmed[0] != '{' {
		v.Add("metadata", "must be a JSON object")
	}
	return v
}

// ValidateSchedule checks an optional future delivery time.
func ValidateSchedule(at *time.Time, now time.Time) *ValidationError {
	v := &ValidationError{}
	if at == nil {
		return v
	}
	if !at.After(now) {
		v.Add("scheduledAt", "must be in the future")
	} else if at.Sub(now) > MaxScheduleAhead {
		v.Add("scheduledAt", "must be within %d days", int(MaxScheduleAhead.Hours()/24))
	}
	return v
}

// ValidateIdempotencyKey checks length and characters of a client supplied key.
func ValidateIdempotencyKey(field, key string) *ValidationError {
	v := &ValidationError{}
	if key == "" {
		return v
	}
	if len(key) > MaxIdempotencyKeyLen {
		v.Add(field, "must be at most %d characters", MaxIdempotencyKeyLen)
	}
	for _, r := range key {
		if r < 0x21 || r > 0x7e {
			v.Add(field, "must contain only visible ASCII characters")
			break
		}
	}
	return v
}
