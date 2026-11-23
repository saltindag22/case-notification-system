package domain

import "unicode/utf16"

// SMS encoding limits (3GPP TS 23.038 / 23.040).
const (
	gsm7SingleLimit  = 160 // septets in a single part message
	gsm7SegmentLimit = 153 // septets per part when concatenated (7 septets of UDH)
	ucs2SingleLimit  = 70  // UTF-16 code units in a single part message
	ucs2SegmentLimit = 67  // UTF-16 code units per part when concatenated
)

// SMSEncoding is the character encoding an SMS will be sent with.
type SMSEncoding string

// SMS encodings.
const (
	EncodingGSM7 SMSEncoding = "GSM-7"
	EncodingUCS2 SMSEncoding = "UCS-2"
)

// gsm7Basic is the GSM 03.38 default alphabet (without the escape character).
// Note that most Turkish specific letters (ş, ğ, ı, İ, ç) are NOT part of it,
// so a single such letter switches the whole message to UCS-2.
const gsm7Basic = "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?" +
	"¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà"

// gsm7Extended characters are sent as ESC + char and therefore cost two septets.
const gsm7Extended = "^{}\\[~]|€\f"

var (
	gsm7BasicSet    = runeSet(gsm7Basic)
	gsm7ExtendedSet = runeSet(gsm7Extended)
)

func runeSet(s string) map[rune]struct{} {
	m := make(map[rune]struct{}, len(s))
	for _, r := range s {
		m[r] = struct{}{}
	}
	return m
}

// SMSInfo describes how a message body will be encoded and split.
type SMSInfo struct {
	Encoding SMSEncoding `json:"encoding"`
	Units    int         `json:"units"` // septets (GSM-7) or UTF-16 code units (UCS-2)
	Segments int         `json:"segments"`
}

// AnalyzeSMS determines encoding, length in encoding units and part count.
func AnalyzeSMS(content string) SMSInfo {
	septets, ok := gsm7Length(content)
	if ok {
		return SMSInfo{Encoding: EncodingGSM7, Units: septets, Segments: segments(septets, gsm7SingleLimit, gsm7SegmentLimit)}
	}
	units := len(utf16.Encode([]rune(content)))
	return SMSInfo{Encoding: EncodingUCS2, Units: units, Segments: segments(units, ucs2SingleLimit, ucs2SegmentLimit)}
}

func gsm7Length(s string) (int, bool) {
	n := 0
	for _, r := range s {
		if _, ok := gsm7BasicSet[r]; ok {
			n++
			continue
		}
		if _, ok := gsm7ExtendedSet[r]; ok {
			n += 2
			continue
		}
		return 0, false
	}
	return n, true
}

func segments(units, single, perSegment int) int {
	if units == 0 {
		return 0
	}
	if units <= single {
		return 1
	}
	return (units + perSegment - 1) / perSegment
}
