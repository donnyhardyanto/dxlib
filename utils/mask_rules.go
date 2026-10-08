package utils

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"unicode"
)

// The standard mask rules for a log, one function per rule. Each is usable on its own for a
// hand-written log field, and the MaskRule values below route MaskForLog and MaskSensitiveValue
// through the same functions, so one value masks the same way wherever it is logged.
//
// Every partial mask writes a fixed "***" for what it hides, never one asterisk per character,
// so the mask does not give away the length of the value.

// MaskRedactedMarker is what a log shows in place of a secret: a credential field or header, a
// token, a password, a PIN, an OTP or an image.
const MaskRedactedMarker = "***REDACTED***"

const (
	maskHidden   = "***"
	maskFull     = "********"
	maskRedacted = MaskRedactedMarker
)

// Named rules for SetMaskRules. RuleMask<front>by<back> keeps that many characters at each end;
// the number rules drop spaces and dashes first. MaskStrict collapses every rule to RuleMaskFull.
var (
	// RuleMaskFull hides the whole value: "********".
	RuleMaskFull = MaskRule{}
	// RuleMask2by2 keeps the first 2 and the last 2 of the whole value: "Pegawai" → "Pe***ai".
	RuleMask2by2 = MaskRule{Front: 2, Back: 2}
	// RuleMaskNumber2by4 is an account number: "1234567890" → "12***7890".
	RuleMaskNumber2by4 = MaskRule{Kind: MaskKindNumber, Front: 2, Back: 4}
	// RuleMaskNumber5by4 is a national ID or family card number: "3201012345671234" →
	// "32010***1234".
	RuleMaskNumber5by4 = MaskRule{Kind: MaskKindNumber, Front: 5, Back: 4}
	// RuleMaskNumber4by3 is a phone number: "081234567789" → "0812***789".
	RuleMaskNumber4by3 = MaskRule{Kind: MaskKindNumber, Front: 4, Back: 3}
	// RuleMaskNumber6by4 is a card number, the PCI DSS display limit: "4111111111111234" →
	// "411111***1234".
	RuleMaskNumber6by4 = MaskRule{Kind: MaskKindNumber, Front: 6, Back: 4}
	// RuleMaskEmail2by2 is an e-mail address (MaskEmail2by2).
	RuleMaskEmail2by2 = MaskRule{Kind: MaskKindEmail}
	// RuleMaskInitials is a name (MaskInitials).
	RuleMaskInitials = MaskRule{Kind: MaskKindInitials}
	// RuleMaskLocation2Decimals is a coordinate (MaskLocation2Decimals).
	RuleMaskLocation2Decimals = MaskRule{Kind: MaskKindLocation}
	// RuleMaskRedacted is a secret or an image: "***REDACTED***".
	RuleMaskRedacted = MaskRule{Kind: MaskKindRedacted}
)

// MaskFull hides the whole value: "********".
func MaskFull(string) string { return maskFull }

// MaskRedacted is the marker for a secret, a token or an image: "***REDACTED***".
func MaskRedacted(string) string { return maskRedacted }

// MaskFrontBack keeps the first front and the last back runes of s with a fixed "***" between
// them. A value no longer than front+back runes keeps only its first rune: "abc" with 2 and 2
// is "a***". An empty value is "***".
func MaskFrontBack(s string, front, back int) string {
	runes := []rune(s)
	front, back = max(front, 0), max(back, 0)
	if len(runes) <= front+back {
		return firstRunes(s, 1) + maskHidden
	}
	return string(runes[:front]) + maskHidden + string(runes[len(runes)-back:])
}

// Mask2by2 keeps the first 2 and the last 2 runes of the whole value.
func Mask2by2(s string) string { return MaskFrontBack(s, 2, 2) }

// MaskNumber is MaskFrontBack on a number after its spaces and dashes are dropped, so the kept
// characters are digits and the grouping, which would show the length, is gone:
// "0812-3456-7789" with 4 and 3 is "0812***789".
func MaskNumber(s string, front, back int) string {
	return MaskFrontBack(stripNumberSeparators(s), front, back)
}

// MaskNumber2by4 is an account number: the first 2 and the last 4 digits.
func MaskNumber2by4(s string) string { return MaskNumber(s, 2, 4) }

// MaskNumber5by4 is a national ID or family card number: the first 5 digits (the region of
// registration) and the last 4.
func MaskNumber5by4(s string) string { return MaskNumber(s, 5, 4) }

// MaskNumber4by3 is a phone number: the first 4 (the operator prefix) and the last 3.
func MaskNumber4by3(s string) string { return MaskNumber(s, 4, 3) }

// MaskNumber6by4 is a card number: the first 6 (the issuer) and the last 4.
func MaskNumber6by4(s string) string { return MaskNumber(s, 6, 4) }

func stripNumberSeparators(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '-' || unicode.IsSpace(r) {
			return -1
		}
		return r
	}, s)
}

// MaskEmail2by2 masks an e-mail address. The local part keeps its first 2 and last 2 runes
// with a fixed "***" between, or only its first rune when it has 4 or fewer. The domain is
// split into a known public ending (MaskEmailKnownEndings, the longest match, kept as written)
// and the rest before it, sub-domains included, which keeps its first 2 runes then "***", or
// only its first rune when it has 2 or fewer. A domain with no known ending keeps its first 2
// runes then "***". A value with no "@" is "***".
//
//	adinda@example.co.id      → ad***da@ex***.co.id
//	x@mail.example.co.id      → x***@ma***.co.id
//	adinda@example.xyz        → ad***da@ex***
func MaskEmail2by2(s string) string {
	at := strings.LastIndex(s, "@")
	if at < 0 {
		return maskHidden
	}
	local, domain := s[:at], s[at+1:]

	var maskedLocal string
	if len([]rune(local)) <= 4 {
		maskedLocal = firstRunes(local, 1) + maskHidden
	} else {
		maskedLocal = MaskFrontBack(local, 2, 2)
	}

	ending := knownEmailEnding(domain)
	if ending == "" {
		return maskedLocal + "@" + firstRunes(domain, 2) + maskHidden
	}
	rest := domain[:len(domain)-len(ending)]
	keep := 2
	if len([]rune(rest)) <= 2 {
		keep = 1
	}
	return maskedLocal + "@" + firstRunes(rest, keep) + maskHidden + domain[len(domain)-len(ending):]
}

// knownEmailEnding returns the longest entry of MaskEmailKnownEndings that domain ends with,
// compared without case, or "" when none does.
func knownEmailEnding(domain string) string {
	lower := strings.ToLower(domain)
	best := ""
	for _, e := range MaskEmailKnownEndings {
		e = strings.ToLower(e)
		if len(e) > len(best) && strings.HasSuffix(lower, e) {
			best = e
		}
	}
	return best
}

// MaskInitials is a name: the first rune of each word followed by a fixed "***", "Budi Santoso"
// → "B*** S***". The rest of the word, however long, is three asterisks, so the mask gives
// away neither the word nor its length. An empty or all-whitespace value is fully masked.
func MaskInitials(s string) string {
	words := strings.Fields(s)
	if len(words) == 0 {
		return maskFull
	}
	initials := make([]string, len(words))
	for i, w := range words {
		initials[i] = firstRunes(w, 1) + "***"
	}
	return strings.Join(initials, " ")
}

// MaskLocation2Decimals is a coordinate rounded to two decimals (about 1 km). A number stays a
// number (as a float64); a string is read as comma-separated coordinates and rounded term by
// term, keeping the string form. Anything that is not a coordinate is fully masked.
func MaskLocation2Decimals(value any) any {
	switch v := value.(type) {
	case float64:
		return roundCoordinate(v)
	case float32:
		return roundCoordinate(float64(v))
	case int:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return maskFull
		}
		return roundCoordinate(f)
	case string:
		parts := strings.Split(v, ",")
		for i, p := range parts {
			f, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
			if err != nil {
				return maskFull
			}
			parts[i] = strconv.FormatFloat(roundCoordinate(f), 'f', 2, 64)
		}
		return strings.Join(parts, ",")
	default:
		return maskFull
	}
}

func roundCoordinate(f float64) float64 {
	return math.Round(f*100) / 100
}
