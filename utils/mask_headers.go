package utils

import (
	"net/http"
	"sort"
	"strings"

	"github.com/donnyhardyanto/dxlib"
)

// ── Credential headers ────────────────────────────────────────────────────────
// A request or response dump written to a log goes through these before its header lines are
// rendered. dxlib declares the headers every HTTP service meets; a host adds its own with
// SetCredentialHeaders. The credential keywords of IsSensitiveField ("token", "auth", "key",
// "cookie", "session", "secret", ...) apply to a header name as well, so a header the list does
// not name is still caught when its name says what it carries. A credential header is never
// dropped from a dump and never partially shown: the line stays, so an operator can see that
// the header was sent, and its value is "***REDACTED***".

// credentialHeaderNames is the declared list, lowercased. The keyword net already catches most
// of these; the list is what documents the intent and what a host extends.
var credentialHeaderNames = map[string]bool{
	"authorization":             true,
	"proxy-authorization":       true,
	"proxy-authenticate":        true,
	"www-authenticate":          true,
	"authentication-info":       true,
	"cookie":                    true,
	"set-cookie":                true,
	"x-api-key":                 true,
	"api-key":                   true,
	"apikey":                    true,
	"x-auth-token":              true,
	"x-access-token":            true,
	"x-refresh-token":           true,
	"x-session-token":           true,
	"x-session-id":              true,
	"x-csrf-token":              true,
	"x-xsrf-token":              true,
	"x-client-secret":           true,
	"x-amz-security-token":      true,
	"x-goog-api-key":            true,
	"ocp-apim-subscription-key": true,
}

var hostCredentialHeaderNames = map[string]bool{}

// SetCredentialHeaders replaces the host's own list of credential headers (called once at
// init). Names are matched case-insensitively. The built-in list and the credential keywords
// stay in force; this only adds.
func SetCredentialHeaders(names []string) {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[strings.ToLower(strings.TrimSpace(n))] = true
	}
	hostCredentialHeaderNames = m
}

// IsCredentialHeader reports whether a header carries a credential: it is in the built-in
// list, in the host's list, or its name holds one of the credential keywords.
func IsCredentialHeader(name string) bool {
	lower := strings.ToLower(name)
	return credentialHeaderNames[lower] || hostCredentialHeaderNames[lower] || IsSensitiveField(lower)
}

// MaskHeaderValue returns the value a dump may show for a header: "***REDACTED***" for a credential
// header, the value itself otherwise. The debug override (IsDebug with OverrideShowPasswordOnLog)
// shows every header raw, as it does for a body.
func MaskHeaderValue(name, value string) string {
	if !IsCredentialHeader(name) {
		return value
	}
	if dxlib.IsDebug && OverrideShowPasswordOnLog {
		return value
	}
	return maskRedacted
}

// WriteHeadersForLog renders h as "Name: value" lines, one per value, in name order, with
// every credential value masked and any line break inside a value turned into a space so a
// value cannot forge a line of its own. Names in exclude are left out (a dump writes Host and
// the framing headers itself). It returns the text, ready to append to a dump.
func WriteHeadersForLog(h http.Header, exclude map[string]bool) string {
	keys := make([]string, 0, len(h))
	for k := range h {
		if exclude[k] {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		for _, v := range h[k] {
			b.WriteString(k)
			b.WriteString(": ")
			b.WriteString(headerLineBreaks.Replace(MaskHeaderValue(k, v)))
			b.WriteString("\r\n")
		}
	}
	return b.String()
}

var headerLineBreaks = strings.NewReplacer("\r\n", " ", "\r", " ", "\n", " ")
