package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime"
	"net/url"
	"sort"
	"strings"

	"github.com/donnyhardyanto/dxlib/utils"
)

// maskedQueryForLog returns a request URI with every query value masked by its
// key, the way a body field is (utils.MaskSensitiveValue): a token, key or
// password in the query never reaches a log. A URI without a query is returned
// as it is; one whose query cannot be parsed keeps its path and drops the
// query, saying so.
func maskedQueryForLog(requestURI string) string {
	path, rawQuery, found := strings.Cut(requestURI, "?")
	if !found || rawQuery == "" {
		return requestURI
	}
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return fmt.Sprintf("%s?(query not dumped, it does not parse: %v)", path, err)
	}
	return path + "?" + maskedFormForLog(values)
}

// maskedFormForLog renders form or query values in key order, each value
// masked by its key.
func maskedFormForLog(values url.Values) string {
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		for _, v := range values[k] {
			if b.Len() > 0 {
				b.WriteByte('&')
			}
			b.WriteString(url.QueryEscape(k))
			b.WriteByte('=')
			b.WriteString(url.QueryEscape(fmt.Sprint(utils.MaskSensitiveValue(k, v))))
		}
	}
	return b.String()
}

// maskedBodyForLog renders a request body for a log with its secrets masked,
// cut to maxBytes. A JSON body goes through utils.MaskForLog at every depth; a
// form body is masked value by value under its keys. Any other body (a file,
// multipart, plain text, JSON that does not parse) cannot be masked field by
// field, so only its size, its Content-Type and, for broken JSON, the parse
// error are written.
func maskedBodyForLog(contentType string, body []byte, maxBytes int) string {
	if len(bytes.TrimSpace(body)) == 0 {
		return ""
	}
	mediaType, _, _ := mime.ParseMediaType(contentType)
	var out string
	switch {
	case mediaType == "application/x-www-form-urlencoded":
		values, err := url.ParseQuery(string(body))
		if err != nil {
			return fmt.Sprintf("(body not dumped: %d bytes of %s that do not parse: %v)", len(body), mediaType, err)
		}
		out = maskedFormForLog(values)
	case mediaType == "" || mediaType == "application/json" || strings.HasSuffix(mediaType, "+json"):
		var parsed any
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.UseNumber()
		if err := decoder.Decode(&parsed); err != nil {
			return fmt.Sprintf("(body not dumped: %d bytes (%s) are not valid JSON: %v; they cannot be masked)", len(body), describeMediaType(mediaType), err)
		}
		var masked any
		switch v := parsed.(type) {
		case map[string]any:
			masked = utils.MaskForLog(v)
		case []any:
			masked = utils.MaskForLog(utils.JSON{"body": v})["body"]
		default:
			masked = v
		}
		b, err := json.Marshal(masked)
		if err != nil {
			return fmt.Sprintf("(body not dumped: masking %d bytes of JSON failed: %v)", len(body), err)
		}
		out = string(b)
	default:
		return fmt.Sprintf("(body not dumped: %d bytes of %s; only JSON and form bodies are dumped, masked)", len(body), mediaType)
	}
	if len(out) > maxBytes {
		return fmt.Sprintf("%s...(truncated, masked body %d bytes, request body %d bytes)", out[:maxBytes], len(out), len(body))
	}
	return out
}

func describeMediaType(mediaType string) string {
	if mediaType == "" {
		return "no Content-Type"
	}
	return mediaType
}

// maskName is the field name a parameter's value is masked under: its own
// name, or for an array element the name of the nearest parameter that has
// one.
func (aeprpv *DXAPIEndPointRequestParameterValue) maskName() string {
	for p := aeprpv; p != nil; p = p.Parent {
		if p.Metadata.NameId != "" {
			return p.Metadata.NameId
		}
	}
	return ""
}

// maskedForMessage masks a parameter's value for an error message, which goes
// to the log and back to the caller: a password, token or key is written as
// ***REDACTED***, at any depth of an object or array, and so is a field the
// host's mask rules cover. Any other value is written as it was sent, so the
// message still shows what was wrong with it.
func (aeprpv *DXAPIEndPointRequestParameterValue) maskedForMessage(v any) any {
	name := aeprpv.maskName()
	return utils.MaskForLog(utils.JSON{name: v})[name]
}
