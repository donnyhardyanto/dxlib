package tls

// OpenSSL and nginx/haproxy spelling for the deny keys.
//
// A bank's cipher policy does not arrive in IANA notation. It arrives as an
// nginx ssl_ciphers line, a haproxy ssl-default-bind-ciphers line, or the
// output of `openssl ciphers -v`, and all three speak OpenSSL names:
// ECDHE-RSA-AES256-GCM-SHA384, not TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384.
// Making an operator translate by hand is friction in the one situation --
// an advisory, at speed, fleet-wide -- where a mistyped suite name is either
// a service that will not start or a service that quietly fails to protect.
// So both spellings resolve to the same suite, and the startup log always
// reports the IANA name, so there is never doubt about what was understood.
//
// The grammar is a different question and gets the opposite answer. An
// OpenSSL cipher string is a small language: ! excludes permanently, - excludes
// but allows a later rule to add the suite back, + reorders rather than adds,
// @STRENGTH sorts, and the aliases compose. Almost nobody remembers those
// rules correctly -- Mozilla publishes a configuration generator largely
// because of it -- and accepting half a dialect is worse than accepting none:
// an operator would paste HIGH:!aNULL:!MD5:@STRENGTH, we would honour the part
// we understood, and they would believe a policy was in force that was not.
// So a token carrying grammar is refused with a message naming what to do
// instead.
//
// The one exception is a bare leading "!". An nginx-trained operator writes it
// by reflex, and in a list that only ever subtracts the intent is unambiguous:
// "!3DES" in deny-cipher-suites can only mean deny 3DES. It is stripped and
// the redundancy logged. That tolerance does not extend to any other grammar
// character.

import (
	"crypto/tls"
	"sort"
	"strings"
)

// opensslSuiteNames maps the OpenSSL spelling of every suite crypto/tls has a
// name for onto the IANA name Go reports. The table is written out rather than
// derived because the transformation is not regular: TLS_RSA_WITH_3DES_EDE_CBC_SHA
// is DES-CBC3-SHA, TLS_RSA_WITH_AES_128_CBC_SHA drops the mode entirely to
// AES128-SHA, the ChaCha suites drop the hash, and the TLS 1.3 suites keep
// their IANA names unchanged. TestEveryGoSuiteHasAnOpenSSLName asserts the
// table covers everything Go reports, so a Go upgrade that adds a suite fails
// the build rather than silently leaving a name unresolvable.
var opensslSuiteNames = map[string]string{
	// TLS 1.3 -- OpenSSL uses the IANA names for these.
	"TLS_AES_128_GCM_SHA256":       "TLS_AES_128_GCM_SHA256",
	"TLS_AES_256_GCM_SHA384":       "TLS_AES_256_GCM_SHA384",
	"TLS_CHACHA20_POLY1305_SHA256": "TLS_CHACHA20_POLY1305_SHA256",

	// ECDHE, AEAD.
	"ECDHE-ECDSA-AES128-GCM-SHA256": "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
	"ECDHE-ECDSA-AES256-GCM-SHA384": "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
	"ECDHE-RSA-AES128-GCM-SHA256":   "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
	"ECDHE-RSA-AES256-GCM-SHA384":   "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
	"ECDHE-ECDSA-CHACHA20-POLY1305": "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
	"ECDHE-RSA-CHACHA20-POLY1305":   "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",

	// ECDHE, CBC.
	"ECDHE-ECDSA-AES128-SHA":    "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
	"ECDHE-ECDSA-AES256-SHA":    "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
	"ECDHE-RSA-AES128-SHA":      "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
	"ECDHE-RSA-AES256-SHA":      "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
	"ECDHE-ECDSA-AES128-SHA256": "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
	"ECDHE-RSA-AES128-SHA256":   "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",

	// ECDHE, legacy.
	"ECDHE-ECDSA-RC4-SHA":    "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA",
	"ECDHE-RSA-RC4-SHA":      "TLS_ECDHE_RSA_WITH_RC4_128_SHA",
	"ECDHE-RSA-DES-CBC3-SHA": "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA",

	// RSA key transport.
	"AES128-GCM-SHA256": "TLS_RSA_WITH_AES_128_GCM_SHA256",
	"AES256-GCM-SHA384": "TLS_RSA_WITH_AES_256_GCM_SHA384",
	"AES128-SHA":        "TLS_RSA_WITH_AES_128_CBC_SHA",
	"AES256-SHA":        "TLS_RSA_WITH_AES_256_CBC_SHA",
	"AES128-SHA256":     "TLS_RSA_WITH_AES_128_CBC_SHA256",
	"DES-CBC3-SHA":      "TLS_RSA_WITH_3DES_EDE_CBC_SHA",
	"RC4-SHA":           "TLS_RSA_WITH_RC4_128_SHA",
}

// opensslFamilyAliases are OpenSSL cipher-list keywords that mean exactly what
// one of our family tokens means. Only exact equivalences are here. kRSA is
// RSA *key exchange*, which is precisely the TLS_RSA_WITH_* set; OpenSSL's SHA
// alias is the SHA-1 HMAC suites. Anything whose meaning only overlaps a token
// is in opensslAmbiguousAliases below instead, because a deny that covers a
// different set than the operator intended is the failure this file exists to
// avoid.
var opensslFamilyAliases = map[string]string{
	"KRSA": "RSA-KEY-TRANSPORT",
	"SHA":  "SHA1",
}

// opensslAmbiguousAliases name real things this stack implements, but not a set
// any single token reproduces. Refused with the reason, rather than mapped to
// something close: aRSA is RSA *authentication*, so it covers ECDHE-RSA as well
// as RSA key transport; HIGH and MEDIUM are strength buckets whose membership
// has shifted across OpenSSL releases; AESGCM and AESCBC name a mode across key
// exchanges. An operator who means one of these should name the suites.
var opensslAmbiguousAliases = map[string]string{
	"ARSA":   "RSA_AUTHENTICATION_INCLUDES_ECDHE_RSA_NOT_ONLY_RSA_KEY_TRANSPORT",
	"HIGH":   "A_STRENGTH_BUCKET_WHOSE_MEMBERSHIP_VARIES_BY_OPENSSL_RELEASE",
	"MEDIUM": "A_STRENGTH_BUCKET_WHOSE_MEMBERSHIP_VARIES_BY_OPENSSL_RELEASE",
	"AESGCM": "A_MODE_ACROSS_KEY_EXCHANGES:NAME_THE_SUITES_OR_USE_AES-128_AES-256",
	"AESCBC": "A_MODE_ACROSS_KEY_EXCHANGES:USE_CBC_WHICH_ALSO_COVERS_3DES_CBC",
	"ECDH":   "COVERS_BOTH_ECDHE_AND_STATIC_ECDH:STATIC_ECDH_IS_NOT_IMPLEMENTED_HERE",
}

// opensslNotImplemented are the aliases a real nginx or haproxy policy carries
// that name things crypto/tls never implemented. A bank's existing policy will
// have !aNULL:!eNULL:!EXPORT:!MD5 in it, and both refusing those and silently
// swallowing them would be wrong: refusing blocks a transcription that is
// entirely correct in intent, and swallowing hides that the exclusion did
// nothing. They are recognised no-ops, logged at info, and the log line doubles
// as the compliance answer -- not offered, so nothing to exclude.
var opensslNotImplemented = map[string]string{
	"ANULL":    "ANONYMOUS_KEY_EXCHANGE_NOT_IMPLEMENTED",
	"ENULL":    "NULL_ENCRYPTION_NOT_IMPLEMENTED",
	"NULL":     "NULL_ENCRYPTION_NOT_IMPLEMENTED",
	"EXPORT":   "EXPORT_GRADE_SUITES_NOT_IMPLEMENTED",
	"EXPORT40": "EXPORT_GRADE_SUITES_NOT_IMPLEMENTED",
	"EXPORT56": "EXPORT_GRADE_SUITES_NOT_IMPLEMENTED",
	"LOW":      "LOW_STRENGTH_SUITES_NOT_IMPLEMENTED",
	"DES":      "SINGLE_DES_NOT_IMPLEMENTED",
	"MD5":      "MD5_SUITES_NOT_IMPLEMENTED",
	"DSS":      "DSA_NOT_IMPLEMENTED",
	"ADSS":     "DSA_NOT_IMPLEMENTED",
	"ADH":      "ANONYMOUS_DH_NOT_IMPLEMENTED",
	"AECDH":    "ANONYMOUS_ECDH_NOT_IMPLEMENTED",
	"KDH":      "STATIC_DH_NOT_IMPLEMENTED",
	"DH":       "FINITE_FIELD_DH_NOT_IMPLEMENTED",
	"EDH":      "FINITE_FIELD_DHE_NOT_IMPLEMENTED",
	"DHE":      "FINITE_FIELD_DHE_NOT_IMPLEMENTED",
	"PSK":      "PSK_SUITES_NOT_IMPLEMENTED",
	"SRP":      "SRP_NOT_IMPLEMENTED",
	"IDEA":     "IDEA_NOT_IMPLEMENTED",
	"SEED":     "SEED_NOT_IMPLEMENTED",
	"CAMELLIA": "CAMELLIA_NOT_IMPLEMENTED",
	"ARIA":     "ARIA_NOT_IMPLEMENTED",
	"KRB5":     "KERBEROS_SUITES_NOT_IMPLEMENTED",
	"GOST":     "GOST_NOT_IMPLEMENTED",
}

// opensslCurveNames maps the OpenSSL and nginx spelling of a curve onto the
// name the curves and deny-curves keys use. nginx writes
// ssl_ecdh_curve X25519:prime256v1, so prime256v1 has to resolve.
var opensslCurveNames = map[string]string{
	"PRIME256V1": "P-256",
	"SECP256R1":  "P-256",
	"SECP384R1":  "P-384",
	"SECP521R1":  "P-521",
	"X25519":     "X25519",
}

// grammarChars are the OpenSSL cipher-string operators. A token containing any
// of them is a pasted cipher string rather than a name, and is refused.
const grammarChars = ":+@,;"

// normalizeDenyToken applies the OpenSSL spelling rules to one raw deny token
// before it is resolved.
//
// It returns the token to resolve, whether a redundant leading "!" was
// stripped, and a non-nil noop reason when the token is a recognised OpenSSL
// alias for something this stack never implemented (in which case the caller
// records it as a no-op and resolves nothing).
func normalizeDenyToken(raw string, key string) (token string, strippedBang bool, noop string, err error) {
	t := strings.TrimSpace(raw)

	// A pasted cipher string. Refuse it, and say what to do instead: the deny
	// keys are already subtractive, so the "!" that makes an OpenSSL string
	// work is not needed here.
	if strings.ContainsAny(t, grammarChars) {
		return "", false, "", configErrorf(key,
			"OPENSSL_CIPHER_STRING_NOT_ACCEPTED:%s:LIST_TOKENS_INDIVIDUALLY_AS_JSON_ARRAY_ENTRIES:%s_IS_ALREADY_SUBTRACTIVE_SO_\"!\"_AND_THE_OPENSSL_GRAMMAR_ARE_NOT_NEEDED",
			echo(raw), key)
	}
	// "-X" removes but permits a later rule to add X back, which has no meaning
	// in a list that only subtracts, and is too easy to read as "!X".
	if strings.HasPrefix(t, "-") {
		return "", false, "", configErrorf(key,
			"OPENSSL_GRAMMAR_NOT_ACCEPTED:%s:\"-\"_MEANS_REMOVE_BUT_ALLOW_RE-ADDING_IN_OPENSSL_AND_HAS_NO_MEANING_HERE:DROP_THE_PREFIX",
			echo(raw))
	}
	// A bare leading "!" is the one forgiving case; see the file comment.
	if strings.HasPrefix(t, "!") {
		t = strings.TrimSpace(strings.TrimPrefix(t, "!"))
		strippedBang = true
		if t == "" {
			return "", false, "", configErrorf(key, "EMPTY_VALUE_AFTER_\"!\":%s", echo(raw))
		}
	}

	upper := strings.ToUpper(t)
	if reason, ok := opensslNotImplemented[upper]; ok {
		return upper, strippedBang, reason, nil
	}
	if reason, ok := opensslAmbiguousAliases[upper]; ok {
		return "", false, "", configErrorf(key,
			"OPENSSL_ALIAS_HAS_NO_EXACT_EQUIVALENT:%s:%s:NAME_THE_SUITES_OR_USE_A_FAMILY_TOKEN=%s",
			echo(raw), reason, strings.Join(suiteFamilyTokens(), "|"))
	}
	if tok, ok := opensslFamilyAliases[upper]; ok {
		return tok, strippedBang, "", nil
	}
	if iana, ok := opensslSuiteNames[canonicalOpenSSLSuiteKey(t)]; ok {
		return iana, strippedBang, "", nil
	}
	return t, strippedBang, "", nil
}

// canonicalOpenSSLSuiteKey folds an OpenSSL suite name to the table's key
// form. OpenSSL names are conventionally upper case with hyphens; operators
// and generated configs sometimes use underscores or lower case.
func canonicalOpenSSLSuiteKey(s string) string {
	u := strings.ToUpper(strings.TrimSpace(s))
	// The TLS 1.3 names keep their underscores; everything else uses hyphens.
	if strings.HasPrefix(u, "TLS_") {
		return u
	}
	return strings.ReplaceAll(u, "_", "-")
}

// resolveCurveTokenName applies the OpenSSL curve spelling before a deny-curves
// entry is looked up.
func resolveCurveTokenName(raw string) string {
	u := strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(raw), "!")))
	if n, ok := opensslCurveNames[u]; ok {
		return n
	}
	return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(raw), "!"))
}

// OpenSSLNameFor returns the OpenSSL spelling of an IANA suite name, for a log
// line or a report that an operator coming from nginx has to recognise. It
// returns "" when there is none.
func OpenSSLNameFor(ianaName string) string {
	for o, i := range opensslSuiteNames {
		if i == ianaName {
			return o
		}
	}
	return ""
}

// opensslAliasVocabulary is the accepted-alias list, for an error message that
// has to tell an operator what they may write.
func opensslAliasVocabulary() string {
	out := make([]string, 0, len(opensslFamilyAliases)+len(opensslNotImplemented))
	for k := range opensslFamilyAliases {
		out = append(out, k)
	}
	for k := range opensslNotImplemented {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, "|")
}

// assertSuiteTableComplete is used by the test that keeps the table in step
// with the standard library. It returns the IANA names Go reports that the
// table has no OpenSSL spelling for.
func assertSuiteTableComplete() []string {
	have := map[string]bool{}
	for _, iana := range opensslSuiteNames {
		have[iana] = true
	}
	var missing []string
	for _, s := range tls.CipherSuites() {
		if !have[s.Name] {
			missing = append(missing, s.Name)
		}
	}
	for _, s := range tls.InsecureCipherSuites() {
		if !have[s.Name] {
			missing = append(missing, s.Name)
		}
	}
	sort.Strings(missing)
	return missing
}
