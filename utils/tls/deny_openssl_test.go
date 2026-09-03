package tls

import (
	"crypto/tls"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/utils"
)

// TestEveryGoSuiteHasAnOpenSSLName is the test that keeps the alias table in
// step with the standard library. A Go upgrade that adds a cipher suite fails
// here rather than leaving an OpenSSL name that silently will not resolve --
// which, in a deny list, would mean an exclusion an operator believed was in
// force and was not.
func TestEveryGoSuiteHasAnOpenSSLName(t *testing.T) {
	if missing := assertSuiteTableComplete(); len(missing) > 0 {
		t.Fatalf("opensslSuiteNames has no entry for %d suite(s) crypto/tls reports: %s",
			len(missing), strings.Join(missing, ", "))
	}
	// And the other direction: no entry may name a suite Go does not have, or
	// the table would be quietly carrying a name that resolves to nothing.
	known := knownSuites()
	for openssl, iana := range opensslSuiteNames {
		if _, ok := known[iana]; !ok {
			t.Errorf("opensslSuiteNames[%q] = %q, which crypto/tls does not report", openssl, iana)
		}
	}
}

// TestOpenSSLSuiteNamesDenyTheSameSuite proves the point of the table: the two
// spellings of one suite are the same deny.
func TestOpenSSLSuiteNamesDenyTheSameSuite(t *testing.T) {
	known := knownSuites()
	for _, c := range []struct{ openssl, iana string }{
		{"ECDHE-RSA-AES128-GCM-SHA256", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"},
		{"ECDHE-ECDSA-CHACHA20-POLY1305", "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256"},
		{"DES-CBC3-SHA", "TLS_RSA_WITH_3DES_EDE_CBC_SHA"},
		{"AES128-SHA", "TLS_RSA_WITH_AES_128_CBC_SHA"},
		{"RC4-SHA", "TLS_RSA_WITH_RC4_128_SHA"},
		// Lower case and underscores, as a generated config may write them.
		{"ecdhe-rsa-aes256-gcm-sha384", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"},
		{"ECDHE_RSA_AES256_GCM_SHA384", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"},
	} {
		gotTok, gotIDs, err := resolveSuiteToken(c.openssl, known, keyDenyCipherSuites)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", c.openssl, err)
		}
		if gotTok != c.iana {
			t.Errorf("%s resolved to token %q, want the IANA name %q", c.openssl, gotTok, c.iana)
		}
		if len(gotIDs) != 1 || gotIDs[0] != known[c.iana] {
			t.Errorf("%s resolved to ids %v, want the single id for %s", c.openssl, gotIDs, c.iana)
		}
	}
}

// TestPastedOpenSSLCipherStringIsRefused covers the half-applied-policy case:
// the worst outcome available is honouring part of a string the operator
// believes they applied whole.
func TestPastedOpenSSLCipherStringIsRefused(t *testing.T) {
	known := knownSuites()
	for _, raw := range []string{
		"HIGH:!aNULL:!MD5",
		"ECDHE+AESGCM:ECDHE+CHACHA20",
		"DEFAULT:@STRENGTH",
		"ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384",
		"ECDHE+AESGCM",
	} {
		_, _, err := resolveSuiteToken(raw, known, keyDenyCipherSuites)
		if err == nil {
			t.Fatalf("%q was accepted; a pasted cipher string must be refused", raw)
		}
		if !strings.Contains(err.Error(), "OPENSSL_CIPHER_STRING_NOT_ACCEPTED") &&
			!strings.Contains(err.Error(), "OPENSSL_GRAMMAR_NOT_ACCEPTED") {
			t.Errorf("%q refused, but the message does not say why: %v", raw, err)
		}
		if !strings.Contains(err.Error(), "LIST_TOKENS_INDIVIDUALLY") &&
			!strings.Contains(err.Error(), "DROP_THE_PREFIX") {
			t.Errorf("%q refused without saying what to do instead: %v", raw, err)
		}
	}
}

// TestLeadingBangIsForgiven is the one tolerated piece of nginx reflex.
func TestLeadingBangIsForgiven(t *testing.T) {
	known := knownSuites()
	tok, ids, err := resolveSuiteToken("!3DES", known, keyDenyCipherSuites)
	if err != nil {
		t.Fatalf("!3DES: %v", err)
	}
	if tok != "3DES" {
		t.Errorf("!3DES resolved to %q, want the 3DES family token", tok)
	}
	if len(ids) == 0 {
		t.Error("!3DES denied nothing; the family should match the 3DES suites")
	}
	// And with an OpenSSL suite name behind the bang.
	tok, ids, err = resolveSuiteToken("!DES-CBC3-SHA", known, keyDenyCipherSuites)
	if err != nil {
		t.Fatalf("!DES-CBC3-SHA: %v", err)
	}
	if tok != "TLS_RSA_WITH_3DES_EDE_CBC_SHA" || len(ids) != 1 {
		t.Errorf("!DES-CBC3-SHA resolved to %q/%v, want the single 3DES RSA suite", tok, ids)
	}
	// A bang on its own is not a token.
	if _, _, err := resolveSuiteToken("!", known, keyDenyCipherSuites); err == nil {
		t.Error(`"!" alone was accepted`)
	}
}

// TestMinusPrefixIsRefusedRatherThanTreatedAsBang keeps the forgiving case
// narrow. In OpenSSL "-X" removes X but lets a later rule add it back, which
// has no meaning in a list that only subtracts and reads too easily as "!X".
func TestMinusPrefixIsRefusedRatherThanTreatedAsBang(t *testing.T) {
	_, _, err := resolveSuiteToken("-3DES", knownSuites(), keyDenyCipherSuites)
	if err == nil {
		t.Fatal("-3DES was accepted; only a leading ! is forgiven")
	}
	if !strings.Contains(err.Error(), "OPENSSL_GRAMMAR_NOT_ACCEPTED") {
		t.Errorf("unexpected message: %v", err)
	}
}

// TestNotImplementedAliasesAreRecognisedNoOps is the case a transcribed bank
// policy depends on. !aNULL:!eNULL:!EXPORT:!MD5 is in every real nginx config;
// erroring on it blocks a correct transcription, and swallowing it silently
// hides that the exclusion did nothing.
func TestNotImplementedAliasesAreRecognisedNoOps(t *testing.T) {
	known := knownSuites()
	for _, raw := range []string{"aNULL", "!eNULL", "EXPORT", "!MD5", "DSS", "IDEA", "CAMELLIA", "PSK", "SRP", "!LOW"} {
		tok, ids, err := resolveSuiteToken(raw, known, keyDenyCipherSuites)
		if err != nil {
			t.Errorf("%s should be a recognised no-op, got error: %v", raw, err)
			continue
		}
		if len(ids) != 0 {
			t.Errorf("%s denied %d suite(s); this stack never implemented it", raw, len(ids))
		}
		if tok == "" {
			t.Errorf("%s resolved to an empty token", raw)
		}
	}
}

// TestAmbiguousAliasesAreRefusedNotApproximated is the other half of that
// judgement: an alias whose meaning only overlaps a token would deny a
// different set than the operator intended, so it is refused with the reason.
func TestAmbiguousAliasesAreRefusedNotApproximated(t *testing.T) {
	known := knownSuites()
	for _, raw := range []string{"HIGH", "MEDIUM", "aRSA", "AESGCM", "AESCBC"} {
		_, _, err := resolveSuiteToken(raw, known, keyDenyCipherSuites)
		if err == nil {
			t.Errorf("%s was accepted; it has no exact equivalent and must be refused", raw)
			continue
		}
		if !strings.Contains(err.Error(), "OPENSSL_ALIAS_HAS_NO_EXACT_EQUIVALENT") {
			t.Errorf("%s refused with an unhelpful message: %v", raw, err)
		}
	}
	// kRSA, by contrast, is exactly the RSA key-transport set.
	tok, ids, err := resolveSuiteToken("kRSA", known, keyDenyCipherSuites)
	if err != nil {
		t.Fatalf("kRSA: %v", err)
	}
	if tok != "RSA-KEY-TRANSPORT" || len(ids) == 0 {
		t.Errorf("kRSA resolved to %q/%d suites, want the RSA-KEY-TRANSPORT family", tok, len(ids))
	}
}

// TestATypoStillErrors is the rule the whole forgiving layer must not weaken:
// a misspelling has to fail loudly, or it is an exclusion that silently
// protects nothing.
func TestATypoStillErrors(t *testing.T) {
	known := knownSuites()
	for _, raw := range []string{"CHACHA2O", "ECDHE-RSA-AES128-GCM-SHA255", "3DEZ", "aNUL", "TLS_ECDHE_RSA_WITH_AES_129_GCM_SHA256"} {
		if _, _, err := resolveSuiteToken(raw, known, keyDenyCipherSuites); err == nil {
			t.Errorf("%q was accepted; a typo must not silently fail to protect", raw)
		}
	}
}

// TestOpenSSLCurveSpellingResolves covers nginx's ssl_ecdh_curve names.
func TestOpenSSLCurveSpellingResolves(t *testing.T) {
	d, err := readDenyList(utils.JSON{keyDenyCurves: []any{"prime256v1", "secp384r1"}})
	if err != nil {
		t.Fatalf("prime256v1/secp384r1: %v", err)
	}
	got := strings.Join(d.CurveTokens, ",")
	if !strings.Contains(got, "P-256") || !strings.Contains(got, "P-384") {
		t.Errorf("curve tokens %q, want the P-256 and P-384 names", got)
	}
	// A colon-joined nginx list is refused, not half-read.
	if _, err := readDenyList(utils.JSON{keyDenyCurves: []any{"X25519:prime256v1"}}); err == nil {
		t.Error("a colon-joined curve list was accepted")
	}
}

// TestOpenSSLNameForIsTheInverse keeps the reporting helper honest, since a
// report an nginx operator cannot recognise defeats the point.
func TestOpenSSLNameForIsTheInverse(t *testing.T) {
	for _, s := range tls.CipherSuites() {
		o := OpenSSLNameFor(s.Name)
		if o == "" {
			t.Errorf("no OpenSSL name reported for %s", s.Name)
			continue
		}
		if opensslSuiteNames[canonicalOpenSSLSuiteKey(o)] != s.Name {
			t.Errorf("OpenSSLNameFor(%s) = %s, which does not resolve back", s.Name, o)
		}
	}
}

// TestATranscribedNginxPolicyIsAcceptedWhole is the end-to-end case: the deny
// list a bank would actually arrive with, token by token, mixing every category
// this file handles.
func TestATranscribedNginxPolicyIsAcceptedWhole(t *testing.T) {
	// ssl_ciphers 'ECDHE-RSA-AES256-GCM-SHA384:!aNULL:!eNULL:!EXPORT:!DES:!MD5:!PSK:!RC4:!3DES';
	// transcribed as individual tokens, keeping the operator's "!" habit.
	d, err := readDenyList(utils.JSON{
		keyDenyCipherSuites: []any{
			"!aNULL", "!eNULL", "!EXPORT", "!DES", "!MD5", "!PSK",
			"!RC4", "!3DES", "!ECDHE-RSA-AES128-SHA",
		},
	})
	if err != nil {
		t.Fatalf("a transcribed nginx policy was refused: %v", err)
	}
	if len(d.SuiteTokens) != 9 {
		t.Errorf("recorded %d tokens, want all 9 to be accepted", len(d.SuiteTokens))
	}
	// The ones that name something real must actually deny it.
	base := []uint16{
		tls.TLS_RSA_WITH_RC4_128_SHA,
		tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	}
	out := subtractDeny(base, nil, d)
	if len(out.suites) != 1 || out.suites[0] != tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384 {
		t.Errorf("after the policy, suites = %v, want only the AES256-GCM suite left", out.suites)
	}
}
