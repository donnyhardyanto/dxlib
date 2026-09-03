package tls

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"sort"
	"strings"

	"github.com/donnyhardyanto/dxlib/log"

	"github.com/donnyhardyanto/dxlib/utils"
)

// The allow-list in policy.go is the primary control: what a handshake offers
// comes from a named profile, and configuration can only remove from it. The
// removal it offered at first was restating the whole list -- cipher-suites,
// curves -- and that is the wrong ergonomics for the day it is needed. An
// advisory lands, it says "all CBC modes" or "this intermediate", and the
// operator has to retype the surviving suite names correctly under pressure,
// fleet-wide, and hope none of them was mistyped into a service outage. nginx
// operators do it the other way round, ssl_ciphers '...:!3DES:!RC4', naming
// what goes and leaving the rest alone. The deny-* keys in this file are that.
//
// A deny can only ever remove. It is subtracted from the resolved profile
// after every narrowing override has been applied, so it cannot put anything
// back that the profile or an override took out, cannot widen a policy, and
// does not touch the narrow-only rule. That is the whole reason it is safe to
// add: the allow-list still bounds what can ever be offered, and the deny-list
// only moves inside that bound.
//
// Two rules about a deny that look contradictory until the reason is stated:
//
//   - A correctly spelled deny that matches nothing in the effective set is a
//     no-op, logged at info, and NOT an error. An incident deny-list is pushed
//     fleet-wide; the services that never offered the thing being denied were
//     already safe, and failing closed on them would take healthy pods down in
//     the middle of a breach. This is a deliberate exception to the
//     refuse-contradictions rule the rest of the package follows.
//   - An unrecognised token is a startup error listing the valid tokens. A
//     typo -- "CHACHA2O" with a letter O -- must never silently fail to
//     protect, and the only way to tell a typo from a no-op is a closed
//     vocabulary, which is also why a regular expression is not accepted: a
//     pattern over cipher names is a footgun (AES matches everything).
//
// Denying down to an empty effective set is an error; you cannot deny your way
// to no ciphers.
//
// The same keys also carry certificate-level denies -- a public key, one
// certificate, a signature algorithm -- because a breach is usually a key or
// a CA, not an algorithm: algorithm breaks arrive with years of warning, a
// compromised intermediate is same-day. Those are enforced in
// VerifyConnection over every certificate in the peer's verified chain, and
// they are the only revocation mechanism that works on an air-gapped host,
// where no OCSP or CRL fetch is permissible in the handshake path.

// suiteFamilies is the closed vocabulary of family tokens deny-cipher-suites
// accepts beside exact IANA names. Each names a property of a suite that an
// advisory would name, matched over the IANA name Go reports for the suite.
// The list is closed on purpose; see the package comment above.
var suiteFamilies = []struct {
	token string
	match func(ianaName string) bool
}{
	{"CBC", func(n string) bool { return strings.Contains(n, "_CBC_") }},
	{"3DES", func(n string) bool { return strings.Contains(n, "3DES") }},
	{"RC4", func(n string) bool { return strings.Contains(n, "RC4") }},
	{"CHACHA20", func(n string) bool { return strings.Contains(n, "CHACHA20") }},
	{"RSA-KEY-TRANSPORT", func(n string) bool { return strings.HasPrefix(n, "TLS_RSA_WITH_") }},
	{"AES-128", func(n string) bool { return strings.Contains(n, "AES_128") }},
	{"AES-256", func(n string) bool { return strings.Contains(n, "AES_256") }},
	// In IANA naming a bare _SHA suffix is the SHA-1 HMAC; SHA-256 and SHA-384
	// are spelled out.
	{"SHA1", func(n string) bool { return strings.HasSuffix(n, "_SHA") }},
}

func suiteFamilyTokens() []string {
	out := make([]string, 0, len(suiteFamilies))
	for _, f := range suiteFamilies {
		out = append(out, f.token)
	}
	return out
}

// knownSuites is every suite crypto/tls has a name for, secure or not: the
// set an exact IANA name in a deny is checked against. A name outside it is a
// typo; a name inside it that the policy never offered is a no-op.
func knownSuites() map[string]uint16 {
	out := map[string]uint16{}
	for _, s := range tls.CipherSuites() {
		out[s.Name] = s.ID
	}
	for _, s := range tls.InsecureCipherSuites() {
		out[s.Name] = s.ID
	}
	return out
}

// deniableCurveNames is every curve crypto/tls speaks, by the configuration
// spelling: the allow-list's four plus P-521, so that an advisory naming P-521
// resolves to a no-op rather than a typo.
var deniableCurveNames = func() map[string]tls.CurveID {
	m := map[string]tls.CurveID{"P-521": tls.CurveP521}
	for n, id := range curveNames {
		m[n] = id
	}
	return m
}()

// knownSignatureAlgorithms are the certificate signature algorithms
// crypto/x509 can name, by their String() form ("SHA256-RSA", "ECDSA-SHA384",
// "SHA256-RSAPSS", "Ed25519"). MD5 and SHA-1 are in the list so that a
// fleet-wide deny of them is a recognised no-op and not a typo; crypto/x509
// already refuses both during chain verification (the x509sha1 GODEBUG was
// removed in Go 1.24), so on a verified chain they never reach this code.
var knownSignatureAlgorithms = []x509.SignatureAlgorithm{
	x509.MD5WithRSA, x509.SHA1WithRSA, x509.SHA256WithRSA, x509.SHA384WithRSA, x509.SHA512WithRSA,
	x509.SHA256WithRSAPSS, x509.SHA384WithRSAPSS, x509.SHA512WithRSAPSS,
	x509.DSAWithSHA1, x509.DSAWithSHA256,
	x509.ECDSAWithSHA1, x509.ECDSAWithSHA256, x509.ECDSAWithSHA384, x509.ECDSAWithSHA512,
	x509.PureEd25519,
	x509.MLDSA44, x509.MLDSA65, x509.MLDSA87,
}

func signatureAlgorithmNames() []string {
	out := make([]string, 0, len(knownSignatureAlgorithms))
	for _, a := range knownSignatureAlgorithms {
		out = append(out, a.String())
	}
	return out
}

// The four deny keys, accepted in the tls block and in the deny-file alike.
const (
	keyDenyCipherSuites        = "deny-cipher-suites"
	keyDenyCurves              = "deny-curves"
	keyDenyCertificates        = "deny-certificates"
	keyDenySignatureAlgorithms = "deny-certificate-signature-algorithms"
	keyDenyFile                = "deny-file"
)

var denyKeys = []string{keyDenyCipherSuites, keyDenyCurves, keyDenyCertificates, keyDenySignatureAlgorithms}

// DXDeniedCertificate is one entry of deny-certificates: either a public key,
// identified by the SHA-256 of its SubjectPublicKeyInfo, or one certificate,
// identified by issuer and serial. The SPKI form is the one to prefer for a
// compromised key: it survives re-issuance of the same key under a new serial,
// which is exactly what a compromised key's owner will do next. The
// issuer-serial form names one certificate and nothing else.
type DXDeniedCertificate struct {
	SPKISHA256 []byte   // 32 bytes, or nil for the issuer-serial form
	Issuer     string   // Issuer.String() as the preflight and the log print it; "" for the SPKI form
	Serial     *big.Int // nil for the SPKI form
	Reason     string   // free text, echoed in the rejection log
}

// Matches reports whether the certificate is the one this entry names.
func (d *DXDeniedCertificate) Matches(c *x509.Certificate) bool {
	if c == nil {
		return false
	}
	if len(d.SPKISHA256) > 0 {
		sum := sha256.Sum256(c.RawSubjectPublicKeyInfo)
		return string(sum[:]) == string(d.SPKISHA256)
	}
	return d.Serial != nil && c.SerialNumber != nil && d.Serial.Cmp(c.SerialNumber) == 0 && c.Issuer.String() == d.Issuer
}

// String is the entry as the log names it.
func (d *DXDeniedCertificate) String() string {
	var s string
	if len(d.SPKISHA256) > 0 {
		s = "spki-sha256=" + hex.EncodeToString(d.SPKISHA256)
	} else {
		s = fmt.Sprintf("issuer=%q serial=%s", d.Issuer, SerialDescription(d.Serial))
	}
	if d.Reason != "" {
		s += fmt.Sprintf(" reason=%q", d.Reason)
	}
	return s
}

// SPKISHA256Hex is the hex SHA-256 of a certificate's SubjectPublicKeyInfo:
// what a deny-certificates entry has to say to name this key. The preflight
// prints it for every certificate it describes, so the value never has to be
// computed by hand on an air-gapped host.
func SPKISHA256Hex(c *x509.Certificate) string {
	if c == nil {
		return ""
	}
	sum := sha256.Sum256(c.RawSubjectPublicKeyInfo)
	return hex.EncodeToString(sum[:])
}

// SerialDescription prints a serial in both forms a deny entry accepts,
// decimal and hex, so whichever the operator copies from the log matches.
func SerialDescription(n *big.Int) string {
	if n == nil {
		return "(none)"
	}
	return fmt.Sprintf("%s(0x%X)", n.String(), n)
}

// DXDenyList is the parsed content of the four deny keys, from the block, from
// the deny-file, or the two merged.
type DXDenyList struct {
	// SuiteTokens and CurveTokens are the tokens as configured, in canonical
	// form, for the summary line. Resolution is in suiteIDs and curveIDs.
	SuiteTokens         []string
	CurveTokens         []string
	Certificates        []DXDeniedCertificate
	SignatureAlgorithms []x509.SignatureAlgorithm

	// suiteIDs is every suite any token names, over all suites Go knows; the
	// intersection with the policy's list is what actually comes out.
	// tokenSuites is the same per token, for the no-op report.
	suiteIDs    map[uint16]bool
	tokenSuites map[string][]uint16
	curveIDs    map[tls.CurveID]bool
	sigAlgs     map[x509.SignatureAlgorithm]bool
}

// IsEmpty reports a list that denies nothing -- an empty deny-file, the
// steady state between incidents.
func (d *DXDenyList) IsEmpty() bool {
	return d == nil || (len(d.SuiteTokens) == 0 && len(d.CurveTokens) == 0 && len(d.Certificates) == 0 && len(d.SignatureAlgorithms) == 0)
}

// MatchCertificate returns the entry that names the certificate, or nil.
func (d *DXDenyList) MatchCertificate(c *x509.Certificate) *DXDeniedCertificate {
	if d == nil {
		return nil
	}
	for i := range d.Certificates {
		if d.Certificates[i].Matches(c) {
			return &d.Certificates[i]
		}
	}
	return nil
}

// DeniesSignatureAlgorithm reports whether the algorithm is on the list.
func (d *DXDenyList) DeniesSignatureAlgorithm(a x509.SignatureAlgorithm) bool {
	return d != nil && d.sigAlgs[a]
}

// Summary is the list as one log fragment.
func (d *DXDenyList) Summary() string {
	if d.IsEmpty() {
		return "none"
	}
	parts := []string{}
	if len(d.SuiteTokens) > 0 {
		parts = append(parts, "cipher-suites="+strings.Join(d.SuiteTokens, ","))
	}
	if len(d.CurveTokens) > 0 {
		parts = append(parts, "curves="+strings.Join(d.CurveTokens, ","))
	}
	if len(d.Certificates) > 0 {
		names := make([]string, 0, len(d.Certificates))
		for i := range d.Certificates {
			names = append(names, d.Certificates[i].String())
		}
		parts = append(parts, fmt.Sprintf("certificates=%d[%s]", len(d.Certificates), strings.Join(names, "; ")))
	}
	if len(d.SignatureAlgorithms) > 0 {
		names := make([]string, 0, len(d.SignatureAlgorithms))
		for _, a := range d.SignatureAlgorithms {
			names = append(names, a.String())
		}
		parts = append(parts, "certificate-signature-algorithms="+strings.Join(names, ","))
	}
	return strings.Join(parts, " ")
}

// readDenyList reads the four deny keys out of a block. It returns nil when
// none of them is present, so a caller can tell "no deny keys" from "deny keys
// that happen to deny nothing". Every token is resolved here, against the
// closed vocabularies, and an unrecognised one is an error; whether a
// recognised one removes anything is the policy's business (see
// DXPolicy.effective), because the same list means different things against
// different profiles.
func readDenyList(kv utils.JSON) (*DXDenyList, error) {
	present := false
	for _, k := range denyKeys {
		if _, ok := kv[k]; ok {
			present = true
			break
		}
	}
	if !present {
		return nil, nil
	}
	d := &DXDenyList{
		suiteIDs:    map[uint16]bool{},
		tokenSuites: map[string][]uint16{},
		curveIDs:    map[tls.CurveID]bool{},
		sigAlgs:     map[x509.SignatureAlgorithm]bool{},
	}

	suites, _, err := readStringSlice(kv, keyDenyCipherSuites)
	if err != nil {
		return nil, err
	}
	known := knownSuites()
	for i, raw := range suites {
		token, ids, err := resolveSuiteToken(raw, known, fmt.Sprintf("%s[%d]", keyDenyCipherSuites, i))
		if err != nil {
			return nil, err
		}
		d.SuiteTokens = append(d.SuiteTokens, token)
		d.tokenSuites[token] = ids
		for _, id := range ids {
			d.suiteIDs[id] = true
		}
	}

	curves, _, err := readStringSlice(kv, keyDenyCurves)
	if err != nil {
		return nil, err
	}
	for i, raw := range curves {
		itemKey := fmt.Sprintf("%s[%d]", keyDenyCurves, i)
		// nginx writes ssl_ecdh_curve X25519:prime256v1, so the OpenSSL curve
		// spelling resolves and a pasted colon-joined list is refused with the
		// same message the suite key uses. See deny_openssl.go.
		if _, _, _, err := normalizeDenyToken(raw, itemKey); err != nil {
			return nil, err
		}
		name := strings.ToUpper(resolveCurveTokenName(raw))
		id, ok := deniableCurveNames[name]
		if !ok {
			return nil, configErrorf(itemKey, "INVALID_VALUE:%s:VALID_VALUES=%s|<its OpenSSL spelling such as prime256v1>", echo(raw), strings.Join(sortedKeys(deniableCurveNames), "|"))
		}
		d.CurveTokens = append(d.CurveTokens, name)
		d.curveIDs[id] = true
	}

	if d.Certificates, err = readDeniedCertificates(kv); err != nil {
		return nil, err
	}

	algs, _, err := readStringSlice(kv, keyDenySignatureAlgorithms)
	if err != nil {
		return nil, err
	}
	for i, raw := range algs {
		name := strings.ToUpper(strings.TrimSpace(raw))
		var found x509.SignatureAlgorithm
		for _, a := range knownSignatureAlgorithms {
			if strings.EqualFold(a.String(), name) {
				found = a
				break
			}
		}
		if found == x509.UnknownSignatureAlgorithm {
			return nil, configErrorf(fmt.Sprintf("%s[%d]", keyDenySignatureAlgorithms, i), "INVALID_VALUE:%s:VALID_VALUES=%s", echo(raw), strings.Join(signatureAlgorithmNames(), "|"))
		}
		d.SignatureAlgorithms = append(d.SignatureAlgorithms, found)
		d.sigAlgs[found] = true
	}
	return d, nil
}

// resolveSuiteToken turns one deny-cipher-suites entry into the suites it
// names: a family token expands over every suite Go knows, an exact IANA name
// is itself. Anything else is a typo and an error listing the vocabulary.
func resolveSuiteToken(raw string, known map[string]uint16, key string) (token string, ids []uint16, err error) {
	// OpenSSL/nginx spelling and grammar are handled first: see deny_openssl.go.
	// A recognised alias for something crypto/tls never implemented comes back
	// with an empty id list, which the existing no-op reporting already handles
	// -- exactly the behaviour wanted for a transcribed !aNULL:!EXPORT.
	normalized, strippedBang, noop, err := normalizeDenyToken(raw, key)
	if err != nil {
		return "", nil, err
	}
	if strippedBang {
		log.Log.Infof("TLS_DENY_TOKEN_REDUNDANT_BANG:%s:%s:the deny keys only ever subtract, so the leading \"!\" was not needed", key, echo(raw))
	}
	if noop != "" {
		log.Log.Infof("TLS_DENY_TOKEN_NOT_IMPLEMENTED:%s:%s:%s:nothing to exclude", key, normalized, noop)
		return normalized, nil, nil
	}
	token = strings.ToUpper(strings.TrimSpace(normalized))
	for _, f := range suiteFamilies {
		if token != f.token {
			continue
		}
		names := sortedKeys(known)
		for _, n := range names {
			if f.match(n) {
				ids = append(ids, known[n])
			}
		}
		return token, ids, nil
	}
	if id, ok := known[token]; ok {
		return token, []uint16{id}, nil
	}
	return "", nil, configErrorf(key, "INVALID_VALUE:%s:VALID_VALUES=%s|<an IANA suite name such as TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256>|<its OpenSSL spelling such as ECDHE-RSA-AES128-GCM-SHA256>|<a recognised OpenSSL alias such as %s>:REGULAR_EXPRESSIONS_ARE_NOT_ACCEPTED", echo(raw), strings.Join(suiteFamilyTokens(), "|"), opensslAliasVocabulary())
}

// readDeniedCertificates reads deny-certificates: a list of objects, each in
// exactly one of two forms,
//
//	{"spki-sha256": "<hex or base64>", "reason": "..."}
//	{"issuer": "<Issuer.String()>", "serial": "<decimal, 0x hex, or aa:bb:cc>", "reason": "..."}
//
// An entry with neither form, both, or a key outside these is refused: a
// misspelled key would otherwise be an entry that denies nothing.
func readDeniedCertificates(kv utils.JSON) ([]DXDeniedCertificate, error) {
	raw, present := kv[keyDenyCertificates]
	if !present {
		return nil, nil
	}
	// JSON decoding yields []any of map[string]any; a configuration built in
	// Go yields []utils.JSON, which is the same type under an alias.
	var items []any
	switch v := raw.(type) {
	case []any:
		items = v
	case []utils.JSON:
		for _, m := range v {
			items = append(items, m)
		}
	default:
		return nil, configErrorf(keyDenyCertificates, "WRONG_TYPE:%T:EXPECTED_LIST_OF_OBJECT", raw)
	}
	var out []DXDeniedCertificate
	for i, item := range items {
		key := fmt.Sprintf("%s[%d]", keyDenyCertificates, i)
		entry, ok := item.(utils.JSON)
		if !ok {
			return nil, configErrorf(key, "WRONG_TYPE:%T:EXPECTED_OBJECT", item)
		}
		for k := range entry {
			switch k {
			case "spki-sha256", "issuer", "serial", "reason":
			default:
				return nil, configErrorf(key+"/"+k, "UNKNOWN_KEY:VALID_KEYS=spki-sha256|issuer,serial|reason")
			}
		}
		spki, spkiPresent, err := readString(entry, "spki-sha256")
		if err != nil {
			return nil, prefixKey(err, key+"/")
		}
		issuer, issuerPresent, err := readString(entry, "issuer")
		if err != nil {
			return nil, prefixKey(err, key+"/")
		}
		serial, serialPresent, err := readString(entry, "serial")
		if err != nil {
			return nil, prefixKey(err, key+"/")
		}
		reason, _, err := readString(entry, "reason")
		if err != nil {
			return nil, prefixKey(err, key+"/")
		}
		d := DXDeniedCertificate{Reason: strings.TrimSpace(reason)}
		switch {
		case spkiPresent && (issuerPresent || serialPresent):
			return nil, configErrorf(key, "ONE_FORM_ONLY:GIVE_spki-sha256_OR_issuer+serial_NOT_BOTH")
		case spkiPresent:
			if d.SPKISHA256, err = parseSPKISHA256(spki); err != nil {
				return nil, configErrorf(key+"/spki-sha256", "%v", err)
			}
		case issuerPresent && serialPresent:
			d.Issuer = strings.TrimSpace(issuer)
			if d.Issuer == "" {
				return nil, configErrorf(key+"/issuer", "EMPTY_VALUE")
			}
			if d.Serial, err = parseSerial(serial); err != nil {
				return nil, configErrorf(key+"/serial", "%v", err)
			}
		case issuerPresent || serialPresent:
			return nil, configErrorf(key, "REQUIRED_TOGETHER:issuer_AND_serial")
		default:
			return nil, configErrorf(key, "NO_IDENTITY:GIVE_spki-sha256_OR_issuer+serial")
		}
		out = append(out, d)
	}
	return out, nil
}

// parseSPKISHA256 accepts the digest as 64 hex characters (colons between
// bytes allowed, as openssl prints them) or as standard base64 (44 characters,
// as a pin is usually quoted). The two are told apart by length after the
// colons are removed, so neither form can be mistaken for the other.
func parseSPKISHA256(s string) ([]byte, error) {
	s = strings.ReplaceAll(strings.TrimSpace(s), ":", "")
	if s == "" {
		return nil, fmt.Errorf("EMPTY_VALUE")
	}
	if len(s) == 64 {
		b, err := hex.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("INVALID_VALUE:%s:EXPECTED_64_HEX_CHARACTERS_OR_44_BASE64_CHARACTERS", echo(s))
		}
		return b, nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		if b, err = base64.RawStdEncoding.DecodeString(s); err != nil {
			return nil, fmt.Errorf("INVALID_VALUE:%s:EXPECTED_64_HEX_CHARACTERS_OR_44_BASE64_CHARACTERS", echo(s))
		}
	}
	if len(b) != sha256.Size {
		return nil, fmt.Errorf("INVALID_VALUE:%s:DECODES_TO_%d_BYTES_NOT_32", echo(s), len(b))
	}
	return b, nil
}

// parseSerial reads a certificate serial the way it appears wherever an
// operator might copy it from: decimal as this package's own log and preflight
// print it ("serial=12345(0x3039)"), hex with a 0x prefix, hex with colons as
// openssl prints it ("1a:2b:3c"), or bare hex when it carries a letter. A bare
// string of digits is decimal; openssl's prefixless hex output of a serial
// that happens to hold only digits has to be written with 0x, and the rule is
// stated in the design notes for that reason.
func parseSerial(s string) (*big.Int, error) {
	s = strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(s), ":", ""), " ", "")
	if s == "" {
		return nil, fmt.Errorf("EMPTY_VALUE")
	}
	base := 10
	switch {
	case strings.HasPrefix(s, "0x"), strings.HasPrefix(s, "0X"):
		s, base = s[2:], 16
	case strings.ContainsAny(s, "abcdefABCDEF"):
		base = 16
	}
	n, ok := new(big.Int).SetString(s, base)
	if !ok || n.Sign() < 0 {
		return nil, fmt.Errorf("INVALID_VALUE:%s:EXPECTED_DECIMAL_OR_0x_HEX_OR_aa:bb:cc_HEX", echo(s))
	}
	return n, nil
}

// loadDenyFile reads a deny-file: a JSON object carrying any of the four deny
// keys and nothing else. An empty object is the normal steady state. A key
// outside the four is refused, because a misspelled key would be a file that
// denies nothing while looking as though it did. Every error is reported
// under the deny-file key with the path, so a reload failure names the file.
func loadDenyFile(path string) (*DXDenyList, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, configErrorf(keyDenyFile, "CANNOT_READ:%s:%v", path, err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, configErrorf(keyDenyFile, "CANNOT_PARSE:%s:%v", path, err)
	}
	for k := range raw {
		if !isDenyKey(k) {
			return nil, configErrorf(keyDenyFile, "%s:UNKNOWN_KEY:%s:VALID_KEYS=%s", path, echo(k), strings.Join(denyKeys, "|"))
		}
	}
	d, err := readDenyList(utils.JSON(raw))
	if err != nil {
		if ce, ok := err.(*DXConfigError); ok {
			return nil, configErrorf(keyDenyFile, "%s:%s:%s", path, ce.Key, ce.Detail)
		}
		return nil, configErrorf(keyDenyFile, "%s:%v", path, err)
	}
	if d == nil {
		// {}: a file with no deny key in it denies nothing, and says so.
		d = emptyDenyList()
	}
	return d, nil
}

func emptyDenyList() *DXDenyList {
	return &DXDenyList{suiteIDs: map[uint16]bool{}, tokenSuites: map[string][]uint16{}, curveIDs: map[tls.CurveID]bool{}, sigAlgs: map[x509.SignatureAlgorithm]bool{}}
}

// prefixKey puts a parent path in front of a DXConfigError's key, so an error
// from a reader run on a nested object names the whole path.
func prefixKey(err error, prefix string) error {
	if ce, ok := err.(*DXConfigError); ok {
		return &DXConfigError{Key: prefix + ce.Key, Detail: ce.Detail}
	}
	return err
}

func isDenyKey(k string) bool {
	for _, d := range denyKeys {
		if d == k {
			return true
		}
	}
	return false
}

// mergeDenyLists is the union of two lists; either may be nil. A union of
// denies is still only a deny.
func mergeDenyLists(a, b *DXDenyList) *DXDenyList {
	if a.IsEmpty() {
		return b
	}
	if b.IsEmpty() {
		return a
	}
	m := &DXDenyList{
		SuiteTokens:         append(append([]string(nil), a.SuiteTokens...), b.SuiteTokens...),
		CurveTokens:         append(append([]string(nil), a.CurveTokens...), b.CurveTokens...),
		Certificates:        append(append([]DXDeniedCertificate(nil), a.Certificates...), b.Certificates...),
		SignatureAlgorithms: append(append([]x509.SignatureAlgorithm(nil), a.SignatureAlgorithms...), b.SignatureAlgorithms...),
		suiteIDs:            map[uint16]bool{},
		tokenSuites:         map[string][]uint16{},
		curveIDs:            map[tls.CurveID]bool{},
		sigAlgs:             map[x509.SignatureAlgorithm]bool{},
	}
	for _, src := range []*DXDenyList{a, b} {
		for id := range src.suiteIDs {
			m.suiteIDs[id] = true
		}
		for tok, ids := range src.tokenSuites {
			m.tokenSuites[tok] = ids
		}
		for id := range src.curveIDs {
			m.curveIDs[id] = true
		}
		for alg := range src.sigAlgs {
			m.sigAlgs[alg] = true
		}
	}
	return m
}

// denyOutcome is what subtracting a deny-list from a policy's resolved lists
// produced: the lists that remain, what was removed by name, and the tokens
// that named nothing in the set -- the no-ops the startup log reports at info.
type denyOutcome struct {
	suites        []uint16
	curves        []tls.CurveID
	removedSuites []string
	removedCurves []string
	noOps         []string
}

// subtractDeny removes what deny names from the two base lists, keeping the
// base order so preference stays the profile's.
func subtractDeny(baseSuites []uint16, baseCurves []tls.CurveID, deny *DXDenyList) denyOutcome {
	o := denyOutcome{}
	if deny.IsEmpty() {
		o.suites, o.curves = baseSuites, baseCurves
		return o
	}
	inBase := map[uint16]bool{}
	for _, id := range baseSuites {
		inBase[id] = true
		if deny.suiteIDs[id] {
			o.removedSuites = append(o.removedSuites, tls.CipherSuiteName(id))
		} else {
			o.suites = append(o.suites, id)
		}
	}
	for _, tok := range deny.SuiteTokens {
		hit := false
		for _, id := range deny.tokenSuites[tok] {
			if inBase[id] {
				hit = true
				break
			}
		}
		if !hit {
			o.noOps = append(o.noOps, keyDenyCipherSuites+"="+tok)
		}
	}
	curveInBase := map[tls.CurveID]bool{}
	for _, id := range baseCurves {
		curveInBase[id] = true
		if deny.curveIDs[id] {
			o.removedCurves = append(o.removedCurves, curveName(id))
		} else {
			o.curves = append(o.curves, id)
		}
	}
	for _, tok := range deny.CurveTokens {
		if !curveInBase[deniableCurveNames[tok]] {
			o.noOps = append(o.noOps, keyDenyCurves+"="+tok)
		}
	}
	return o
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// checkDeniedChain applies the certificate-level denies to one chain. It
// looks at every certificate, not only the leaf: a denied intermediate has
// to reject every leaf under it, or denying the intermediate would mean
// nothing. The signature-algorithm check exempts the trust anchor at the end
// of a verified chain, because nobody verifies a trust anchor's own
// signature -- it is trusted by identity -- and a bank's internal root
// self-signed years ago with an algorithm since retired is not a weakness in
// any handshake that chains to it. anchorExempt says whether the last element
// is such an anchor; under insecure-skip-verify there is no verified chain
// and the caller passes the presented certificates, exempting a self-signed
// one the same way.
func checkDeniedChain(deny *DXDenyList, chain []*x509.Certificate, anchorExempt bool) error {
	if deny.IsEmpty() {
		return nil
	}
	for i, c := range chain {
		if d := deny.MatchCertificate(c); d != nil {
			return fmt.Errorf("TLS_PEER_REVOKED:%s:chain[%d]=%q:%s", PeerIdentity(chain[0]), i, c.Subject.String(), d.String())
		}
	}
	for i, c := range chain {
		isAnchor := (anchorExempt && i == len(chain)-1) || (!anchorExempt && string(c.RawIssuer) == string(c.RawSubject))
		if isAnchor {
			continue
		}
		if deny.DeniesSignatureAlgorithm(c.SignatureAlgorithm) {
			return fmt.Errorf("TLS_PEER_REVOKED:%s:chain[%d]=%q:certificate-signature-algorithm=%s:ON_%s", PeerIdentity(chain[0]), i, c.Subject.String(), c.SignatureAlgorithm, keyDenySignatureAlgorithms)
		}
	}
	return nil
}
