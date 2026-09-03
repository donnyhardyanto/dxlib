package tls

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/donnyhardyanto/dxlib/utils"
)

// Handshake policy is expressed as an allow-list first.
//
// A deny-list of broken parameters can only ever name what has already been
// published, so it is permanently one disclosure behind. A named profile that
// lists the small set this library intends to speak is complete by
// construction: everything unnamed is refused, including whatever gets broken
// next year. So the parameters a handshake offers come from the profile, and
// the operator can only remove from it, never add to it.
//
// Removal comes in two shapes, applied in this order. The narrowing overrides
// (cipher-suites, curves, min-version) restate the list the operator wants,
// and are intersected with the profile. The deny keys (deny-cipher-suites,
// deny-curves, in the block or in a hot-reloaded deny-file) name what to take
// out of whatever that left, for the day an advisory says "all CBC modes" and
// nobody should be retyping the survivors under pressure. Both can only
// remove; see deny.go for why that makes the second safe to have.
//
// Go's own InsecureCipherSuites() is a third layer with one narrow job:
// refusing an operator who writes something already known to be broken into
// a narrowing override, usually to satisfy a checklist. That list is
// crypto/tls's, not one maintained here, so it keeps moving as Go demotes
// suites without anybody in this repository having to notice.
//
// Note what the cipher list does not do: nothing, under TLS 1.3. Go hardcodes
// the three 1.3 suites (AES-128-GCM, AES-256-GCM, ChaCha20-Poly1305) and
// exposes no knob, so tls.Config.CipherSuites is ignored for 1.3 handshakes.
// Any statement that "weak ciphers were blacklisted" is vacuous for 1.3
// traffic; the protocol and the standard library already constrain it. There
// is deliberately no 1.3 cipher key here that would silently do nothing.

// The two profiles. "modern" is TLS 1.3 only. It is reachable for in-cluster
// service-to-service traffic in a way it never is for a public endpoint,
// because both ends of every connection are ours -- that is the payoff of
// doing this inside the mesh. "intermediate" adds TLS 1.2 restricted to ECDHE
// key exchange with AEAD ciphers, for a peer that is not yet on 1.3.
const (
	PolicyModern       = "modern"
	PolicyIntermediate = "intermediate"
)

var policyValues = []string{PolicyModern, PolicyIntermediate}

// intermediateSuites12 is the whole TLS 1.2 allow-list: ECDHE for forward
// secrecy, AEAD for integrity, nothing else. By exclusion, and by the attack
// that motivates each exclusion:
//
//   - every CBC suite: Lucky13 and the padding-oracle family (BEAST, POODLE's
//     TLS variant). Go still lists four ECDHE+CBC-SHA1 suites as "secure";
//     they are not in this profile.
//   - every RSA key-transport suite (TLS_RSA_WITH_*): no forward secrecy, and
//     ROBOT. Go moved these to its insecure list in 1.22; the profile would
//     exclude them regardless.
//   - 3DES: Sweet32. RC4: the NOMORE biases. Both on Go's insecure list.
//   - static and anonymous DH: crypto/tls never implemented them, so there is
//     nothing to exclude, but an auditor will ask.
var intermediateSuites12 = []uint16{
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
}

// allowedCurves is the key-exchange allow-list for both profiles.
// X25519MLKEM768 leads: it is the post-quantum hybrid Go has offered by
// default since 1.24, and keeping it is what protects today's recordings from
// a harvest-now-decrypt-later adversary. Setting CurvePreferences at all would
// otherwise drop it, which is why it is named here rather than left implicit.
// P-521 is not broken but is not needed; P-224 does not exist in crypto/tls.
var allowedCurves = []tls.CurveID{
	tls.X25519MLKEM768,
	tls.X25519,
	tls.CurveP256,
	tls.CurveP384,
}

var curveNames = map[string]tls.CurveID{
	"X25519MLKEM768": tls.X25519MLKEM768,
	"X25519":         tls.X25519,
	"P-256":          tls.CurveP256,
	"P-384":          tls.CurveP384,
}

// The certificate-strength floors. Chain validation says nothing about key
// strength -- a 1024-bit RSA leaf that chains correctly to the internal CA
// verifies fine, and Go's own floor (checkKeySize in the handshake) is 1024.
// These are checked on every peer leaf in VerifyConnection and on our own
// certificate and CA files at load. They can be raised in configuration, never
// lowered below these values.
const (
	MinRSABitsFloor   = 2048
	MinECDSABitsFloor = 256
)

// DXPolicy is a resolved handshake policy: the named profile, narrowed by any
// operator override, minus the block's own deny keys, ready to be copied onto
// a tls.Config.
type DXPolicy struct {
	Profile      string
	MinVersion   uint16
	MaxVersion   uint16
	CipherSuites []uint16 // TLS 1.2 only; nil under a 1.3-only policy. Effective: after narrowing and after the denies.
	Curves       []tls.CurveID
	MinRSABits   int
	MinECDSABits int

	// Deny is the block's own deny-cipher-suites, deny-curves,
	// deny-certificates and deny-certificate-signature-algorithms; nil when the
	// block carries none. The deny-file's list is merged with it by the server
	// and client builders, not here, because this type is built without
	// touching the filesystem.
	Deny *DXDenyList

	// narrowedSuites and narrowedCurves are the lists after the profile and
	// the narrowing overrides and before any deny: the base a hot-reloaded
	// deny is subtracted from, so that a deny-file that stops denying
	// something puts it back to exactly here and no further.
	narrowedSuites []uint16
	narrowedCurves []tls.CurveID
}

// readPolicy reads tls-policy and the keys that may narrow it: min-version,
// cipher-suites, curves, min-rsa-bits, min-ecdsa-bits; then the deny keys.
// tls-policy is required for the same reason ca-trust is -- it is a security
// decision, and a security decision that is defaulted is one that a
// configuration review cannot see.
//
// Every override is an intersection with the profile. A name outside the
// profile is an error naming that entry, whether it is on Go's insecure list
// (reported as INSECURE_SUITE, so the operator knows it is broken and not
// merely disallowed) or is something Go still considers fine but the profile
// does not (reported as NOT_IN_POLICY). Otherwise "hardening configuration"
// would be the mechanism by which a stack gets un-hardened.
func readPolicy(kv utils.JSON) (*DXPolicy, error) {
	profile, _, err := readEnum(kv, "tls-policy", true, policyValues)
	if err != nil {
		return nil, err
	}
	p := &DXPolicy{
		Profile:      profile,
		Curves:       append([]tls.CurveID(nil), allowedCurves...),
		MinRSABits:   MinRSABitsFloor,
		MinECDSABits: MinECDSABitsFloor,
	}
	switch profile {
	case PolicyModern:
		p.MinVersion, p.MaxVersion = tls.VersionTLS13, tls.VersionTLS13
	case PolicyIntermediate:
		p.MinVersion, p.MaxVersion = tls.VersionTLS12, tls.VersionTLS13
		p.CipherSuites = append([]uint16(nil), intermediateSuites12...)
	}

	// min-version may only raise the floor. Under "modern" the only value that
	// is not a widening is "1.3", which is a no-op; "1.2" is refused.
	minVersion, minVersionPresent, err := readMinVersion(kv)
	if err != nil {
		return nil, err
	}
	if minVersionPresent {
		if minVersion < p.MinVersion {
			return nil, configErrorf("min-version", "WIDENS_POLICY:%s:tls-policy=%s_ALREADY_REQUIRES_%s", versionName(minVersion), profile, versionName(p.MinVersion))
		}
		p.MinVersion = minVersion
	}
	if p.MinVersion == tls.VersionTLS13 {
		// Nothing below 1.3 is offered, so the 1.2 list would apply to nothing.
		// Under "intermediate" with min-version 1.3 that is a narrowing the
		// operator asked for; a cipher-suites list beside it is a contradiction.
		p.CipherSuites = nil
	}

	suites, suitesPresent, err := readStringSlice(kv, "cipher-suites")
	if err != nil {
		return nil, err
	}
	if suitesPresent {
		if p.MinVersion == tls.VersionTLS13 {
			return nil, configErrorf("cipher-suites", "NO_EFFECT_UNDER_TLS_1.3:GO_FIXES_THE_1.3_SUITES:REMOVE_THE_KEY")
		}
		p.CipherSuites, err = narrowSuites(suites, intermediateSuites12)
		if err != nil {
			return nil, err
		}
	}

	curves, curvesPresent, err := readStringSlice(kv, "curves")
	if err != nil {
		return nil, err
	}
	if curvesPresent {
		p.Curves, err = narrowCurves(curves)
		if err != nil {
			return nil, err
		}
	}

	p.MinRSABits, err = readFloor(kv, "min-rsa-bits", MinRSABitsFloor)
	if err != nil {
		return nil, err
	}
	p.MinECDSABits, err = readFloor(kv, "min-ecdsa-bits", MinECDSABitsFloor)
	if err != nil {
		return nil, err
	}

	// The denies come last, after every narrowing, so they subtract from what
	// the operator actually ended up with. The lists as they stand here are
	// the base every later deny -- inline now, from the deny-file at startup
	// and on each reload -- is subtracted from.
	p.narrowedSuites, p.narrowedCurves = p.CipherSuites, p.Curves
	if p.Deny, err = readDenyList(kv); err != nil {
		return nil, err
	}
	outcome, err := p.effective(nil)
	if err != nil {
		return nil, err
	}
	p.CipherSuites, p.Curves = outcome.suites, outcome.curves
	return p, nil
}

// effective subtracts the block's deny and an extra list (the deny-file's,
// or nil) from the narrowed base, and refuses a result with nothing left to
// offer. The emptiness check is against a base that had something in it: under
// a 1.3-only policy the 1.2 list is already empty by design, every suite deny
// is a no-op there, and a no-op is not an error.
func (p *DXPolicy) effective(extra *DXDenyList) (denyOutcome, error) {
	o := subtractDeny(p.narrowedSuites, p.narrowedCurves, mergeDenyLists(p.Deny, extra))
	if len(p.narrowedSuites) > 0 && len(o.suites) == 0 {
		return o, configErrorf(keyDenyCipherSuites, "EMPTY_AFTER_DENY:EVERY_TLS_1.2_SUITE_IN_tls-policy=%s_IS_DENIED:YOU_CANNOT_DENY_YOUR_WAY_TO_NO_CIPHERS", p.Profile)
	}
	if len(o.curves) == 0 {
		return o, configErrorf(keyDenyCurves, "EMPTY_AFTER_DENY:EVERY_CURVE_IS_DENIED:YOU_CANNOT_DENY_YOUR_WAY_TO_NO_KEY_EXCHANGE")
	}
	return o, nil
}

// denyReport is the startup and reload log fragment for a deny outcome: what
// was removed, and which tokens removed nothing. It is the one place the
// no-op exception is made visible, and it says why a 1.3-only policy makes
// every suite deny a no-op, so the line is not read as the deny having worked.
func (p *DXPolicy) denyReport(o denyOutcome) string {
	parts := []string{}
	if len(o.removedSuites) > 0 {
		parts = append(parts, "removed-suites=["+strings.Join(o.removedSuites, ",")+"]")
	}
	if len(o.removedCurves) > 0 {
		parts = append(parts, "removed-curves=["+strings.Join(o.removedCurves, ",")+"]")
	}
	if len(o.noOps) > 0 {
		note := ""
		if len(p.narrowedSuites) == 0 {
			note = " (the policy offers no TLS 1.2 suite, and crypto/tls fixes the 1.3 suites, so no suite deny can remove anything here)"
		}
		parts = append(parts, "no-op=["+strings.Join(o.noOps, ",")+"] -- matched nothing in the effective set; not an error, so a fleet-wide deny does not fail closed on a service that was already safe"+note)
	}
	if len(parts) == 0 {
		return "nothing removed"
	}
	return strings.Join(parts, " ")
}

var minVersionValues = []string{"1.2", "1.3"}

// readMinVersion reads the optional min-version. 1.0 and 1.1 are not accepted
// under any spelling. A JSON number (1.2 rather than "1.2") is accepted because
// a hand-written file will sooner or later drop the quotes.
func readMinVersion(kv utils.JSON) (version uint16, present bool, err error) {
	if raw, ok := kv["min-version"]; ok {
		if f, isNumber := raw.(float64); isNumber {
			kv = utils.JSON{"min-version": strconv.FormatFloat(f, 'f', -1, 64)}
		}
	}
	name, present, err := readEnum(kv, "min-version", false, minVersionValues)
	if err != nil || !present {
		return 0, present, err
	}
	if name == "1.3" {
		return tls.VersionTLS13, true, nil
	}
	return tls.VersionTLS12, true, nil
}

// narrowSuites intersects an operator's cipher-suites list with the profile's.
// The result keeps the profile's order, so preference is the profile's and not
// an accident of how the operator typed the list.
func narrowSuites(names []string, profile []uint16) ([]uint16, error) {
	if len(names) == 0 {
		return nil, configErrorf("cipher-suites", "EMPTY_LIST:REMOVE_THE_KEY_TO_USE_THE_POLICY_LIST")
	}
	insecure := map[string]bool{}
	for _, s := range tls.InsecureCipherSuites() {
		insecure[s.Name] = true
	}
	allowed := map[uint16]bool{}
	allowedNames := make([]string, 0, len(profile))
	for _, id := range profile {
		allowed[id] = true
		allowedNames = append(allowedNames, tls.CipherSuiteName(id))
	}
	byName := map[string]uint16{}
	for _, s := range tls.CipherSuites() {
		byName[s.Name] = s.ID
	}
	wanted := map[uint16]bool{}
	for i, n := range names {
		n = strings.TrimSpace(n)
		key := fmt.Sprintf("cipher-suites[%d]", i)
		if insecure[n] {
			return nil, configErrorf(key, "INSECURE_SUITE:%s:ON_GO_INSECURE_LIST", echo(n))
		}
		id, known := byName[n]
		if !known {
			return nil, configErrorf(key, "INVALID_VALUE:%s:VALID_VALUES=%s", echo(n), strings.Join(allowedNames, "|"))
		}
		if !allowed[id] {
			return nil, configErrorf(key, "NOT_IN_POLICY:%s:OVERRIDES_MAY_ONLY_NARROW:VALID_VALUES=%s", echo(n), strings.Join(allowedNames, "|"))
		}
		wanted[id] = true
	}
	var result []uint16
	for _, id := range profile {
		if wanted[id] {
			result = append(result, id)
		}
	}
	return result, nil
}

// narrowCurves intersects an operator's curves list with allowedCurves, in the
// allow-list's order.
func narrowCurves(names []string) ([]tls.CurveID, error) {
	if len(names) == 0 {
		return nil, configErrorf("curves", "EMPTY_LIST:REMOVE_THE_KEY_TO_USE_THE_POLICY_LIST")
	}
	valid := make([]string, 0, len(curveNames))
	for n := range curveNames {
		valid = append(valid, n)
	}
	sort.Strings(valid)
	wanted := map[tls.CurveID]bool{}
	for i, n := range names {
		n = strings.TrimSpace(n)
		id, ok := curveNames[n]
		if !ok {
			return nil, configErrorf(fmt.Sprintf("curves[%d]", i), "NOT_IN_POLICY:%s:OVERRIDES_MAY_ONLY_NARROW:VALID_VALUES=%s", echo(n), strings.Join(valid, "|"))
		}
		wanted[id] = true
	}
	var result []tls.CurveID
	for _, id := range allowedCurves {
		if wanted[id] {
			result = append(result, id)
		}
	}
	return result, nil
}

// readFloor reads a key-size floor that may be raised and not lowered.
func readFloor(kv utils.JSON, key string, floor int) (int, error) {
	raw, present := kv[key]
	if !present {
		return floor, nil
	}
	var v int
	switch n := raw.(type) {
	case float64:
		v = int(n)
	case int:
		v = n
	case int64:
		v = int(n)
	default:
		return 0, configErrorf(key, "WRONG_TYPE:%T:EXPECTED_NUMBER", raw)
	}
	if v < floor {
		return 0, configErrorf(key, "BELOW_FLOOR:%d:MINIMUM_IS_%d", v, floor)
	}
	return v, nil
}

// apply copies the policy onto a tls.Config.
func (p *DXPolicy) apply(c *tls.Config) {
	c.MinVersion = p.MinVersion
	c.MaxVersion = p.MaxVersion
	c.CipherSuites = p.CipherSuites
	c.CurvePreferences = p.Curves
}

// CheckKeyStrength applies the floors to one certificate's public key. Ed25519
// has a single fixed strength and passes. Any other key type is refused: the
// profiles do not speak it, so a certificate carrying it is a mistake.
func (p *DXPolicy) CheckKeyStrength(cert *x509.Certificate) error {
	switch pub := cert.PublicKey.(type) {
	case *rsa.PublicKey:
		if bits := pub.N.BitLen(); bits < p.MinRSABits {
			return fmt.Errorf("KEY_TOO_WEAK:RSA-%d:MINIMUM_RSA-%d:%s", bits, p.MinRSABits, cert.Subject.CommonName)
		}
	case *ecdsa.PublicKey:
		if bits := pub.Curve.Params().BitSize; bits < p.MinECDSABits {
			return fmt.Errorf("KEY_TOO_WEAK:ECDSA-%d:MINIMUM_ECDSA-%d:%s", bits, p.MinECDSABits, cert.Subject.CommonName)
		}
	case ed25519.PublicKey:
	default:
		return fmt.Errorf("UNSUPPORTED_KEY_TYPE:%T:%s", cert.PublicKey, cert.Subject.CommonName)
	}
	return nil
}

// CheckChainStrength applies the floors to every certificate in a verified
// chain. The root is ours and was checked at load; checking it again here
// costs nothing and keeps the rule uniform.
func (p *DXPolicy) CheckChainStrength(chain []*x509.Certificate) error {
	for _, c := range chain {
		if err := p.CheckKeyStrength(c); err != nil {
			return err
		}
	}
	return nil
}

// Summary is the policy as one log line, for the startup log and the preflight
// report: profile, resolved versions, the 1.2 suite list actually offered,
// curves, floors, and the block's own denies.
func (p *DXPolicy) Summary() string {
	deny := ""
	if !p.Deny.IsEmpty() {
		deny = " deny=[" + p.Deny.Summary() + "]"
	}
	return fmt.Sprintf("tls-policy=%s versions=%s..%s suites-1.2=[%s] curves=[%s] min-rsa-bits=%d min-ecdsa-bits=%d%s",
		p.Profile, versionName(p.MinVersion), versionName(p.MaxVersion), suiteNames(p.CipherSuites), strings.Join(curveNamesOf(p.Curves), ","), p.MinRSABits, p.MinECDSABits, deny)
}

// suiteNames is a suite list as the log prints it.
func suiteNames(ids []uint16) string {
	if len(ids) == 0 {
		return "none(1.3-only)"
	}
	names := make([]string, 0, len(ids))
	for _, id := range ids {
		names = append(names, tls.CipherSuiteName(id))
	}
	return strings.Join(names, ",")
}

func curveNamesOf(ids []tls.CurveID) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, curveName(id))
	}
	return out
}

func curveName(id tls.CurveID) string {
	for n, v := range deniableCurveNames {
		if v == id {
			return n
		}
	}
	return id.String()
}

// versionName is the configuration spelling of a crypto/tls version constant.
func versionName(v uint16) string {
	switch v {
	case tls.VersionTLS13:
		return "1.3"
	case tls.VersionTLS12:
		return "1.2"
	case tls.VersionTLS11:
		return "1.1"
	case tls.VersionTLS10:
		return "1.0"
	default:
		return fmt.Sprintf("0x%04x", v)
	}
}

// KeyDescription names a public key for a report: "RSA-2048", "ECDSA-256",
// "Ed25519".
func KeyDescription(pub any) string {
	switch k := pub.(type) {
	case *rsa.PublicKey:
		return fmt.Sprintf("RSA-%d", k.N.BitLen())
	case *ecdsa.PublicKey:
		return fmt.Sprintf("ECDSA-%d", k.Curve.Params().BitSize)
	case ed25519.PublicKey:
		return "Ed25519"
	default:
		return fmt.Sprintf("%T", pub)
	}
}
