// Package tls builds the crypto/tls configuration for the API server and for
// the process's outbound HTTP and WebSocket clients from a configuration block,
// so that every hop that terminates or originates TLS in a dxlib process reads
// its trust the same way and from the same code.
//
// The one rule the package is built around: the trust source is always named
// in configuration and always installed explicitly. crypto/tls treats a nil
// ClientCAs (server) or a nil RootCAs (client) as "use the system roots", which
// is the wrong answer for a service identity inside a bank -- any certificate a
// public CA would issue becomes a valid caller -- and it is reached simply by
// leaving a field out. So ca-trust is a required key with three named values
// and no default, the resulting pool is always assigned, and a pool that turns
// out empty (a distroless image with no /etc/ssl/certs) is refused at startup
// rather than discovered at the first handshake. There is deliberately no value
// meaning "no trust source", which is what makes "refuse to start with no trust
// source" a property of the key rather than a separate check.
//
// Import it under an alias; the name collides with crypto/tls at call sites:
//
//	utilsTLS "github.com/donnyhardyanto/dxlib/utils/tls"
package tls

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// The three trust sources. "custom" is the CAs in ca-files and nothing else;
// "system" is the host root store and nothing else; "system-and-custom" is both.
// A bank's inbound service identity is normally "custom": a single internal CA
// issues every service certificate, and letting a publicly-issued certificate
// through as well would mean anyone who can buy a certificate for a name they
// control can present it as a caller. "system" and "system-and-custom" exist for
// the outbound side, where public upstreams are a fact of life.
const (
	CATrustCustom          = "custom"
	CATrustSystem          = "system"
	CATrustSystemAndCustom = "system-and-custom"
)

// The three transport modes a server block names in its required "mode" key.
// The transport used to be inferred: plaintext from an absent block, TLS from
// "client-auth: none", mTLS from "client-auth: require-and-verify". That is the
// same inference trap ca-trust was introduced to remove -- a reviewer had to
// know what the code did with a combination of keys to read the transport off
// the configuration -- so the transport is now one word, and the keys that
// only make sense under one transport are refused under the others.
//
//   - "http": no TLS. Any TLS-bearing key in the block is a contradiction.
//   - "https": TLS with a server certificate and no client authentication.
//     ca-trust and ca-files are refused here, because on a server they name the
//     pool clients are verified against, and this mode verifies none.
//   - "mtls": TLS with client certificates required and verified.
const (
	ModeHTTP  = "http"
	ModeHTTPS = "https"
	ModeMTLS  = "mtls"
)

var modeValues = []string{ModeHTTP, ModeHTTPS, ModeMTLS}

// The two intermediate rungs of the mTLS rollout ladder, by the names used in
// the optional client-auth-migration key. The key is valid only under
// "mode: mtls" and is the one key in this package that loosens rather than
// narrows: "request" asks for a certificate and verifies nothing, and
// "verify-if-given" verifies a certificate only when one arrives. Both exist
// so a listener can be switched to mTLS in production without refusing a
// caller nobody knew about; neither is a place to stay, which is why the key is
// named as a migration state and warn-logged on every start. Absent means
// crypto/tls RequireAndVerifyClientCert -- full mTLS.
//
// Go's RequireAnyClientCert is left out on purpose: it makes the server demand
// a certificate and then not verify it, so the "identity" it yields is whatever
// the peer typed in, and nothing here would be able to tell the difference.
const (
	ClientAuthMigrationRequest       = "request"
	ClientAuthMigrationVerifyIfGiven = "verify-if-given"
)

var clientAuthMigrationValues = []string{ClientAuthMigrationRequest, ClientAuthMigrationVerifyIfGiven}

// retiredKeys are keys an earlier shape of the block carried, each mapped to
// what replaced it. A block that still carries one is refused with the
// replacement named, rather than the key being ignored as unknown: under
// "mode: https" a leftover "client-auth: require-and-verify" would otherwise
// sit in the file saying something the listener does not do.
var retiredKeys = map[string]string{
	"client-auth": "mode=" + strings.Join(modeValues, "|") + "_AND_OPTIONALLY:client-auth-migration=" + strings.Join(clientAuthMigrationValues, "|"),
}

// DefaultMinVersion is TLS 1.2. It is the lowest version with no known
// protocol-level weakness and the highest that every party inside a cluster
// can be assumed to speak. A compliance-pinned deployment sets min-version
// explicitly rather than relying on this; the effective value is logged either
// way.
const DefaultMinVersion = tls.VersionTLS12

// validValueEchoLimit caps how much of an unrecognised value is echoed in an
// error. The value is also %q-quoted, so a newline or control character inside
// it cannot split or forge a log line.
const validValueEchoLimit = 64

// DXConfigError is a configuration mistake in a tls block. Key is the path of
// the offending key relative to the block ("ca-trust", "ca-files[1]"), and
// Detail is what is wrong with it. Callers prefix the block's own location.
//
// The error text is built only from the key path, the value's type and, for an
// unrecognised enumerated value, a capped and quoted echo of the value. It never
// includes the enclosing map. utils/json.GetString formats "can not get %s as
// %T from %v" with the whole map on failure, and a configuration map is exactly
// where key material could sit, so none of its error text is allowed to reach a
// log from here.
type DXConfigError struct {
	Key    string
	Detail string
}

func (e *DXConfigError) Error() string {
	return e.Key + ":" + e.Detail
}

func configErrorf(key, format string, args ...any) error {
	return &DXConfigError{Key: key, Detail: fmt.Sprintf(format, args...)}
}

// echo formats a rejected value for an error message: quoted, and cut at
// validValueEchoLimit so a long value cannot flood the line.
func echo(v string) string {
	if len(v) > validValueEchoLimit {
		v = v[:validValueEchoLimit] + "..."
	}
	return strconv.Quote(v)
}

// readString reads an optional string key. present reports whether the key was
// there at all, which is what lets a caller tell "missing" from "" -- two
// different operator mistakes that get two different messages.
func readString(kv utils.JSON, key string) (value string, present bool, err error) {
	raw, present := kv[key]
	if !present {
		return "", false, nil
	}
	switch v := raw.(type) {
	case string:
		return v, true, nil
	case []byte:
		return string(v), true, nil
	default:
		return "", true, configErrorf(key, "WRONG_TYPE:%T:EXPECTED_STRING", raw)
	}
}

// readStringSlice reads an optional list of strings. JSON decoding yields
// []any; a configuration built in Go yields []string; both are accepted. A bare
// string is not -- "ca-files": "/one.pem" is refused rather than guessed at.
func readStringSlice(kv utils.JSON, key string) (values []string, present bool, err error) {
	raw, present := kv[key]
	if !present {
		return nil, false, nil
	}
	switch v := raw.(type) {
	case []string:
		return append([]string(nil), v...), true, nil
	case []any:
		values = make([]string, 0, len(v))
		for i, item := range v {
			s, ok := item.(string)
			if !ok {
				return nil, true, configErrorf(fmt.Sprintf("%s[%d]", key, i), "WRONG_TYPE:%T:EXPECTED_STRING", item)
			}
			values = append(values, s)
		}
		return values, true, nil
	default:
		return nil, true, configErrorf(key, "WRONG_TYPE:%T:EXPECTED_LIST_OF_STRING", raw)
	}
}

// readBool reads an optional boolean. Only a JSON boolean is accepted; the
// number and string coercions utils/json.GetBool performs are not wanted for a
// security switch.
func readBool(kv utils.JSON, key string, defaultValue bool) (bool, error) {
	raw, present := kv[key]
	if !present {
		return defaultValue, nil
	}
	v, ok := raw.(bool)
	if !ok {
		return false, configErrorf(key, "WRONG_TYPE:%T:EXPECTED_BOOLEAN", raw)
	}
	return v, nil
}

// readEnum reads a required enumerated key and resolves it to its canonical
// form. Surrounding whitespace is trimmed and the comparison is
// case-insensitive: explicitness is about the trust source being named in the
// configuration, not about capitalisation, and a hand-edited dev file refusing
// to boot over "System" is friction with no security in it, while a trailing
// space inside a JSON string is close to invisible when it does. What was
// understood is logged in canonical form, so there is never a question of which
// spelling won.
//
// Missing, empty and unrecognised are three different mistakes and get three
// different messages, each listing the valid values -- with three of them, the
// list is the whole "did you mean".
func readEnum(kv utils.JSON, key string, required bool, valid []string) (canonical string, present bool, err error) {
	validList := strings.Join(valid, "|")
	raw, present, err := readString(kv, key)
	if err != nil {
		return "", present, err
	}
	if !present {
		if required {
			return "", false, configErrorf(key, "REQUIRED_KEY_MISSING:VALID_VALUES=%s", validList)
		}
		return "", false, nil
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", true, configErrorf(key, "EMPTY_VALUE:VALID_VALUES=%s", validList)
	}
	for _, v := range valid {
		if strings.EqualFold(trimmed, v) {
			return v, true, nil
		}
	}
	return "", true, configErrorf(key, "INVALID_VALUE:%s:VALID_VALUES=%s", echo(raw), validList)
}

var caTrustValues = []string{CATrustCustom, CATrustSystem, CATrustSystemAndCustom}

// readCATrust reads ca-trust and ca-files together, because they are only
// meaningful together and a contradiction between them is a mistake to report,
// not a list to drop on the floor: files given under "system" are refused, and
// "custom" or "system-and-custom" with no files is refused.
func readCATrust(kv utils.JSON) (mode string, files []string, err error) {
	mode, _, err = readEnum(kv, "ca-trust", true, caTrustValues)
	if err != nil {
		return "", nil, err
	}
	files, filesPresent, err := readStringSlice(kv, "ca-files")
	if err != nil {
		return "", nil, err
	}
	for i, f := range files {
		if strings.TrimSpace(f) == "" {
			return "", nil, configErrorf(fmt.Sprintf("ca-files[%d]", i), "EMPTY_VALUE")
		}
	}
	switch mode {
	case CATrustSystem:
		if filesPresent && len(files) > 0 {
			return "", nil, configErrorf("ca-files", "NOT_ALLOWED_WITH:ca-trust=%s:REMOVE_THE_LIST_OR_USE:ca-trust=%s", CATrustSystem, CATrustSystemAndCustom)
		}
	default:
		if len(files) == 0 {
			return "", nil, configErrorf("ca-files", "REQUIRED_WITH:ca-trust=%s:GIVE_AT_LEAST_ONE_PEM_FILE", mode)
		}
	}
	return mode, files, nil
}

// ClientAuthName is the crypto/tls client-authentication type by the name the
// log and the preflight report use for it. It is the mode the listener is
// actually in, which under "mode: mtls" with a client-auth-migration rung is
// not full mTLS -- which is exactly what the warn line has to be able to say.
func ClientAuthName(t tls.ClientAuthType) string {
	switch t {
	case tls.NoClientCert:
		return "none"
	case tls.RequestClientCert:
		return "request"
	case tls.VerifyClientCertIfGiven:
		return "verify-if-given"
	case tls.RequireAndVerifyClientCert:
		return "require-and-verify"
	case tls.RequireAnyClientCert:
		return "require-any(unverified)"
	}
	return fmt.Sprintf("ClientAuthType(%d)", int(t))
}

// clientAuthVerifies reports whether a client-auth type ever verifies a
// presented certificate. Under NoClientCert and RequestClientCert a
// certificate may sit in PeerCertificates without having been checked against
// anything, which is why an identity allow-list is a contradiction with those.
func clientAuthVerifies(t tls.ClientAuthType) bool {
	return t == tls.VerifyClientCertIfGiven || t == tls.RequireAndVerifyClientCert
}

// refuseRetiredKeys reports the first retired key present in the block.
func refuseRetiredKeys(kv utils.JSON) error {
	for key, replacement := range retiredKeys {
		if _, present := kv[key]; present {
			return configErrorf(key, "RETIRED_KEY:USE:%s", replacement)
		}
	}
	return nil
}

// refuseKeysUnder reports the first of keys present in the block, as a
// contradiction with the named mode. It is how "mode: http" refuses a
// cert-file and "mode: https" refuses a ca-trust: the key is not ignored, the
// operator is told which key cannot be there and what would make it legal.
func refuseKeysUnder(kv utils.JSON, mode string, keys []string, alternative string) error {
	for _, key := range keys {
		if _, present := kv[key]; present {
			return configErrorf(key, "NOT_ALLOWED_WITH:mode=%s:REMOVE_THE_KEY_OR_SET:%s", mode, alternative)
		}
	}
	return nil
}

// readCertPair reads cert-file and key-file. required says whether the pair
// may be absent altogether (an outbound client that does plain TLS has no
// certificate); one half without the other is a mistake either way.
func readCertPair(kv utils.JSON, required bool) (certFile, keyFile string, err error) {
	certFile, certPresent, err := readString(kv, "cert-file")
	if err != nil {
		return "", "", err
	}
	keyFile, keyPresent, err := readString(kv, "key-file")
	if err != nil {
		return "", "", err
	}
	certFile = strings.TrimSpace(certFile)
	keyFile = strings.TrimSpace(keyFile)
	switch {
	case !certPresent && !keyPresent:
		if required {
			return "", "", configErrorf("cert-file", "REQUIRED_KEY_MISSING:PEM_PATH_OF_THE_SERVER_CERTIFICATE")
		}
		return "", "", nil
	case certPresent && !keyPresent:
		return "", "", configErrorf("key-file", "REQUIRED_WITH:cert-file")
	case !certPresent && keyPresent:
		return "", "", configErrorf("cert-file", "REQUIRED_WITH:key-file")
	case certFile == "":
		return "", "", configErrorf("cert-file", "EMPTY_VALUE")
	case keyFile == "":
		return "", "", configErrorf("key-file", "EMPTY_VALUE")
	}
	return certFile, keyFile, nil
}

// loadCAFile parses every CERTIFICATE block in one PEM file, which may be a
// bundle. A file with no certificate in it is a misconfiguration -- the wrong
// path, or a key where a certificate was meant -- and is refused rather than
// contributing nothing in silence.
func loadCAFile(path string) ([]*x509.Certificate, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Wrapf(err, "CANNOT_READ:%s", path)
	}
	var certs []*x509.Certificate
	rest := data
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, errors.Wrapf(err, "CANNOT_PARSE_CERTIFICATE:%s", path)
		}
		certs = append(certs, cert)
	}
	if len(certs) == 0 {
		return nil, errors.Errorf("NO_CERTIFICATE_FOUND:%s", path)
	}
	return certs, nil
}

// newSystemPool returns a copy of the host root store, refusing an empty one.
//
// Go reports an empty store as success: on Linux, loadSystemRoots returns a
// pool with no certificates and a nil error when none of the well-known files
// or directories exist, which is exactly the state of a distroless or scratch
// image. Nothing would be wrong until the first handshake, which would then
// fail with an "unknown authority" that looks like the peer's fault.
//
// The emptiness check is CertPool.Equal against a fresh, empty pool. Equal
// compares the certificate set and the systemPool flag; on Linux the pool is
// loaded from files and the flag is off, so an empty store is Equal to an empty
// pool. On macOS and Windows the store is the platform verifier, the flag is
// on, and Equal is false regardless -- the check is a no-op there, which is
// right, because on those platforms Go cannot enumerate the store and the
// distroless failure mode does not exist. The deprecated Subjects() would also
// have been empty for a platform pool, which is why it is not used.
func newSystemPool() (*x509.CertPool, error) {
	pool, err := x509.SystemCertPool()
	if err != nil {
		return nil, errors.Wrap(err, "SYSTEM_ROOTS_UNAVAILABLE")
	}
	if pool == nil || pool.Equal(x509.NewCertPool()) {
		return nil, errors.New("SYSTEM_ROOTS_EMPTY:THE_HOST_HAS_NO_ROOT_STORE(DISTROLESS_IMAGE?):USE_ca-trust=custom_OR_ADD_ca-certificates")
	}
	return pool, nil
}

// buildPool assembles the trust pool for a mode. The system store, when the
// mode includes it, is checked for emptiness on its own before anything is
// appended: under "system-and-custom" a merged pool is never empty, so a
// post-merge check would let a distroless image quietly degrade to
// custom-only, which is the emergent behaviour this package exists to remove.
//
// certs and count are the certificates loaded from files. The system store is
// not counted: Go does not expose its size without the deprecated Subjects(),
// and on a platform verifier there is no enumerable size at all.
func buildPool(mode string, files []string) (pool *x509.CertPool, certs []*x509.Certificate, count int, err error) {
	switch mode {
	case CATrustSystem, CATrustSystemAndCustom:
		pool, err = newSystemPool()
		if err != nil {
			return nil, nil, 0, &DXConfigError{Key: "ca-trust", Detail: err.Error()}
		}
	case CATrustCustom:
		pool = x509.NewCertPool()
	default:
		return nil, nil, 0, configErrorf("ca-trust", "INVALID_VALUE:%s:VALID_VALUES=%s", echo(mode), strings.Join(caTrustValues, "|"))
	}
	for i, f := range files {
		fileCerts, err := loadCAFile(f)
		if err != nil {
			return nil, nil, 0, &DXConfigError{Key: fmt.Sprintf("ca-files[%d]", i), Detail: err.Error()}
		}
		for _, c := range fileCerts {
			pool.AddCert(c)
		}
		certs = append(certs, fileCerts...)
		count += len(fileCerts)
	}
	if mode == CATrustCustom && count == 0 {
		// Unreachable through readCATrust, which insists on at least one file,
		// and loadCAFile, which insists on at least one certificate per file;
		// kept so the invariant "the pool is never empty" does not depend on
		// the callers staying that way.
		return nil, nil, 0, configErrorf("ca-files", "NO_CERTIFICATE_LOADED")
	}
	return pool, certs, count, nil
}
