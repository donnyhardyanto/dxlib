package tls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DXServerTLSSettings is a parsed and validated server tls block. Every field
// that is a security decision -- the transport mode, the policy, the trust
// source -- was named in configuration; nothing here was defaulted into place.
type DXServerTLSSettings struct {
	Enabled bool
	// Mode is the transport: ModeHTTP, ModeHTTPS or ModeMTLS. Under ModeHTTP
	// every other field is zero and no TLS state is built.
	Mode     string
	CertFile string
	KeyFile  string
	Policy   *DXPolicy // nil under ModeHTTP
	// ClientAuthMigration is the client-auth-migration key as written, or ""
	// when absent, which is the enforcing state. Only ever set under ModeMTLS.
	ClientAuthMigration string
	// ClientAuth is the crypto/tls mode the listener actually runs, and
	// ClientAuthName is its name for the log: NoClientCert under https,
	// RequireAndVerifyClientCert under mtls, or the migration rung.
	ClientAuth     tls.ClientAuthType
	ClientAuthName string
	CATrust        string   // "" under https, where no client is verified
	CAFiles        []string // likewise
	// DenyFile is the path of the hot-reloaded deny-file, or "".
	DenyFile          string
	AllowedClientSANs []string
	// AllowedClientSANsLogOnly makes the allow-list observe instead of
	// enforce: a caller whose SANs match nothing is logged as would-have-been
	// refused and admitted anyway. It exists so an allow-list can be switched
	// on in production and read for a week before it starts refusing anyone.
	AllowedClientSANsLogOnly bool
}

// serverTLSKeys is every key a server block may carry besides mode and
// enabled: the list "mode: http" refuses by name, so a block that says
// plaintext and also names a certificate is reported as the contradiction it
// is rather than having the certificate ignored.
var serverTLSKeys = []string{
	"cert-file", "key-file",
	"tls-policy", "min-version", "cipher-suites", "curves", "min-rsa-bits", "min-ecdsa-bits",
	"ca-trust", "ca-files",
	"client-auth-migration",
	"allowed-client-sans", "allowed-client-sans-log-only",
	keyDenyCipherSuites, keyDenyCurves, keyDenyCertificates, keyDenySignatureAlgorithms, keyDenyFile,
}

// httpsRefusedKeys are the keys that only mean something when clients are
// verified, and "mode: https" verifies none. ca-trust and ca-files on a server
// name the pool clients are checked against; an allow-list of client SANs has
// no verified SAN to match; a migration rung is a step towards mTLS, not
// something https is on the way to.
var httpsRefusedKeys = []string{"ca-trust", "ca-files", "client-auth-migration", "allowed-client-sans", "allowed-client-sans-log-only"}

// ParseServerSettings validates a server tls block without touching the
// filesystem. It is the whole of what runs for a block with enabled=false: a
// block that is present is checked in full whether or not it is in force,
// because a typo behind enabled=false is still a typo, and it is a great deal
// cheaper to find on the day it is written than on the day someone flips the
// switch in production. Only the parts that need the files -- loading them,
// building the pool, reading the deny-file -- wait for NewServerTLS.
func ParseServerSettings(kv utils.JSON) (*DXServerTLSSettings, error) {
	s := &DXServerTLSSettings{}
	var err error

	if s.Enabled, err = readBool(kv, "enabled", true); err != nil {
		return nil, err
	}
	if err = refuseRetiredKeys(kv); err != nil {
		return nil, err
	}
	if s.Mode, _, err = readEnum(kv, "mode", true, modeValues); err != nil {
		return nil, err
	}

	switch s.Mode {
	case ModeHTTP:
		// Plaintext, said out loud. Anything TLS-bearing beside it is a
		// contradiction, named.
		if err = refuseKeysUnder(kv, ModeHTTP, serverTLSKeys, "mode="+ModeHTTPS+"|"+ModeMTLS); err != nil {
			return nil, err
		}
		return s, nil
	case ModeHTTPS:
		if err = refuseKeysUnder(kv, ModeHTTPS, httpsRefusedKeys, "mode="+ModeMTLS); err != nil {
			return nil, err
		}
		s.ClientAuth = tls.NoClientCert
	case ModeMTLS:
		if s.CATrust, s.CAFiles, err = readCATrust(kv); err != nil {
			return nil, err
		}
		s.ClientAuth = tls.RequireAndVerifyClientCert
		if s.ClientAuthMigration, _, err = readEnum(kv, "client-auth-migration", false, clientAuthMigrationValues); err != nil {
			return nil, err
		}
		switch s.ClientAuthMigration {
		case ClientAuthMigrationRequest:
			s.ClientAuth = tls.RequestClientCert
		case ClientAuthMigrationVerifyIfGiven:
			s.ClientAuth = tls.VerifyClientCertIfGiven
		}
	}
	s.ClientAuthName = ClientAuthName(s.ClientAuth)

	if s.CertFile, s.KeyFile, err = readCertPair(kv, true); err != nil {
		return nil, err
	}
	if s.Policy, err = readPolicy(kv); err != nil {
		return nil, err
	}
	denyFile, denyFilePresent, err := readString(kv, keyDenyFile)
	if err != nil {
		return nil, err
	}
	if denyFilePresent {
		if s.DenyFile = strings.TrimSpace(denyFile); s.DenyFile == "" {
			return nil, configErrorf(keyDenyFile, "EMPTY_VALUE:REMOVE_THE_KEY_OR_GIVE_A_PATH")
		}
	}

	sans, sansPresent, err := readStringSlice(kv, "allowed-client-sans")
	if err != nil {
		return nil, err
	}
	if sansPresent {
		if len(sans) == 0 {
			return nil, configErrorf("allowed-client-sans", "EMPTY_LIST:REMOVE_THE_KEY_OR_NAME_AT_LEAST_ONE_SAN")
		}
		for i, san := range sans {
			if strings.TrimSpace(san) == "" {
				return nil, configErrorf(fmt.Sprintf("allowed-client-sans[%d]", i), "EMPTY_VALUE")
			}
		}
		if !clientAuthVerifies(s.ClientAuth) {
			// Under the "request" rung a presented certificate is never
			// verified, so a SAN in it is an unverified claim; matching it
			// against an allow-list would authorize whatever the peer typed.
			// (Under https the key was already refused above.)
			return nil, configErrorf("allowed-client-sans", "REQUIRES_A_VERIFYING_MODE:REMOVE:client-auth-migration=%s:OR_USE:client-auth-migration=%s", s.ClientAuthMigration, ClientAuthMigrationVerifyIfGiven)
		}
		s.AllowedClientSANs = sans
	}
	if s.AllowedClientSANsLogOnly, err = readBool(kv, "allowed-client-sans-log-only", false); err != nil {
		return nil, err
	}
	if s.AllowedClientSANsLogOnly && !sansPresent {
		return nil, configErrorf("allowed-client-sans-log-only", "REQUIRES:allowed-client-sans")
	}
	return s, nil
}

// DXServerTLS is a server's live TLS state: the settings, the tls.Config to
// hand to http.Server, and the reloaders behind its hooks.
type DXServerTLS struct {
	Settings    *DXServerTLSSettings
	Config      *tls.Config
	Certificate *DXCertificateReloader
	ClientCAs   *DXCAPoolReloader   // nil under https, where no client is verified
	DenyFile    *DXDenyFileReloader // nil when no deny-file is configured

	// current is the config clone served after a CA-pool or deny-file reload,
	// or nil while the original is still current. See getConfigForClient.
	current atomic.Pointer[tls.Config]
	// deny is the certificate-level deny list in force: the block's own merged
	// with the deny-file's as of its last reload. verifyConnection reads it on
	// every handshake, which is how a pushed deny-file refuses a certificate
	// without a restart.
	deny atomic.Pointer[DXDenyList]
}

// NewServerTLS parses the block, loads the files and builds the config. Any
// failure is a startup error: a process that boots with an unresolved trust
// source and then makes accept/reject decisions is strictly worse than one
// that never boots. Under Kubernetes the pod goes to CrashLoopBackOff with the
// reason in its log, the rollout stalls, and the previous healthy pods keep
// serving -- the right blast radius for a misconfigured trust store.
//
// A block with enabled=false, or with mode=http, is validated and then returns
// (settings, nil, nil): no files are read and no config is built. For
// enabled=false that is so a dev host without the production certificate
// paths can still start; for mode=http there is nothing to build.
func NewServerTLS(kv utils.JSON) (*DXServerTLS, error) {
	settings, err := ParseServerSettings(kv)
	if err != nil {
		return nil, err
	}
	s := &DXServerTLS{Settings: settings}
	if !settings.Enabled || settings.Mode == ModeHTTP {
		return s, nil
	}

	s.Certificate, err = NewCertificateReloader(settings.CertFile, settings.KeyFile, settings.Policy.CheckKeyStrength)
	if err != nil {
		return nil, err
	}
	s.Certificate.OnReload = func(leaf *x509.Certificate) { observeCertificate("server", settings.CertFile, leaf) }
	observeCertificate("server", settings.CertFile, s.Certificate.Leaf())

	// ClientCAs is never nil. Under mtls it is the pool the operator named,
	// including under a migration rung where Go may not consult it. Under
	// https there is no named pool, and the field is set to an empty one
	// rather than left nil: crypto/tls reads a nil ClientCAs as "the system
	// roots", so if anything ever raised ClientAuth on this config the empty
	// pool fails closed where a nil would have admitted every public CA.
	pool := x509.NewCertPool()
	if settings.Mode == ModeMTLS {
		s.ClientCAs, err = NewCAPoolReloader(settings.CATrust, settings.CAFiles, settings.Policy.CheckKeyStrength)
		if err != nil {
			return nil, err
		}
		pool = s.ClientCAs.Pool()
	}

	// The deny-file, if any. Its list is subtracted from the policy now, so
	// the suites and curves on the config -- and the ones the startup line
	// reports -- are the ones in force, and the merged certificate denies are
	// stored for verifyConnection.
	var fileDeny *DXDenyList
	if settings.DenyFile != "" {
		s.DenyFile, err = NewDenyFileReloader(settings.DenyFile, func(list *DXDenyList) error {
			_, err := settings.Policy.effective(list)
			return err
		})
		if err != nil {
			return nil, err
		}
		fileDeny = s.DenyFile.List()
	}
	outcome, err := settings.Policy.effective(fileDeny)
	if err != nil {
		return nil, err
	}
	settings.Policy.CipherSuites, settings.Policy.Curves = outcome.suites, outcome.curves
	s.deny.Store(mergeDenyLists(settings.Policy.Deny, fileDeny))

	s.Config = &tls.Config{
		GetCertificate:     s.Certificate.GetCertificate,
		ClientAuth:         settings.ClientAuth,
		ClientCAs:          pool,
		GetConfigForClient: s.getConfigForClient,
		VerifyConnection:   s.verifyConnection,
	}
	settings.Policy.apply(s.Config)

	if settings.ClientAuthMigration != "" {
		// Every start, not once: the whole point of naming this a migration
		// state is that it should not be possible to forget it is on.
		log.Log.Warnf("TLS server: client-auth-migration=%s -- this listener is NOT yet enforcing mTLS; effective client-auth=%s (%s). Remove the key when every caller presents a valid certificate.",
			settings.ClientAuthMigration, settings.ClientAuthName, migrationMeaning(settings.ClientAuth))
	}
	if deny := s.deny.Load(); !deny.IsEmpty() {
		log.Log.Infof("TLS server deny: [%s] %s", deny.Summary(), settings.Policy.denyReport(outcome))
	}
	log.Log.Infof("TLS server: %s", s.Summary())
	return s, nil
}

// migrationMeaning spells out what a rung does, for the warn line.
func migrationMeaning(t tls.ClientAuthType) string {
	switch t {
	case tls.RequestClientCert:
		return "a certificate is asked for; whether one arrives, and whether it is valid, changes nothing"
	case tls.VerifyClientCertIfGiven:
		return "a certificate that arrives must be valid; a caller that sends none is still admitted"
	}
	return "enforcing"
}

// BuildServerConfig is the one-call form for a caller that only wants the
// tls.Config. It returns nil, nil for a block with enabled=false or mode=http.
func BuildServerConfig(kv utils.JSON) (*tls.Config, error) {
	s, err := NewServerTLS(kv)
	if err != nil {
		return nil, err
	}
	return s.Config, nil
}

// getConfigForClient is the tls.Config.GetConfigForClient hook. It returns nil
// -- keep using the config already in use -- unless the CA files or the
// deny-file changed, in which case it returns a clone carrying the new pool
// and the policy with the new denies subtracted, and caches it for the
// handshakes that follow. Go swaps to the returned config before choosing the
// version, suite and curve, so a suite the deny-file just took out is not
// offered to this handshake.
//
// The pool on the clone is always the reloader's current pool, not the
// original config's: a deny-file change after a CA rotation must not put the
// retired pool back.
//
// The clone is taken from s.Config. That is only equivalent to the config in
// use if s.Config carries the same NextProtos, and on this toolchain it does
// not by itself: http.Server.ServeTLS puts "h2" and "http/1.1" on a private
// clone (setupTLSConfig) and leaves TLSConfig untouched. A clone made here
// from a config with no NextProtos would negotiate no ALPN, and every
// connection after the first reload would silently drop to HTTP/1.1.
// ConfigForHTTPServer pins the list up front so the two agree; the CA-reload
// test asserts HTTP/2 after the rotation to keep it that way.
func (s *DXServerTLS) getConfigForClient(*tls.ClientHelloInfo) (*tls.Config, error) {
	changed := false
	var pool *x509.CertPool
	if s.ClientCAs != nil {
		var poolChanged bool
		pool, poolChanged = s.ClientCAs.Get()
		changed = changed || poolChanged
	}
	var fileDeny *DXDenyList
	if s.DenyFile != nil {
		var denyChanged bool
		fileDeny, denyChanged = s.DenyFile.Get()
		changed = changed || denyChanged
	}
	if !changed {
		return s.current.Load(), nil
	}
	clone := s.Config.Clone()
	clone.GetConfigForClient = nil
	if pool != nil {
		clone.ClientCAs = pool
	}
	if s.DenyFile != nil {
		outcome, err := s.Settings.Policy.effective(fileDeny)
		if err != nil {
			// Unreachable: the reloader's Check refused any list that would
			// empty the policy before it became current. Kept so the invariant
			// does not depend on that staying true.
			log.Log.Warnf("TLS_DENY_FILE_NOT_APPLIED:%v:KEEPING_CURRENT_CONFIG", err)
			return s.current.Load(), nil
		}
		clone.CipherSuites, clone.CurvePreferences = outcome.suites, outcome.curves
		s.deny.Store(mergeDenyLists(s.Settings.Policy.Deny, fileDeny))
		log.Log.Infof("TLS_DENY_FILE_APPLIED:%s:suites-1.2=[%s] curves=[%s] %s", s.DenyFile.Path, suiteNames(outcome.suites), strings.Join(curveNamesOf(outcome.curves), ","), s.Settings.Policy.denyReport(outcome))
	}
	s.current.Store(clone)
	return clone, nil
}

// ConfigForHTTPServer returns the config to assign to http.Server.TLSConfig,
// with NextProtos pinned to the two protocols http.Server negotiates by
// default. Go's own setup would arrive at the same list on its private clone;
// naming it here is what keeps the clone getConfigForClient hands back after a
// reload identical to the one in use before it. A caller that disables HTTP/2
// on its http.Server should not use this method.
func (s *DXServerTLS) ConfigForHTTPServer() *tls.Config {
	if s == nil || s.Config == nil {
		return nil
	}
	if len(s.Config.NextProtos) == 0 {
		s.Config.NextProtos = []string{"h2", "http/1.1"}
	}
	return s.Config
}

// verifyConnection runs inside the handshake, after Go has verified the chain
// and before any byte of HTTP is read. A rejection here is a handshake
// failure: the caller sees a bad-certificate alert and no request is ever
// constructed, so nothing downstream -- not PreProcessRequest, not the E2EE
// unpack, not a middleware -- runs for a caller that is not allowed in.
//
// It does four things, in order, and each is a check Go does not do itself:
//
//  1. The deny list, over every certificate in every verified chain: a
//     denied key or certificate anywhere in the peer's provenance, or a
//     denied signature algorithm on anything but the trust anchor, refuses the
//     peer. This runs first so the log says REVOKED for a revoked
//     certificate even when it would also have failed a later check.
//  2. Key strength. Every certificate in the verified chain has to meet the
//     policy floor. Go's own floor is 1024-bit RSA.
//  3. No CA as a leaf. A certificate with BasicConstraints CA:TRUE chains
//     fine as a leaf; refusing it here closes the door on a mis-issued
//     intermediate being used as a client identity.
//  4. The SAN allow-list, when one is configured, in enforce or log-only mode.
//
// Go does already require ExtKeyUsageClientAuth on a verified client
// certificate (crypto/tls processCertsFromClient sets KeyUsages to it), so
// that is not duplicated here.
//
// When VerifiedChains is empty nothing was verified -- mode is https, or the
// migration rung is "request", or it is "verify-if-given" and no certificate
// came -- and there is nothing to authorize. Go has already enforced whether a
// certificate had to be present. Under "request", PeerCertificates may hold an
// unverified certificate; it is deliberately not looked at.
func (s *DXServerTLS) verifyConnection(cs tls.ConnectionState) error {
	if len(cs.VerifiedChains) == 0 || len(cs.VerifiedChains[0]) == 0 {
		return nil
	}
	chain := cs.VerifiedChains[0]
	leaf := chain[0]
	if deny := s.deny.Load(); !deny.IsEmpty() {
		for _, verified := range cs.VerifiedChains {
			if err := checkDeniedChain(deny, verified, true); err != nil {
				log.Log.Warnf("TLS_CLIENT_REJECTED:%s:%v", HandshakeClassRevoked, err)
				return err
			}
		}
	}
	if err := s.Settings.Policy.CheckChainStrength(chain); err != nil {
		log.Log.Warnf("TLS_CLIENT_REJECTED:KEY_STRENGTH:%s:%v", PeerIdentity(leaf), err)
		return err
	}
	if leaf.IsCA {
		log.Log.Warnf("TLS_CLIENT_REJECTED:CA_CERTIFICATE_AS_LEAF:%s", PeerIdentity(leaf))
		return errors.Errorf("TLS_PEER_IS_A_CA_CERTIFICATE:%s", PeerIdentity(leaf))
	}
	if len(s.Settings.AllowedClientSANs) > 0 && !MatchesAllowedSAN(leaf, s.Settings.AllowedClientSANs) {
		if s.Settings.AllowedClientSANsLogOnly {
			log.Log.Warnf("TLS_CLIENT_WOULD_BE_REJECTED:NOT_IN_ALLOWED_CLIENT_SANS:%s:sans=%v:LOG_ONLY_MODE_ADMITTED", PeerIdentity(leaf), SANs(leaf))
			return nil
		}
		log.Log.Warnf("TLS_CLIENT_REJECTED:NOT_IN_ALLOWED_CLIENT_SANS:%s:sans=%v", PeerIdentity(leaf), SANs(leaf))
		return errors.Errorf("TLS_PEER_NOT_ALLOWED:%s", PeerIdentity(leaf))
	}
	return nil
}

// DenyList is the certificate-level deny list currently in force: the block's
// own merged with the deny-file's as of its last reload. For the preflight
// and for tests.
func (s *DXServerTLS) DenyList() *DXDenyList {
	return s.deny.Load()
}

// Summary is the effective posture as one line: what an operator or an
// auditor needs to read the enforced configuration out of the log without
// inferring it from what is absent.
func (s *DXServerTLS) Summary() string {
	st := s.Settings
	if st.Mode == ModeHTTP {
		return fmt.Sprintf("mode=%s (plaintext; no TLS) enabled=%t", st.Mode, st.Enabled)
	}
	auth := "client-auth=" + st.ClientAuthName
	if st.ClientAuthMigration != "" {
		auth += fmt.Sprintf(" client-auth-migration=%s (NOT ENFORCING MTLS)", st.ClientAuthMigration)
	}
	trust := ""
	if st.Mode == ModeMTLS {
		trust = " ca-trust=" + st.CATrust
	}
	if !st.Enabled {
		return fmt.Sprintf("enabled=false (block validated, TLS not in force) mode=%s %s%s %s deny-file=%s", st.Mode, auth, trust, st.Policy.Summary(), orNone(st.DenyFile))
	}
	allow := "none"
	if len(st.AllowedClientSANs) > 0 {
		mode := "enforce"
		if st.AllowedClientSANsLogOnly {
			mode = "log-only"
		}
		allow = fmt.Sprintf("%d entries (%s)", len(st.AllowedClientSANs), mode)
	}
	if st.Mode == ModeMTLS {
		caCerts := fmt.Sprintf("%d", s.ClientCAs.Count())
		if st.CATrust != CATrustCustom {
			caCerts += "+system"
		}
		trust += fmt.Sprintf(" ca-certs=%s ca-files=%v", caCerts, st.CAFiles)
	}
	leaf := s.Certificate.Leaf()
	return fmt.Sprintf("mode=%s cert=%s key=%s subject=%q not-after=%s %s%s allowed-client-sans=%s deny-file=%s deny-in-force=[%s] %s",
		st.Mode, st.CertFile, st.KeyFile, leaf.Subject.CommonName, leaf.NotAfter.UTC().Format("2006-01-02T15:04:05Z"),
		auth, trust, allow, orNone(st.DenyFile), s.deny.Load().Summary(), st.Policy.Summary())
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}
