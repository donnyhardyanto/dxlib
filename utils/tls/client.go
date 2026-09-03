package tls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"strings"
	"sync/atomic"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DXClientTLSSettings is a parsed and validated client tls block: what this
// process presents and what it trusts when it dials out.
type DXClientTLSSettings struct {
	Enabled  bool
	CertFile string // empty when the client presents no certificate
	KeyFile  string
	Policy   *DXPolicy
	CATrust  string
	CAFiles  []string
	// DenyFile is the path of the hot-reloaded deny-file, or "".
	DenyFile string
	// ServerName is the name the peer's certificate is verified against when
	// it is not the host in the URL. An in-cluster call to https://svc:8443
	// has a host the certificate may not carry; naming the SAN it does carry is
	// the escape hatch that is not InsecureSkipVerify.
	ServerName string
	// InsecureSkipVerify turns off verification of the peer. It is here so a
	// developer can reach a self-signed endpoint without generating a CA, is
	// off unless written down, and is warned about on every start. It does not
	// exist on the server side at all.
	InsecureSkipVerify bool
	// PreflightDial is the one "host:port" the preflight is allowed to dial
	// when it is run over HTTP. It is configuration, not a request parameter,
	// so an OAM route cannot be turned into a way of opening connections from
	// the service to addresses of the caller's choosing.
	PreflightDial string
}

// ParseClientSettings validates a client tls block without touching the
// filesystem; see ParseServerSettings for why a disabled block is still
// validated in full.
func ParseClientSettings(kv utils.JSON) (*DXClientTLSSettings, error) {
	s := &DXClientTLSSettings{}
	var err error

	if s.Enabled, err = readBool(kv, "enabled", true); err != nil {
		return nil, err
	}
	if s.CertFile, s.KeyFile, err = readCertPair(kv, false); err != nil {
		return nil, err
	}
	if s.Policy, err = readPolicy(kv); err != nil {
		return nil, err
	}
	if s.CATrust, s.CAFiles, err = readCATrust(kv); err != nil {
		return nil, err
	}
	denyFile, present, err := readString(kv, keyDenyFile)
	if err != nil {
		return nil, err
	}
	if present {
		if s.DenyFile = strings.TrimSpace(denyFile); s.DenyFile == "" {
			return nil, configErrorf(keyDenyFile, "EMPTY_VALUE:REMOVE_THE_KEY_OR_GIVE_A_PATH")
		}
	}
	serverName, present, err := readString(kv, "server-name")
	if err != nil {
		return nil, err
	}
	if present {
		serverName = strings.TrimSpace(serverName)
		if serverName == "" {
			return nil, configErrorf("server-name", "EMPTY_VALUE:REMOVE_THE_KEY_TO_USE_THE_URL_HOST")
		}
		s.ServerName = serverName
	}
	if s.InsecureSkipVerify, err = readBool(kv, "insecure-skip-verify", false); err != nil {
		return nil, err
	}
	dial, present, err := readString(kv, "preflight-dial")
	if err != nil {
		return nil, err
	}
	if present {
		dial = strings.TrimSpace(dial)
		if _, _, splitErr := net.SplitHostPort(dial); splitErr != nil {
			return nil, configErrorf("preflight-dial", "INVALID_VALUE:%s:EXPECTED_host:port", echo(dial))
		}
		s.PreflightDial = dial
	}
	return s, nil
}

// DXClientTLS is a client's live TLS state.
type DXClientTLS struct {
	Settings    *DXClientTLSSettings
	Config      *tls.Config
	Certificate *DXCertificateReloader // nil when no certificate is configured
	RootCAs     *DXCAPoolReloader
	DenyFile    *DXDenyFileReloader // nil when no deny-file is configured

	// deny is the certificate-level deny list in force, refreshed from the
	// deny-file inside verifyConnection; see NewClientTLS for what does and
	// does not hot-reload on this side.
	deny atomic.Pointer[DXDenyList]
	// appliedSuites and appliedCurves are the deny-file's suite and curve
	// tokens as they were subtracted at startup, to notice when a reload
	// changed them -- which this side cannot act on until a restart.
	appliedSuites string
	appliedCurves string
}

// NewClientTLS parses the block, loads the files and builds the config; any
// failure is a startup error, for the same reason as on the server.
//
// What hot-reloads on this side, and what does not. The leaf hot-reloads
// through GetClientCertificate. The certificate-level denies -- deny-
// certificates and deny-certificate-signature-algorithms in the deny-file --
// hot-reload too, because they are enforced in VerifyConnection, which
// crypto/tls calls on the client in every handshake, and that hook re-reads
// the file. The root pool and the deny-file's cipher-suite and curve denies do
// not: they live on tls.Config fields that http.Transport clones per dial, and
// crypto/tls has no per-handshake hook on the client side that could swap
// them the way GetConfigForClient does on the server. A CA rotation, or a
// deny-file change to suites or curves, takes effect on the outbound side at
// the next restart; the reload logs a warning saying exactly that. CA
// certificates live for years and are rotated by adding the successor to the
// bundle well before the predecessor expires, so a restart at the next deploy
// is the normal cadence; the limitation is stated here so nobody has to
// discover it.
func NewClientTLS(kv utils.JSON) (*DXClientTLS, error) {
	settings, err := ParseClientSettings(kv)
	if err != nil {
		return nil, err
	}
	c := &DXClientTLS{Settings: settings}
	if !settings.Enabled {
		return c, nil
	}

	if settings.CertFile != "" {
		c.Certificate, err = NewCertificateReloader(settings.CertFile, settings.KeyFile, settings.Policy.CheckKeyStrength)
		if err != nil {
			return nil, err
		}
		c.Certificate.OnReload = func(leaf *x509.Certificate) { observeCertificate("client", settings.CertFile, leaf) }
		observeCertificate("client", settings.CertFile, c.Certificate.Leaf())
	}

	// Built and assigned even under insecure-skip-verify: RootCAs is never
	// nil, so what is trusted is always what was named.
	c.RootCAs, err = NewCAPoolReloader(settings.CATrust, settings.CAFiles, settings.Policy.CheckKeyStrength)
	if err != nil {
		return nil, err
	}

	var fileDeny *DXDenyList
	if settings.DenyFile != "" {
		c.DenyFile, err = NewDenyFileReloader(settings.DenyFile, func(list *DXDenyList) error {
			_, err := settings.Policy.effective(list)
			return err
		})
		if err != nil {
			return nil, err
		}
		fileDeny = c.DenyFile.List()
		c.appliedSuites, c.appliedCurves = strings.Join(fileDeny.SuiteTokens, ","), strings.Join(fileDeny.CurveTokens, ",")
	}
	outcome, err := settings.Policy.effective(fileDeny)
	if err != nil {
		return nil, err
	}
	settings.Policy.CipherSuites, settings.Policy.Curves = outcome.suites, outcome.curves
	c.deny.Store(mergeDenyLists(settings.Policy.Deny, fileDeny))

	c.Config = &tls.Config{
		RootCAs:            c.RootCAs.Pool(),
		ServerName:         settings.ServerName,
		InsecureSkipVerify: settings.InsecureSkipVerify,
		VerifyConnection:   c.verifyConnection,
	}
	if c.Certificate != nil {
		c.Config.GetClientCertificate = c.Certificate.GetClientCertificate
	}
	settings.Policy.apply(c.Config)

	if settings.InsecureSkipVerify {
		log.Log.Warn("TLS client: insecure-skip-verify=true -- the peer's certificate is NOT verified; any server can impersonate the upstream. This is for a development host only.")
	}
	if deny := c.deny.Load(); !deny.IsEmpty() {
		log.Log.Infof("TLS client deny: [%s] %s", deny.Summary(), settings.Policy.denyReport(outcome))
	}
	log.Log.Infof("TLS client: %s", c.Summary())
	return c, nil
}

// BuildClientConfig is the one-call form for a caller that only wants the
// tls.Config. It returns nil, nil for a block with enabled=false.
func BuildClientConfig(kv utils.JSON) (*tls.Config, error) {
	c, err := NewClientTLS(kv)
	if err != nil {
		return nil, err
	}
	return c.Config, nil
}

// refreshDenyFile re-reads the deny-file if it changed and puts the new
// certificate-level denies in force. A change to its suite or curve tokens is
// warned about, because on this side those cannot take effect before a
// restart (see NewClientTLS).
func (c *DXClientTLS) refreshDenyFile() {
	if c.DenyFile == nil {
		return
	}
	list, changed := c.DenyFile.Get()
	if !changed {
		return
	}
	c.deny.Store(mergeDenyLists(c.Settings.Policy.Deny, list))
	suites, curves := strings.Join(list.SuiteTokens, ","), strings.Join(list.CurveTokens, ",")
	if suites != c.appliedSuites || curves != c.appliedCurves {
		log.Log.Warnf("TLS_DENY_FILE_CLIENT_RESTART_NEEDED:%s:deny-cipher-suites/deny-curves changed (was [%s]/[%s], now [%s]/[%s]) -- on the client side these live on tls.Config fields the transport clones per dial and take effect at the next restart; the certificate denies in the same file are in force now",
			c.DenyFile.Path, c.appliedSuites, c.appliedCurves, suites, curves)
	}
}

// verifyConnection applies the deny list, the key-strength floor and the
// no-CA-as-leaf rule to the server's chain. Under insecure-skip-verify there
// is no verified chain; the checks are then applied to whatever the peer
// presented, which is the only thing left that can be checked -- the denies
// over every presented certificate, exempting a self-signed one from the
// signature-algorithm check the way a verified chain's anchor is exempt.
func (c *DXClientTLS) verifyConnection(cs tls.ConnectionState) error {
	c.refreshDenyFile()
	var chain []*x509.Certificate
	var denyChains [][]*x509.Certificate
	anchorExempt := true
	switch {
	case len(cs.VerifiedChains) > 0 && len(cs.VerifiedChains[0]) > 0:
		chain = cs.VerifiedChains[0]
		denyChains = cs.VerifiedChains
	case c.Settings.InsecureSkipVerify && len(cs.PeerCertificates) > 0:
		chain = cs.PeerCertificates[:1]
		denyChains = [][]*x509.Certificate{cs.PeerCertificates}
		anchorExempt = false
	default:
		return nil
	}
	if deny := c.deny.Load(); !deny.IsEmpty() {
		for _, dc := range denyChains {
			if err := checkDeniedChain(deny, dc, anchorExempt); err != nil {
				log.Log.Warnf("TLS_SERVER_REJECTED:%s:%v", HandshakeClassRevoked, err)
				return err
			}
		}
	}
	if err := c.Settings.Policy.CheckChainStrength(chain); err != nil {
		log.Log.Warnf("TLS_SERVER_REJECTED:KEY_STRENGTH:%s:%v", PeerIdentity(chain[0]), err)
		return err
	}
	if chain[0].IsCA {
		log.Log.Warnf("TLS_SERVER_REJECTED:CA_CERTIFICATE_AS_LEAF:%s", PeerIdentity(chain[0]))
		return errors.Errorf("TLS_PEER_IS_A_CA_CERTIFICATE:%s", PeerIdentity(chain[0]))
	}
	return nil
}

// DenyList is the certificate-level deny list currently in force. For the
// preflight and for tests.
func (c *DXClientTLS) DenyList() *DXDenyList {
	return c.deny.Load()
}

// Summary is the effective outbound posture as one line.
func (c *DXClientTLS) Summary() string {
	st := c.Settings
	if !st.Enabled {
		return fmt.Sprintf("enabled=false (block validated, TLS settings not in force) %s ca-trust=%s deny-file=%s", st.Policy.Summary(), st.CATrust, orNone(st.DenyFile))
	}
	cert := "none"
	if c.Certificate != nil {
		leaf := c.Certificate.Leaf()
		cert = fmt.Sprintf("%s subject=%q not-after=%s", st.CertFile, leaf.Subject.CommonName, leaf.NotAfter.UTC().Format("2006-01-02T15:04:05Z"))
	}
	caCerts := fmt.Sprintf("%d", c.RootCAs.Count())
	if st.CATrust != CATrustCustom {
		caCerts += "+system"
	}
	serverName := st.ServerName
	if serverName == "" {
		serverName = "(url host)"
	}
	return fmt.Sprintf("client-cert=%s ca-trust=%s ca-certs=%s ca-files=%v server-name=%s insecure-skip-verify=%t deny-file=%s deny-in-force=[%s] %s",
		cert, st.CATrust, caCerts, st.CAFiles, serverName, st.InsecureSkipVerify, orNone(st.DenyFile), c.deny.Load().Summary(), st.Policy.Summary())
}
