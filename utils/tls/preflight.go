package tls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/utils"
)

// In production there is no internet, no openssl on the image and no test
// suite; what there is, is the configuration and the files it points at. The
// preflight reads exactly those and reports what a handshake would find,
// before any handshake happens. It is how an air-gapped deployment finds the
// wrong CA mounted, the certificate that expired last week, or the host clock
// that is a year out, at deploy time and from a log line rather than at the
// first refused caller.
//
// Only the optional dial touches the network, and only when asked to.

// PreflightExpiryWarningDays is how far ahead a certificate's NotAfter is
// flagged as a problem in the report.
const PreflightExpiryWarningDays = 30

// DXPreflightReport is the outcome: OK when nothing was found that would stop
// or degrade a handshake, the Problems that were, and the full text.
type DXPreflightReport struct {
	OK       bool
	Problems []string
	Text     string
	lines    []string
	now      time.Time
}

func (r *DXPreflightReport) linef(format string, args ...any) {
	r.lines = append(r.lines, fmt.Sprintf(format, args...))
}

func (r *DXPreflightReport) problemf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	r.Problems = append(r.Problems, msg)
	r.lines = append(r.lines, "  PROBLEM: "+msg)
}

func (r *DXPreflightReport) finish() *DXPreflightReport {
	r.OK = len(r.Problems) == 0
	verdict := "OK"
	if !r.OK {
		verdict = fmt.Sprintf("FAILED (%d problems)", len(r.Problems))
	}
	r.lines = append([]string{fmt.Sprintf("TLS PREFLIGHT: %s   now=%s", verdict, r.now.UTC().Format(time.RFC3339))}, r.lines...)
	r.Text = strings.Join(r.lines, "\n") + "\n"
	return r
}

// describeCertificate writes one certificate's facts, with the current time
// beside the validity window so that a clock problem reads as a clock problem.
func (r *DXPreflightReport) describeCertificate(label string, c *x509.Certificate, policy *DXPolicy) {
	daysLeft := c.NotAfter.Sub(r.now).Hours() / 24
	r.linef("  %s:", label)
	// issuer and serial are printed exactly as a deny-certificates entry has
	// to spell them, and spki-sha256 is the value the SPKI form wants, so a
	// deny can be written from this report on an air-gapped host.
	r.linef("    subject=%q issuer=%q serial=%s", c.Subject.String(), c.Issuer.String(), SerialDescription(c.SerialNumber))
	r.linef("    spki-sha256=%s signature-algorithm=%s", SPKISHA256Hex(c), c.SignatureAlgorithm)
	if sans := SANs(c); len(sans) > 0 {
		r.linef("    sans=%v", sans)
	} else {
		r.linef("    sans=(none) -- an allow-list cannot match this certificate and modern clients will not match its CN")
	}
	r.linef("    key=%s ca=%t", KeyDescription(c.PublicKey), c.IsCA)
	r.linef("    not-before=%s not-after=%s now=%s days-remaining=%.1f",
		c.NotBefore.UTC().Format(time.RFC3339), c.NotAfter.UTC().Format(time.RFC3339), r.now.UTC().Format(time.RFC3339), daysLeft)
	switch {
	case r.now.Before(c.NotBefore):
		r.problemf("%s is NOT YET VALID at this host's clock (starts %s, now %s): if the certificate was just issued, this host's clock is behind -- check NTP before the PKI", label, c.NotBefore.UTC().Format(time.RFC3339), r.now.UTC().Format(time.RFC3339))
	case r.now.After(c.NotAfter):
		r.problemf("%s has EXPIRED at this host's clock (ended %s, now %s): if it was renewed recently, this host's clock is ahead -- check NTP before the PKI", label, c.NotAfter.UTC().Format(time.RFC3339), r.now.UTC().Format(time.RFC3339))
	case daysLeft < PreflightExpiryWarningDays:
		r.problemf("%s expires in %.1f days (%s)", label, daysLeft, c.NotAfter.UTC().Format(time.RFC3339))
	}
	if policy != nil {
		if err := policy.CheckKeyStrength(c); err != nil {
			r.problemf("%s fails the key-strength floor: %v", label, err)
		}
	}
}

// chainCheck verifies that a leaf chains to the configured pool. In a
// single-CA cluster the CA that issued this process's certificate is the CA
// its peers are verified against, and a leaf that does not chain to it is the
// classic wrong-CA-mounted mistake. A deployment where a different CA issues
// leaves will see this as a problem; that is a deliberate bias towards the
// common case, stated here so the report can be read correctly.
func (r *DXPreflightReport) chainCheck(label string, leaf *x509.Certificate, intermediates []*x509.Certificate, pool *x509.CertPool) {
	inter := x509.NewCertPool()
	for _, c := range intermediates {
		inter.AddCert(c)
	}
	_, err := leaf.Verify(x509.VerifyOptions{
		Roots:         pool,
		Intermediates: inter,
		CurrentTime:   r.now,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	})
	if err != nil {
		class, advice := ClassifyHandshakeError(err)
		r.problemf("%s does NOT chain to the configured ca-trust pool [%s]: %v -- %s", label, class, err, advice)
		return
	}
	r.linef("  %s chains to the configured ca-trust pool: yes", label)
}

// PreflightServer checks a server tls block. It never starts a listener.
func PreflightServer(name string, kv utils.JSON) *DXPreflightReport {
	r := &DXPreflightReport{now: time.Now()}
	r.linef("server %s:", name)
	settings, err := ParseServerSettings(kv)
	if err != nil {
		r.problemf("configuration: %v", err)
		return r.finish()
	}
	if settings.Mode == ModeHTTP {
		r.linef("  mode=http: plaintext listener, no TLS; nothing to check")
		return r.finish()
	}
	r.linef("  mode=%s", settings.Mode)
	r.linef("  policy: %s", settings.Policy.Summary())
	if settings.Mode == ModeMTLS {
		r.linef("  client-auth=%s ca-trust=%s ca-files=%v", settings.ClientAuthName, settings.CATrust, settings.CAFiles)
		if settings.ClientAuthMigration != "" {
			r.linef("  client-auth-migration=%s: NOT ENFORCING MTLS -- %s", settings.ClientAuthMigration, migrationMeaning(settings.ClientAuth))
		}
	} else {
		r.linef("  client-auth=%s: no client is verified; ca-trust does not apply", settings.ClientAuthName)
	}
	if len(settings.AllowedClientSANs) > 0 {
		mode := "enforce"
		if settings.AllowedClientSANsLogOnly {
			mode = "log-only"
		}
		r.linef("  allowed-client-sans=%v (%s)", settings.AllowedClientSANs, mode)
	} else if settings.Mode == ModeMTLS {
		r.linef("  allowed-client-sans=(none) -- every certificate the CA issued is an accepted caller")
	}
	if !settings.Enabled {
		r.linef("  enabled=false: block validated, files not checked, TLS not in force")
		return r.finish()
	}
	deny := r.describeDeny(settings.Policy, settings.DenyFile)
	var pool *x509.CertPool
	if settings.Mode == ModeMTLS {
		pool = r.describeFiles(settings.CertFile, settings.KeyFile, settings.CATrust, settings.CAFiles, settings.Policy, deny)
	} else {
		r.describeFiles(settings.CertFile, settings.KeyFile, "", nil, settings.Policy, deny)
	}
	if pool != nil {
		if leaf, chain, ok := r.loadLeaf(settings.CertFile, settings.KeyFile); ok {
			r.chainCheck("cert-file", leaf, chain, pool)
		}
	}
	return r.finish()
}

// describeDeny reports the block's denies and, when a deny-file is configured,
// reads it and reports what it holds and what the policy offers once it is
// subtracted -- or the reason it could not be read, as a problem. The merged
// list is returned so the certificates on disk can be checked against it: a
// service whose own certificate, or whose CA, is on the deny list will refuse
// or be refused by everyone, and that is worth knowing before the first
// handshake.
func (r *DXPreflightReport) describeDeny(policy *DXPolicy, denyFile string) *DXDenyList {
	if denyFile == "" {
		return policy.Deny
	}
	r.linef("  deny-file: %s", denyFile)
	list, err := loadDenyFile(denyFile)
	if err != nil {
		r.problemf("deny-file %s: %v -- the process would refuse to start on this; a running process keeps its previous list and warns", denyFile, err)
		return policy.Deny
	}
	r.linef("    holds: %s", list.Summary())
	outcome, err := policy.effective(list)
	if err != nil {
		r.problemf("deny-file %s: %v", denyFile, err)
		return policy.Deny
	}
	r.linef("    after subtraction: suites-1.2=[%s] curves=[%s] -- %s", suiteNames(outcome.suites), strings.Join(curveNamesOf(outcome.curves), ","), policy.denyReport(outcome))
	return mergeDenyLists(policy.Deny, list)
}

// denyCheck reports a certificate of ours that is on the deny list.
func (r *DXPreflightReport) denyCheck(label string, c *x509.Certificate, deny *DXDenyList) {
	if deny.IsEmpty() || c == nil {
		return
	}
	if d := deny.MatchCertificate(c); d != nil {
		r.problemf("%s is ON THE DENY LIST (%s): every peer enforcing this list will refuse it", label, d.String())
	}
	if deny.DeniesSignatureAlgorithm(c.SignatureAlgorithm) && string(c.RawIssuer) != string(c.RawSubject) {
		r.problemf("%s is signed with %s, which is on deny-certificate-signature-algorithms", label, c.SignatureAlgorithm)
	}
}

// PreflightClient checks a client tls block. When dialAddr is not empty
// ("host:port") it also performs one real handshake and reports what was
// negotiated -- the only network action in this file, and an explicit one.
func PreflightClient(kv utils.JSON, dialAddr string) *DXPreflightReport {
	r := &DXPreflightReport{now: time.Now()}
	r.linef("http-client:")
	settings, err := ParseClientSettings(kv)
	if err != nil {
		r.problemf("configuration: %v", err)
		return r.finish()
	}
	r.linef("  policy: %s", settings.Policy.Summary())
	serverName := settings.ServerName
	if serverName == "" {
		serverName = "(url host)"
	}
	r.linef("  ca-trust=%s ca-files=%v server-name=%s insecure-skip-verify=%t", settings.CATrust, settings.CAFiles, serverName, settings.InsecureSkipVerify)
	if settings.InsecureSkipVerify {
		r.problemf("insecure-skip-verify=true: the peer is not verified; acceptable on a development host only")
	}
	if !settings.Enabled {
		r.linef("  enabled=false: block validated, files not checked, TLS settings not in force")
		return r.finish()
	}
	deny := r.describeDeny(settings.Policy, settings.DenyFile)
	pool := r.describeFiles(settings.CertFile, settings.KeyFile, settings.CATrust, settings.CAFiles, settings.Policy, deny)
	if pool != nil && settings.CertFile != "" {
		if leaf, chain, ok := r.loadLeaf(settings.CertFile, settings.KeyFile); ok {
			r.chainCheck("cert-file", leaf, chain, pool)
		}
	}
	if dialAddr != "" {
		r.dial(kv, dialAddr)
	}
	return r.finish()
}

// describeFiles reports on the certificate pair and every CA file, checks each
// against the deny list, and returns the trust pool so the chain check can use
// it, or nil if it could not be built. An empty caTrust means there is no pool
// to build -- a server in mode=https -- and only the certificate is described.
func (r *DXPreflightReport) describeFiles(certFile, keyFile, caTrust string, caFiles []string, policy *DXPolicy, deny *DXDenyList) *x509.CertPool {
	if certFile != "" {
		if leaf, chain, ok := r.loadLeaf(certFile, keyFile); ok {
			r.describeCertificate("cert-file "+certFile, leaf, policy)
			r.denyCheck("cert-file "+certFile, leaf, deny)
			for i, c := range chain {
				label := fmt.Sprintf("cert-file %s intermediate[%d]", certFile, i)
				r.describeCertificate(label, c, policy)
				r.denyCheck(label, c, deny)
			}
		}
	} else {
		r.linef("  cert-file: (none) -- this client presents no certificate; a server in mode=mtls will refuse it")
	}
	if caTrust == "" {
		return nil
	}
	for i, f := range caFiles {
		certs, err := loadCAFile(f)
		if err != nil {
			r.problemf("ca-files[%d] %s: %v", i, f, err)
			continue
		}
		for j, c := range certs {
			label := fmt.Sprintf("ca-files[%d] %s cert[%d]", i, f, j)
			r.describeCertificate(label, c, policy)
			r.denyCheck(label, c, deny)
		}
	}
	pool, _, count, err := buildPool(caTrust, caFiles)
	if err != nil {
		r.problemf("ca-trust pool: %v", err)
		return nil
	}
	if caTrust == CATrustCustom {
		r.linef("  ca-trust pool: %d certificate(s) from files", count)
	} else {
		r.linef("  ca-trust pool: %d certificate(s) from files plus the host root store (not enumerable; on an air-gapped or distroless host this is the part most likely to be empty or useless)", count)
	}
	return pool
}

// loadLeaf reads a certificate pair, reporting any problem, and returns the
// leaf and its intermediates.
func (r *DXPreflightReport) loadLeaf(certFile, keyFile string) (leaf *x509.Certificate, intermediates []*x509.Certificate, ok bool) {
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		r.problemf("cert-file/key-file %s, %s: %v", certFile, keyFile, err)
		return nil, nil, false
	}
	for i, der := range pair.Certificate {
		c, err := x509.ParseCertificate(der)
		if err != nil {
			r.problemf("cert-file %s: certificate[%d]: %v", certFile, i, err)
			return nil, nil, false
		}
		if i == 0 {
			leaf = c
		} else {
			intermediates = append(intermediates, c)
		}
	}
	return leaf, intermediates, true
}

// dial performs one handshake with the client configuration and reports the
// negotiated parameters and the peer's identity.
func (r *DXPreflightReport) dial(kv utils.JSON, addr string) {
	r.linef("  dial %s:", addr)
	cfg, err := BuildClientConfig(kv)
	if err != nil {
		r.problemf("dial %s: building client config: %v", addr, err)
		return
	}
	if cfg == nil {
		r.linef("    skipped: enabled=false")
		return
	}
	if cfg.ServerName == "" {
		host, _, err := net.SplitHostPort(addr)
		if err == nil {
			cfg = cfg.Clone()
			cfg.ServerName = host
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dialer := &tls.Dialer{NetDialer: &net.Dialer{}, Config: cfg}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		class, advice := ClassifyHandshakeError(err)
		r.problemf("dial %s: handshake failed [%s]: %v -- %s", addr, class, err, advice)
		return
	}
	defer func() { _ = conn.Close() }()
	cs := conn.(*tls.Conn).ConnectionState()
	r.linef("    negotiated version=%s cipher=%s alpn=%q", tls.VersionName(cs.Version), tls.CipherSuiteName(cs.CipherSuite), cs.NegotiatedProtocol)
	if len(cs.PeerCertificates) > 0 {
		r.linef("    peer identity=%s", PeerIdentity(cs.PeerCertificates[0]))
		r.describeCertificate("peer certificate", cs.PeerCertificates[0], nil)
	}
}
