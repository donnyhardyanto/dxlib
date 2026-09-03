package tls

import (
	"crypto/x509"
	"net"
	"strings"
)

// Chain validation is not authorization. Inside one cluster a single internal
// CA issues every service certificate, so "chains to our CA" is true of every
// service, and a server that stops there has authenticated the caller as
// "something in the cluster" -- which is not nothing, but is not an identity.
// The identity is in the certificate's subject alternative names, and this
// file is where they are read and matched.

// PeerIdentity is one string naming a certificate's subject, for logs, audit
// entries and the allow-list message. Preference order: the first URI SAN
// (a SPIFFE ID, when the PKI issues them), then the first DNS SAN, then the
// first IP SAN, then the common name marked as such, then the serial. The
// common name is last because nothing in modern verification checks it; it is
// reported only when the certificate carries no SAN at all.
func PeerIdentity(c *x509.Certificate) string {
	if c == nil {
		return ""
	}
	if len(c.URIs) > 0 {
		return c.URIs[0].String()
	}
	if len(c.DNSNames) > 0 {
		return c.DNSNames[0]
	}
	if len(c.IPAddresses) > 0 {
		return c.IPAddresses[0].String()
	}
	if c.Subject.CommonName != "" {
		return "CN=" + c.Subject.CommonName
	}
	return "serial=" + c.SerialNumber.String()
}

// SANs lists every subject alternative name a certificate carries, as the
// strings an allow-list entry would have to match.
func SANs(c *x509.Certificate) []string {
	if c == nil {
		return nil
	}
	out := make([]string, 0, len(c.URIs)+len(c.DNSNames)+len(c.IPAddresses)+len(c.EmailAddresses))
	for _, u := range c.URIs {
		out = append(out, u.String())
	}
	out = append(out, c.DNSNames...)
	for _, ip := range c.IPAddresses {
		out = append(out, ip.String())
	}
	out = append(out, c.EmailAddresses...)
	return out
}

// MatchesAllowedSAN reports whether any of the certificate's SANs is in the
// allow-list. Matching is exact: no wildcards, no suffix rules. DNS names are
// compared case-insensitively, as DNS is. IP entries are compared as parsed
// addresses, so "::1" and "0:0:0:0:0:0:0:1" are the same entry. URIs are
// compared as strings, which for a SPIFFE ID is the right comparison. The
// common name is not consulted; an allow-list entry has to name a SAN.
func MatchesAllowedSAN(c *x509.Certificate, allowed []string) bool {
	if c == nil || len(allowed) == 0 {
		return false
	}
	for _, entry := range allowed {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		for _, u := range c.URIs {
			if u.String() == entry {
				return true
			}
		}
		for _, d := range c.DNSNames {
			if strings.EqualFold(d, entry) {
				return true
			}
		}
		if ip := net.ParseIP(entry); ip != nil {
			for _, cip := range c.IPAddresses {
				if cip.Equal(ip) {
					return true
				}
			}
		}
		for _, e := range c.EmailAddresses {
			if strings.EqualFold(e, entry) {
				return true
			}
		}
	}
	return false
}
