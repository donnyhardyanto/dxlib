// Package tlstest builds a throwaway PKI in memory for tests: a root, an
// intermediate, and leaves with whatever SANs, validity windows and key sizes
// a test needs, written to PEM files on demand.
//
// Everything is crypto/x509 and crypto/ecdsa from the standard library. No
// fixture files, no openssl, no network: the same test runs identically on a
// developer's laptop, in CI, and on an air-gapped build host with no module
// cache access, which is the point of having the rule.
package tlstest

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// CA is a signing authority, either a self-signed root or an intermediate.
type CA struct {
	Cert   *x509.Certificate
	Key    crypto.Signer
	Parent *CA // nil for a root
}

// Leaf is an end-entity certificate with its key.
type Leaf struct {
	Cert *x509.Certificate
	Key  crypto.Signer
	// Chain is the leaf followed by every intermediate up to, but not
	// including, the root: what a server or client should present.
	Chain []*x509.Certificate
}

// LeafOptions shape a leaf. The zero value is a valid-now ECDSA P-256
// certificate with no SANs and both server and client EKUs.
type LeafOptions struct {
	CommonName string
	DNSNames   []string
	IPs        []net.IP
	URIs       []string
	NotBefore  time.Time // zero: one hour ago
	NotAfter   time.Time // zero: one year from now
	RSABits    int       // 0: ECDSA P-256; otherwise an RSA key of this size
	IsCA       bool      // mark BasicConstraints CA:TRUE on a leaf, for the negative test
	ServerOnly bool      // EKU serverAuth only
	ClientOnly bool      // EKU clientAuth only
	// SignatureAlgorithm is the algorithm the issuer signs with; zero lets
	// crypto/x509 pick the default for the issuer's key. It has to suit the
	// issuer's key type (an ECDSA CA can sign ECDSA-SHA384, not SHA256-RSA).
	SignatureAlgorithm x509.SignatureAlgorithm
	// Key, when set, is used instead of generating one: a re-issuance of the
	// same key under a new serial, which is what a deny by SPKI has to catch
	// and a deny by issuer-serial has to miss.
	Key crypto.Signer
}

var serial = big.NewInt(time.Now().UnixNano())

func nextSerial() *big.Int {
	serial = new(big.Int).Add(serial, big.NewInt(1))
	return new(big.Int).Set(serial)
}

func newKey(t testing.TB, rsaBits int) crypto.Signer {
	t.Helper()
	if rsaBits > 0 {
		k, err := rsa.GenerateKey(rand.Reader, rsaBits)
		if err != nil {
			t.Fatalf("tlstest: rsa key: %v", err)
		}
		return k
	}
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("tlstest: ecdsa key: %v", err)
	}
	return k
}

// NewRootCA makes a self-signed root, signed with crypto/x509's default for
// an ECDSA key (ECDSA-SHA256).
func NewRootCA(t testing.TB, name string) *CA {
	t.Helper()
	return NewRootCASignedWith(t, name, x509.UnknownSignatureAlgorithm)
}

// NewRootCASignedWith makes a self-signed root whose own signature uses alg,
// for the test that a trust anchor's signature algorithm is exempt from the
// deny list.
func NewRootCASignedWith(t testing.TB, name string, alg x509.SignatureAlgorithm) *CA {
	t.Helper()
	key := newKey(t, 0)
	tmpl := &x509.Certificate{
		SerialNumber:          nextSerial(),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		SignatureAlgorithm:    alg,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		t.Fatalf("tlstest: root: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("tlstest: root parse: %v", err)
	}
	return &CA{Cert: cert, Key: key}
}

// NewIntermediate makes an intermediate signed by parent.
func (ca *CA) NewIntermediate(t testing.TB, name string) *CA {
	t.Helper()
	key := newKey(t, 0)
	tmpl := &x509.Certificate{
		SerialNumber:          nextSerial(),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(5 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, key.Public(), ca.Key)
	if err != nil {
		t.Fatalf("tlstest: intermediate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("tlstest: intermediate parse: %v", err)
	}
	return &CA{Cert: cert, Key: key, Parent: ca}
}

// Issue signs a leaf.
func (ca *CA) Issue(t testing.TB, o LeafOptions) *Leaf {
	t.Helper()
	key := o.Key
	if key == nil {
		key = newKey(t, o.RSABits)
	}
	notBefore, notAfter := o.NotBefore, o.NotAfter
	if notBefore.IsZero() {
		notBefore = time.Now().Add(-time.Hour)
	}
	if notAfter.IsZero() {
		notAfter = time.Now().Add(365 * 24 * time.Hour)
	}
	eku := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	if o.ServerOnly {
		eku = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}
	if o.ClientOnly {
		eku = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	tmpl := &x509.Certificate{
		SerialNumber:          nextSerial(),
		Subject:               pkix.Name{CommonName: o.CommonName},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           eku,
		BasicConstraintsValid: true,
		IsCA:                  o.IsCA,
		DNSNames:              o.DNSNames,
		IPAddresses:           o.IPs,
		SignatureAlgorithm:    o.SignatureAlgorithm,
	}
	if _, isRSA := key.(*rsa.PrivateKey); isRSA {
		tmpl.KeyUsage |= x509.KeyUsageKeyEncipherment
	}
	if o.IsCA {
		tmpl.KeyUsage |= x509.KeyUsageCertSign
	}
	for _, u := range o.URIs {
		parsed, err := url.Parse(u)
		if err != nil {
			t.Fatalf("tlstest: uri %q: %v", u, err)
		}
		tmpl.URIs = append(tmpl.URIs, parsed)
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, key.Public(), ca.Key)
	if err != nil {
		t.Fatalf("tlstest: leaf: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("tlstest: leaf parse: %v", err)
	}
	chain := []*x509.Certificate{cert}
	for p := ca; p != nil && p.Parent != nil; p = p.Parent {
		chain = append(chain, p.Cert)
	}
	return &Leaf{Cert: cert, Key: key, Chain: chain}
}

// Root walks up to the self-signed certificate at the top of ca's chain.
func (ca *CA) Root() *CA {
	for ca.Parent != nil {
		ca = ca.Parent
	}
	return ca
}

// CertPEM encodes one or more certificates.
func CertPEM(certs ...*x509.Certificate) []byte {
	var out []byte
	for _, c := range certs {
		out = append(out, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Raw})...)
	}
	return out
}

// KeyPEM encodes a private key as PKCS#8.
func KeyPEM(t testing.TB, key crypto.Signer) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("tlstest: key pem: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

// WriteFile writes data under dir and returns the path. It also sets the
// file's mtime one hour into the past, so a test that rewrites the file to
// exercise a reload gets a different mtime even on a filesystem with coarse
// timestamps.
func WriteFile(t testing.TB, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("tlstest: write %s: %v", path, err)
	}
	past := time.Now().Add(-time.Hour)
	if err := os.Chtimes(path, past, past); err != nil {
		t.Fatalf("tlstest: chtimes %s: %v", path, err)
	}
	return path
}

// WriteLeaf writes a leaf's chain and key as <name>.crt and <name>.key.
func WriteLeaf(t testing.TB, dir, name string, leaf *Leaf) (certFile, keyFile string) {
	t.Helper()
	certFile = WriteFile(t, dir, name+".crt", CertPEM(leaf.Chain...))
	keyFile = WriteFile(t, dir, name+".key", KeyPEM(t, leaf.Key))
	return certFile, keyFile
}

// WriteCA writes a CA's certificate as <name>.pem.
func WriteCA(t testing.TB, dir, name string, ca *CA) string {
	t.Helper()
	return WriteFile(t, dir, name+".pem", CertPEM(ca.Cert))
}
