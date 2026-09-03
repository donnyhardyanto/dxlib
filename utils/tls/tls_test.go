package tls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	stdlog "log"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/donnyhardyanto/dxlib/utils/tls/tlstest"
)

// The whole PKI is built in memory per test: a root, an intermediate under it,
// and leaves. Nothing is checked in and nothing is fetched, so these run the
// same on a laptop, in CI, and on a build host with no network at all.

type pki struct {
	dir          string
	root         *tlstest.CA
	intermediate *tlstest.CA
	otherRoot    *tlstest.CA // a second, unrelated CA for the wrong-CA cases
	rootFile     string
	otherFile    string
	server       *tlstest.Leaf
	serverCert   string
	serverKey    string
}

func newPKI(t *testing.T) *pki {
	t.Helper()
	p := &pki{dir: t.TempDir()}
	p.root = tlstest.NewRootCA(t, "dxlib test root")
	p.intermediate = p.root.NewIntermediate(t, "dxlib test intermediate")
	p.otherRoot = tlstest.NewRootCA(t, "some other root")
	p.rootFile = tlstest.WriteCA(t, p.dir, "root", p.root)
	p.otherFile = tlstest.WriteCA(t, p.dir, "other", p.otherRoot)
	p.server = p.intermediate.Issue(t, tlstest.LeafOptions{
		CommonName: "api.test",
		DNSNames:   []string{"api.test", "localhost"},
		IPs:        []net.IP{net.ParseIP("127.0.0.1")},
	})
	p.serverCert, p.serverKey = tlstest.WriteLeaf(t, p.dir, "server", p.server)
	return p
}

// serverBlock is a complete, valid server tls block that the cases below
// then perturb one key at a time.
func (p *pki) serverBlock(overrides utils.JSON) utils.JSON {
	kv := utils.JSON{
		"mode":       ModeMTLS,
		"cert-file":  p.serverCert,
		"key-file":   p.serverKey,
		"tls-policy": PolicyIntermediate,
		"ca-trust":   CATrustCustom,
		"ca-files":   []string{p.rootFile},
	}
	for k, v := range overrides {
		if v == nil {
			delete(kv, k)
		} else {
			kv[k] = v
		}
	}
	return kv
}

// httpsBlock is the same server with mode=https: a certificate, a policy, and
// no client verification, so no trust pool.
func (p *pki) httpsBlock(overrides utils.JSON) utils.JSON {
	kv := p.serverBlock(utils.JSON{"mode": ModeHTTPS, "ca-trust": nil, "ca-files": nil})
	for k, v := range overrides {
		if v == nil {
			delete(kv, k)
		} else {
			kv[k] = v
		}
	}
	return kv
}

// clientLeaf issues a client certificate under the intermediate and writes it.
func (p *pki) clientLeaf(t *testing.T, name string, o tlstest.LeafOptions) (leaf *tlstest.Leaf, certFile, keyFile string) {
	t.Helper()
	if o.CommonName == "" {
		o.CommonName = name
	}
	leaf = p.intermediate.Issue(t, o)
	certFile, keyFile = tlstest.WriteLeaf(t, p.dir, name, leaf)
	return leaf, certFile, keyFile
}

func (p *pki) clientBlock(certFile, keyFile string, overrides utils.JSON) utils.JSON {
	kv := utils.JSON{
		"tls-policy":  PolicyIntermediate,
		"ca-trust":    CATrustCustom,
		"ca-files":    []string{p.rootFile},
		"server-name": "api.test",
	}
	if certFile != "" {
		kv["cert-file"] = certFile
		kv["key-file"] = keyFile
	}
	for k, v := range overrides {
		if v == nil {
			delete(kv, k)
		} else {
			kv[k] = v
		}
	}
	return kv
}

// startServer serves handler over TLS with cfg, exactly the way DXAPI does:
// http.Server with TLSConfig set and ServeTLS with empty file arguments, which
// is what ListenAndServeTLS calls. httptest.Server.StartTLS is not used, for
// two reasons that would each make the tests pass or fail for the wrong
// reason: it injects its own self-signed certificate into Certificates when
// the slice is empty, and Go then skips GetCertificate for a dial to an IP
// address (no SNI); and it forces NextProtos to http/1.1, which would hide
// the HTTP/2-on-by-default path the WebSocket test has to exercise.
func startServer(t *testing.T, cfg *tls.Config, handler http.Handler) (addr string, srv *http.Server, errorLog *logCapture) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	errorLog = &logCapture{}
	srv = &http.Server{Handler: handler, TLSConfig: cfg, ErrorLog: stdlog.New(errorLog, "", 0)}
	go func() { _ = srv.ServeTLS(ln, "", "") }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String(), srv, errorLog
}

type logCapture struct {
	mu    sync.Mutex
	lines []string
}

func (l *logCapture) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, strings.TrimSpace(string(p)))
	return len(p), nil
}

func (l *logCapture) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.lines)
}

// grewPast waits for the server to have logged at least one more line than
// before: the proof that a refusal happened on the server, and not in the
// client's own hello construction.
func (l *logCapture) grewPast(before int) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if l.count() > before {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func (l *logCapture) contains(t *testing.T, substr string) bool {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		l.mu.Lock()
		for _, line := range l.lines {
			if strings.Contains(line, substr) {
				l.mu.Unlock()
				return true
			}
		}
		l.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func okHandler(hits *atomic.Int32) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits != nil {
			hits.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	})
}

func httpsClient(cfg *tls.Config) *http.Client {
	return &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: cfg, ForceAttemptHTTP2: true}}
}

func get(t *testing.T, client *http.Client, addr string) (*http.Response, error) {
	t.Helper()
	resp, err := client.Get("https://" + addr + "/")
	if err == nil {
		t.Cleanup(func() { _ = resp.Body.Close() })
	}
	return resp, err
}

func mustBuildServer(t *testing.T, kv utils.JSON) *DXServerTLS {
	t.Helper()
	s, err := NewServerTLS(kv)
	if err != nil {
		t.Fatalf("NewServerTLS: %v", err)
	}
	// What DXAPI.StartAndWait does before handing the config to http.Server.
	s.ConfigForHTTPServer()
	return s
}

func mustBuildClient(t *testing.T, kv utils.JSON) *tls.Config {
	t.Helper()
	cfg, err := BuildClientConfig(kv)
	if err != nil {
		t.Fatalf("BuildClientConfig: %v", err)
	}
	return cfg
}

func wantConfigError(t *testing.T, err error, key string, detail string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error on %s containing %q, got nil", key, detail)
	}
	var ce *DXConfigError
	if !asConfigError(err, &ce) {
		t.Fatalf("expected a *DXConfigError, got %T: %v", err, err)
	}
	if ce.Key != key {
		t.Errorf("error key = %q, want %q (error: %v)", ce.Key, key, err)
	}
	if !strings.Contains(ce.Detail, detail) {
		t.Errorf("error detail %q does not contain %q", ce.Detail, detail)
	}
}

func asConfigError(err error, target **DXConfigError) bool {
	for err != nil {
		if ce, ok := err.(*DXConfigError); ok {
			*target = ce
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// ---------------------------------------------------------------------------
// Configuration: the required keys, their three failure shapes, contradictions
// ---------------------------------------------------------------------------

// Missing, empty and unrecognised are three different operator mistakes and
// must produce three different messages. The value is echoed for the third,
// quoted and capped, so a hostile value cannot mangle the log line.
func TestRequiredEnumsHaveThreeDistinctFailures(t *testing.T) {
	p := newPKI(t)
	for _, key := range []string{"mode", "ca-trust", "tls-policy"} {
		_, err := ParseServerSettings(p.serverBlock(utils.JSON{key: nil}))
		wantConfigError(t, err, key, "REQUIRED_KEY_MISSING:VALID_VALUES=")

		_, err = ParseServerSettings(p.serverBlock(utils.JSON{key: ""}))
		wantConfigError(t, err, key, "EMPTY_VALUE:VALID_VALUES=")

		_, err = ParseServerSettings(p.serverBlock(utils.JSON{key: "ABSC"}))
		wantConfigError(t, err, key, `INVALID_VALUE:"ABSC":VALID_VALUES=`)

		long := strings.Repeat("x", 200) + "\nforged log line"
		_, err = ParseServerSettings(p.serverBlock(utils.JSON{key: long}))
		if err == nil || strings.Contains(err.Error(), "\n") || len(err.Error()) > 400 {
			t.Errorf("%s: a long or newline-bearing value must be capped and quoted, got %v", key, err)
		}
	}
	// min-version is optional, but present-and-wrong gets the same treatment.
	_, err := ParseServerSettings(p.serverBlock(utils.JSON{"min-version": ""}))
	wantConfigError(t, err, "min-version", "EMPTY_VALUE")
	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"min-version": "1.0"}))
	wantConfigError(t, err, "min-version", `INVALID_VALUE:"1.0"`)
	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"min-version": "1.1"}))
	wantConfigError(t, err, "min-version", `INVALID_VALUE:"1.1"`)
}

// The error text must never carry the enclosing map, which is where key
// material would sit if a future key ever held any.
func TestConfigErrorsNeverEchoTheMap(t *testing.T) {
	p := newPKI(t)
	kv := p.serverBlock(utils.JSON{"ca-trust": 42, "secret-looking-thing": "DO-NOT-LEAK-THIS"})
	_, err := ParseServerSettings(kv)
	if err == nil {
		t.Fatal("expected an error for a non-string ca-trust")
	}
	if strings.Contains(err.Error(), "DO-NOT-LEAK-THIS") {
		t.Errorf("error text leaked the enclosing map: %v", err)
	}
	wantConfigError(t, err, "ca-trust", "WRONG_TYPE:int")
}

// Explicitness is about the value being named, not about its capitalisation.
func TestEnumsAreTrimmedAndCaseInsensitive(t *testing.T) {
	p := newPKI(t)
	s, err := ParseServerSettings(p.serverBlock(utils.JSON{
		"mode":                  " MTLS ",
		"ca-trust":              "  Custom ",
		"tls-policy":            " INTERMEDIATE",
		"client-auth-migration": "Verify-If-Given",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if s.Mode != ModeMTLS || s.CATrust != CATrustCustom || s.Policy.Profile != PolicyIntermediate || s.ClientAuthMigration != ClientAuthMigrationVerifyIfGiven {
		t.Errorf("canonical forms not resolved: %+v", s)
	}
}

// ca-trust and ca-files are validated together. A list under "system" is a
// contradiction; a missing list under "custom" is another.
func TestCATrustContradictionsAreRefused(t *testing.T) {
	p := newPKI(t)
	_, err := ParseServerSettings(p.serverBlock(utils.JSON{"ca-trust": CATrustSystem}))
	wantConfigError(t, err, "ca-files", "NOT_ALLOWED_WITH:ca-trust=system")

	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"ca-trust": CATrustCustom, "ca-files": nil}))
	wantConfigError(t, err, "ca-files", "REQUIRED_WITH:ca-trust=custom")

	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"ca-trust": CATrustSystemAndCustom, "ca-files": []string{}}))
	wantConfigError(t, err, "ca-files", "REQUIRED_WITH:ca-trust=system-and-custom")

	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"ca-files": "/one/file.pem"}))
	wantConfigError(t, err, "ca-files", "WRONG_TYPE:string:EXPECTED_LIST_OF_STRING")

	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"key-file": nil}))
	wantConfigError(t, err, "key-file", "REQUIRED_WITH:cert-file")
}

// A file that yields no certificate, and an allow-list beside a mode that
// never verifies a client, are both refused.
func TestFileAndAllowListContradictionsAreRefused(t *testing.T) {
	p := newPKI(t)
	empty := tlstest.WriteFile(t, p.dir, "empty.pem", []byte("not a certificate\n"))
	_, err := NewServerTLS(p.serverBlock(utils.JSON{"ca-files": []string{empty}}))
	wantConfigError(t, err, "ca-files[0]", "NO_CERTIFICATE_FOUND")

	// The "request" rung never verifies, so a SAN there is a claim.
	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"client-auth-migration": ClientAuthMigrationRequest, "allowed-client-sans": []string{"a.test"}}))
	wantConfigError(t, err, "allowed-client-sans", "REQUIRES_A_VERIFYING_MODE")
	// "verify-if-given" does verify what arrives, so the list is allowed.
	if _, err = ParseServerSettings(p.serverBlock(utils.JSON{"client-auth-migration": ClientAuthMigrationVerifyIfGiven, "allowed-client-sans": []string{"a.test"}})); err != nil {
		t.Errorf("an allow-list under verify-if-given was refused: %v", err)
	}
	// https verifies nobody, so the key itself is out of place.
	_, err = ParseServerSettings(p.httpsBlock(utils.JSON{"allowed-client-sans": []string{"a.test"}}))
	wantConfigError(t, err, "allowed-client-sans", "NOT_ALLOWED_WITH:mode=https")

	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"allowed-client-sans-log-only": true}))
	wantConfigError(t, err, "allowed-client-sans-log-only", "REQUIRES:allowed-client-sans")
}

// The profile is the allow-list; an override can remove from it and never add
// to it, and Go's own insecure list is a second, independently worded refusal.
func TestPolicyOverridesMayOnlyNarrow(t *testing.T) {
	p := newPKI(t)

	_, err := ParseServerSettings(p.serverBlock(utils.JSON{"tls-policy": PolicyModern, "min-version": "1.2"}))
	wantConfigError(t, err, "min-version", "WIDENS_POLICY:1.2")

	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"tls-policy": PolicyModern, "cipher-suites": []string{"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"}}))
	wantConfigError(t, err, "cipher-suites", "NO_EFFECT_UNDER_TLS_1.3")

	// Go still calls ECDHE+CBC-SHA1 "secure"; the profile does not have it.
	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"cipher-suites": []string{"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"}}))
	wantConfigError(t, err, "cipher-suites[0]", "NOT_IN_POLICY")

	// 3DES is on Go's insecure list: a different message, naming the reason.
	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"cipher-suites": []string{"TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA"}}))
	wantConfigError(t, err, "cipher-suites[0]", "INSECURE_SUITE")
	// So is RSA key transport, which Go demoted in 1.22.
	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"cipher-suites": []string{"TLS_RSA_WITH_AES_128_GCM_SHA256"}}))
	wantConfigError(t, err, "cipher-suites[0]", "INSECURE_SUITE")

	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"cipher-suites": []string{"TLS_MADE_UP"}}))
	wantConfigError(t, err, "cipher-suites[0]", "INVALID_VALUE")

	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"curves": []string{"P-521"}}))
	wantConfigError(t, err, "curves[0]", "NOT_IN_POLICY")

	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"min-rsa-bits": 1024}))
	wantConfigError(t, err, "min-rsa-bits", "BELOW_FLOOR:1024:MINIMUM_IS_2048")
	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"min-ecdsa-bits": 224}))
	wantConfigError(t, err, "min-ecdsa-bits", "BELOW_FLOOR:224:MINIMUM_IS_256")

	// A narrowing that is legal resolves to the intersection in profile order.
	s, err := ParseServerSettings(p.serverBlock(utils.JSON{
		"cipher-suites": []string{"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384", "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"},
		"curves":        []string{"P-256", "X25519"},
		"min-rsa-bits":  3072,
		"min-version":   1.2, // a JSON number, not a string
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Policy.CipherSuites) != 2 || s.Policy.CipherSuites[0] != tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 {
		t.Errorf("narrowed suites = %v, want the two named, in profile order", s.Policy.CipherSuites)
	}
	if len(s.Policy.Curves) != 2 || s.Policy.Curves[0] != tls.X25519 {
		t.Errorf("narrowed curves = %v, want [X25519 P-256]", s.Policy.Curves)
	}
	if s.Policy.MinRSABits != 3072 {
		t.Errorf("min-rsa-bits = %d, want 3072", s.Policy.MinRSABits)
	}
	// Under intermediate, min-version 1.3 is a legal narrowing that empties
	// the 1.2 list; a cipher-suites list beside it is then a contradiction.
	s, err = ParseServerSettings(p.serverBlock(utils.JSON{"min-version": "1.3"}))
	if err != nil || s.Policy.MinVersion != tls.VersionTLS13 || s.Policy.CipherSuites != nil {
		t.Errorf("intermediate narrowed to 1.3: err=%v min=%x suites=%v", err, s.Policy.MinVersion, s.Policy.CipherSuites)
	}
	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"min-version": "1.3", "cipher-suites": []string{"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"}}))
	wantConfigError(t, err, "cipher-suites", "NO_EFFECT_UNDER_TLS_1.3")
}

// A block behind enabled=false is still a block, and a typo in it is still a
// typo. What is skipped is only the part that needs the files.
func TestDisabledBlockIsStillValidated(t *testing.T) {
	p := newPKI(t)
	_, err := NewServerTLS(p.serverBlock(utils.JSON{"enabled": false, "ca-trust": "ABSC"}))
	wantConfigError(t, err, "ca-trust", `INVALID_VALUE:"ABSC"`)

	s, err := NewServerTLS(p.serverBlock(utils.JSON{
		"enabled":   false,
		"cert-file": "/nonexistent/tls.crt",
		"key-file":  "/nonexistent/tls.key",
		"ca-files":  []string{"/nonexistent/ca.pem"},
	}))
	if err != nil {
		t.Fatalf("a disabled block must not need its files: %v", err)
	}
	if s.Config != nil {
		t.Error("a disabled block produced a tls.Config")
	}
	_, err = NewServerTLS(p.serverBlock(utils.JSON{"enabled": "yes"}))
	wantConfigError(t, err, "enabled", "WRONG_TYPE:string:EXPECTED_BOOLEAN")
}

// ---------------------------------------------------------------------------
// Trust: the three sources on each side
// ---------------------------------------------------------------------------

func TestServerTrustSources(t *testing.T) {
	p := newPKI(t)
	_, ourCert, ourKey := p.clientLeaf(t, "ours", tlstest.LeafOptions{DNSNames: []string{"ours.test"}})
	otherLeaf := p.otherRoot.Issue(t, tlstest.LeafOptions{CommonName: "other", DNSNames: []string{"other.test"}})
	otherCert, otherKey := tlstest.WriteLeaf(t, p.dir, "otherclient", otherLeaf)

	ours := mustBuildClient(t, p.clientBlock(ourCert, ourKey, nil))
	other := mustBuildClient(t, p.clientBlock(otherCert, otherKey, nil))

	cases := []struct {
		name        string
		caTrust     string
		caFiles     any
		wantOurs    bool
		wantOther   bool
		skipMessage string
	}{
		{"custom: only our CA's clients", CATrustCustom, []string{p.rootFile}, true, false, ""},
		{"system: neither private CA is trusted", CATrustSystem, nil, false, false, ""},
		{"system-and-custom: ours yes, the other private CA still no", CATrustSystemAndCustom, []string{p.rootFile}, true, false, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := NewServerTLS(p.serverBlock(utils.JSON{"ca-trust": c.caTrust, "ca-files": c.caFiles}))
			if err != nil {
				t.Fatalf("NewServerTLS: %v", err)
			}
			if s.Config.ClientCAs == nil {
				t.Fatal("ClientCAs is nil: the implicit system-roots fallback is reachable")
			}
			addr, _, _ := startServer(t, s.Config, okHandler(nil))
			_, errOurs := get(t, httpsClient(ours), addr)
			_, errOther := get(t, httpsClient(other), addr)
			if (errOurs == nil) != c.wantOurs {
				t.Errorf("our client: err=%v, want accepted=%t", errOurs, c.wantOurs)
			}
			if (errOther == nil) != c.wantOther {
				t.Errorf("other CA's client: err=%v, want accepted=%t", errOther, c.wantOther)
			}
		})
	}
}

func TestClientTrustSources(t *testing.T) {
	p := newPKI(t)
	s := mustBuildServer(t, p.httpsBlock(nil))
	addr, _, _ := startServer(t, s.Config, okHandler(nil))

	cases := []struct {
		name    string
		caTrust string
		caFiles any
		want    bool
	}{
		{"custom trusts our server", CATrustCustom, []string{p.rootFile}, true},
		{"system does not trust a private CA", CATrustSystem, nil, false},
		{"system-and-custom trusts our server", CATrustSystemAndCustom, []string{p.rootFile}, true},
		{"custom with the wrong CA file does not", CATrustCustom, []string{p.otherFile}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := mustBuildClient(t, p.clientBlock("", "", utils.JSON{"ca-trust": c.caTrust, "ca-files": c.caFiles}))
			if cfg.RootCAs == nil {
				t.Fatal("RootCAs is nil: the implicit system-roots fallback is reachable")
			}
			_, err := get(t, httpsClient(cfg), addr)
			if (err == nil) != c.want {
				t.Errorf("err=%v, want connected=%t", err, c.want)
			}
			if err != nil {
				if class, _ := ClassifyHandshakeError(err); class != HandshakeClassTrust {
					t.Errorf("a wrong-CA failure classified as %s, want %s (%v)", class, HandshakeClassTrust, err)
				}
			}
		})
	}

	t.Run("insecure-skip-verify connects to anything and is a client-side knob only", func(t *testing.T) {
		cfg := mustBuildClient(t, p.clientBlock("", "", utils.JSON{"ca-files": []string{p.otherFile}, "insecure-skip-verify": true}))
		if _, err := get(t, httpsClient(cfg), addr); err != nil {
			t.Errorf("insecure-skip-verify still failed: %v", err)
		}
		_, err := ParseServerSettings(p.serverBlock(utils.JSON{"insecure-skip-verify": true}))
		if err != nil {
			t.Fatalf("an unknown key in the server block is ignored, as every other unknown key is: %v", err)
		}
	})
}

// The emptiness check on the system store. On this toolchain the check is
// CertPool.Equal against an empty pool, which is only ever true for a pool
// loaded from files (Linux) that found none; a platform-verifier pool (macOS,
// Windows) carries a flag that makes Equal false. So this case is only
// reachable on Linux with the store pointed at nothing, and is run that way
// in a container -- see the design notes. Anywhere else it reports what it
// saw and passes without asserting, rather than skipping in silence.
func TestSystemPoolEmptinessDetection(t *testing.T) {
	pool, err := newSystemPool()
	expectEmpty := runtime.GOOS == "linux" && os.Getenv("DXLIB_TLS_TEST_EXPECT_EMPTY_SYSTEM_POOL") == "1"
	switch {
	case expectEmpty && err == nil:
		t.Fatal("SSL_CERT_FILE and SSL_CERT_DIR point at nothing, yet the system pool was accepted as non-empty")
	case expectEmpty:
		if !strings.Contains(err.Error(), "SYSTEM_ROOTS_EMPTY") {
			t.Errorf("empty store refused with the wrong message: %v", err)
		}
		t.Logf("empty system store refused as intended: %v", err)
	case err != nil:
		t.Fatalf("this host's system store was refused: %v", err)
	default:
		t.Logf("GOOS=%s: system store accepted (equal-to-empty=%t); the empty case needs Linux with the store pointed at nothing", runtime.GOOS, pool.Equal(x509.NewCertPool()))
	}
}

// ---------------------------------------------------------------------------
// Transport mode and the migration rungs
// ---------------------------------------------------------------------------

// The transport is one explicit word. What each word refuses beside it, and
// what each mode does to a client at the handshake.
func TestModeIsExplicitAndRefusesContradictions(t *testing.T) {
	p := newPKI(t)

	// The retired key is refused with its replacement named, under any mode.
	_, err := ParseServerSettings(p.serverBlock(utils.JSON{"client-auth": "require-and-verify"}))
	wantConfigError(t, err, "client-auth", "RETIRED_KEY:USE:mode=http|https|mtls")

	t.Run("http refuses every TLS-bearing key by name", func(t *testing.T) {
		plain := utils.JSON{"mode": ModeHTTP}
		s, err := NewServerTLS(plain)
		if err != nil || s.Config != nil || s.Settings.Mode != ModeHTTP {
			t.Fatalf("a bare mode=http block: err=%v config=%v", err, s.Config)
		}
		if !strings.Contains(s.Summary(), "plaintext") {
			t.Errorf("summary does not say plaintext: %s", s.Summary())
		}
		refused := utils.JSON{
			"cert-file":                             "/x.crt",
			"tls-policy":                            PolicyModern,
			"ca-trust":                              CATrustCustom,
			"client-auth-migration":                 ClientAuthMigrationRequest,
			"deny-cipher-suites":                    []string{"CBC"},
			"deny-file":                             "/deny.json",
			"allowed-client-sans":                   []string{"a"},
			"min-version":                           "1.3",
			"deny-certificates":                     []any{},
			"deny-curves":                           []string{"P-521"},
			"deny-certificate-signature-algorithms": []string{"SHA1-RSA"},
		}
		for key, value := range refused {
			_, err := ParseServerSettings(utils.JSON{"mode": ModeHTTP, key: value})
			wantConfigError(t, err, key, "NOT_ALLOWED_WITH:mode=http")
		}
		// enabled is not a TLS-bearing key; false and http together are
		// redundant, not contradictory.
		if _, err := ParseServerSettings(utils.JSON{"mode": ModeHTTP, "enabled": false}); err != nil {
			t.Errorf("enabled=false under mode=http refused: %v", err)
		}
	})

	t.Run("https refuses the client-verification keys and needs the rest", func(t *testing.T) {
		refused := utils.JSON{
			"ca-trust":              CATrustCustom,
			"ca-files":              []string{p.rootFile},
			"client-auth-migration": ClientAuthMigrationRequest,
			"allowed-client-sans":   []string{"a"},
		}
		for key, value := range refused {
			_, err := ParseServerSettings(p.httpsBlock(utils.JSON{key: value}))
			wantConfigError(t, err, key, "NOT_ALLOWED_WITH:mode=https:REMOVE_THE_KEY_OR_SET:mode=mtls")
		}
		_, err := ParseServerSettings(p.httpsBlock(utils.JSON{"cert-file": nil, "key-file": nil}))
		wantConfigError(t, err, "cert-file", "REQUIRED_KEY_MISSING")
		_, err = ParseServerSettings(p.httpsBlock(utils.JSON{"tls-policy": nil}))
		wantConfigError(t, err, "tls-policy", "REQUIRED_KEY_MISSING")

		s := mustBuildServer(t, p.httpsBlock(nil))
		if s.Config.ClientAuth != tls.NoClientCert {
			t.Errorf("https ClientAuth = %v, want NoClientCert", s.Config.ClientAuth)
		}
		if s.Config.ClientCAs == nil {
			t.Error("https left ClientCAs nil: the system-roots fallback is reachable if ClientAuth is ever raised")
		}
		if s.ClientCAs != nil {
			t.Error("https built a CA pool reloader it has no files for")
		}
		// It serves a client with no certificate, and one with a certificate
		// from a CA it has never heard of: nothing about the client is checked.
		addr, _, _ := startServer(t, s.Config, okHandler(nil))
		if _, err := get(t, httpsClient(mustBuildClient(t, p.clientBlock("", "", nil))), addr); err != nil {
			t.Errorf("https refused a client with no certificate: %v", err)
		}
		otherLeaf := p.otherRoot.Issue(t, tlstest.LeafOptions{CommonName: "other", DNSNames: []string{"other.test"}})
		otherCert, otherKey := tlstest.WriteLeaf(t, p.dir, "otherclient-https", otherLeaf)
		if _, err := get(t, httpsClient(mustBuildClient(t, p.clientBlock(otherCert, otherKey, nil))), addr); err != nil {
			t.Errorf("https refused a client presenting a certificate from an unknown CA: %v", err)
		}
	})

	t.Run("mtls needs a trust source", func(t *testing.T) {
		_, err := ParseServerSettings(p.serverBlock(utils.JSON{"ca-trust": nil, "ca-files": nil}))
		wantConfigError(t, err, "ca-trust", "REQUIRED_KEY_MISSING")
		s, err := ParseServerSettings(p.serverBlock(nil))
		if err != nil || s.ClientAuth != tls.RequireAndVerifyClientCert || s.ClientAuthName != "require-and-verify" || s.ClientAuthMigration != "" {
			t.Errorf("mtls without a migration key: %+v err=%v", s, err)
		}
	})

	t.Run("enabled=false under mtls still validates every key", func(t *testing.T) {
		bad := utils.JSON{
			"ca-trust":              "ABSC",
			"tls-policy":            "strong",
			"client-auth-migration": "maybe",
			"deny-cipher-suites":    []string{"CHACHA2O"},
			"deny-curves":           []string{"P-1024"},
			"deny-file":             "",
			"min-version":           "1.1",
		}
		for key, value := range bad {
			_, err := NewServerTLS(p.serverBlock(utils.JSON{"enabled": false, key: value}))
			if err == nil {
				t.Errorf("enabled=false let a bad %s through", key)
				continue
			}
			var ce *DXConfigError
			if !asConfigError(err, &ce) || !strings.HasPrefix(ce.Key, key) {
				t.Errorf("enabled=false with bad %s: error on %v, want it on %s", key, err, key)
			}
		}
		_, err := NewServerTLS(p.serverBlock(utils.JSON{"enabled": false, "client-auth": "none"}))
		wantConfigError(t, err, "client-auth", "RETIRED_KEY")
		// And the files are not needed.
		s, err := NewServerTLS(p.serverBlock(utils.JSON{"enabled": false, "cert-file": "/nonexistent.crt", "key-file": "/nonexistent.key", "ca-files": []string{"/nonexistent.pem"}, "deny-file": "/nonexistent.json"}))
		if err != nil || s.Config != nil {
			t.Errorf("a disabled mtls block needed its files or built a config: err=%v config=%v", err, s.Config)
		}
	})
}

// The two migration rungs loosen, so they are named as a state, refused
// outside mtls, and visible in the summary as not enforcing.
func TestClientAuthMigrationIsAStateNotAMode(t *testing.T) {
	p := newPKI(t)
	_, err := ParseServerSettings(p.serverBlock(utils.JSON{"client-auth-migration": "require-and-verify"}))
	wantConfigError(t, err, "client-auth-migration", `INVALID_VALUE:"require-and-verify":VALID_VALUES=request|verify-if-given`)
	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"client-auth-migration": ""}))
	wantConfigError(t, err, "client-auth-migration", "EMPTY_VALUE")

	for _, rung := range []string{ClientAuthMigrationRequest, ClientAuthMigrationVerifyIfGiven} {
		s := mustBuildServer(t, p.serverBlock(utils.JSON{"client-auth-migration": rung}))
		want := map[string]tls.ClientAuthType{ClientAuthMigrationRequest: tls.RequestClientCert, ClientAuthMigrationVerifyIfGiven: tls.VerifyClientCertIfGiven}[rung]
		if s.Config.ClientAuth != want || s.Settings.ClientAuthName != rung {
			t.Errorf("%s: ClientAuth=%v name=%s", rung, s.Config.ClientAuth, s.Settings.ClientAuthName)
		}
		if !strings.Contains(s.Summary(), "NOT ENFORCING MTLS") || !strings.Contains(s.Summary(), "client-auth-migration="+rung) {
			t.Errorf("%s: the summary does not say the listener is not enforcing: %s", rung, s.Summary())
		}
		// A disabled block carries the same state in its summary.
		d, err := NewServerTLS(p.serverBlock(utils.JSON{"enabled": false, "client-auth-migration": rung}))
		if err != nil || !strings.Contains(d.Summary(), "NOT ENFORCING MTLS") {
			t.Errorf("%s disabled: err=%v summary=%s", rung, err, d.Summary())
		}
	}
	if s := mustBuildServer(t, p.serverBlock(nil)); strings.Contains(s.Summary(), "NOT ENFORCING") {
		t.Errorf("an enforcing listener's summary claims otherwise: %s", s.Summary())
	}
}

// The transport modes and rungs at the handshake, with real clients.
func TestModesAndRungsAtTheHandshake(t *testing.T) {
	p := newPKI(t)
	_, goodCert, goodKey := p.clientLeaf(t, "good", tlstest.LeafOptions{DNSNames: []string{"good.test"}})
	otherLeaf := p.otherRoot.Issue(t, tlstest.LeafOptions{CommonName: "other", DNSNames: []string{"other.test"}})
	otherCert, otherKey := tlstest.WriteLeaf(t, p.dir, "otherclient", otherLeaf)

	noCert := mustBuildClient(t, p.clientBlock("", "", nil))
	good := mustBuildClient(t, p.clientBlock(goodCert, goodKey, nil))
	wrongCA := mustBuildClient(t, p.clientBlock(otherCert, otherKey, nil))

	// The handler reports whether the connection carried a verified chain,
	// which is what PeerCertificate on the request is gated on.
	verifiedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS != nil && len(r.TLS.VerifiedChains) > 0 {
			w.Header().Set("X-Verified", PeerIdentity(r.TLS.VerifiedChains[0][0]))
		}
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			w.Header().Set("X-Presented", PeerIdentity(r.TLS.PeerCertificates[0]))
		}
		w.WriteHeader(http.StatusOK)
	})

	cases := []struct {
		name                              string
		block                             utils.JSON
		verifies                          bool
		noCertOK, goodOK, wrongCAOK       bool
		wrongCAVerified, wrongCAPresented bool
	}{
		{"mtls", p.serverBlock(nil), true, false, true, false, false, false},
		{"mtls+verify-if-given", p.serverBlock(utils.JSON{"client-auth-migration": ClientAuthMigrationVerifyIfGiven}), true, true, true, false, false, false},
		{"mtls+request", p.serverBlock(utils.JSON{"client-auth-migration": ClientAuthMigrationRequest}), false, true, true, true, false, true},
		{"https", p.httpsBlock(nil), false, true, true, true, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := mustBuildServer(t, c.block)
			addr, _, _ := startServer(t, s.Config, verifiedHandler)

			_, err := get(t, httpsClient(noCert), addr)
			if (err == nil) != c.noCertOK {
				t.Errorf("no certificate: err=%v, want accepted=%t", err, c.noCertOK)
			}
			resp, err := get(t, httpsClient(good), addr)
			if (err == nil) != c.goodOK {
				t.Errorf("good certificate: err=%v, want accepted=%t", err, c.goodOK)
			}
			// Only the two verifying modes produce a verified chain. Under
			// "request" even a good certificate is unverified, which is why
			// PeerCertificate on a request stays nil in that mode.
			if err == nil && (resp.Header.Get("X-Verified") == "good.test") != c.verifies {
				t.Errorf("good certificate under %s: X-Verified=%q, want verified=%t", c.name, resp.Header.Get("X-Verified"), c.verifies)
			}
			resp, err = get(t, httpsClient(wrongCA), addr)
			if (err == nil) != c.wrongCAOK {
				t.Errorf("wrong-CA certificate: err=%v, want accepted=%t", err, c.wrongCAOK)
			}
			if err == nil {
				// The trap: under "request" the certificate is in
				// PeerCertificates but not in VerifiedChains. Anything that
				// reads PeerCertificates[0] as an identity is fooled here.
				if (resp.Header.Get("X-Verified") != "") != c.wrongCAVerified {
					t.Errorf("wrong-CA certificate verified=%q, want verified=%t", resp.Header.Get("X-Verified"), c.wrongCAVerified)
				}
				if (resp.Header.Get("X-Presented") != "") != c.wrongCAPresented {
					t.Errorf("wrong-CA certificate presented=%q, want presented=%t", resp.Header.Get("X-Presented"), c.wrongCAPresented)
				}
			}
		})
	}
}

// A leaf issued by the intermediate, presented with the intermediate, chains
// to a pool holding only the root.
func TestIntermediateChainIsAccepted(t *testing.T) {
	p := newPKI(t)
	leaf, certFile, keyFile := p.clientLeaf(t, "chained", tlstest.LeafOptions{DNSNames: []string{"chained.test"}})
	if len(leaf.Chain) != 2 {
		t.Fatalf("test leaf chain has %d certificates, want leaf+intermediate", len(leaf.Chain))
	}
	s := mustBuildServer(t, p.serverBlock(nil))
	addr, _, _ := startServer(t, s.Config, okHandler(nil))
	if _, err := get(t, httpsClient(mustBuildClient(t, p.clientBlock(certFile, keyFile, nil))), addr); err != nil {
		t.Fatalf("chained client refused: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Identity: the allow-list is enforced in the handshake, before any HTTP
// ---------------------------------------------------------------------------

func TestAllowedClientSANsRejectAtHandshake(t *testing.T) {
	p := newPKI(t)
	_, allowedCert, allowedKey := p.clientLeaf(t, "allowed", tlstest.LeafOptions{
		DNSNames: []string{"queue-scheduler.dcc.svc"},
		URIs:     []string{"spiffe://cluster.local/ns/dcc/sa/queue-scheduler"},
	})
	_, strangerCert, strangerKey := p.clientLeaf(t, "stranger", tlstest.LeafOptions{DNSNames: []string{"stranger.dcc.svc"}})
	allowed := mustBuildClient(t, p.clientBlock(allowedCert, allowedKey, nil))
	stranger := mustBuildClient(t, p.clientBlock(strangerCert, strangerKey, nil))

	var hits atomic.Int32
	t.Run("enforce", func(t *testing.T) {
		hits.Store(0)
		s := mustBuildServer(t, p.serverBlock(utils.JSON{"allowed-client-sans": []string{"spiffe://cluster.local/ns/dcc/sa/queue-scheduler"}}))
		addr, _, errorLog := startServer(t, s.Config, okHandler(&hits))

		_, err := get(t, httpsClient(stranger), addr)
		if err == nil {
			t.Fatal("a valid certificate from our CA with a SAN not in the allow-list was admitted")
		}
		if hits.Load() != 0 {
			t.Fatal("the handler ran for a caller the allow-list refused: the rejection is not at the handshake")
		}
		if class, _ := ClassifyHandshakeError(err); class != HandshakeClassPeerRejectedUs {
			t.Errorf("client saw class %s, want %s (%v)", class, HandshakeClassPeerRejectedUs, err)
		}
		if !errorLog.contains(t, "TLS_PEER_NOT_ALLOWED:stranger.dcc.svc") {
			t.Errorf("server error log does not name the refused identity: %v", errorLog.lines)
		}

		resp, err := get(t, httpsClient(allowed), addr)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("allowed client: err=%v", err)
		}
		if hits.Load() != 1 {
			t.Errorf("handler hits = %d, want 1", hits.Load())
		}
	})

	t.Run("dns entries match case-insensitively", func(t *testing.T) {
		s := mustBuildServer(t, p.serverBlock(utils.JSON{"allowed-client-sans": []string{"Queue-Scheduler.DCC.svc"}}))
		addr, _, _ := startServer(t, s.Config, okHandler(nil))
		if _, err := get(t, httpsClient(allowed), addr); err != nil {
			t.Errorf("DNS SAN should match case-insensitively: %v", err)
		}
	})

	t.Run("log-only admits and records", func(t *testing.T) {
		hits.Store(0)
		s := mustBuildServer(t, p.serverBlock(utils.JSON{
			"allowed-client-sans":          []string{"spiffe://cluster.local/ns/dcc/sa/queue-scheduler"},
			"allowed-client-sans-log-only": true,
		}))
		addr, _, _ := startServer(t, s.Config, okHandler(&hits))
		if _, err := get(t, httpsClient(stranger), addr); err != nil {
			t.Fatalf("log-only mode refused the stranger: %v", err)
		}
		if hits.Load() != 1 {
			t.Errorf("handler hits = %d, want 1", hits.Load())
		}
	})
}

// ---------------------------------------------------------------------------
// Hardening: key strength, CA-as-leaf, and what the policy refuses to speak
// ---------------------------------------------------------------------------

// The floor is real, not merely configured: a 1024-bit RSA leaf that chains
// correctly to the trusted CA is still refused, on both sides.
func TestKeyStrengthFloorRefusesAValidWeakCertificate(t *testing.T) {
	p := newPKI(t)
	_, weakCert, weakKey := p.clientLeaf(t, "weak", tlstest.LeafOptions{DNSNames: []string{"weak.test"}, RSABits: 1024})
	_, strongCert, strongKey := p.clientLeaf(t, "strong", tlstest.LeafOptions{DNSNames: []string{"strong.test"}, RSABits: 2048})

	// Our own certificate is checked at load.
	_, err := NewServerTLS(p.serverBlock(utils.JSON{"cert-file": weakCert, "key-file": weakKey}))
	wantConfigError(t, err, "cert-file", "KEY_TOO_WEAK:RSA-1024")
	_, err = BuildClientConfig(p.clientBlock(weakCert, weakKey, nil))
	wantConfigError(t, err, "cert-file", "KEY_TOO_WEAK:RSA-1024")

	// A peer's is checked in the handshake. The weak client has to be built
	// with the floor lowered on its own side, which this library refuses to
	// do, so its config is assembled by hand here.
	weakPair, err := tls.LoadX509KeyPair(weakCert, weakKey)
	if err != nil {
		t.Fatal(err)
	}
	strong := mustBuildClient(t, p.clientBlock(strongCert, strongKey, nil))
	weak := strong.Clone()
	weak.GetClientCertificate = nil
	weak.Certificates = []tls.Certificate{weakPair}

	var hits atomic.Int32
	s := mustBuildServer(t, p.serverBlock(nil))
	addr, _, errorLog := startServer(t, s.Config, okHandler(&hits))
	if _, err := get(t, httpsClient(weak), addr); err == nil {
		t.Fatal("a 1024-bit RSA client certificate that chains to our CA was admitted")
	}
	if hits.Load() != 0 {
		t.Error("the handler ran for the weak client")
	}
	if !errorLog.contains(t, "KEY_TOO_WEAK:RSA-1024") {
		t.Errorf("server error log does not name the weak key: %v", errorLog.lines)
	}
	if _, err := get(t, httpsClient(strong), addr); err != nil {
		t.Errorf("a 2048-bit RSA client was refused: %v", err)
	}

	// The floor may be raised: 3072 refuses the 2048-bit client.
	s = mustBuildServer(t, p.serverBlock(utils.JSON{"min-rsa-bits": 3072}))
	addr, _, _ = startServer(t, s.Config, okHandler(nil))
	if _, err := get(t, httpsClient(strong), addr); err == nil {
		t.Error("min-rsa-bits=3072 admitted a 2048-bit client")
	}

	// And the client applies the same floor to the server it dials.
	weakServer := p.intermediate.Issue(t, tlstest.LeafOptions{CommonName: "weak-server", DNSNames: []string{"api.test"}, RSABits: 1024})
	weakServerPair, err := tls.X509KeyPair(tlstest.CertPEM(weakServer.Chain...), tlstest.KeyPEM(t, weakServer.Key))
	if err != nil {
		t.Fatal(err)
	}
	addr, _, _ = startServer(t, &tls.Config{Certificates: []tls.Certificate{weakServerPair}}, okHandler(nil))
	if _, err := get(t, httpsClient(strong), addr); err == nil {
		t.Error("a 1024-bit RSA server certificate was accepted by the client")
	}
}

// A certificate with BasicConstraints CA:TRUE chains fine as a leaf; it is
// refused anyway. Go's own check that the client certificate carries
// ExtKeyUsageClientAuth is confirmed here too rather than assumed.
func TestCACertificateAsLeafAndMissingClientEKUAreRefused(t *testing.T) {
	p := newPKI(t)
	_, caLeafCert, caLeafKey := p.clientLeaf(t, "caleaf", tlstest.LeafOptions{DNSNames: []string{"caleaf.test"}, IsCA: true})
	_, serverOnlyCert, serverOnlyKey := p.clientLeaf(t, "serveronly", tlstest.LeafOptions{DNSNames: []string{"serveronly.test"}, ServerOnly: true})

	s := mustBuildServer(t, p.serverBlock(nil))
	addr, _, errorLog := startServer(t, s.Config, okHandler(nil))

	if _, err := get(t, httpsClient(mustBuildClient(t, p.clientBlock(caLeafCert, caLeafKey, nil))), addr); err == nil {
		t.Error("a CA:TRUE certificate was accepted as a client identity")
	}
	if !errorLog.contains(t, "TLS_PEER_IS_A_CA_CERTIFICATE") {
		t.Errorf("server error log does not name the CA-as-leaf refusal: %v", errorLog.lines)
	}
	if _, err := get(t, httpsClient(mustBuildClient(t, p.clientBlock(serverOnlyCert, serverOnlyKey, nil))), addr); err == nil {
		t.Error("a certificate without ExtKeyUsageClientAuth was accepted; Go was expected to refuse it")
	}
}

// What the policy refuses to speak, driven by real clients that offer only the
// refused parameter. Go's client will construct a TLS 1.0-only hello and a
// CBC-only or 3DES-only suite list when told to explicitly, so each negative
// case is a real handshake and not a claim.
func TestPolicyRefusesDowngradedClients(t *testing.T) {
	p := newPKI(t)
	_, certFile, keyFile := p.clientLeaf(t, "c", tlstest.LeafOptions{DNSNames: []string{"c.test"}})

	base := mustBuildClient(t, p.clientBlock(certFile, keyFile, nil))
	offering := func(mutate func(*tls.Config)) *tls.Config {
		c := base.Clone()
		c.VerifyConnection = nil
		mutate(c)
		return c
	}

	t.Run("intermediate", func(t *testing.T) {
		s := mustBuildServer(t, p.serverBlock(nil))
		addr, _, errorLog := startServer(t, s.Config, okHandler(nil))

		if _, err := get(t, httpsClient(base), addr); err != nil {
			t.Fatalf("a compliant client was refused: %v", err)
		}
		resp, err := get(t, httpsClient(offering(func(c *tls.Config) { c.MaxVersion = tls.VersionTLS12 })), addr)
		if err != nil {
			t.Fatalf("a TLS 1.2 client with the default suites was refused under intermediate: %v", err)
		}
		if resp.TLS.Version != tls.VersionTLS12 || !isAEAD(resp.TLS.CipherSuite) {
			t.Errorf("negotiated %s %s, want TLS 1.2 with an ECDHE+AEAD suite", tls.VersionName(resp.TLS.Version), tls.CipherSuiteName(resp.TLS.CipherSuite))
		}

		refused := []struct {
			name   string
			mutate func(*tls.Config)
		}{
			{"TLS 1.0 only", func(c *tls.Config) { c.MinVersion, c.MaxVersion = tls.VersionTLS10, tls.VersionTLS10 }},
			{"TLS 1.1 only", func(c *tls.Config) { c.MinVersion, c.MaxVersion = tls.VersionTLS11, tls.VersionTLS11 }},
			{"CBC only", func(c *tls.Config) {
				c.MaxVersion = tls.VersionTLS12
				c.CipherSuites = []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA, tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA}
			}},
			{"3DES only", func(c *tls.Config) {
				c.MaxVersion = tls.VersionTLS12
				c.CipherSuites = []uint16{tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA}
			}},
			{"RSA key transport only", func(c *tls.Config) {
				c.MaxVersion = tls.VersionTLS12
				c.CipherSuites = []uint16{tls.TLS_RSA_WITH_AES_128_GCM_SHA256}
			}},
			{"P-521 only", func(c *tls.Config) { c.CurvePreferences = []tls.CurveID{tls.CurveP521} }},
		}
		for _, r := range refused {
			t.Run(r.name, func(t *testing.T) {
				before := errorLog.count()
				_, err := get(t, httpsClient(offering(r.mutate)), addr)
				if err == nil {
					t.Fatalf("a client offering only %s was accepted", r.name)
				}
				// Two things separate "the server refused it" from "Go's
				// client would not even build the hello": the error is the
				// server's alert, and the server logged a handshake error for
				// it. A locally constructed failure has neither.
				if !strings.Contains(err.Error(), "remote error: tls:") {
					t.Errorf("%s: error is not a server alert, so the parameter may never have been offered: %v", r.name, err)
				}
				if !errorLog.grewPast(before) {
					t.Errorf("%s: the server logged no handshake error, so the refusal did not happen there: %v", r.name, err)
				}
				t.Logf("%s refused by the server: %v", r.name, err)
			})
		}
	})

	t.Run("modern", func(t *testing.T) {
		s := mustBuildServer(t, p.serverBlock(utils.JSON{"tls-policy": PolicyModern}))
		addr, _, _ := startServer(t, s.Config, okHandler(nil))
		resp, err := get(t, httpsClient(base), addr)
		if err != nil {
			t.Fatalf("a compliant client was refused under modern: %v", err)
		}
		if resp.TLS.Version != tls.VersionTLS13 {
			t.Errorf("negotiated %s, want TLS 1.3", tls.VersionName(resp.TLS.Version))
		}
		if _, err := get(t, httpsClient(offering(func(c *tls.Config) { c.MaxVersion = tls.VersionTLS12 })), addr); err == nil {
			t.Error("a TLS 1.2-max client was accepted under modern")
		}
	})
}

func isAEAD(suite uint16) bool {
	for _, id := range intermediateSuites12 {
		if id == suite {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Hot reload
// ---------------------------------------------------------------------------

func serialSeenBy(t *testing.T, cfg *tls.Config, addr string) string {
	t.Helper()
	resp, err := get(t, httpsClient(cfg), addr)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	return resp.TLS.PeerCertificates[0].SerialNumber.String()
}

func TestServerCertificateHotReload(t *testing.T) {
	p := newPKI(t)
	s := mustBuildServer(t, p.httpsBlock(nil))
	addr, _, _ := startServer(t, s.Config, okHandler(nil))
	client := mustBuildClient(t, p.clientBlock("", "", nil))

	first := serialSeenBy(t, client, addr)
	if first != p.server.Cert.SerialNumber.String() {
		t.Fatalf("served serial %s, want %s", first, p.server.Cert.SerialNumber)
	}

	// A broken key on disk: the previous certificate keeps serving.
	if err := os.WriteFile(p.serverKey, []byte("-----BEGIN PRIVATE KEY-----\ngarbage\n-----END PRIVATE KEY-----\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := serialSeenBy(t, client, addr); got != first {
		t.Errorf("after a broken key the served serial changed to %s", got)
	}

	// A good rotation: the next handshake serves the new leaf, no restart.
	renewed := p.intermediate.Issue(t, tlstest.LeafOptions{CommonName: "api.test renewed", DNSNames: []string{"api.test"}})
	if err := os.WriteFile(p.serverCert, tlstest.CertPEM(renewed.Chain...), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.serverKey, tlstest.KeyPEM(t, renewed.Key), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := serialSeenBy(t, client, addr); got != renewed.Cert.SerialNumber.String() {
		t.Errorf("after rotation served serial %s, want %s", got, renewed.Cert.SerialNumber)
	}

	// A rotation to a certificate that fails the floor is refused and the
	// previous one stays in service.
	weak := p.intermediate.Issue(t, tlstest.LeafOptions{CommonName: "weak", DNSNames: []string{"api.test"}, RSABits: 1024})
	if err := os.WriteFile(p.serverCert, tlstest.CertPEM(weak.Chain...), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.serverKey, tlstest.KeyPEM(t, weak.Key), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := serialSeenBy(t, client, addr); got != renewed.Cert.SerialNumber.String() {
		t.Errorf("a weak rotation was put into service (serial %s)", got)
	}
}

func TestClientCertificateHotReload(t *testing.T) {
	p := newPKI(t)
	first, certFile, keyFile := p.clientLeaf(t, "rotating", tlstest.LeafOptions{DNSNames: []string{"rotating.test"}})
	var seen atomic.Value
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Store(r.TLS.VerifiedChains[0][0].SerialNumber.String())
	})
	s := mustBuildServer(t, p.serverBlock(nil))
	addr, _, _ := startServer(t, s.Config, handler)
	client := httpsClient(mustBuildClient(t, p.clientBlock(certFile, keyFile, nil)))

	if _, err := get(t, client, addr); err != nil {
		t.Fatal(err)
	}
	if seen.Load() != first.Cert.SerialNumber.String() {
		t.Fatalf("server saw %v, want %s", seen.Load(), first.Cert.SerialNumber)
	}
	renewed := p.intermediate.Issue(t, tlstest.LeafOptions{CommonName: "rotating", DNSNames: []string{"rotating.test"}})
	if err := os.WriteFile(certFile, tlstest.CertPEM(renewed.Chain...), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, tlstest.KeyPEM(t, renewed.Key), 0o600); err != nil {
		t.Fatal(err)
	}
	client.CloseIdleConnections() // a kept-alive connection would not handshake again
	if _, err := get(t, client, addr); err != nil {
		t.Fatal(err)
	}
	if seen.Load() != renewed.Cert.SerialNumber.String() {
		t.Errorf("after rotation server saw %v, want %s", seen.Load(), renewed.Cert.SerialNumber)
	}
}

// The CA pool reloads too, through GetConfigForClient, and the config it hands
// back still negotiates HTTP/2 -- the clone is taken from the config in use,
// after http.Server added "h2" to it.
func TestClientCAPoolHotReloadKeepsHTTP2(t *testing.T) {
	p := newPKI(t)
	_, oldCert, oldKey := p.clientLeaf(t, "old", tlstest.LeafOptions{DNSNames: []string{"old.test"}})
	newRoot := tlstest.NewRootCA(t, "successor root")
	newLeaf := newRoot.Issue(t, tlstest.LeafOptions{CommonName: "new", DNSNames: []string{"new.test"}})
	newCert, newKey := tlstest.WriteLeaf(t, p.dir, "new", newLeaf)

	s := mustBuildServer(t, p.serverBlock(nil))
	addr, _, _ := startServer(t, s.Config, okHandler(nil))
	oldClient := httpsClient(mustBuildClient(t, p.clientBlock(oldCert, oldKey, nil)))
	newClient := httpsClient(mustBuildClient(t, p.clientBlock(newCert, newKey, nil)))

	resp, err := get(t, oldClient, addr)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ProtoMajor != 2 {
		t.Fatalf("before reload negotiated HTTP/%d, want 2", resp.ProtoMajor)
	}
	if _, err := get(t, newClient, addr); err == nil {
		t.Fatal("the successor CA's client was accepted before the pool was rotated")
	}

	if err := os.WriteFile(p.rootFile, tlstest.CertPEM(newRoot.Cert), 0o600); err != nil {
		t.Fatal(err)
	}
	newClient.CloseIdleConnections()
	oldClient.CloseIdleConnections()
	resp, err = get(t, newClient, addr)
	if err != nil {
		t.Fatalf("after rotating the CA file the successor's client was still refused: %v", err)
	}
	if resp.ProtoMajor != 2 {
		t.Errorf("after reload negotiated HTTP/%d, want 2: the clone lost h2", resp.ProtoMajor)
	}
	if _, err := get(t, oldClient, addr); err == nil {
		t.Error("the retired CA's client was still accepted after the pool was rotated")
	}
}

// ---------------------------------------------------------------------------
// server-name, and telling a clock problem from a trust problem
// ---------------------------------------------------------------------------

func TestServerNameOverride(t *testing.T) {
	p := newPKI(t)
	// A server whose certificate names only api.test, dialled by IP.
	nameOnly := p.intermediate.Issue(t, tlstest.LeafOptions{CommonName: "api.test", DNSNames: []string{"api.test"}})
	certFile, keyFile := tlstest.WriteLeaf(t, p.dir, "nameonly", nameOnly)
	s := mustBuildServer(t, p.httpsBlock(utils.JSON{"cert-file": certFile, "key-file": keyFile}))
	addr, _, _ := startServer(t, s.Config, okHandler(nil))

	_, err := get(t, httpsClient(mustBuildClient(t, p.clientBlock("", "", utils.JSON{"server-name": nil}))), addr)
	if err == nil {
		t.Fatal("dialling by IP with no server-name verified against a name-only certificate")
	}
	if class, _ := ClassifyHandshakeError(err); class != HandshakeClassName {
		t.Errorf("classified as %s, want %s: %v", class, HandshakeClassName, err)
	}
	if _, err := get(t, httpsClient(mustBuildClient(t, p.clientBlock("", "", utils.JSON{"server-name": "api.test"}))), addr); err != nil {
		t.Errorf("server-name override did not take: %v", err)
	}
}

// An operator on an air-gapped host needs "your clock is wrong" and "your CA
// is wrong" to be different messages, on both sides of the connection.
func TestValidityWindowIsDistinguishableFromTrust(t *testing.T) {
	p := newPKI(t)
	client := mustBuildClient(t, p.clientBlock("", "", nil))

	t.Run("client side, server certificate not yet valid", func(t *testing.T) {
		future := p.intermediate.Issue(t, tlstest.LeafOptions{CommonName: "future", DNSNames: []string{"api.test"}, NotBefore: time.Now().Add(24 * time.Hour), NotAfter: time.Now().Add(48 * time.Hour)})
		pair, err := tls.X509KeyPair(tlstest.CertPEM(future.Chain...), tlstest.KeyPEM(t, future.Key))
		if err != nil {
			t.Fatal(err)
		}
		addr, _, _ := startServer(t, &tls.Config{Certificates: []tls.Certificate{pair}}, okHandler(nil))
		_, err = get(t, httpsClient(client), addr)
		if err == nil {
			t.Fatal("a not-yet-valid server certificate was accepted")
		}
		class, advice := ClassifyHandshakeError(err)
		if class != HandshakeClassValidityWindow || !strings.Contains(advice, "clock") {
			t.Errorf("classified as %s (%q), want %s with clock advice: %v", class, advice, HandshakeClassValidityWindow, err)
		}
	})

	t.Run("server side, client certificate not yet valid vs wrong CA", func(t *testing.T) {
		_, futureCert, futureKey := p.clientLeaf(t, "future", tlstest.LeafOptions{DNSNames: []string{"future.test"}, NotBefore: time.Now().Add(24 * time.Hour), NotAfter: time.Now().Add(48 * time.Hour)})
		otherLeaf := p.otherRoot.Issue(t, tlstest.LeafOptions{CommonName: "other", DNSNames: []string{"other.test"}})
		otherCert, otherKey := tlstest.WriteLeaf(t, p.dir, "other", otherLeaf)

		s := mustBuildServer(t, p.serverBlock(nil))
		addr, _, errorLog := startServer(t, s.Config, okHandler(nil))

		_, _ = get(t, httpsClient(mustBuildClient(t, p.clientBlock(futureCert, futureKey, nil))), addr)
		_, _ = get(t, httpsClient(mustBuildClient(t, p.clientBlock(otherCert, otherKey, nil))), addr)

		var sawValidity, sawTrust bool
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) && !(sawValidity && sawTrust) {
			errorLog.mu.Lock()
			for _, line := range errorLog.lines {
				class, _ := ClassifyHandshakeText(line)
				switch class {
				case HandshakeClassValidityWindow:
					sawValidity = true
				case HandshakeClassTrust:
					sawTrust = true
				}
			}
			errorLog.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
		if !sawValidity || !sawTrust {
			t.Errorf("server log lines did not classify into both VALIDITY_WINDOW and TRUST (validity=%t trust=%t): %v", sawValidity, sawTrust, errorLog.lines)
		}
	})
}

// ---------------------------------------------------------------------------
// Preflight and the expiry gauge
// ---------------------------------------------------------------------------

func TestPreflightReportsWhatAHandshakeWouldFind(t *testing.T) {
	p := newPKI(t)

	good := PreflightServer("api", p.serverBlock(nil))
	if !good.OK {
		t.Fatalf("a good configuration failed preflight:\n%s", good.Text)
	}
	for _, want := range []string{"mode=mtls", "chains to the configured ca-trust pool: yes", "tls-policy=intermediate", "client-auth=require-and-verify", "now=", "days-remaining=", "spki-sha256="} {
		if !strings.Contains(good.Text, want) {
			t.Errorf("report lacks %q:\n%s", want, good.Text)
		}
	}
	if plain := PreflightServer("oam", utils.JSON{"mode": ModeHTTP}); !plain.OK || !strings.Contains(plain.Text, "mode=http: plaintext") {
		t.Errorf("mode=http report:\n%s", plain.Text)
	}
	if https := PreflightServer("api", p.httpsBlock(nil)); !https.OK || !strings.Contains(https.Text, "no client is verified") || strings.Contains(https.Text, "ca-trust pool") {
		t.Errorf("mode=https report:\n%s", https.Text)
	}

	wrongCA := PreflightServer("api", p.serverBlock(utils.JSON{"ca-files": []string{p.otherFile}}))
	if wrongCA.OK || !strings.Contains(wrongCA.Text, "does NOT chain") {
		t.Errorf("the wrong CA mounted was not reported:\n%s", wrongCA.Text)
	}

	future := p.intermediate.Issue(t, tlstest.LeafOptions{CommonName: "future", DNSNames: []string{"api.test"}, NotBefore: time.Now().Add(24 * time.Hour), NotAfter: time.Now().Add(48 * time.Hour)})
	futureCert, futureKey := tlstest.WriteLeaf(t, p.dir, "future", future)
	clock := PreflightServer("api", p.serverBlock(utils.JSON{"cert-file": futureCert, "key-file": futureKey}))
	if clock.OK || !strings.Contains(clock.Text, "NOT YET VALID") || !strings.Contains(clock.Text, "clock") {
		t.Errorf("a not-yet-valid certificate was not reported as a possible clock problem:\n%s", clock.Text)
	}

	weak := p.intermediate.Issue(t, tlstest.LeafOptions{CommonName: "weak", DNSNames: []string{"api.test"}, RSABits: 1024})
	weakCert, weakKey := tlstest.WriteLeaf(t, p.dir, "weak", weak)
	floor := PreflightServer("api", p.serverBlock(utils.JSON{"cert-file": weakCert, "key-file": weakKey}))
	if floor.OK || !strings.Contains(floor.Text, "KEY_TOO_WEAK:RSA-1024") {
		t.Errorf("a weak key was not reported:\n%s", floor.Text)
	}

	soon := p.intermediate.Issue(t, tlstest.LeafOptions{CommonName: "soon", DNSNames: []string{"api.test"}, NotAfter: time.Now().Add(10 * 24 * time.Hour)})
	soonCert, soonKey := tlstest.WriteLeaf(t, p.dir, "soon", soon)
	expiring := PreflightServer("api", p.serverBlock(utils.JSON{"cert-file": soonCert, "key-file": soonKey}))
	if expiring.OK || !strings.Contains(expiring.Text, "expires in") {
		t.Errorf("a certificate inside the warning window was not reported:\n%s", expiring.Text)
	}

	bad := PreflightServer("api", p.serverBlock(utils.JSON{"ca-trust": "ABSC"}))
	if bad.OK || !strings.Contains(bad.Text, `ca-trust:INVALID_VALUE:"ABSC"`) {
		t.Errorf("a configuration error was not reported:\n%s", bad.Text)
	}

	// The client report, with a dial.
	s := mustBuildServer(t, p.httpsBlock(nil))
	addr, _, _ := startServer(t, s.Config, okHandler(nil))
	dialled := PreflightClient(p.clientBlock("", "", nil), addr)
	if !dialled.OK || !strings.Contains(dialled.Text, "negotiated version=TLS 1.3") || !strings.Contains(dialled.Text, "peer identity=api.test") {
		t.Errorf("dial report:\n%s", dialled.Text)
	}
	refused := PreflightClient(p.clientBlock("", "", utils.JSON{"ca-files": []string{p.otherFile}}), addr)
	if refused.OK || !strings.Contains(refused.Text, "[TRUST]") {
		t.Errorf("a refused dial was not classified:\n%s", refused.Text)
	}
}

func TestCertificatesInServiceAreObservedForTheExpiryGauge(t *testing.T) {
	p := newPKI(t)
	_ = mustBuildServer(t, p.serverBlock(nil))
	for _, c := range ObservedCertificates() {
		if c.Role == "server" && c.File == p.serverCert {
			if time.Until(c.NotAfter) < 300*24*time.Hour {
				t.Errorf("observed NotAfter %s is not the test certificate's", c.NotAfter)
			}
			return
		}
	}
	t.Errorf("the server certificate is not registered for the expiry gauge: %+v", ObservedCertificates())
}

// A context-bearing dial through the reloading client config, to make sure
// nothing in the hooks trips over a cancelled context.
func TestDialWithContextThroughReloadingConfig(t *testing.T) {
	p := newPKI(t)
	_, certFile, keyFile := p.clientLeaf(t, "ctx", tlstest.LeafOptions{DNSNames: []string{"ctx.test"}})
	s := mustBuildServer(t, p.serverBlock(nil))
	addr, _, _ := startServer(t, s.Config, okHandler(nil))
	cfg := mustBuildClient(t, p.clientBlock(certFile, keyFile, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d := tls.Dialer{Config: cfg}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
}

// ---------------------------------------------------------------------------
// Deny-lists: subtractive, family tokens, no-op versus typo, never to empty
// ---------------------------------------------------------------------------

func suiteIDs(names ...string) []uint16 {
	byName := map[string]uint16{}
	for _, s := range tls.CipherSuites() {
		byName[s.Name] = s.ID
	}
	out := make([]uint16, 0, len(names))
	for _, n := range names {
		out = append(out, byName[n])
	}
	return out
}

func suiteList(ids []uint16) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, tls.CipherSuiteName(id))
	}
	return out
}

// A deny names what goes; the rest of the resolved policy stays, in the
// profile's order. A family token expands; an exact IANA name is itself; a
// token that removes nothing is accepted; a token outside the vocabulary is
// not; and nothing can be denied down to an empty set.
func TestDenyListsSubtractFromTheResolvedPolicy(t *testing.T) {
	p := newPKI(t)

	s, err := ParseServerSettings(p.serverBlock(utils.JSON{"deny-cipher-suites": []string{" chacha20 "}}))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256", "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"}
	if got := suiteList(s.Policy.CipherSuites); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("CHACHA20 denied: suites = %v, want %v", got, want)
	}
	if len(s.Policy.Deny.SuiteTokens) != 1 || s.Policy.Deny.SuiteTokens[0] != "CHACHA20" {
		t.Errorf("token not canonicalised: %v", s.Policy.Deny.SuiteTokens)
	}
	if !strings.Contains(s.Policy.Summary(), "deny=[cipher-suites=CHACHA20]") {
		t.Errorf("summary does not carry the deny: %s", s.Policy.Summary())
	}

	s, err = ParseServerSettings(p.serverBlock(utils.JSON{"deny-cipher-suites": []string{"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"}}))
	if err != nil || len(s.Policy.CipherSuites) != 5 {
		t.Errorf("one IANA name denied: err=%v suites=%v", err, suiteList(s.Policy.CipherSuites))
	}
	for _, id := range s.Policy.CipherSuites {
		if id == tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 {
			t.Error("the denied suite is still offered")
		}
	}

	// After narrowing: the operator kept two, the deny takes one of them.
	s, err = ParseServerSettings(p.serverBlock(utils.JSON{
		"cipher-suites":      []string{"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384", "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"},
		"deny-cipher-suites": []string{"AES-128"},
	}))
	if err != nil || len(s.Policy.CipherSuites) != 1 || s.Policy.CipherSuites[0] != tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384 {
		t.Errorf("deny after narrowing: err=%v suites=%v", err, suiteList(s.Policy.CipherSuites))
	}

	// Recognised tokens that name nothing the profile offers: accepted, and the
	// list is untouched. This is the fleet-wide-advisory case.
	s, err = ParseServerSettings(p.serverBlock(utils.JSON{"deny-cipher-suites": []string{"CBC", "3DES", "RC4", "RSA-KEY-TRANSPORT", "SHA1", "TLS_RSA_WITH_RC4_128_SHA"}}))
	if err != nil || len(s.Policy.CipherSuites) != len(intermediateSuites12) {
		t.Errorf("no-op denies: err=%v suites=%v", err, suiteList(s.Policy.CipherSuites))
	}
	outcome, _ := s.Policy.effective(nil)
	if len(outcome.noOps) != 6 || len(outcome.removedSuites) != 0 {
		t.Errorf("no-op report: removed=%v no-ops=%v", outcome.removedSuites, outcome.noOps)
	}
	// Under a 1.3-only policy every suite deny is a no-op, and the report says
	// why, rather than the key being refused as it is for cipher-suites.
	s, err = ParseServerSettings(p.serverBlock(utils.JSON{"tls-policy": PolicyModern, "deny-cipher-suites": []string{"CHACHA20"}}))
	if err != nil {
		t.Errorf("a suite deny under modern was refused: %v", err)
	} else if outcome, _ = s.Policy.effective(nil); !strings.Contains(s.Policy.denyReport(outcome), "crypto/tls fixes the 1.3 suites") {
		t.Errorf("1.3 no-op not explained: %s", s.Policy.denyReport(outcome))
	}

	// A typo is a startup error naming the vocabulary; so is a pattern.
	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"deny-cipher-suites": []string{"CHACHA2O"}}))
	wantConfigError(t, err, "deny-cipher-suites[0]", `INVALID_VALUE:"CHACHA2O":VALID_VALUES=CBC|3DES|RC4|CHACHA20|RSA-KEY-TRANSPORT|AES-128|AES-256|SHA1|`)
	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"deny-cipher-suites": []string{"AES.*"}}))
	wantConfigError(t, err, "deny-cipher-suites[0]", "REGULAR_EXPRESSIONS_ARE_NOT_ACCEPTED")

	// You cannot deny your way to no ciphers.
	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"deny-cipher-suites": []string{"AES-128", "AES-256", "CHACHA20"}}))
	wantConfigError(t, err, "deny-cipher-suites", "EMPTY_AFTER_DENY")

	// Curves: the same rules.
	s, err = ParseServerSettings(p.serverBlock(utils.JSON{"deny-curves": []string{"x25519mlkem768", "P-521"}}))
	if err != nil || len(s.Policy.Curves) != 3 || s.Policy.Curves[0] != tls.X25519 {
		t.Errorf("curve deny: err=%v curves=%v", err, s.Policy.Curves)
	}
	outcome, _ = s.Policy.effective(nil)
	if len(outcome.noOps) != 1 || outcome.noOps[0] != "deny-curves=P-521" {
		t.Errorf("P-521 should be a recognised no-op: %v", outcome.noOps)
	}
	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"deny-curves": []string{"P-1024"}}))
	wantConfigError(t, err, "deny-curves[0]", `INVALID_VALUE:"P-1024":VALID_VALUES=P-256|P-384|P-521|X25519|X25519MLKEM768`)
	_, err = ParseServerSettings(p.serverBlock(utils.JSON{"deny-curves": []string{"X25519MLKEM768", "X25519", "P-256", "P-384"}}))
	wantConfigError(t, err, "deny-curves", "EMPTY_AFTER_DENY")

	// The client block takes the same keys.
	c, err := ParseClientSettings(p.clientBlock("", "", utils.JSON{"deny-cipher-suites": []string{"AES-256"}, "deny-curves": []string{"P-384"}}))
	if err != nil || len(c.Policy.CipherSuites) != 4 || len(c.Policy.Curves) != 3 {
		t.Errorf("client deny: err=%v suites=%v curves=%v", err, suiteList(c.Policy.CipherSuites), c.Policy.Curves)
	}
	// A genuine typo still errors. "DES" is deliberately not used here: it is a
	// recognised OpenSSL alias for single DES, which crypto/tls never
	// implemented, so it is a no-op rather than a mistake (see deny_openssl.go).
	_, err = ParseClientSettings(p.clientBlock("", "", utils.JSON{"deny-cipher-suites": []string{"AES-512"}}))
	wantConfigError(t, err, "deny-cipher-suites[0]", "INVALID_VALUE")
}

// The deny is real at the wire: a server that denies CHACHA20 refuses a TLS 1.2
// client that offers only ChaCha, and a client that denies two curves does not
// offer them.
func TestDenyListsAreEnforcedAtTheHandshake(t *testing.T) {
	p := newPKI(t)
	_, certFile, keyFile := p.clientLeaf(t, "c", tlstest.LeafOptions{DNSNames: []string{"c.test"}})
	base := mustBuildClient(t, p.clientBlock(certFile, keyFile, nil))
	offering := func(mutate func(*tls.Config)) *tls.Config {
		c := base.Clone()
		mutate(c)
		return c
	}
	chachaOnly := offering(func(c *tls.Config) {
		c.MaxVersion = tls.VersionTLS12
		c.CipherSuites = []uint16{tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256, tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256}
	})
	aesOnly := offering(func(c *tls.Config) {
		c.MaxVersion = tls.VersionTLS12
		c.CipherSuites = []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256}
	})

	t.Run("server denies CHACHA20", func(t *testing.T) {
		s := mustBuildServer(t, p.serverBlock(utils.JSON{"deny-cipher-suites": []string{"CHACHA20"}}))
		addr, _, errorLog := startServer(t, s.Config, okHandler(nil))
		before := errorLog.count()
		_, err := get(t, httpsClient(chachaOnly), addr)
		if err == nil {
			t.Fatal("a ChaCha-only TLS 1.2 client was accepted by a server denying CHACHA20")
		}
		if !strings.Contains(err.Error(), "remote error: tls:") || !errorLog.grewPast(before) {
			t.Errorf("the refusal did not happen on the server: %v", err)
		}
		resp, err := get(t, httpsClient(aesOnly), addr)
		if err != nil || resp.TLS.CipherSuite != tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 {
			t.Errorf("an AES client: err=%v suite=%s", err, tls.CipherSuiteName(resp.TLS.CipherSuite))
		}
		// TLS 1.3 is untouched: Go fixes its suites, as the design notes say.
		if resp, err := get(t, httpsClient(base), addr); err != nil || resp.TLS.Version != tls.VersionTLS13 {
			t.Errorf("a 1.3 client under a suite deny: err=%v", err)
		}
	})

	t.Run("client denies curves", func(t *testing.T) {
		nistOnly := mustBuildClient(t, p.clientBlock(certFile, keyFile, utils.JSON{"deny-curves": []string{"X25519MLKEM768", "X25519"}}))
		if len(nistOnly.CurvePreferences) != 2 || nistOnly.CurvePreferences[0] != tls.CurveP256 {
			t.Fatalf("client curves = %v, want [P-256 P-384]", nistOnly.CurvePreferences)
		}
		// A server narrowed to X25519 has nothing in common with it.
		s := mustBuildServer(t, p.serverBlock(utils.JSON{"curves": []string{"X25519"}}))
		addr, _, errorLog := startServer(t, s.Config, okHandler(nil))
		before := errorLog.count()
		if _, err := get(t, httpsClient(nistOnly), addr); err == nil {
			t.Error("no common curve, yet the handshake succeeded")
		} else if !strings.Contains(err.Error(), "remote error: tls:") || !errorLog.grewPast(before) {
			t.Errorf("the refusal did not happen on the server: %v", err)
		}
		// The unnarrowed server meets it on P-256.
		s = mustBuildServer(t, p.serverBlock(nil))
		addr, _, _ = startServer(t, s.Config, okHandler(nil))
		if _, err := get(t, httpsClient(nistOnly), addr); err != nil {
			t.Errorf("P-256 should have been agreed: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// The deny-file: a push takes effect on the next handshake, a bad push does not
// ---------------------------------------------------------------------------

func writeDeny(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestDenyFileHotReloadsOnTheServer(t *testing.T) {
	p := newPKI(t)
	_, certFile, keyFile := p.clientLeaf(t, "c", tlstest.LeafOptions{DNSNames: []string{"c.test"}})
	base := mustBuildClient(t, p.clientBlock(certFile, keyFile, nil))
	chachaOnly := base.Clone()
	chachaOnly.MaxVersion = tls.VersionTLS12
	chachaOnly.CipherSuites = []uint16{tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256, tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256}
	chacha := httpsClient(chachaOnly)
	any13 := httpsClient(base)

	// The deny-file has to exist; empty is the steady state.
	_, err := NewServerTLS(p.serverBlock(utils.JSON{"deny-file": "/nonexistent/deny.json"}))
	wantConfigError(t, err, "deny-file", "CANNOT_STAT")
	denyFile := tlstest.WriteFile(t, p.dir, "deny.json", []byte("{}"))
	s := mustBuildServer(t, p.serverBlock(utils.JSON{"deny-file": denyFile}))
	if !s.DenyList().IsEmpty() {
		t.Fatalf("an empty deny-file denies something: %s", s.DenyList().Summary())
	}
	addr, _, _ := startServer(t, s.Config, okHandler(nil))

	dial := func(client *http.Client) error {
		client.CloseIdleConnections() // a kept-alive connection would not handshake again
		_, err := get(t, client, addr)
		return err
	}
	if err := dial(chacha); err != nil {
		t.Fatalf("before any deny, ChaCha refused: %v", err)
	}

	// The push. No restart; the next handshake sees it.
	writeDeny(t, denyFile, `{"deny-cipher-suites": ["CHACHA20"]}`)
	if err := dial(chacha); err == nil {
		t.Fatal("after pushing a CHACHA20 deny the ChaCha client was still accepted")
	}
	resp, err := get(t, any13, addr)
	if err != nil {
		t.Fatalf("a 1.3 client after the push: %v", err)
	}
	if resp.ProtoMajor != 2 {
		t.Errorf("after the deny-file reload negotiated HTTP/%d, want 2: the clone lost h2", resp.ProtoMajor)
	}
	if got := s.DenyList().SuiteTokens; len(got) != 1 || got[0] != "CHACHA20" {
		t.Errorf("DenyList() after push = %v", got)
	}

	// Bad pushes keep the previous list in force: the ChaCha client stays
	// refused and nothing else changes. Each is a different size so the stat
	// sees it even within one timestamp tick.
	for name, content := range map[string]string{
		"malformed JSON":       `{not json`,
		"typo":                 `{"deny-cipher-suites": ["CHACHA2O"]} `,
		"unknown key":          `{"deny-ciphers": ["CHACHA20"]}  `,
		"regex":                `{"deny-cipher-suites": ["AES.*"]}   `,
		"empties the policy":   `{"deny-cipher-suites": ["AES-128", "AES-256", "CHACHA20"]}`,
		"bad certificate form": `{"deny-certificates": [{"issuer": "CN=x"}]}     `,
	} {
		writeDeny(t, denyFile, content)
		if err := dial(chacha); err == nil {
			t.Errorf("%s: the previous deny was dropped; ChaCha accepted", name)
		}
		if err := dial(any13); err != nil {
			t.Errorf("%s: a compliant client was refused: %v", name, err)
		}
		if got := s.DenyList().SuiteTokens; len(got) != 1 || got[0] != "CHACHA20" {
			t.Errorf("%s: DenyList() = %v, want the previous list", name, got)
		}
	}

	// Lifting the deny is the same push.
	writeDeny(t, denyFile, `{}`)
	if err := dial(chacha); err != nil {
		t.Errorf("after the deny was lifted, ChaCha still refused: %v", err)
	}
	if !s.DenyList().IsEmpty() {
		t.Errorf("DenyList() after lifting = %s", s.DenyList().Summary())
	}

	// A deny-file that would leave nothing at startup is a startup error, with
	// the path in it.
	writeDeny(t, denyFile, `{"deny-cipher-suites": ["AES-128", "AES-256", "CHACHA20"]}`)
	_, err = NewServerTLS(p.serverBlock(utils.JSON{"deny-file": denyFile}))
	wantConfigError(t, err, "deny-file", "EMPTY_AFTER_DENY")
	_, err = NewServerTLS(p.serverBlock(utils.JSON{"deny-file": ""}))
	wantConfigError(t, err, "deny-file", "EMPTY_VALUE")
}

// ---------------------------------------------------------------------------
// Certificate-level denies: a key, a certificate, an intermediate, an algorithm
// ---------------------------------------------------------------------------

func colonHex(h string) string {
	var b strings.Builder
	for i := 0; i < len(h); i += 2 {
		if i > 0 {
			b.WriteByte(':')
		}
		b.WriteString(h[i : i+2])
	}
	return b.String()
}

func denyEntries(entries ...utils.JSON) []any {
	out := make([]any, 0, len(entries))
	for _, e := range entries {
		out = append(out, e)
	}
	return out
}

func TestDenyCertificatesRevokeAtTheHandshake(t *testing.T) {
	p := newPKI(t)
	victim, victimCert, victimKey := p.clientLeaf(t, "victim", tlstest.LeafOptions{DNSNames: []string{"victim.test"}})
	_, bystanderCert, bystanderKey := p.clientLeaf(t, "bystander", tlstest.LeafOptions{DNSNames: []string{"bystander.test"}})
	victimClient := mustBuildClient(t, p.clientBlock(victimCert, victimKey, nil))
	bystanderClient := mustBuildClient(t, p.clientBlock(bystanderCert, bystanderKey, nil))
	// The same key re-issued under a new serial: what the compromised key's
	// holder does next.
	reissued := p.intermediate.Issue(t, tlstest.LeafOptions{CommonName: "victim", DNSNames: []string{"victim.test"}, Key: victim.Key})
	reissuedCert, reissuedKey := tlstest.WriteLeaf(t, p.dir, "reissued", reissued)
	reissuedClient := mustBuildClient(t, p.clientBlock(reissuedCert, reissuedKey, nil))
	if reissued.Cert.SerialNumber.Cmp(victim.Cert.SerialNumber) == 0 || SPKISHA256Hex(reissued.Cert) != SPKISHA256Hex(victim.Cert) {
		t.Fatal("the re-issued certificate should share the key and not the serial")
	}

	spkiHex := SPKISHA256Hex(victim.Cert)
	spkiRaw, _ := hex.DecodeString(spkiHex)
	spkiB64 := base64.StdEncoding.EncodeToString(spkiRaw)

	// refusedAs asserts a client is refused at the handshake, the handler
	// never runs, the server log classifies the line as REVOKED (not TRUST),
	// and the entry's reason is in it.
	refusedAs := func(t *testing.T, s *DXServerTLS, client *tls.Config, wantInLog string) {
		t.Helper()
		var hits atomic.Int32
		addr, _, errorLog := startServer(t, s.Config, okHandler(&hits))
		_, err := get(t, httpsClient(client), addr)
		if err == nil {
			t.Fatal("a denied certificate was admitted")
		}
		if hits.Load() != 0 {
			t.Error("the handler ran for a denied certificate")
		}
		if class, _ := ClassifyHandshakeError(err); class != HandshakeClassPeerRejectedUs {
			t.Errorf("client saw class %s, want %s", class, HandshakeClassPeerRejectedUs)
		}
		if !errorLog.contains(t, "TLS_PEER_REVOKED") || !errorLog.contains(t, wantInLog) {
			t.Errorf("server log lacks TLS_PEER_REVOKED with %q: %v", wantInLog, errorLog.lines)
		}
		errorLog.mu.Lock()
		defer errorLog.mu.Unlock()
		for _, line := range errorLog.lines {
			if strings.Contains(line, "TLS_PEER_REVOKED") {
				if class, _ := ClassifyHandshakeText(line); class != HandshakeClassRevoked {
					t.Errorf("server line classified %s, want %s: %s", class, HandshakeClassRevoked, line)
				}
			}
		}
	}
	admitted := func(t *testing.T, s *DXServerTLS, client *tls.Config) {
		t.Helper()
		addr, _, _ := startServer(t, s.Config, okHandler(nil))
		if _, err := get(t, httpsClient(client), addr); err != nil {
			t.Errorf("a certificate not on the list was refused: %v", err)
		}
	}

	t.Run("by public key, in every accepted spelling", func(t *testing.T) {
		for _, form := range []string{spkiHex, strings.ToUpper(spkiHex), colonHex(spkiHex), spkiB64} {
			s := mustBuildServer(t, p.serverBlock(utils.JSON{"deny-certificates": denyEntries(utils.JSON{"spki-sha256": form, "reason": "key compromised 2026-09-01"})}))
			refusedAs(t, s, victimClient, "key compromised 2026-09-01")
			admitted(t, s, bystanderClient)
			// The point of the SPKI form: the re-issued certificate carries
			// the same key and is refused too.
			refusedAs(t, s, reissuedClient, "spki-sha256="+spkiHex)
		}
	})

	t.Run("by issuer and serial, in every accepted spelling", func(t *testing.T) {
		issuer := victim.Cert.Issuer.String()
		serialHex := victim.Cert.SerialNumber.Text(16)
		if len(serialHex)%2 == 1 {
			serialHex = "0" + serialHex
		}
		for _, serial := range []string{victim.Cert.SerialNumber.String(), "0x" + serialHex, colonHex(serialHex), strings.ToUpper(serialHex)} {
			s := mustBuildServer(t, p.serverBlock(utils.JSON{"deny-certificates": denyEntries(utils.JSON{"issuer": issuer, "serial": serial})}))
			refusedAs(t, s, victimClient, "serial="+victim.Cert.SerialNumber.String())
			admitted(t, s, bystanderClient)
			// One certificate, not its key: the re-issuance is not caught.
			admitted(t, s, reissuedClient)
		}
		// The issuer has to match as printed; a different CA's identical
		// serial is a different certificate.
		s := mustBuildServer(t, p.serverBlock(utils.JSON{"deny-certificates": denyEntries(utils.JSON{"issuer": p.otherRoot.Cert.Subject.String(), "serial": victim.Cert.SerialNumber.String()})}))
		admitted(t, s, victimClient)
	})

	t.Run("a denied intermediate takes every leaf under it", func(t *testing.T) {
		s := mustBuildServer(t, p.serverBlock(utils.JSON{"deny-certificates": denyEntries(utils.JSON{"spki-sha256": SPKISHA256Hex(p.intermediate.Cert), "reason": "intermediate compromised"})}))
		refusedAs(t, s, victimClient, "intermediate compromised")
		refusedAs(t, s, bystanderClient, "chain[1]")
		// A leaf the root issued directly does not pass through it.
		direct := p.root.Issue(t, tlstest.LeafOptions{CommonName: "direct", DNSNames: []string{"direct.test"}})
		directCert, directKey := tlstest.WriteLeaf(t, p.dir, "direct", direct)
		admitted(t, s, mustBuildClient(t, p.clientBlock(directCert, directKey, nil)))
	})

	t.Run("the client denies a server the same way", func(t *testing.T) {
		s := mustBuildServer(t, p.httpsBlock(nil))
		addr, _, _ := startServer(t, s.Config, okHandler(nil))
		denying := mustBuildClient(t, p.clientBlock("", "", utils.JSON{"deny-certificates": denyEntries(utils.JSON{"spki-sha256": SPKISHA256Hex(p.server.Cert)})}))
		_, err := get(t, httpsClient(denying), addr)
		if err == nil {
			t.Fatal("the client connected to a server whose key it denies")
		}
		if class, _ := ClassifyHandshakeError(err); class != HandshakeClassRevoked {
			t.Errorf("client classified %s, want %s: %v", class, HandshakeClassRevoked, err)
		}
		// By the intermediate too.
		denying = mustBuildClient(t, p.clientBlock("", "", utils.JSON{"deny-certificates": denyEntries(utils.JSON{"spki-sha256": SPKISHA256Hex(p.intermediate.Cert)})}))
		if _, err := get(t, httpsClient(denying), addr); err == nil {
			t.Error("the client connected through an intermediate it denies")
		}
		// And under insecure-skip-verify the denies still apply to what was
		// presented: the deny is the one check left.
		denying = mustBuildClient(t, p.clientBlock("", "", utils.JSON{"insecure-skip-verify": true, "ca-files": []string{p.otherFile}, "deny-certificates": denyEntries(utils.JSON{"spki-sha256": SPKISHA256Hex(p.server.Cert)})}))
		if _, err := get(t, httpsClient(denying), addr); err == nil {
			t.Error("insecure-skip-verify bypassed the deny list")
		}
	})

	t.Run("entries are checked, not guessed at", func(t *testing.T) {
		for name, entry := range map[string]utils.JSON{
			"NO_IDENTITY":       {"reason": "x"},
			"ONE_FORM_ONLY":     {"spki-sha256": spkiHex, "issuer": "CN=x", "serial": "1"},
			"REQUIRED_TOGETHER": {"issuer": "CN=x"},
			"UNKNOWN_KEY":       {"spki-sha256": spkiHex, "spki": "typo"},
		} {
			_, err := ParseServerSettings(p.serverBlock(utils.JSON{"deny-certificates": denyEntries(entry)}))
			if err == nil || !strings.Contains(err.Error(), name) {
				t.Errorf("%s: err=%v", name, err)
			}
		}
		_, err := ParseServerSettings(p.serverBlock(utils.JSON{"deny-certificates": denyEntries(utils.JSON{"spki-sha256": "abcd"})}))
		wantConfigError(t, err, "deny-certificates[0]/spki-sha256", "INVALID_VALUE")
		_, err = ParseServerSettings(p.serverBlock(utils.JSON{"deny-certificates": denyEntries(utils.JSON{"issuer": "CN=x", "serial": "-5"})}))
		wantConfigError(t, err, "deny-certificates[0]/serial", "INVALID_VALUE")
		_, err = ParseServerSettings(p.serverBlock(utils.JSON{"deny-certificates": denyEntries(utils.JSON{"issuer": "CN=x", "serial": "zz"})}))
		wantConfigError(t, err, "deny-certificates[0]/serial", "INVALID_VALUE")
		_, err = ParseServerSettings(p.serverBlock(utils.JSON{"deny-certificates": "not a list"}))
		wantConfigError(t, err, "deny-certificates", "WRONG_TYPE")
	})
}

// A certificate signature algorithm on the deny list refuses any certificate
// in the chain that carries it -- except the trust anchor, whose signature
// nobody verifies. The test uses algorithms Go still accepts; SHA-1 and MD5
// are already refused by crypto/x509 before this code runs.
func TestDenyCertificateSignatureAlgorithms(t *testing.T) {
	dir := t.TempDir()
	root := tlstest.NewRootCASignedWith(t, "sha512 root", x509.ECDSAWithSHA512)
	inter := root.NewIntermediate(t, "sha256 intermediate") // signed ECDSA-SHA256
	rootFile := tlstest.WriteCA(t, dir, "root", root)
	server := inter.Issue(t, tlstest.LeafOptions{CommonName: "api.test", DNSNames: []string{"api.test"}, IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	serverCert, serverKey := tlstest.WriteLeaf(t, dir, "server", server)
	c384 := inter.Issue(t, tlstest.LeafOptions{CommonName: "c384", DNSNames: []string{"c384.test"}, SignatureAlgorithm: x509.ECDSAWithSHA384})
	c384Cert, c384Key := tlstest.WriteLeaf(t, dir, "c384", c384)
	c256 := inter.Issue(t, tlstest.LeafOptions{CommonName: "c256", DNSNames: []string{"c256.test"}})
	c256Cert, c256Key := tlstest.WriteLeaf(t, dir, "c256", c256)
	if c384.Cert.SignatureAlgorithm != x509.ECDSAWithSHA384 || c256.Cert.SignatureAlgorithm != x509.ECDSAWithSHA256 || root.Cert.SignatureAlgorithm != x509.ECDSAWithSHA512 {
		t.Fatalf("test PKI algorithms: leaf384=%s leaf256=%s root=%s", c384.Cert.SignatureAlgorithm, c256.Cert.SignatureAlgorithm, root.Cert.SignatureAlgorithm)
	}
	serverBlock := func(deny ...string) utils.JSON {
		kv := utils.JSON{"mode": ModeMTLS, "cert-file": serverCert, "key-file": serverKey, "tls-policy": PolicyIntermediate, "ca-trust": CATrustCustom, "ca-files": []string{rootFile}}
		if len(deny) > 0 {
			kv["deny-certificate-signature-algorithms"] = deny
		}
		return kv
	}
	clientBlock := func(certFile, keyFile string, deny ...string) utils.JSON {
		kv := utils.JSON{"tls-policy": PolicyIntermediate, "ca-trust": CATrustCustom, "ca-files": []string{rootFile}, "server-name": "api.test"}
		if certFile != "" {
			kv["cert-file"], kv["key-file"] = certFile, keyFile
		}
		if len(deny) > 0 {
			kv["deny-certificate-signature-algorithms"] = deny
		}
		return kv
	}
	client384 := mustBuildClient(t, clientBlock(c384Cert, c384Key))
	client256 := mustBuildClient(t, clientBlock(c256Cert, c256Key))

	_, err := ParseServerSettings(serverBlock("SHA1-RSAX"))
	wantConfigError(t, err, "deny-certificate-signature-algorithms[0]", `INVALID_VALUE:"SHA1-RSAX":VALID_VALUES=MD5-RSA|SHA1-RSA|SHA256-RSA|`)
	if _, err := ParseServerSettings(serverBlock("sha1-rsa", "MD5-RSA", "ECDSA-SHA1")); err != nil {
		t.Errorf("the already-refused algorithms must be recognised no-ops, not typos: %v", err)
	}

	outcome := func(t *testing.T, block utils.JSON, client *tls.Config, wantOK bool, wantInLog string) {
		t.Helper()
		s := mustBuildServer(t, block)
		addr, _, errorLog := startServer(t, s.Config, okHandler(nil))
		_, err := get(t, httpsClient(client), addr)
		if (err == nil) != wantOK {
			t.Errorf("err=%v, want accepted=%t", err, wantOK)
		}
		if !wantOK && !errorLog.contains(t, wantInLog) {
			t.Errorf("server log lacks %q: %v", wantInLog, errorLog.lines)
		}
	}
	// The leaf's algorithm.
	outcome(t, serverBlock("ECDSA-SHA384"), client384, false, "certificate-signature-algorithm=ECDSA-SHA384")
	outcome(t, serverBlock("ECDSA-SHA384"), client256, true, "")
	// The intermediate's: the SHA-384 leaf falls at chain[1], the
	// intermediate, so the whole chain is checked and not only the leaf. The
	// SHA-256 leaf falls at chain[0] on its own signature first.
	outcome(t, serverBlock("ECDSA-SHA256"), client384, false, "chain[1]")
	outcome(t, serverBlock("ECDSA-SHA256"), client256, false, "chain[0]=\"CN=c256\":certificate-signature-algorithm=ECDSA-SHA256")
	// The root's own: exempt, so nothing changes.
	outcome(t, serverBlock("ECDSA-SHA512"), client384, true, "")
	outcome(t, serverBlock("ECDSA-SHA512"), client256, true, "")

	// The client applies the same rule to the server's chain.
	s := mustBuildServer(t, serverBlock())
	addr, _, _ := startServer(t, s.Config, okHandler(nil))
	if _, err := get(t, httpsClient(mustBuildClient(t, clientBlock(c256Cert, c256Key, "ECDSA-SHA256"))), addr); err == nil {
		t.Error("the client accepted a server chain signed with an algorithm it denies")
	} else if class, _ := ClassifyHandshakeError(err); class != HandshakeClassRevoked {
		t.Errorf("client classified %s, want %s: %v", class, HandshakeClassRevoked, err)
	}
	if _, err := get(t, httpsClient(mustBuildClient(t, clientBlock(c256Cert, c256Key, "ECDSA-SHA512"))), addr); err != nil {
		t.Errorf("the client refused a chain over the anchor's own algorithm: %v", err)
	}
}

// The client re-reads its deny-file inside VerifyConnection, so a pushed
// certificate deny is in force on the outbound side too, without a restart.
// Its suite and curve denies are not: they sit on tls.Config fields the
// transport clones per dial.
func TestDenyFileCertificateDeniesHotReloadOnTheClient(t *testing.T) {
	p := newPKI(t)
	s := mustBuildServer(t, p.httpsBlock(nil))
	addr, _, _ := startServer(t, s.Config, okHandler(nil))
	denyFile := tlstest.WriteFile(t, p.dir, "client-deny.json", []byte("{}"))
	c, err := NewClientTLS(p.clientBlock("", "", utils.JSON{"deny-file": denyFile}))
	if err != nil {
		t.Fatal(err)
	}
	client := httpsClient(c.Config)
	dial := func() error {
		client.CloseIdleConnections()
		_, err := get(t, client, addr)
		return err
	}
	if err := dial(); err != nil {
		t.Fatalf("before any deny: %v", err)
	}
	writeDeny(t, denyFile, `{"deny-certificates": [{"spki-sha256": "`+SPKISHA256Hex(p.server.Cert)+`", "reason": "server key leaked"}]}`)
	if err := dial(); err == nil {
		t.Fatal("after pushing the server's key onto the client's deny-file, the client still connected")
	} else if class, _ := ClassifyHandshakeError(err); class != HandshakeClassRevoked {
		t.Errorf("classified %s, want %s: %v", class, HandshakeClassRevoked, err)
	}
	writeDeny(t, denyFile, `{"deny-certificates": []}`)
	if err := dial(); err != nil {
		t.Errorf("after lifting the deny: %v", err)
	}

	// A suite deny pushed to the client is read -- the list reports it -- but
	// what the client offers does not change until a restart.
	before := append([]uint16(nil), c.Config.CipherSuites...)
	writeDeny(t, denyFile, `{"deny-cipher-suites": ["AES-128"]}`)
	if err := dial(); err != nil {
		t.Fatalf("a suite deny on the client must not break the connection: %v", err)
	}
	if got := c.DenyList().SuiteTokens; len(got) != 1 || got[0] != "AES-128" {
		t.Errorf("the client did not read the pushed suite deny: %v", got)
	}
	if strings.Join(suiteList(c.Config.CipherSuites), ",") != strings.Join(suiteList(before), ",") {
		t.Errorf("the client's offered suites changed without a restart, which the transport's per-dial clone cannot support: %v", suiteList(c.Config.CipherSuites))
	}
}

// The preflight reads the deny-file, says what it holds and what it leaves,
// and flags our own certificate or CA if the list names it.
func TestPreflightReportsTheDenyList(t *testing.T) {
	p := newPKI(t)
	denyFile := tlstest.WriteFile(t, p.dir, "deny.json", []byte(`{"deny-cipher-suites": ["CHACHA20", "CBC"]}`))
	r := PreflightServer("api", p.serverBlock(utils.JSON{"deny-file": denyFile}))
	if !r.OK {
		t.Fatalf("a good deny-file failed preflight:\n%s", r.Text)
	}
	for _, want := range []string{"deny-file: " + denyFile, "holds: cipher-suites=CHACHA20,CBC", "removed-suites=[TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256]", "no-op=[deny-cipher-suites=CBC]"} {
		if !strings.Contains(r.Text, want) {
			t.Errorf("report lacks %q:\n%s", want, r.Text)
		}
	}

	ownKey := tlstest.WriteFile(t, p.dir, "deny-own.json", []byte(`{"deny-certificates": [{"spki-sha256": "`+SPKISHA256Hex(p.server.Cert)+`", "reason": "test"}]}`))
	r = PreflightServer("api", p.serverBlock(utils.JSON{"deny-file": ownKey}))
	if r.OK || !strings.Contains(r.Text, "cert-file "+p.serverCert+" is ON THE DENY LIST") {
		t.Errorf("our own denied certificate was not flagged:\n%s", r.Text)
	}
	ownCA := tlstest.WriteFile(t, p.dir, "deny-ca.json", []byte(`{"deny-certificates": [{"issuer": "`+p.root.Cert.Issuer.String()+`", "serial": "`+p.root.Cert.SerialNumber.String()+`"}]}`))
	r = PreflightServer("api", p.serverBlock(utils.JSON{"deny-file": ownCA}))
	if r.OK || !strings.Contains(r.Text, "ca-files[0]") || !strings.Contains(r.Text, "ON THE DENY LIST") {
		t.Errorf("our own denied CA was not flagged:\n%s", r.Text)
	}

	broken := tlstest.WriteFile(t, p.dir, "deny-broken.json", []byte(`{"deny-cipher-suites": ["CHACHA2O"]}`))
	r = PreflightServer("api", p.serverBlock(utils.JSON{"deny-file": broken}))
	if r.OK || !strings.Contains(r.Text, "PROBLEM: deny-file") || !strings.Contains(r.Text, "CHACHA2O") {
		t.Errorf("a broken deny-file was not reported:\n%s", r.Text)
	}

	r = PreflightClient(p.clientBlock("", "", utils.JSON{"deny-file": denyFile}), "")
	if !r.OK || !strings.Contains(r.Text, "holds: cipher-suites=CHACHA20,CBC") {
		t.Errorf("client report:\n%s", r.Text)
	}
}
