package api

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/sync/errgroup"

	dxlibConfiguration "github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsHttp "github.com/donnyhardyanto/dxlib/utils/http"
	utilsHttpClient "github.com/donnyhardyanto/dxlib/utils/http/client"
	utilsTLS "github.com/donnyhardyanto/dxlib/utils/tls"
	"github.com/donnyhardyanto/dxlib/utils/tls/tlstest"
	wsclient "github.com/donnyhardyanto/dxlib/websocket/client"
)

// These drive the real DXAPI: ApplyConfigurations reading the tls block,
// StartAndWait listening with ListenAndServeTLS, routeHandler building the
// request, and the three outbound clients dialling it. The utils/tls tests
// prove the handshake rules; these prove the library is wired to them.

type apiPKI struct {
	dir        string
	root       *tlstest.CA
	rootFile   string
	serverCert string
	serverKey  string
	clientCert string
	clientKey  string
}

func newAPIPKI(t *testing.T) *apiPKI {
	t.Helper()
	p := &apiPKI{dir: t.TempDir()}
	p.root = tlstest.NewRootCA(t, "api test root")
	p.rootFile = tlstest.WriteCA(t, p.dir, "root", p.root)
	server := p.root.Issue(t, tlstest.LeafOptions{CommonName: "api.test", DNSNames: []string{"api.test"}, IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	p.serverCert, p.serverKey = tlstest.WriteLeaf(t, p.dir, "server", server)
	client := p.root.Issue(t, tlstest.LeafOptions{
		CommonName: "queue-scheduler",
		DNSNames:   []string{"queue-scheduler.dcc.svc"},
		URIs:       []string{"spiffe://cluster.local/ns/dcc/sa/queue-scheduler"},
	})
	p.clientCert, p.clientKey = tlstest.WriteLeaf(t, p.dir, "client", client)
	return p
}

func (p *apiPKI) serverBlock(overrides utils.JSON) utils.JSON {
	kv := utils.JSON{
		"cert-file":           p.serverCert,
		"key-file":            p.serverKey,
		"tls-policy":          utilsTLS.PolicyIntermediate,
		"mode":                utilsTLS.ModeMTLS,
		"ca-trust":            utilsTLS.CATrustCustom,
		"ca-files":            []string{p.rootFile},
		"allowed-client-sans": []string{"spiffe://cluster.local/ns/dcc/sa/queue-scheduler"},
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

func (p *apiPKI) clientBlock(overrides utils.JSON) utils.JSON {
	kv := utils.JSON{
		"cert-file":   p.clientCert,
		"key-file":    p.clientKey,
		"tls-policy":  utilsTLS.PolicyIntermediate,
		"ca-trust":    utilsTLS.CATrustCustom,
		"ca-files":    []string{p.rootFile},
		"server-name": "api.test",
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

// useOutboundTLS installs the shared client configuration for the duration of
// a test and puts the bare default back afterwards, through the same
// enabled=false path a configuration would use.
func useOutboundTLS(t *testing.T, block utils.JSON) {
	t.Helper()
	if err := utilsHttpClient.ApplyTLSConfiguration(block); err != nil {
		t.Fatalf("ApplyTLSConfiguration: %v", err)
	}
	t.Cleanup(func() {
		disabled := utils.JSON{}
		for k, v := range block {
			disabled[k] = v
		}
		disabled["enabled"] = false
		_ = utilsHttpClient.ApplyTLSConfiguration(disabled)
	})
}

func freeAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// startAPI registers a DXAPI under the manager with the given tls block (nil
// for plaintext), starts it the way app.start does, and waits for the port.
func startAPI(t *testing.T, name string, tlsBlock utils.JSON, define func(a *DXAPI)) *DXAPI {
	t.Helper()
	a, err := Manager.NewAPI(name)
	if err != nil {
		t.Fatal(err)
	}
	a.Address = freeAddress(t)
	a.WriteTimeoutSec, a.ReadTimeoutSec = 30, 30
	if tlsBlock != nil {
		a.TLS, err = utilsTLS.NewServerTLS(tlsBlock)
		if err != nil {
			t.Fatalf("NewServerTLS: %v", err)
		}
	}
	define(a)
	group, _ := errgroup.WithContext(context.Background())
	if err := a.StartAndWait(group); err != nil {
		t.Fatalf("StartAndWait: %v", err)
	}
	t.Cleanup(func() {
		if a.HTTPServer != nil {
			_ = a.HTTPServer.Close()
		}
		delete(Manager.APIs, name)
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", a.Address, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return a
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s did not start listening at %s", name, a.Address)
	return nil
}

func echoPeerEndpoint(a *DXAPI) {
	a.NewEndPoint("whoami", "", "/whoami", "POST", EndPointTypeHTTPJSON, utilsHttp.RequestContentTypeApplicationJSON,
		nil, func(aepr *DXAPIEndPointRequest) error {
			aepr.WriteResponseAsJSON(http.StatusOK, nil, utils.JSON{
				"peer_identity": aepr.PeerIdentity,
				"peer_cn":       peerCN(aepr),
			})
			return nil
		}, nil, nil, nil, nil, 0, "")
}

func peerCN(aepr *DXAPIEndPointRequest) string {
	if aepr.PeerCertificate == nil {
		return ""
	}
	return aepr.PeerCertificate.Subject.CommonName
}

// A request over mTLS through the real router: the handler sees the verified
// caller on the request object, the audit entry carries it, and the listener
// negotiated HTTP/2 -- which is the default, and the reason the WebSocket test
// below has to pass as well.
func TestDXAPIServesMTLSAndExposesThePeer(t *testing.T) {
	p := newAPIPKI(t)
	var audited []string
	a := startAPI(t, "mtls-whoami", p.serverBlock(nil), func(a *DXAPI) {
		echoPeerEndpoint(a)
		a.OnAuditLogStart = func(ctx context.Context, id int64, e *DXAPIAuditLogEntry) (int64, error) {
			audited = append(audited, e.PeerIdentity)
			return 1, nil
		}
	})
	useOutboundTLS(t, p.clientBlock(nil))

	// utils/http/client.HTTPClient, the first of the three call sites.
	_, resp, err := utilsHttpClient.HTTPClientReadAll(context.Background(), http.MethodPost, "https://"+a.Address+"/whoami",
		map[string]string{"Content-Type": "application/json"}, "{}")
	if err != nil {
		t.Fatalf("HTTPClient over mTLS: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, resp.BodyAsString())
	}
	body := resp.BodyAsString()
	if !strings.Contains(body, "spiffe://cluster.local/ns/dcc/sa/queue-scheduler") || !strings.Contains(body, `"queue-scheduler"`) {
		t.Errorf("the handler did not see the verified peer: %s", body)
	}
	if len(audited) == 0 || audited[0] != "spiffe://cluster.local/ns/dcc/sa/queue-scheduler" {
		t.Errorf("audit entry peer identity = %v, want the SPIFFE ID", audited)
	}

	// The negotiated protocol, checked with a raw client on the same transport.
	raw := utilsHttpClient.NewHTTPClient(5 * time.Second)
	r, err := raw.Post("https://"+a.Address+"/whoami", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Body.Close()
	if r.ProtoMajor != 2 {
		t.Errorf("negotiated HTTP/%d; HTTP/2 was expected to be on by default over TLS", r.ProtoMajor)
	}
	if r.TLS == nil || r.TLS.Version != tls.VersionTLS13 {
		t.Errorf("negotiated %+v, want TLS 1.3 between two intermediate-policy ends", r.TLS)
	}

	// DXAPIEndPointRequest.HTTPClientDo, the second call site.
	aepr, recorder := proxyRequest(t)
	pr, err := aepr.HTTPClientDo(http.MethodPost, "https://"+a.Address+"/whoami", utils.JSON{}, nil)
	if err != nil {
		t.Fatalf("HTTPClientDo over mTLS: %v (recorder %d)", err, recorder.Code)
	}
	_ = pr.Body.Close()
	if pr.StatusCode != http.StatusOK {
		t.Errorf("HTTPClientDo status %d", pr.StatusCode)
	}

	// Without the client certificate the same server refuses the handshake.
	useOutboundTLS(t, p.clientBlock(utils.JSON{"cert-file": nil, "key-file": nil}))
	if _, _, err := utilsHttpClient.HTTPClientReadAll(context.Background(), http.MethodPost, "https://"+a.Address+"/whoami", nil, "{}"); err == nil {
		t.Error("a client with no certificate was admitted by a require-and-verify listener")
	}
}

// The WebSocket endpoint over the same TLS listener, dialled by the library's
// own WebSocket client -- the third call site -- with HTTP/2 enabled on the
// server. gorilla speaks HTTP/1.1 over the TLS connection without offering
// ALPN, so the upgrade goes through the HTTP/1.1 server path; nothing about
// TLSNextProto had to change for this to hold.
func TestDXAPIWebSocketOverMTLS(t *testing.T) {
	p := newAPIPKI(t)
	a := startAPI(t, "mtls-ws", p.serverBlock(nil), func(a *DXAPI) {
		a.NewWSEndPoint("echo", "", "/ws", "GET", nil,
			func(aepr *DXAPIEndPointRequest, message []byte) ([]byte, error) {
				return []byte(aepr.PeerIdentity + ":" + string(message)), nil
			}, nil, nil, 0, nil, nil, "")
	})
	useOutboundTLS(t, p.clientBlock(nil))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := wsclient.Dial(ctx, "wss://"+a.Address+"/ws", nil)
	if err != nil {
		t.Fatalf("WebSocket dial over mTLS: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, reply, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(reply) != "spiffe://cluster.local/ns/dcc/sa/queue-scheduler:ping" {
		t.Errorf("reply %q: the WebSocket request did not carry the peer identity", reply)
	}

	// A dialer without a client certificate is refused at the handshake.
	useOutboundTLS(t, p.clientBlock(utils.JSON{"cert-file": nil, "key-file": nil}))
	if _, _, err := wsclient.Dial(ctx, "wss://"+a.Address+"/ws", nil); err == nil {
		t.Error("a WebSocket dial with no client certificate was admitted")
	}
}

// ApplyConfigurations: no tls block is plaintext and leaves TLS nil, exactly
// as before the block existed; a block is read and built.
func TestApplyConfigurationsReadsTheTLSBlock(t *testing.T) {
	p := newAPIPKI(t)
	data := utils.JSON{
		"plain":    utils.JSON{"address": "127.0.0.1:0"},
		"explicit": utils.JSON{"address": "127.0.0.1:0", "tls": utils.JSON{"mode": utilsTLS.ModeHTTP}},
		"secure":   utils.JSON{"address": "127.0.0.1:0", "tls": p.serverBlock(nil)},
		"parked":   utils.JSON{"address": "127.0.0.1:0", "tls": p.serverBlock(utils.JSON{"enabled": false})},
	}
	dxlibConfiguration.Manager.Configurations["api-tls-test"] = &dxlibConfiguration.DXConfiguration{NameId: "api-tls-test", Data: &data}
	t.Cleanup(func() { delete(dxlibConfiguration.Manager.Configurations, "api-tls-test") })

	for _, name := range []string{"plain", "explicit", "secure", "parked"} {
		a, _ := Manager.NewAPI(name)
		t.Cleanup(func() { delete(Manager.APIs, name) })
		if err := a.ApplyConfigurations("api-tls-test"); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		switch name {
		case "plain":
			if a.TLS != nil {
				t.Error("plain: TLS is set with no tls block")
			}
		case "explicit":
			// mode=http: the block is read and kept, so the mode can be
			// reported, and no config is built, so the listener is plaintext.
			if a.TLS == nil || a.TLS.Config != nil || a.TLS.Settings.Mode != utilsTLS.ModeHTTP {
				t.Errorf("explicit: mode=http should parse and build nothing: %+v", a.TLS)
			}
		case "secure":
			if a.TLS == nil || a.TLS.Config == nil || a.TLS.Settings.Mode != utilsTLS.ModeMTLS || a.TLS.Config.ClientAuth != tls.RequireAndVerifyClientCert {
				t.Errorf("secure: TLS not built from the block: %+v", a.TLS)
			}
		case "parked":
			if a.TLS == nil || a.TLS.Config != nil {
				t.Errorf("parked: enabled=false should validate and not build: %+v", a.TLS)
			}
		}
	}

	// A plaintext listener still serves, byte for byte as before -- with no
	// block, and with an explicit mode=http block.
	for name, block := range map[string]utils.JSON{"plain-listener": nil, "explicit-http-listener": {"mode": utilsTLS.ModeHTTP}} {
		a := startAPI(t, name, block, echoPeerEndpoint)
		resp, err := http.Post("http://"+a.Address+"/whoami", "application/json", strings.NewReader("{}"))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s: plaintext status %d", name, resp.StatusCode)
		}
	}
}

// A tls block with an unresolvable trust source must stop the process from
// starting, not fail at the first request. ApplyConfigurations reports it
// through log.Log.FatalAndCreateErrorf, which exits, so the check runs in a
// child process: the parent re-executes this test binary with an environment
// flag and asserts the exit status and the message.
func TestApplyConfigurationsRefusesToStartOnABadTrustSource(t *testing.T) {
	const flag = "DXLIB_API_TLS_CRASH_CHILD"
	if os.Getenv(flag) == "1" {
		p := newAPIPKI(t)
		data := utils.JSON{"secure": utils.JSON{"address": "127.0.0.1:0", "tls": p.serverBlock(utils.JSON{"ca-trust": "ABSC"})}}
		dxlibConfiguration.Manager.Configurations["api-crash"] = &dxlibConfiguration.DXConfiguration{NameId: "api-crash", Data: &data}
		a, _ := Manager.NewAPI("secure")
		_ = a.ApplyConfigurations("api-crash")
		// Reaching here means the process did not exit; make that visible.
		fmt.Fprintln(os.Stderr, "APPLY_CONFIGURATIONS_RETURNED_INSTEAD_OF_EXITING")
		os.Exit(3)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestApplyConfigurationsRefusesToStartOnABadTrustSource$", "-test.v")
	cmd.Env = append(os.Environ(), flag+"=1")
	out, err := cmd.CombinedOutput()
	exitErr, isExit := err.(*exec.ExitError)
	if !isExit {
		t.Fatalf("child did not exit with a failure status (err=%v); output:\n%s", err, out)
	}
	if code := exitErr.ExitCode(); code != 1 {
		t.Errorf("child exit code %d, want 1 (the fatal-log exit); output:\n%s", code, out)
	}
	want := `TLS_CONFIG_ERROR:api-crash.secure.tls/ca-trust:INVALID_VALUE:\"ABSC\":VALID_VALUES=custom|system|system-and-custom`
	if !strings.Contains(string(out), want) && !strings.Contains(string(out), strings.ReplaceAll(want, `\"`, `"`)) {
		t.Errorf("child output does not carry the three-part message %q:\n%s", want, out)
	}
}

// The preflight over the process configuration, as a CLI flag or an OAM route
// would run it.
func TestTLSPreflightReportCoversEveryBlock(t *testing.T) {
	p := newAPIPKI(t)
	apiData := utils.JSON{
		"plain":  utils.JSON{"address": "127.0.0.1:0"},
		"secure": utils.JSON{"address": "127.0.0.1:0", "tls": p.serverBlock(nil)},
	}
	clientData := utils.JSON{"tls": p.clientBlock(nil)}
	dxlibConfiguration.Manager.Configurations["api"] = &dxlibConfiguration.DXConfiguration{NameId: "api", Data: &apiData}
	dxlibConfiguration.Manager.Configurations["http-client"] = &dxlibConfiguration.DXConfiguration{NameId: "http-client", Data: &clientData}
	t.Cleanup(func() {
		delete(dxlibConfiguration.Manager.Configurations, "api")
		delete(dxlibConfiguration.Manager.Configurations, "http-client")
	})

	report, ok := TLSPreflightReport("")
	if !ok {
		t.Errorf("preflight failed:\n%s", report)
	}
	for _, want := range []string{"server plain: no tls block", "server secure:", "http-client:", "chains to the configured ca-trust pool: yes", "allowed-client-sans=[spiffe://cluster.local/ns/dcc/sa/queue-scheduler] (enforce)"} {
		if !strings.Contains(report, want) {
			t.Errorf("report lacks %q:\n%s", want, report)
		}
	}

	// Through the handler: 200 when fine, 503 when not.
	aepr, recorder := proxyRequest(t)
	if err := APIHandlerTLSPreflight(aepr); err != nil || recorder.Code != http.StatusOK {
		t.Errorf("handler: err=%v code=%d", err, recorder.Code)
	}
	apiData["secure"].(utils.JSON)["tls"].(utils.JSON)["ca-files"] = []string{p.clientCert}
	aepr, recorder = proxyRequest(t)
	if err := APIHandlerTLSPreflight(aepr); err != nil || recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("handler with a broken block: err=%v code=%d", err, recorder.Code)
	}
}
