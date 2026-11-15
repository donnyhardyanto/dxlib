package redis

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	dxlibConfiguration "github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/donnyhardyanto/dxlib/utils/tls/tlstest"
)

// fakeTLSRedis is a TLS listener that speaks just enough RESP for go-redis to
// connect: +PONG to PING, +OK to anything else (AUTH, SELECT). It records the
// client certificate each handshake presented, so a test can see mTLS happen.
type fakeTLSRedis struct {
	addr string

	mu          sync.Mutex
	clientCNs   []string
	handshakeOK int
}

func startFakeTLSRedis(t *testing.T, cfg *tls.Config) *fakeTLSRedis {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	f := &fakeTLSRedis{addr: ln.Addr().String()}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(c.(*tls.Conn))
		}
	}()
	return f
}

func (f *fakeTLSRedis) serve(c *tls.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	if err := c.Handshake(); err != nil {
		return
	}
	f.mu.Lock()
	f.handshakeOK++
	if pc := c.ConnectionState().PeerCertificates; len(pc) > 0 {
		f.clientCNs = append(f.clientCNs, pc[0].Subject.CommonName)
	}
	f.mu.Unlock()
	r := bufio.NewReader(c)
	for {
		args, err := readRESPArray(r)
		if err != nil {
			return
		}
		reply := "+OK\r\n"
		if len(args) > 0 && strings.EqualFold(args[0], "PING") {
			reply = "+PONG\r\n"
		}
		if _, err := io.WriteString(c, reply); err != nil {
			return
		}
	}
}

func (f *fakeTLSRedis) seen() (handshakes int, clientCNs []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.handshakeOK, append([]string(nil), f.clientCNs...)
}

func readRESPArray(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("not an array: %q", line)
	}
	n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimSpace(hdr[1:]))
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}

type testPKI struct {
	dir                  string
	caFile               string
	serverCert           tls.Certificate
	clientCrt, clientKey string
	pool                 *x509.CertPool
}

// newTestPKI issues a server certificate for the DNS name redis.test only, so
// dialing 127.0.0.1 verifies only when server-name says redis.test.
func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	root := tlstest.NewRootCA(t, "redis-test-root")
	server := root.Issue(t, tlstest.LeafOptions{CommonName: "redis.test", DNSNames: []string{"redis.test"}, ServerOnly: true})
	client := root.Issue(t, tlstest.LeafOptions{CommonName: "session-store", ClientOnly: true})
	dir := t.TempDir()
	p := &testPKI{dir: dir, caFile: tlstest.WriteCA(t, dir, "ca", root)}
	p.clientCrt, p.clientKey = tlstest.WriteLeaf(t, dir, "client", client)
	sc, sk := tlstest.WriteLeaf(t, dir, "server", server)
	var err error
	if p.serverCert, err = tls.LoadX509KeyPair(sc, sk); err != nil {
		t.Fatalf("server pair: %v", err)
	}
	p.pool = x509.NewCertPool()
	p.pool.AddCert(root.Cert)
	return p
}

func (p *testPKI) serverConfig(requireClientCert bool) *tls.Config {
	c := &tls.Config{Certificates: []tls.Certificate{p.serverCert}, MinVersion: tls.VersionTLS12}
	if requireClientCert {
		c.ClientAuth = tls.RequireAndVerifyClientCert
		c.ClientCAs = p.pool
	}
	return c
}

func newTestManager() *DXRedisManager {
	return &DXRedisManager{Redises: map[string]*DXRedis{}}
}

func setRedisConfiguration(t *testing.T, data utils.JSON) {
	t.Helper()
	prev, had := dxlibConfiguration.Manager.Configurations["redis"]
	dxlibConfiguration.Manager.Configurations["redis"] = &dxlibConfiguration.DXConfiguration{NameId: "redis", Data: &data}
	t.Cleanup(func() {
		if had {
			dxlibConfiguration.Manager.Configurations["redis"] = prev
		} else {
			delete(dxlibConfiguration.Manager.Configurations, "redis")
		}
	})
}

func TestOptionsCarryTLSWithTheFloorRaised(t *testing.T) {
	given := &tls.Config{MinVersion: tls.VersionTLS10, ServerName: "redis.test"}
	r := &DXRedis{Address: "127.0.0.1:6379", TLSConfig: given,
		SentinelMasterName: "m", SentinelAddresses: []string{"127.0.0.1:26379"}, HasSentinel: true}

	for name, got := range map[string]*tls.Config{
		"ring":     r.ringOptions().TLSConfig,
		"failover": r.failoverOptions().TLSConfig,
	} {
		if got == nil {
			t.Fatalf("%s: TLSConfig not set", name)
		}
		if got.MinVersion != tls.VersionTLS12 {
			t.Errorf("%s: MinVersion = %s, want TLS 1.2", name, tls.VersionName(got.MinVersion))
		}
		if got.ServerName != "redis.test" {
			t.Errorf("%s: ServerName = %q", name, got.ServerName)
		}
	}
	if given.MinVersion != tls.VersionTLS10 {
		t.Errorf("caller's config changed in place: MinVersion = %s", tls.VersionName(given.MinVersion))
	}

	r.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13}
	if got := r.ringOptions().TLSConfig; got != r.TLSConfig {
		t.Errorf("a config already at or above TLS 1.2 should be used as given")
	}

	r.TLSConfig = nil
	if r.ringOptions().TLSConfig != nil || r.failoverOptions().TLSConfig != nil {
		t.Errorf("no TLSConfig must mean plain TCP on both paths")
	}
}

func TestConnectFromConfigurationWithTLSAndClientCertificate(t *testing.T) {
	p := newTestPKI(t)
	srv := startFakeTLSRedis(t, p.serverConfig(true))
	setRedisConfiguration(t, utils.JSON{"session": utils.JSON{
		"address":        srv.addr,
		"database_index": 0,
		"user_name":      "session",
		"password":       "secret",
		"tls": utils.JSON{
			"tls-policy":  "intermediate",
			"ca-trust":    "custom",
			"ca-files":    []any{p.caFile},
			"cert-file":   p.clientCrt,
			"key-file":    p.clientKey,
			"server-name": "redis.test",
		},
	}})

	r := newTestManager().NewRedis("session", false, false)
	r.Context = context.Background()
	if err := r.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = r.Disconnect() })
	if r.TLS == nil || r.TLSConfig == nil {
		t.Fatalf("TLS not set from the configuration block")
	}
	if r.TLSConfig.MinVersion < tls.VersionTLS12 {
		t.Errorf("intermediate policy should floor at TLS 1.2, got %s", tls.VersionName(r.TLSConfig.MinVersion))
	}
	if err := r.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	n, cns := srv.seen()
	if n == 0 || len(cns) == 0 || cns[0] != "session-store" {
		t.Fatalf("server saw handshakes=%d client certificates=%v, want the session-store certificate", n, cns)
	}
	if s := r.tlsSummary(); !strings.HasPrefix(s, "tls=on (") {
		t.Errorf("tlsSummary = %q", s)
	}
}

func TestConnectDirectlyBuiltWithTLSConfig(t *testing.T) {
	p := newTestPKI(t)
	srv := startFakeTLSRedis(t, p.serverConfig(false))

	build := func(serverName string) *DXRedis {
		return &DXRedis{
			NameId: "direct", IsConfigured: true, Address: srv.addr, Context: context.Background(),
			TLSConfig: &tls.Config{RootCAs: p.pool, ServerName: serverName},
		}
	}

	r := build("redis.test")
	if err := r.Connect(); err != nil {
		t.Fatalf("Connect with server-name: %v", err)
	}
	_ = r.Disconnect()

	// The certificate names redis.test only; the dial host is 127.0.0.1.
	r = build("")
	err := r.Connect()
	if err == nil {
		_ = r.Disconnect()
		t.Fatalf("Connect without server-name verified a certificate that does not name 127.0.0.1")
	}
	if !strings.Contains(err.Error(), "certificate") || !strings.Contains(err.Error(), "tls=on") {
		t.Errorf("error should name the certificate failure and the TLS posture: %v", err)
	}
}

func TestConnectWithoutTheClientCertificateIsRefused(t *testing.T) {
	p := newTestPKI(t)
	srv := startFakeTLSRedis(t, p.serverConfig(true))
	r := &DXRedis{NameId: "nocert", IsConfigured: true, Address: srv.addr, Context: context.Background()}
	if err := r.SetTLSFromBlock(utils.JSON{
		"tls-policy": "intermediate", "ca-trust": "custom", "ca-files": []any{p.caFile}, "server-name": "redis.test",
	}); err != nil {
		t.Fatalf("SetTLSFromBlock: %v", err)
	}
	if err := r.Connect(); err == nil {
		_ = r.Disconnect()
		t.Fatalf("a server that requires a client certificate accepted a client without one")
	}
}

func TestConnectRefusesAServerBelowTLS12(t *testing.T) {
	p := newTestPKI(t)
	cfg := p.serverConfig(false)
	cfg.MinVersion, cfg.MaxVersion = tls.VersionTLS10, tls.VersionTLS11
	srv := startFakeTLSRedis(t, cfg)
	r := &DXRedis{NameId: "old", IsConfigured: true, Address: srv.addr, Context: context.Background(),
		TLSConfig: &tls.Config{RootCAs: p.pool, ServerName: "redis.test", MinVersion: tls.VersionTLS10}}
	if err := r.Connect(); err == nil {
		_ = r.Disconnect()
		t.Fatalf("connected to a TLS 1.1 server; the floor is TLS 1.2")
	}
}

func TestTLSConfigurationErrorsAreReported(t *testing.T) {
	p := newTestPKI(t)
	cases := map[string]struct {
		tls  any
		want string
	}{
		"not an object":      {tls: "yes", want: "WRONG_TYPE"},
		"ca-trust missing":   {tls: utils.JSON{"tls-policy": "intermediate"}, want: "ca-trust"},
		"ca file unreadable": {tls: utils.JSON{"tls-policy": "intermediate", "ca-trust": "custom", "ca-files": []any{p.dir + "/missing.pem"}}, want: "missing.pem"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			setRedisConfiguration(t, utils.JSON{"bad": utils.JSON{"address": "127.0.0.1:1", "database_index": 0, "tls": c.tls}})
			r := newTestManager().NewRedis("bad", false, false)
			err := r.ApplyFromConfiguration()
			if err == nil || !strings.Contains(err.Error(), c.want) || !strings.Contains(err.Error(), "redis.bad.tls") {
				t.Fatalf("err = %v, want one naming redis.bad.tls and %q", err, c.want)
			}
		})
	}
}

func TestTLSBlockAbsentOrDisabledMeansPlainTCP(t *testing.T) {
	setRedisConfiguration(t, utils.JSON{
		"plain":    utils.JSON{"address": "127.0.0.1:1", "database_index": 0},
		"disabled": utils.JSON{"address": "127.0.0.1:1", "database_index": 0, "tls": utils.JSON{"enabled": false, "tls-policy": "intermediate", "ca-trust": "system"}},
		"system":   utils.JSON{"address": "127.0.0.1:1", "database_index": 0, "tls": utils.JSON{"tls-policy": "intermediate", "ca-trust": "system"}},
	})
	m := newTestManager()
	for _, name := range []string{"plain", "disabled"} {
		r := m.NewRedis(name, false, false)
		if err := r.ApplyFromConfiguration(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if r.TLSConfig != nil || r.ringOptions().TLSConfig != nil {
			t.Errorf("%s: TLS is on", name)
		}
		if s := r.tlsSummary(); s != "tls=off" {
			t.Errorf("%s: tlsSummary = %q", name, s)
		}
	}
	r := m.NewRedis("system", false, false)
	if err := r.ApplyFromConfiguration(); err != nil {
		t.Fatalf("system: %v", err)
	}
	if r.TLSConfig == nil || r.TLSConfig.RootCAs == nil {
		t.Fatalf("ca-trust=system should give a TLS config with the system roots")
	}
}
