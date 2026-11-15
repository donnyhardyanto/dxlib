package redis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/donnyhardyanto/dxlib/utils/tls/tlstest"
)

// The Valkey test runs only when DXLIB_VALKEY_TLS_IT=1. It starts
// valkey/valkey:9.1.3 with TLS on (plain port off, client certificates
// required, a user limited to keys under sess:) in a container and connects
// DXRedis to it both ways: from a configuration block and built by hand.
//
// DXLIB_VALKEY_PODMAN names the podman command (default "podman"; it may hold
// several words, such as a wrapper followed by its subcommand). The PEM files
// are copied into the container with podman cp, not bind-mounted, so the test
// does not depend on which host folders the podman machine shares.
const valkeyImage = "valkey/valkey:9.1.3"

func podmanCommand() []string {
	if v := strings.Fields(os.Getenv("DXLIB_VALKEY_PODMAN")); len(v) > 0 {
		return v
	}
	return []string{"podman"}
}

func podman(t *testing.T, args ...string) string {
	t.Helper()
	cmd := podmanCommand()
	out, err := exec.Command(cmd[0], append(cmd[1:], args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", strings.Join(cmd, " "), strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

type valkeyTLS struct {
	addr                 string
	caFile               string
	clientCrt, clientKey string
	otherCAFile          string
	pool                 *x509.CertPool
}

func startValkeyTLS(t *testing.T) *valkeyTLS {
	t.Helper()
	root := tlstest.NewRootCA(t, "valkey-test-root")
	other := tlstest.NewRootCA(t, "unrelated-root")
	server := root.Issue(t, tlstest.LeafOptions{CommonName: "valkey.test", DNSNames: []string{"valkey.test"}, ServerOnly: true})
	client := root.Issue(t, tlstest.LeafOptions{CommonName: "session-store", ClientOnly: true})

	dir := t.TempDir()
	v := &valkeyTLS{caFile: tlstest.WriteCA(t, dir, "ca", root), otherCAFile: tlstest.WriteCA(t, dir, "other", other)}
	v.clientCrt, v.clientKey = tlstest.WriteLeaf(t, dir, "client", client)
	v.pool = x509.NewCertPool()
	v.pool.AddCert(root.Cert)

	// What goes into the container: world-readable, since valkey does not run
	// as the user who owns the copied files. Throwaway test keys only.
	ctr := filepath.Join(t.TempDir(), "tls")
	if err := os.Mkdir(ctr, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		"ca.crt":     tlstest.CertPEM(root.Cert),
		"server.crt": tlstest.CertPEM(server.Chain...),
		"server.key": tlstest.KeyPEM(t, server.Key),
	} {
		if err := os.WriteFile(filepath.Join(ctr, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	name := fmt.Sprintf("dxlib-valkey-tls-%d", os.Getpid())
	_ = exec.Command(podmanCommand()[0], append(podmanCommand()[1:], "rm", "-f", name)...).Run()
	podman(t, "create", "--name", name, "-p", "127.0.0.1::6379", valkeyImage,
		"valkey-server",
		"--port", "0",
		"--tls-port", "6379",
		"--tls-cert-file", "/tls/server.crt",
		"--tls-key-file", "/tls/server.key",
		"--tls-ca-cert-file", "/tls/ca.crt",
		"--tls-auth-clients", "yes",
		// One config line spread over arguments: valkey joins them; a single
		// quoted argument would be read as one user name with spaces in it.
		"--user", "session", "on", ">s3ss10n-pass", "~sess:*", "+@all",
	)
	t.Cleanup(func() { podman(t, "rm", "-f", name) })
	podman(t, "cp", ctr, name+":/tls")
	podman(t, "start", name)

	port := podman(t, "port", name, "6379/tcp")
	// "127.0.0.1:41234", possibly one line per address family.
	v.addr = strings.TrimSpace(strings.Split(port, "\n")[0])
	if !strings.HasPrefix(v.addr, "127.0.0.1:") {
		t.Fatalf("unexpected published port %q; container log:\n%s", port, podman(t, "logs", name))
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		c, err := tls.Dial("tcp", v.addr, &tls.Config{RootCAs: v.pool, ServerName: "valkey.test", Certificates: []tls.Certificate{v.mustClientPair(t)}})
		if err == nil {
			_ = c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("valkey did not accept a TLS handshake on %s within 30s: %v\n%s", v.addr, err, podman(t, "logs", name))
		}
		time.Sleep(300 * time.Millisecond)
	}
	return v
}

func (v *valkeyTLS) mustClientPair(t *testing.T) tls.Certificate {
	t.Helper()
	pair, err := tls.LoadX509KeyPair(v.clientCrt, v.clientKey)
	if err != nil {
		t.Fatal(err)
	}
	return pair
}

func (v *valkeyTLS) block(overrides utils.JSON) utils.JSON {
	b := utils.JSON{
		"tls-policy":  "intermediate",
		"ca-trust":    "custom",
		"ca-files":    []any{v.caFile},
		"cert-file":   v.clientCrt,
		"key-file":    v.clientKey,
		"server-name": "valkey.test",
	}
	for k, val := range overrides {
		if val == nil {
			delete(b, k)
		} else {
			b[k] = val
		}
	}
	return b
}

func TestValkeyTLS(t *testing.T) {
	if os.Getenv("DXLIB_VALKEY_TLS_IT") != "1" {
		t.Skip("set DXLIB_VALKEY_TLS_IT=1 to run against " + valkeyImage + " in a container")
	}
	v := startValkeyTLS(t)
	ctx := context.Background()

	t.Run("from configuration, user and password, key prefix ACL", func(t *testing.T) {
		setRedisConfiguration(t, utils.JSON{"session": utils.JSON{
			"address": v.addr, "database_index": 0,
			"user_name": "session", "password": "s3ss10n-pass",
			"tls": v.block(nil),
		}})
		r := newTestManager().NewRedis("session", false, false)
		r.Context = ctx
		if err := r.Connect(); err != nil {
			t.Fatalf("Connect: %v", err)
		}
		defer func() { _ = r.Disconnect() }()
		if err := r.Set(ctx, "sess:abc", utils.JSON{"user": "u1"}, time.Minute); err != nil {
			t.Fatalf("Set sess:abc: %v", err)
		}
		got, err := r.Get(ctx, "sess:abc")
		if err != nil || got["user"] != "u1" {
			t.Fatalf("Get sess:abc = %v, %v", got, err)
		}
		if err := r.Set(ctx, "other:abc", utils.JSON{"x": 1}, time.Minute); err == nil || !strings.Contains(strings.ToUpper(err.Error()), "NOPERM") {
			t.Fatalf("Set outside the user's key prefix: err = %v, want NOPERM", err)
		}
	})

	t.Run("built directly with IsConfigured and TLSConfig", func(t *testing.T) {
		r := &DXRedis{
			NameId: "direct", IsConfigured: true, Address: v.addr, Context: ctx,
			UserName: "session", HasUserName: true, Password: "s3ss10n-pass", HasPassword: true,
			TLSConfig: &tls.Config{RootCAs: v.pool, ServerName: "valkey.test", Certificates: []tls.Certificate{v.mustClientPair(t)}},
		}
		if err := r.Connect(); err != nil {
			t.Fatalf("Connect: %v", err)
		}
		defer func() { _ = r.Disconnect() }()
		if err := r.Ping(ctx); err != nil {
			t.Fatalf("Ping: %v", err)
		}
	})

	refusals := []struct {
		name  string
		block utils.JSON // nil: plain TCP
	}{
		{"no server-name (the certificate names valkey.test, the dial is 127.0.0.1)", v.block(utils.JSON{"server-name": nil})},
		{"no client certificate", v.block(utils.JSON{"cert-file": nil, "key-file": nil})},
		{"wrong CA", v.block(utils.JSON{"ca-files": []any{v.otherCAFile}})},
		{"plain TCP to the TLS port", nil},
	}
	for _, c := range refusals {
		t.Run("refused: "+c.name, func(t *testing.T) {
			r := &DXRedis{NameId: "refused", IsConfigured: true, Address: v.addr, Context: ctx,
				UserName: "session", HasUserName: true, Password: "s3ss10n-pass", HasPassword: true}
			if c.block != nil {
				if err := r.SetTLSFromBlock(c.block); err != nil {
					t.Fatalf("SetTLSFromBlock: %v", err)
				}
			}
			err := r.Connect()
			if err == nil {
				_ = r.Disconnect()
				t.Fatalf("connected; want a refusal")
			}
			t.Logf("refused as expected: %v", err)
		})
	}
}
