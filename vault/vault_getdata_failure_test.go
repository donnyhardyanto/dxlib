package vault

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/log"
)

// VaultGetData must FAIL, never ABORT.
//
// Why this test exists. On 2026-08-03 service-configuration died at startup on the
// bank's OKD development cluster with a bare SIGSEGV inside VaultGetData - no
// message, no path, no indication that Vault was even involved:
//
//	panic: runtime error: invalid memory address or nil pointer dereference
//	[signal SIGSEGV: ... addr=0x30 ...]
//	dxlib/vault.(*DXHashicorpVault).VaultGetData(...) vault/vault.go:440
//	dxlib/vault.(*DXHashicorpVault).GetStringOrEnvOrDefault(...) vault/vault.go:314
//	bms-common/infrastructure.DefineConfiguration() define_configuration.go:43
//
// The caller is always one of the Get*OrEnvOrDefault helpers, whose entire contract
// is "Vault -> env -> default". A returned error just moves to the next tier; a
// panic takes the process down AND makes that fallback unreachable, so a service
// with perfectly good run.env values still dies. Every case below therefore asserts
// two things: an error is returned, and the process is still alive to see it.
//
// The panic-free assertion is not incidental - `go test` reports a panic as a
// failure of the running test, so each subtest doubles as the regression guard.

// The most important case, and the one that actually crashed in production: the
// HashiCorp Vault API returns (nil, nil) for a path that DOES NOT EXIST. A missing
// secret is not an error there, so `err != nil` is false and execution used to fall
// through to `secret.Data[...]` on a nil *Secret.
func TestVaultGetData_PathNotFound_ReturnsErrorNotPanic(t *testing.T) {
	// KV v2 replies 404 with an empty errors array for an absent path, which the
	// Vault client turns into (nil, nil) rather than an error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"errors":[]}`))
	}))
	defer srv.Close()

	hv := NewHashiCorpVault(srv.URL, "test-token", "", "secret/data/does-not-exist")
	if err := hv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	data, err := hv.VaultGetData(context.Background(), &log.Log)
	if err == nil {
		t.Fatal("want an error for a path that does not exist, got nil")
	}
	if data != nil {
		t.Errorf("want nil data on failure, got %v", data)
	}
	// The message must name the path: the crash this replaced gave operators a
	// stack trace and no way to tell WHICH path was missing.
	if !strings.Contains(err.Error(), "does-not-exist") {
		t.Errorf("error should name the failing path, got: %v", err)
	}
}

// Donny's case: an invalid / unreachable address. It must still fail - just not by
// dying. The read error path already returned, but it logged via Fatalf, which read
// as "the process is going down" when it was not.
func TestVaultGetData_InvalidURL_ReturnsErrorNotPanic(t *testing.T) {
	for _, tc := range []struct {
		name    string
		address string
	}{
		// Port 1 on loopback: nothing listens, so this fails fast and offline -
		// no DNS, no external network, safe in CI.
		{"connection refused", "http://127.0.0.1:1"},
		{"malformed scheme", "not-a-url://%%%"},
		{"empty address", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hv := NewHashiCorpVault(tc.address, "test-token", "", "secret/data/anything")
			// Start may itself reject the address; that is a legitimate failure
			// mode and is equally not a panic.
			if err := hv.Start(); err != nil {
				return
			}
			data, err := hv.VaultGetData(context.Background(), &log.Log)
			if err == nil {
				t.Fatalf("want an error for address %q, got nil", tc.address)
			}
			if data != nil {
				t.Errorf("want nil data on failure, got %v", data)
			}
		})
	}
}

// Start() never called, or it failed: hv.Client is nil, and hv.Client.Logical()
// used to panic. Reached in practice whenever config resolution runs before or
// without a working Vault client.
func TestVaultGetData_NilClient_ReturnsErrorNotPanic(t *testing.T) {
	hv := NewHashiCorpVault("http://127.0.0.1:1", "test-token", "", "secret/data/anything")
	// Deliberately NOT calling Start().
	data, err := hv.VaultGetData(context.Background(), &log.Log)
	if err == nil {
		t.Fatal("want an error when the client is not initialized, got nil")
	}
	if data != nil {
		t.Errorf("want nil data on failure, got %v", data)
	}
	if !strings.Contains(err.Error(), "not initialized") {
		t.Errorf("error should say the client is not initialized, got: %v", err)
	}
}

// The contract that the panic was breaking: a Vault failure must fall through to
// the env/default tier rather than taking the service down. This is the assertion
// that would have caught the production crash - the value below is only reachable
// if VaultGetData returns instead of panicking.
func TestGetStringOrEnvOrDefault_FallsBackWhenVaultUnreachable(t *testing.T) {
	const key = "DXLIB_TEST_VAULT_FALLBACK_KEY"

	hv := NewHashiCorpVault("http://127.0.0.1:1", "test-token", "", "secret/data/anything")
	if err := hv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if got := hv.GetStringOrEnvOrDefault(context.Background(), key, "compiled-default"); got != "compiled-default" {
		t.Errorf("want the compiled default when Vault is unreachable, got %q", got)
	}

	t.Setenv(key, "from-env")
	if got := hv.GetStringOrEnvOrDefault(context.Background(), key, "compiled-default"); got != "from-env" {
		t.Errorf("want the env value when Vault is unreachable but env is set, got %q", got)
	}
}
