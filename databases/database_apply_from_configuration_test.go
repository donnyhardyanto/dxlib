package databases

import (
	"testing"

	"github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/utils"
)

// ApplyFromConfiguration used to call GetConnectionString() unconditionally,
// even for a database that is neither must_connected nor is_connect_at_start
// (e.g. a demo/compat-check target registered with no real address yet, or an
// address left blank because nothing in that environment ever configured it).
// ConnectAllAtStart (database_manager.go) calls ApplyFromConfiguration for
// EVERY registered database before checking IsConnectAtStart, so a single
// unusable optional database address crashed the entire app boot — exactly
// the opposite of what is_connect_at_start:false/must_connected:false is
// supposed to guarantee ("list + compat-target without blocking boot").
func TestApplyFromConfiguration_OptionalDatabaseWithUnusableAddressDoesNotError(t *testing.T) {
	configuration.Manager.NewIfNotExistConfiguration("storage", "storage.json", "json", false, false, utils.JSON{
		"demo-optional": utils.JSON{
			"nameid":              "demo-optional",
			"database_type":       "postgres",
			"address":             "", // unusable: net.SplitHostPort("") fails
			"user_name":           "u",
			"user_password":       "p",
			"database_name":       "d",
			"must_connected":      false,
			"is_connect_at_start": false,
		},
	}, nil)

	d := &DXDatabase{NameId: "demo-optional", MustConnected: false, IsConnectAtStart: false}
	if err := d.ApplyFromConfiguration(); err != nil {
		t.Fatalf("ApplyFromConfiguration() on an optional, non-eager database must not error on an unusable address, got: %v", err)
	}
	if !d.IsConfigured {
		t.Errorf("IsConfigured should be true after a (tolerated) configuration pass")
	}
}

// The same unusable address on a database that DOES need to connect at start
// (or is must_connected) must still fail loudly — this fix must not silence
// real misconfiguration for databases that matter.
func TestApplyFromConfiguration_RequiredDatabaseWithUnusableAddressStillErrors(t *testing.T) {
	configuration.Manager.NewIfNotExistConfiguration("storage", "storage.json", "json", false, false, utils.JSON{
		"demo-required": utils.JSON{
			"nameid":              "demo-required",
			"database_type":       "postgres",
			"address":             "",
			"user_name":           "u",
			"user_password":       "p",
			"database_name":       "d",
			"must_connected":      true,
			"is_connect_at_start": true,
		},
	}, nil)

	d := &DXDatabase{NameId: "demo-required", MustConnected: true, IsConnectAtStart: true}
	if err := d.ApplyFromConfiguration(); err == nil {
		t.Fatalf("ApplyFromConfiguration() on a required database must still error on an unusable address")
	}
}

// A valid address on an optional database is unaffected: it still gets
// eagerly configured (ConnectionString built), matching pre-fix behavior.
func TestApplyFromConfiguration_OptionalDatabaseWithValidAddressStillConfigures(t *testing.T) {
	configuration.Manager.NewIfNotExistConfiguration("storage", "storage.json", "json", false, false, utils.JSON{
		"demo-valid": utils.JSON{
			"nameid":              "demo-valid",
			"database_type":       "postgres",
			"address":             "mariadb:5432",
			"user_name":           "u",
			"user_password":       "p",
			"database_name":       "d",
			"must_connected":      false,
			"is_connect_at_start": false,
		},
	}, nil)

	d := &DXDatabase{NameId: "demo-valid", MustConnected: false, IsConnectAtStart: false}
	if err := d.ApplyFromConfiguration(); err != nil {
		t.Fatalf("ApplyFromConfiguration() with a valid address must not error, got: %v", err)
	}
	if d.ConnectionString == "" {
		t.Errorf("ConnectionString should be built when the address is valid, even for an optional database")
	}
}
