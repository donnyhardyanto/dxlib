package databases

import (
	"testing"

	"github.com/donnyhardyanto/dxlib/base"
)

// MariaDB was previously unsupported in GetConnectionString (returned an error),
// which crashed app boot whenever a mariadb database was registered. It must now
// produce a valid go-sql-driver/mysql DSN: user:pass@tcp(host:port)/db?parseTime=true[&opts].
// parseTime=true is always on (v1.109.0): without it DATETIME/TIMESTAMP columns
// scan as []byte instead of time.Time, which base64-encodes in every JSON read.
func TestGetConnectionStringMariaDB(t *testing.T) {
	d := &DXDatabase{
		NameId:       "demo",
		DatabaseType: base.DXDatabaseTypeMariaDB,
		Address:      "mariadb:3306",
		UserName:     "root",
		UserPassword: "root",
		DatabaseName: "public",
	}
	s, err := d.GetConnectionString()
	if err != nil {
		t.Fatalf("MariaDB GetConnectionString errored: %v", err)
	}
	if s != "root:root@tcp(mariadb:3306)/public?parseTime=true" {
		t.Errorf("DSN = %q, want root:root@tcp(mariadb:3306)/public?parseTime=true", s)
	}
	d.ConnectionOptions = "timeout=5s"
	s2, err := d.GetConnectionString()
	if err != nil {
		t.Fatalf("with options errored: %v", err)
	}
	if s2 != "root:root@tcp(mariadb:3306)/public?parseTime=true&timeout=5s" {
		t.Errorf("DSN(opts) = %q, want ...?parseTime=true&timeout=5s", s2)
	}
}
