package databases

import (
	"testing"

	"github.com/donnyhardyanto/dxlib/base"
)

// MariaDB was previously unsupported in GetConnectionString (returned an error),
// which crashed app boot whenever a mariadb database was registered. It must now
// produce a valid go-sql-driver/mysql DSN: user:pass@tcp(host:port)/db[?opts].
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
	if s != "root:root@tcp(mariadb:3306)/public" {
		t.Errorf("DSN = %q, want root:root@tcp(mariadb:3306)/public", s)
	}
	d.ConnectionOptions = "parseTime=true"
	s2, err := d.GetConnectionString()
	if err != nil {
		t.Fatalf("with options errored: %v", err)
	}
	if s2 != "root:root@tcp(mariadb:3306)/public?parseTime=true" {
		t.Errorf("DSN(opts) = %q, want ...?parseTime=true", s2)
	}
}
