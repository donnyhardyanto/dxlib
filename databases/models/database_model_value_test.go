package models

import (
	"sort"
	"testing"
	"time"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/types"
)

func isoField() *ModelDBField {
	return &ModelDBField{Type: types.DataTypeISO8601}
}

// TestISO8601IsTextOnMariaDBOnly pins the divergence itself. MariaDB has no
// column type that holds a UTC offset; the other three do, and must keep using
// them.
func TestISO8601IsTextOnMariaDBOnly(t *testing.T) {
	byDB := types.DataTypeISO8601.TypeByDatabaseType

	if got := byDB[base.DXDatabaseTypeMariaDB]; got != "VARCHAR(35)" {
		t.Errorf("MariaDB = %q, want VARCHAR(35): DATETIME drops the offset on write "+
			"and TIMESTAMP renders through the reader's session zone", got)
	}
	for db, want := range map[base.DXDatabaseType]string{
		base.DXDatabaseTypePostgreSQL: "TIMESTAMP WITH TIME ZONE",
		base.DXDatabaseTypeSQLServer:  "DATETIMEOFFSET",
		base.DXDatabaseTypeOracle:     "TIMESTAMP WITH TIME ZONE",
	} {
		if got := byDB[db]; got != want {
			t.Errorf("%v = %q, want %q -- this engine has a real instant type, so it must use it", db, got, want)
		}
	}
}

// TestNormalizeWritesISOTextOnMariaDB is the half of the change that stops it
// being a regression. Left to the driver, a time.Time reaches a VARCHAR as a
// MySQL datetime literal with no offset at all.
func TestNormalizeWritesISOTextOnMariaDB(t *testing.T) {
	jakarta := time.FixedZone("WIB", 7*3600)
	in := time.Date(2026, 8, 31, 19, 0, 0, 0, jakarta) // 12:00:00Z

	got := NormalizeFieldValueForDBType(isoField(), base.DXDatabaseTypeMariaDB, in)
	s, ok := got.(string)
	if !ok {
		t.Fatalf("MariaDB got %T, want a string -- the driver would encode a time.Time "+
			"as a MySQL datetime literal and the offset would be lost", got)
	}
	if s != "2026-08-31T12:00:00.000000000Z" {
		t.Errorf("stored %q, want the UTC instant in the fixed-width layout", s)
	}

	// The same instant, whatever zone the caller was in.
	utc := NormalizeFieldValueForDBType(isoField(), base.DXDatabaseTypeMariaDB, in.UTC())
	if utc != got {
		t.Errorf("the same instant stored two ways: %q from +07:00, %q from UTC", got, utc)
	}
}

// TestNormalizeLeavesRealInstantTypesAlone. On an engine with a proper type the
// driver's own time.Time binding is right, and turning it into text would break it.
func TestNormalizeLeavesRealInstantTypesAlone(t *testing.T) {
	in := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	for _, db := range []base.DXDatabaseType{
		base.DXDatabaseTypePostgreSQL, base.DXDatabaseTypeSQLServer, base.DXDatabaseTypeOracle,
	} {
		got := NormalizeFieldValueForDBType(isoField(), db, in)
		if _, isString := got.(string); isString {
			t.Errorf("%v: value was turned into text, but this engine stores a real instant", db)
		}
		if got != any(in) {
			t.Errorf("%v: value changed from %v to %v", db, in, got)
		}
	}
}

// TestNormalizeRerendersAnIncomingString. The API layer hands this type over as
// RFC3339, and an offset-bearing string would not sort against the normalised
// ones.
func TestNormalizeRerendersAnIncomingString(t *testing.T) {
	for _, in := range []string{
		"2026-08-31T19:00:00+07:00",
		"2026-08-31T12:00:00Z",
		"2026-08-31T12:00:00.000000000Z",
	} {
		got := NormalizeFieldValueForDBType(isoField(), base.DXDatabaseTypeMariaDB, in)
		if got != "2026-08-31T12:00:00.000000000Z" {
			t.Errorf("%q normalised to %q, want the one layout that sorts", in, got)
		}
	}

	// Not a timestamp: left alone, so the database raises it rather than this
	// function hiding it.
	if got := NormalizeFieldValueForDBType(isoField(), base.DXDatabaseTypeMariaDB, "tomorrow"); got != "tomorrow" {
		t.Errorf("an unparseable value was rewritten to %q", got)
	}
	if got := NormalizeFieldValueForDBType(isoField(), base.DXDatabaseTypeMariaDB, nil); got != nil {
		t.Errorf("nil became %v", got)
	}
}

// TestStoredTextSortsChronologically is the reason for the fixed width, and the
// property ORDER BY, BETWEEN, MIN and MAX on this column all depend on.
func TestStoredTextSortsChronologically(t *testing.T) {
	base0 := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	instants := []time.Time{
		base0.Add(-time.Hour),
		base0,
		base0.Add(500 * time.Millisecond),
		base0.Add(505 * time.Millisecond),
		base0.Add(time.Second),
		base0.AddDate(0, 0, 1),
	}

	stored := make([]string, len(instants))
	for i, in := range instants {
		// Deliberately from a mix of zones: the instants are what must order.
		z := time.FixedZone("odd", (i%3)*3600)
		stored[i] = NormalizeFieldValueForDBType(isoField(), base.DXDatabaseTypeMariaDB, in.In(z)).(string)
	}

	sorted := append([]string(nil), stored...)
	sort.Strings(sorted)
	for i := range stored {
		if stored[i] != sorted[i] {
			t.Fatalf("byte order is not chronological order:\n  chronological %v\n  sorted        %v", stored, sorted)
		}
	}

	// And every value is the same width, which is what keeps that true.
	for _, s := range stored {
		if len(s) != len(stored[0]) {
			t.Errorf("widths differ: %q and %q -- a trimmed fraction breaks ordering", stored[0], s)
		}
		if len(s) > 35 {
			t.Errorf("%q is %d characters, wider than the VARCHAR(35) it is stored in", s, len(s))
		}
	}
}
