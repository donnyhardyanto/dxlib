package json

import (
	"testing"

	"github.com/donnyhardyanto/dxlib/utils"
)

// GetInt64 is what DXRawTable.InsertReturningId and TxInsertReturningId call to
// turn a RETURNING value into a row id, and until 2026-08-29 both discarded its
// error and returned the zero value. This pins the error contract they now rely
// on: every shape Oracle can produce for a row id has to come back as an error,
// because a nil error is what the caller reads as "id 0 is real".
//
// Scope, stated plainly: this covers the precondition, not the call site. The two
// functions need a live database, and the module has no fake or sqlmock, so the
// fixed call site is exercised by bpm7's four-engine e2e suite rather than here.
func TestGetInt64ErrorsOnEveryOracleRowIdShape(t *testing.T) {
	// Both come from go-ora: a NUMBER row id arrives as a decimal string, and an
	// empty string is what the driver leaves when RETURNING yielded no value.
	for _, tc := range []struct {
		name string
		val  any
	}{
		{"oracle number as decimal string", "1234.0"},
		{"oracle number as plain string", "1234"},
		{"returning yielded nothing", ""},
		{"column present but null", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := GetInt64(utils.JSON{"id": tc.val}, "id")
			if err == nil {
				t.Fatalf("got id %d with a nil error; the caller would treat %#v as row id %d",
					got, tc.val, got)
			}
			if got != 0 {
				t.Fatalf("error path should return 0, got %d", got)
			}
		})
	}

	// The column being absent entirely must also error, not read as id 0.
	if _, err := GetInt64(utils.JSON{}, "id"); err == nil {
		t.Fatal("a missing column returned a nil error")
	}
}

// The shapes that must keep working, so the fix above cannot be mistaken for a
// reason to widen GetInt64 into accepting strings.
func TestGetInt64AcceptsWhatTheDriversActuallyReturn(t *testing.T) {
	for _, tc := range []struct {
		name string
		val  any
		want int64
	}{
		{"postgres and sqlserver bigint", int64(1234), 1234},
		{"json-decoded number", float64(1234), 1234},
		{"mariadb before normalisation", []byte("1234"), 1234},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := GetInt64(utils.JSON{"id": tc.val}, "id")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}
