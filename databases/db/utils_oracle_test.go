package db

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/utils"
)

// namedArgNames extracts the bind names from the []any of sql.Named args.
func namedArgNames(args []any) map[string]any {
	out := map[string]any{}
	for _, a := range args {
		na := a.(sql.NamedArg)
		out[na.Name] = na.Value
	}
	return out
}

func TestOracleSafeBindNamesRewritesOnlyArgBinds(t *testing.T) {
	sqlStatement := `INSERT INTO system0.commands ("UID","TYPE") VALUES (:uid,:type) RETURNING "ID" INTO :id_out`
	got, args := OracleSafeBindNames(sqlStatement, utils.JSON{"uid": "u1", "type": "PING"})

	want := `INSERT INTO system0.commands ("UID","TYPE") VALUES (:p_uid,:p_type) RETURNING "ID" INTO :id_out`
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	names := namedArgNames(args)
	if names["p_uid"] != "u1" || names["p_type"] != "PING" {
		t.Fatalf("args mis-keyed: %+v", names)
	}
}

// The one-pass scan must never re-match its own output: with args "id" AND
// "p_id" in the same statement, a progressive search-and-replace turned :id
// into :p_id and then rewrote BOTH into :p_p_id (binding one value to two
// columns, silently). Each original token is rewritten exactly once.
func TestOracleSafeBindNamesPrefixCollision(t *testing.T) {
	sqlStatement := `INSERT INTO tbl (id, p_id) VALUES (:id, :p_id)`
	got, args := OracleSafeBindNames(sqlStatement, utils.JSON{"id": 1, "p_id": 2})

	want := `INSERT INTO tbl (id, p_id) VALUES (:p_id, :p_p_id)`
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	names := namedArgNames(args)
	if names["p_id"] != 1 || names["p_p_id"] != 2 {
		t.Fatalf("args mis-keyed: %+v", names)
	}
}

func TestOracleSafeBindNamesSkipsStringLiteralsAndAssignments(t *testing.T) {
	sqlStatement := `DECLARE v NUMBER; BEGIN UPDATE t SET x = :x, note = 'at :x o''clock HH24:MI' WHERE y = :y; :out_v := v; END;`
	got, _ := OracleSafeBindNames(sqlStatement, utils.JSON{"x": 1, "y": 2, "mi": 3})

	if !strings.Contains(got, "SET x = :p_x") || !strings.Contains(got, "WHERE y = :p_y") {
		t.Fatalf("binds not rewritten: %q", got)
	}
	if !strings.Contains(got, `'at :x o''clock HH24:MI'`) {
		t.Fatalf("string literal was modified: %q", got)
	}
	if !strings.Contains(got, ":out_v := v") {
		t.Fatalf("PL/SQL assignment was modified: %q", got)
	}
}

func TestOracleSafeBindNamesLeavesUnknownBinds(t *testing.T) {
	sqlStatement := `SELECT 1 FROM dual WHERE a = :known AND b = :unknown`
	got, _ := OracleSafeBindNames(sqlStatement, utils.JSON{"known": 1})

	want := `SELECT 1 FROM dual WHERE a = :p_known AND b = :unknown`
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
