package db

import (
	"context"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
	_ "github.com/sijms/go-ora/v2"
)

// The guard this pins is the one that every other test missed.
//
// QueryRows and Exec used to call sqlx.Named unconditionally and only then
// switch on the engine -- discarding its output for Oracle, which re-derives
// from the original SQL because go-ora binds by name. That was harmless while
// expressions arrived colon-escaped. The moment BUG-035 was fixed and they began
// arriving raw, the dead pre-pass failed the query outright: sqlx reads ':30'
// inside a string literal as a named parameter and cannot find it.
//
// Every unit test for that fix passed while the real Oracle path returned an
// error. It took running against a container to see it, and nothing committed
// would catch a revert -- dxlib's own suite does not reach QueryRows, and bpm7's
// end-to-end suite deliberately sends colon-free expressions on Oracle.
//
// No database is needed. sqlx.Open only parses the DSN, so the query gets as far
// as dialling a dead port. What distinguishes the two versions is WHERE it
// stops: without the guard it never dials at all.
func TestOracleSkipsTheDeadNamedParameterPass(t *testing.T) {
	db, err := sqlx.Open("oracle", "oracle://u:p@127.0.0.1:1/X")
	if err != nil {
		t.Fatalf("opening should only parse the DSN: %v", err)
	}
	defer db.Close()

	// A colon inside a string literal, which is what a SQLExpression now sends to
	// Oracle unescaped.
	where := utils.JSON{"__expr": SQLExpression{Expression: "name = 'a:b'"}}
	q := "SELECT name FROM t WHERE " + SQLPartWhereAndFieldNameValues(where, "oracle")
	if strings.Contains(q, "::") {
		t.Fatalf("precondition: Oracle must get raw colons, got %s", q)
	}

	_, _, err = QueryRows(context.Background(), db, nil, q, utils.JSON{})
	if err == nil {
		t.Fatal("expected a connection error against a dead port")
	}
	if strings.Contains(err.Error(), "failed to convert named parameters") {
		t.Fatalf("sqlx.Named ran for Oracle and rejected the statement before "+
			"anything was dialled; the engine guard is gone: %v", err)
	}
	t.Logf("reached the driver as intended: %v", err)
}
