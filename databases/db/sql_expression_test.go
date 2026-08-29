package db

import (
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
)

// BUG-035. SQLExpression.String() doubled every colon unconditionally. That is
// the escape sqlx's named-parameter compiler wants, and the three sqlx engines
// un-double it again -- but the Oracle path discards sqlx's output and
// re-derives from the original SQL, and OracleSafeBindNames treats "::" as a
// cast and copies both bytes through. Nothing un-doubled it, so Oracle compared
// against a string the caller never wrote and returned the wrong rows with no
// error.
//
// Reproduced against Oracle 23ai before the fix: a row holding 'a:b', a WHERE
// built from name = 'a:b', 0 rows and a nil error.
func TestWhereBuilderDoesNotDoubleColonsForOracle(t *testing.T) {
	where := utils.JSON{"__expr": SQLExpression{Expression: "name = 'a:b'"}}

	got := SQLPartWhereAndFieldNameValues(where, "oracle")
	if strings.Contains(got, "::") {
		t.Fatalf("Oracle received doubled colons and never un-doubles them: %s", got)
	}
	if got != "name = 'a:b'" {
		t.Fatalf("got %q, want the expression as written", got)
	}
}

// The other three engines must keep the doubling. Handing sqlx.Named a raw
// expression is a parse error, not a silent difference, so dropping the escape
// for everyone would break three engines to fix one.
func TestWhereBuilderKeepsDoublingForTheSqlxEngines(t *testing.T) {
	expr := "name = 'a:b' AND ts >= TO_TIMESTAMP('2026-01-01 01:30:00','YYYY-MM-DD HH24:MI:SS')"
	where := utils.JSON{"__expr": SQLExpression{Expression: expr}}

	for _, driver := range []string{"postgres", "mysql", "sqlserver"} {
		t.Run(driver, func(t *testing.T) {
			built := SQLPartWhereAndFieldNameValues(where, driver)
			if !strings.Contains(built, "::") {
				t.Fatalf("%s needs the escape, got %s", driver, built)
			}
			// The round trip is what matters: after sqlx compiles the statement,
			// the caller's expression must be back exactly as written.
			out, _, err := sqlx.Named("SELECT * FROM t WHERE "+built, utils.JSON{})
			if err != nil {
				t.Fatalf("%s: sqlx rejected the statement: %v", driver, err)
			}
			if want := "SELECT * FROM t WHERE " + expr; out != want {
				t.Fatalf("%s round trip changed the expression:\n got %s\nwant %s", driver, out, want)
			}
		})
	}
}

// Raw through sqlx is a hard failure. This pins the reason the fix has to be
// driver-aware rather than simply deleting the doubling.
func TestRawExpressionIsUnusableOnTheSqlxEngines(t *testing.T) {
	raw := "name = 'a:b'"
	if _, _, err := sqlx.Named("SELECT * FROM t WHERE "+raw, utils.JSON{}); err == nil {
		t.Fatal("expected sqlx to reject a raw colon; if this now passes, the escape may no longer be needed")
	}
}

// String() feeds fmt.Stringer, and formatJSONForLog renders values with %v, so
// this ran on every DB_UPDATE and DB_DELETE debug line -- on all four engines --
// and printed colons that were never sent anywhere.
func TestStringIsTheExpressionAsWritten(t *testing.T) {
	se := SQLExpression{Expression: "ts = '01:30:00'"}
	if se.String() != "ts = '01:30:00'" {
		t.Fatalf("String() must not escape; got %s", se.String())
	}
	if se.StringForNamedQuery() != "ts = '01::30::00'" {
		t.Fatalf("StringForNamedQuery() must escape; got %s", se.StringForNamedQuery())
	}
}

// The INSERT and SET builders share the WHERE builder's contract.
func TestInsertAndSetBuildersFollowTheSameRule(t *testing.T) {
	kv := utils.JSON{"col": SQLExpression{Expression: "TO_DATE('01:02','HH24:MI')"}}

	_, oracleValues := SQLPartInsertFieldNamesFieldValues(kv, "oracle")
	if strings.Contains(oracleValues, "::") {
		t.Errorf("INSERT VALUES doubled for oracle: %s", oracleValues)
	}
	_, pgValues := SQLPartInsertFieldNamesFieldValues(kv, "postgres")
	if !strings.Contains(pgValues, "::") {
		t.Errorf("INSERT VALUES did not escape for postgres: %s", pgValues)
	}

	if s := SQLPartUpdateSetFieldValues(kv, "oracle"); strings.Contains(s, "::") {
		t.Errorf("SET doubled for oracle: %s", s)
	}
	if s := SQLPartUpdateSetFieldValues(kv, "postgres"); !strings.Contains(s, "::") {
		t.Errorf("SET did not escape for postgres: %s", s)
	}
}
