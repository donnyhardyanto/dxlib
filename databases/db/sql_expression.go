package db

import (
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
)

// SQLExpression is a raw SQL fragment supplied in place of a bound value. It is
// inlined into the statement text rather than parameterised.
type SQLExpression struct {
	Expression string
}

// String returns the expression exactly as written.
//
// It also satisfies fmt.Stringer, and that is why the raw form has to be the
// default. formatJSONForLog renders values with %v, so every DB_UPDATE and
// DB_DELETE debug line ran this method -- and until 2026-08-29 it printed
// colons that were never sent to any database, on all four engines.
func (se SQLExpression) String() string {
	return se.Expression
}

// StringForNamedQuery doubles every colon so it survives sqlx's named-parameter
// compiler, which reduces "::" back to ":".
//
// Only for statements that genuinely pass through sqlx.Named. Anything else
// receives the doubled colons verbatim and compares against a string the caller
// never wrote. Prefer SQLExpressionForDriver, which decides per engine.
func (se SQLExpression) StringForNamedQuery() string {
	return strings.ReplaceAll(se.Expression, ":", "::")
}

// SQLExpressionForDriver renders an expression for a statement that sqlx.Named
// will compile on its way to the driver.
//
// Oracle is the exception, and the whole reason this function exists. QueryRows
// and Exec discard sqlx's output for Oracle and re-derive from the ORIGINAL SQL,
// because go-ora binds by name. OracleSafeBindNames then treats "::" as a cast
// and copies both bytes through, so nothing ever un-doubles it: Oracle compared
// against the wrong string and returned the wrong rows with no error at all.
//
// The doubling cannot simply be dropped for everyone. Handing sqlx.Named a raw
// expression is a parse failure, not a difference:
//
//	unexpected `:` while reading named param at 75
//
// so PostgreSQL, MariaDB and SQL Server still need it.
func SQLExpressionForDriver(se SQLExpression, driverName string) string {
	return SQLExpressionForDatabaseType(se, base.StringToDXDatabaseType(driverName))
}

// SQLExpressionForDatabaseType is SQLExpressionForDriver for callers that
// already resolved the engine.
func SQLExpressionForDatabaseType(se SQLExpression, dbType base.DXDatabaseType) string {
	if dbType == base.DXDatabaseTypeOracle {
		return se.Expression
	}
	return se.StringForNamedQuery()
}
