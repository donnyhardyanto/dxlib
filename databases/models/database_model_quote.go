package models

import (
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
)

// quoteIdent quotes ONE SQL identifier for the engine, for EXECUTED DDL.
//
// NB: MariaDB uses backticks — double quotes are string literals there unless
// ANSI_QUOTES is set (verified live: `SELECT "code"` yields the string 'code',
// not the column). So we deliberately do NOT reuse
// db/query/utils.QuoteIdentifierByDbType here (it emits "…" for MariaDB, which is
// fine for the display-only DB browser but wrong for DDL that actually runs).
func quoteIdent(dbType base.DXDatabaseType, id string) string {
	switch dbType {
	case base.DXDatabaseTypeSQLServer:
		return "[" + strings.ReplaceAll(id, "]", "]]") + "]"
	case base.DXDatabaseTypeMariaDB:
		return "`" + strings.ReplaceAll(id, "`", "``") + "`"
	case base.DXDatabaseTypeOracle:
		// Oracle: quoted identifiers are case-SENSITIVE, and runtime SQL references
		// them UNQUOTED (folded to uppercase by the engine) — so DDL must create
		// UPPERCASE objects or nothing resolves. Quoting (vs bare) keeps reserved
		// words like UID usable as column names.
		return `"` + strings.ReplaceAll(strings.ToUpper(id), `"`, `""`) + `"`
	default: // PostgreSQL
		return `"` + strings.ReplaceAll(id, `"`, `""`) + `"`
	}
}

// renderDefaultForDBType renders a column DEFAULT value per engine, translating
// the two portable sentinels the model uses:
//   - the current-timestamp function (`now()` / CURRENT_TIMESTAMP) → the engine's
//     own now-function (PG/MariaDB CURRENT_TIMESTAMP; SQL Server SYSDATETIMEOFFSET();
//     Oracle SYSTIMESTAMP) — a bare `now()` is invalid on SQL Server/Oracle.
//   - a Go bool → true/false on PG/MariaDB, but 1/0 on SQL Server (BIT) and Oracle
//     (NUMBER(1)), which have no boolean literal.
// Anything else falls back to the plain literal renderer.
func renderDefaultForDBType(dbType base.DXDatabaseType, field ModelDBField, v any) string {
	if s, ok := v.(string); ok {
		switch strings.ToLower(strings.TrimSpace(s)) {
		case "now()", "current_timestamp", "current_timestamp()":
			switch dbType {
			case base.DXDatabaseTypeSQLServer:
				return "SYSDATETIMEOFFSET()"
			case base.DXDatabaseTypeOracle:
				return "SYSTIMESTAMP"
			default: // PostgreSQL, MariaDB
				return "CURRENT_TIMESTAMP"
			}
		}
	}
	if b, ok := v.(bool); ok {
		switch dbType {
		case base.DXDatabaseTypeSQLServer, base.DXDatabaseTypeOracle:
			if b {
				return "1"
			}
			return "0"
		default:
			if b {
				return "true"
			}
			return "false"
		}
	}
	return valueToSQLLiteral(field, v)
}

// qualifiedTableName renders schema.table quoted per engine.
//
//   - PostgreSQL / SQL Server / Oracle: a schema is a real namespace, so each
//     part is quoted separately — "schema"."table" / [schema].[table].
//   - MariaDB: no schema layer. The schema is VIRTUAL, realized as a SINGLE
//     quoted identifier `schema.table` (the dot is part of the table name),
//     living inside a normally-created database whose name is unrelated to the
//     schema (so `CREATE DATABASE mydb` + many virtual schemas can coexist).
func qualifiedTableName(dbType base.DXDatabaseType, schema, table string) string {
	if schema == "" {
		return quoteIdent(dbType, table)
	}
	if dbType == base.DXDatabaseTypeMariaDB {
		return quoteIdent(dbType, schema+"."+table)
	}
	return quoteIdent(dbType, schema) + "." + quoteIdent(dbType, table)
}
