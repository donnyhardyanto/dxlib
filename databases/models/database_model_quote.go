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
	default: // PostgreSQL, Oracle
		return `"` + strings.ReplaceAll(id, `"`, `""`) + `"`
	}
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
