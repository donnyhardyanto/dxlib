package named

import (
	"context"
	"database/sql"

	databaseDb "github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
)

// NamedExec2 executes a named non-query statement (INSERT/UPDATE/DELETE without RETURNING).
// Delegates to db.Exec which handles named parameter conversion for all database types.
func NamedExec2(ctx context.Context, db *sqlx.DB, query string, arg utils.JSON) (sql.Result, error) {
	return databaseDb.Exec(ctx, db, query, arg)
}
