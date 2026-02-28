package named

import (
	"context"
	"database/sql"

	"github.com/donnyhardyanto/dxlib/databases"
	databaseDb "github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/utils"
)

// TxNamedExec2 executes a named non-query statement within a transaction (INSERT/UPDATE/DELETE without RETURNING).
// Delegates to db.TxExec which handles named parameter conversion for all database types.
func TxNamedExec2(ctx context.Context, dtx *databases.DXDatabaseTx, query string, arg utils.JSON) (sql.Result, error) {
	return databaseDb.TxExec(ctx, dtx.Tx, query, arg)
}
