package query

import (
	"context"
	"database/sql"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db/query/builder"
	"github.com/donnyhardyanto/dxlib/databases/db/query/named"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// TxDeleteWithDeleteQueryBuilder2 executes a DELETE within a transaction using DeleteQueryBuilder.
// If RETURNING fields are specified, returns the deleted rows.
// Otherwise returns sql.Result info.
func TxDeleteWithDeleteQueryBuilder2(ctx context.Context, dtx *databases.DXDatabaseTx, qb *builder.DeleteQueryBuilder) (result sql.Result, returningRows []utils.JSON, err error) {
	if qb.Error != nil {
		return nil, nil, qb.Error
	}

	driverName := base.NormalizeDriverName(dtx.Tx.DriverName())

	// Oracle: two-step SELECT-then-DELETE for RETURNING support
	if driverName == "oracle" && len(qb.OutFields) > 0 {
		return oracleTxDeleteWithReturning(ctx, dtx, qb)
	}

	query, args, err := buildDeleteSQL(driverName, qb)
	if err != nil {
		return nil, nil, err
	}

	if len(qb.OutFields) > 0 {
		_, rows, err := named.TxNamedQueryRows2(ctx, dtx, query, args, nil)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "TX_DELETE_WITH_RETURNING_ERROR")
		}
		return nil, rows, nil
	}

	result, err = named.TxNamedExec2(ctx, dtx, query, args)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "TX_DELETE_ERROR")
	}
	return result, nil, nil
}
