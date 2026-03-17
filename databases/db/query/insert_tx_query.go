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

// TxInsertWithInsertQueryBuilder2 executes an INSERT within a transaction using InsertQueryBuilder.
// If RETURNING fields are specified, returns the inserted row.
// Otherwise returns sql.Result info.
func TxInsertWithInsertQueryBuilder2(ctx context.Context, dtx *databases.DXDatabaseTx, qb *builder.InsertQueryBuilder) (result sql.Result, returningRow utils.JSON, err error) {
	if qb.Error != nil {
		return nil, nil, qb.Error
	}

	driverName := base.NormalizeDriverName(dtx.Tx.DriverName())

	// Oracle: use RETURNING INTO with sql.Out binds
	if driverName == "oracle" && len(qb.OutFields) > 0 {
		return oracleTxInsertWithReturningInto(ctx, dtx, qb)
	}

	query, args, err := buildInsertSQL(driverName, qb)
	if err != nil {
		return nil, nil, err
	}

	if len(qb.OutFields) > 0 {
		_, row, err := named.TxNamedQueryRow2(ctx, dtx, query, args, nil)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "TX_INSERT_WITH_RETURNING_ERROR")
		}
		return nil, row, nil
	}

	result, err = named.TxNamedExec2(ctx, dtx, query, args)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "TX_INSERT_ERROR")
	}
	return result, nil, nil
}
