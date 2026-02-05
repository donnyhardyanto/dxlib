package query

import (
	"database/sql"

	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db/query/builder"
	"github.com/donnyhardyanto/dxlib/databases/db/query/named"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// TxUpdateWithUpdateQueryBuilder2 executes an UPDATE within a transaction using UpdateQueryBuilder.
// If RETURNING fields are specified, returns the affected rows.
// Otherwise, returns sql.Result info.
func TxUpdateWithUpdateQueryBuilder2(dtx *databases.DXDatabaseTx, qb *builder.UpdateQueryBuilder) (result sql.Result, returningRows []utils.JSON, err error) {
	if qb.Error != nil {
		return nil, nil, qb.Error
	}

	driverName := dtx.Tx.DriverName()
	query, args, err := buildUpdateSQL(driverName, qb)
	if err != nil {
		return nil, nil, err
	}

	if len(qb.OutFields) > 0 {
		_, rows, err := named.TxNamedQueryRows2(dtx, query, args)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "TX_UPDATE_WITH_RETURNING_ERROR")
		}
		return nil, rows, nil
	}

	result, err = named.TxNamedExec2(dtx, query, args)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "TX_UPDATE_ERROR")
	}
	return result, nil, nil
}
