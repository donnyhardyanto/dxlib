package base

import (
	"context"

	"github.com/donnyhardyanto/dxlib/databases"
	databaseDb "github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
)

// TxBaseQueryRows2 executes a query within a transaction and returns all matching rows
// It supports both named parameters (map/struct) and positional parameters (slice)
// If fieldTypeMapping is provided, applies type conversion to the results
func TxBaseQueryRows2(ctx context.Context, dtx *databases.DXDatabaseTx, query string, arg any, fieldTypeMapping databaseDb.DXDatabaseTableFieldTypeMapping) (rowsInfo *databaseDb.DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	ctx, endOtel := databaseDb.DbOtelStart(ctx, "db.TX_SELECT", query, 3)
	defer func() { endOtel(err, int64(len(r))) }()

	r = []utils.JSON{}
	if arg == nil {
		arg = utils.JSON{}
	}

	// Check if arg is a slice (positional parameters) or map/struct (named parameters)
	var rows *sqlx.Rows
	switch v := arg.(type) {
	case []any:
		// Positional parameters - use QueryxContext
		if len(v) == 0 {
			rows, err = dtx.Tx.QueryxContext(ctx, query)
		} else {
			rows, err = dtx.Tx.QueryxContext(ctx, query, v...)
		}
	default:
		// Named parameters - use NamedQueryContext
		rows, err = sqlx.NamedQueryContext(ctx, dtx.Tx, query, arg)
	}
	if err != nil {
		return nil, nil, errors.Wrapf(err, "TX_QUERY_ROWS_ERROR:QUERY=%s", query)
	}
	defer func() {
		_ = rows.Close()
	}()

	rowsInfo = &databaseDb.DXDatabaseTableRowsInfo{}
	rowsInfo.Columns, err = rows.Columns()
	if err != nil {
		return nil, r, errors.Wrapf(err, "TX_QUERY_ROWS_COLUMNS_ERROR:QUERY=%s", query)
	}

	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "TX_QUERY_ROWS_SCAN_ERROR:QUERY=%s", query)
		}
		r = append(r, rowJSON)
	}

	// Apply field type conversion if fieldTypeMapping is provided
	if fieldTypeMapping != nil && len(fieldTypeMapping) > 0 {
		for i, row := range r {
			convertedRow, err := databaseDb.DeformatKeys(row, dtx.Tx.DriverName(), fieldTypeMapping)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "FIELD_TYPE_CONVERSION_ERROR:QUERY=%s", query)
			}
			r[i] = convertedRow
		}
	}

	return rowsInfo, r, nil
}

// TxBaseQueryRow2 executes a query within a transaction and returns a single row
func TxBaseQueryRow2(ctx context.Context, dtx *databases.DXDatabaseTx, query string, arg any, fieldTypeMapping databaseDb.DXDatabaseTableFieldTypeMapping) (rowsInfo *databaseDb.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, rows, err := TxBaseQueryRows2(ctx, dtx, query, arg, fieldTypeMapping)
	if err != nil {
		return rowsInfo, nil, err
	}
	if len(rows) == 0 {
		return rowsInfo, nil, nil
	}
	return rowsInfo, rows[0], nil
}
