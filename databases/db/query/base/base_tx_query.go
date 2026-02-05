package base

import (
	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/databases"
	databaseDb "github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
)

// TxBaseQueryRows2 executes a query within a transaction and returns all matching rows
// It supports both named parameters (map/struct) and positional parameters (slice)
func TxBaseQueryRows2(dtx *databases.DXDatabaseTx, query string, arg any) (rowsInfo *databaseDb.DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	r = []utils.JSON{}
	if arg == nil {
		arg = utils.JSON{}
	}

	dbt := base.StringToDXDatabaseType(dtx.Tx.DriverName())
	err = databaseDb.CheckAll(dbt, query, arg)
	if err != nil {
		return nil, nil, errors.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %+v=%s +%v", err, query, arg)
	}

	// Check if arg is a slice (positional parameters) or map/struct (named parameters)
	var rows *sqlx.Rows
	switch v := arg.(type) {
	case []any:
		// Positional parameters - use Queryx
		if len(v) == 0 {
			rows, err = dtx.Tx.Queryx(query)
		} else {
			rows, err = dtx.Tx.Queryx(query, v...)
		}
	default:
		// Named parameters - use NamedQuery
		rows, err = dtx.Tx.NamedQuery(query, arg)
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
	return rowsInfo, r, nil
}

// TxBaseQueryRow2 executes a query within a transaction and returns a single row
func TxBaseQueryRow2(dtx *databases.DXDatabaseTx, query string, arg any) (rowsInfo *databaseDb.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, rows, err := TxBaseQueryRows2(dtx, query, arg)
	if err != nil {
		return rowsInfo, nil, err
	}
	if len(rows) == 0 {
		return rowsInfo, nil, nil
	}
	return rowsInfo, rows[0], nil
}
