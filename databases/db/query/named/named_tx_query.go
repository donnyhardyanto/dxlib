package named

import (
	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/databases"
	databaseDb "github.com/donnyhardyanto/dxlib/databases/db"
	base2 "github.com/donnyhardyanto/dxlib/databases/db/query/base"
	query2 "github.com/donnyhardyanto/dxlib/databases/db/query/utils"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// TxNamedQueryRows2 executes a named query within a transaction and returns all matching rows.
// Based on database type: if the database supports named parameters (:name),
// calls TxBaseQueryRows2 directly. Otherwise, converts to positional parameters first.
func TxNamedQueryRows2(dtx *databases.DXDatabaseTx, query string, arg utils.JSON) (rowsInfo *databaseDb.DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	dbType := base.StringToDXDatabaseType(dtx.Tx.DriverName())

	// MariaDB and Oracle do not support named parameters, convert to positional
	if dbType == base.DXDatabaseTypeMariaDB || dbType == base.DXDatabaseTypeOracle {
		positionalQuery, positionalArgs, err := query2.ParameterizedSQLQueryNamedBasedToIndexBased(dbType, query, arg)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "PARAMETER_CONVERSION_ERROR:QUERY=%s", query)
		}
		return base2.TxBaseQueryRows2(dtx, positionalQuery, positionalArgs)
	}

	// PostgreSQL, SQL Server support named parameters
	return base2.TxBaseQueryRows2(dtx, query, arg)
}

// TxShouldNamedQueryRows2 executes a named query within a transaction and returns all matching rows,
// erroring if no rows found.
func TxShouldNamedQueryRows2(dtx *databases.DXDatabaseTx, query string, arg utils.JSON) (rowsInfo *databaseDb.DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	rowsInfo, r, err = TxNamedQueryRows2(dtx, query, arg)
	if err != nil {
		return rowsInfo, r, err
	}
	if len(r) == 0 {
		err = errors.New("ROWS_MUST_EXIST:TX_NAMED_QUERY")
		return rowsInfo, r, err
	}
	return rowsInfo, r, nil
}

// TxNamedQueryRow2 executes a named query within a transaction and returns a single row.
func TxNamedQueryRow2(dtx *databases.DXDatabaseTx, query string, arg utils.JSON) (rowsInfo *databaseDb.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, rows, err := TxNamedQueryRows2(dtx, query, arg)
	if err != nil {
		return rowsInfo, nil, err
	}
	if len(rows) == 0 {
		return rowsInfo, nil, nil
	}
	return rowsInfo, rows[0], nil
}

// TxShouldNamedQueryRow2 executes a named query within a transaction and returns a single row,
// erroring if no row found.
func TxShouldNamedQueryRow2(dtx *databases.DXDatabaseTx, query string, arg utils.JSON) (rowsInfo *databaseDb.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = TxNamedQueryRow2(dtx, query, arg)
	if err != nil {
		return rowsInfo, r, err
	}
	if r == nil {
		err = errors.New("ROW_MUST_EXIST:TX_NAMED_QUERY")
		return rowsInfo, r, err
	}
	return rowsInfo, r, nil
}
