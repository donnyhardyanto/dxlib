package named

import (
	"github.com/donnyhardyanto/dxlib/base"
	databaseDb "github.com/donnyhardyanto/dxlib/databases/db"
	base2 "github.com/donnyhardyanto/dxlib/databases/db/query/base"
	query2 "github.com/donnyhardyanto/dxlib/databases/db/query/utils"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
)

// NamedQueryRows2 executes a named query and returns all matching rows.
// Based on database type: if the database supports named parameters (:name),
// calls BaseQueryRows2 directly. Otherwise, converts to positional parameters first.
func NamedQueryRows2(db *sqlx.DB, query string, arg utils.JSON, fieldTypeMapping databaseDb.DXDatabaseTableFieldTypeMapping) (rowsInfo *databaseDb.DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	dbType := base.StringToDXDatabaseType(db.DriverName())

	// MariaDB and Oracle do not support named parameters, convert to positional
	if dbType == base.DXDatabaseTypeMariaDB || dbType == base.DXDatabaseTypeOracle {
		positionalQuery, positionalArgs, err := query2.ParameterizedSQLQueryNamedBasedToIndexBased(dbType, query, arg)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "PARAMETER_CONVERSION_ERROR:QUERY=%s", query)
		}
		return base2.BaseQueryRows2(db, positionalQuery, positionalArgs, fieldTypeMapping)
	}

	// PostgreSQL, SQL Server support named parameters
	return base2.BaseQueryRows2(db, query, arg, fieldTypeMapping)
}

// ShouldNamedQueryRows2 executes a named query and returns all matching rows,
// erroring if no rows found.
func ShouldNamedQueryRows2(db *sqlx.DB, query string, arg utils.JSON, fieldTypeMapping databaseDb.DXDatabaseTableFieldTypeMapping) (rowsInfo *databaseDb.DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	rowsInfo, r, err = NamedQueryRows2(db, query, arg, fieldTypeMapping)
	if err != nil {
		return rowsInfo, r, err
	}
	if len(r) == 0 {
		err = errors.New("ROWS_MUST_EXIST:NAMED_QUERY")
		return rowsInfo, r, err
	}
	return rowsInfo, r, nil
}

// NamedQueryRow2 executes a named query and returns a single row.
func NamedQueryRow2(db *sqlx.DB, query string, arg utils.JSON, fieldTypeMapping databaseDb.DXDatabaseTableFieldTypeMapping) (rowsInfo *databaseDb.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, rows, err := NamedQueryRows2(db, query, arg, fieldTypeMapping)
	if err != nil {
		return rowsInfo, nil, err
	}
	if len(rows) == 0 {
		return rowsInfo, nil, nil
	}
	return rowsInfo, rows[0], nil
}

// ShouldNamedQueryRow2 executes a named query and returns a single row,
// erroring if no row found.
func ShouldNamedQueryRow2(db *sqlx.DB, query string, arg utils.JSON, fieldTypeMapping databaseDb.DXDatabaseTableFieldTypeMapping) (rowsInfo *databaseDb.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = NamedQueryRow2(db, query, arg, fieldTypeMapping)
	if err != nil {
		return rowsInfo, r, err
	}
	if r == nil {
		err = errors.New("ROW_MUST_EXIST:NAMED_QUERY")
		return rowsInfo, r, err
	}
	return rowsInfo, r, nil
}
