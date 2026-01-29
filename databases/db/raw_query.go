package db

import (
	"database/sql"
	"fmt"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
)

func RawQueryRows(db *sqlx.DB, fieldTypeMapping DXDatabaseTableFieldTypeMapping, query string, arg []any) (rowsInfo *DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	r = []utils.JSON{}
	dbt := base.StringToDXDatabaseType(db.DriverName())
	err = CheckAll(dbt, query, arg)
	if err != nil {
		return nil, r, errors.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %+v", err)
	}

	rows, err := db.Queryx(query, arg...)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "DB_QUERY_ERROR sql=%s", query)
	}
	defer func() {
		_ = rows.Close()
	}()
	rowsInfo = &DXDatabaseTableRowsInfo{}
	rowsInfo.Columns, err = rows.Columns()
	if err != nil {
		return rowsInfo, r, errors.Wrap(err, "failed to get columns")
	}
	//rowsInfo.ColumnTypes, err = rows.ColumnTypes()
	/*	if err != nil {
		return rowsInfo, r, err
	}*/
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to scan row")
		}
		rowJSON, err = DeformatKeys(rowJSON, db.DriverName(), fieldTypeMapping)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to deformat keys")
		}
		r = append(r, rowJSON)
	}
	return rowsInfo, r, nil
}

func RawTxQueryRows(tx *sqlx.Tx, fieldTypeMapping DXDatabaseTableFieldTypeMapping, query string, arg []any) (rowsInfo *DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	r = []utils.JSON{}

	dbt := base.StringToDXDatabaseType(tx.DriverName())
	err = CheckAll(dbt, query, arg)
	if err != nil {
		return nil, nil, errors.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %+v=%s +%v", err, query, arg)
	}

	rows, err := tx.Queryx(query, arg...)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "DB_TX_QUERY_ERROR sql=%s", query)
	}
	defer func() {
		_ = rows.Close()
	}()
	rowsInfo = &DXDatabaseTableRowsInfo{}
	rowsInfo.Columns, err = rows.Columns()
	if err != nil {
		return rowsInfo, r, errors.Wrap(err, "failed to get columns")
	}
	//rowsInfo.ColumnTypes, err = rows.ColumnTypes()
	/*	if err != nil {
		return rowsInfo, r, err
	}*/
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to scan row")
		}
		rowJSON, err = DeformatKeys(rowJSON, tx.DriverName(), fieldTypeMapping)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to deformat keys")
		}
		r = append(r, rowJSON)
	}
	return rowsInfo, r, nil
}

func QueryRows(
	db *sqlx.DB,
	fieldTypeMapping DXDatabaseTableFieldTypeMapping,
	sqlStatement string,
	sqlArguments utils.JSON,
) (rowsInfo *DXDatabaseTableRowsInfo, rows []utils.JSON, err error) {
	var (
		modifiedSQL string
		args        []interface{}
	)
	dbt := base.StringToDXDatabaseType(db.DriverName())

	// First, convert named parameters to positional parameters (? placeholders)
	modifiedSQL, args, err = sqlx.Named(sqlStatement, sqlArguments)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to convert named parameters")
	}

	// Then handle databases-specific parameter styles
	switch dbt {
	case base.DXDatabaseTypePostgreSQL:
		// PostgreSQL uses $1, $2, etc.
		modifiedSQL = db.Rebind(modifiedSQL)

	case base.DXDatabaseTypeOracle:
		// For go-ora, we need to use sql.Named for each parameter
		// Keep the original SQL with :name parameters (no modification needed)

		// Convert JSON arguments to sql.Named arguments
		args = make([]interface{}, 0, len(sqlArguments))
		for name, value := range sqlArguments {
			args = append(args, sql.Named(name, value))
		}

	case base.DXDatabaseTypeMariaDB:
		// MariaDB uses ? placeholders
		// Convert to question mark format if needed for IN clauses
		modifiedSQL, args, err = sqlx.In(modifiedSQL, args...)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to convert to MariaDB parameter format")
		}
		modifiedSQL = db.Rebind(modifiedSQL)

	case base.DXDatabaseTypeSQLServer:
		// SQL Server uses @p1, @p2, etc.
		modifiedSQL = db.Rebind(modifiedSQL)

	default:
		return nil, nil, errors.Errorf("unsupported databases driver: %s", db.DriverName())
	}

	// Call the original RawQueryRows function with the modified SQL and arguments
	return RawQueryRows(db, fieldTypeMapping, modifiedSQL, args)
}

func RawCount(
	db *sqlx.DB,
	fromWhereJoinPartSqlStatement string,
	sqlArguments utils.JSON,
) (count int64, err error) {
	var (
		modifiedSQL string
		args        []interface{}
	)
	dbt := base.StringToDXDatabaseType(db.DriverName())

	magicVariableName := "dx_internal_rowcount_x58f2"
	s := fmt.Sprintf("select count(*) as %s %s", magicVariableName, fromWhereJoinPartSqlStatement)

	// First, convert named parameters to positional parameters (? placeholders)
	modifiedSQL, args, err = sqlx.Named(s, sqlArguments)
	if err != nil {
		return 0, errors.Wrap(err, "failed to convert named parameters")
	}

	// Then handle databases-specific parameter styles
	switch dbt {
	case base.DXDatabaseTypePostgreSQL:
		// PostgreSQL uses $1, $2, etc.
		modifiedSQL = db.Rebind(modifiedSQL)

	case base.DXDatabaseTypeOracle:
		// For go-ora, we need to use sql.Named for each parameter
		// Keep the original SQL with :name parameters (no modification needed)

		// Convert JSON arguments to sql.Named arguments
		args = make([]interface{}, 0, len(sqlArguments))
		for name, value := range sqlArguments {
			args = append(args, sql.Named(name, value))
		}

	case base.DXDatabaseTypeMariaDB:
		// MariaDB uses ? placeholders
		// Convert to question mark format if needed for IN clauses
		modifiedSQL, args, err = sqlx.In(modifiedSQL, args...)
		if err != nil {
			return 0, errors.Wrap(err, "failed to convert to MariaDB parameter format")
		}
		modifiedSQL = db.Rebind(modifiedSQL)

	case base.DXDatabaseTypeSQLServer:
		// SQL Server uses @p1, @p2, etc.
		modifiedSQL = db.Rebind(modifiedSQL)

	default:
		return 0, errors.Errorf("unsupported databases driver: %s", db.DriverName())
	}

	// Call the original RawQueryRows function with the modified SQL and arguments
	_, r, err := RawQueryRows(db, nil, modifiedSQL, args)
	if err != nil {
		return 0, errors.Wrapf(err, "error executing count query %s with args %+v", modifiedSQL, args)
	}

	if len(r) != 1 {
		return 0, errors.New("unexpected number of rows returned from count query")
	}
	c, ok := r[0][magicVariableName].(int64)
	if !ok {
		// Handle potential type conversion for different databases
		switch v := r[0][magicVariableName].(type) {
		case int:
			count = int64(v)
		case float64:
			count = int64(v)
		default:
			return 0, errors.New("unexpected type for count result")
		}
	}
	return c, nil
}

func TxQueryRows(
	tx *sqlx.Tx,
	fieldTypeMapping DXDatabaseTableFieldTypeMapping,
	sqlStatement string,
	sqlArguments utils.JSON,
) (rowsInfo *DXDatabaseTableRowsInfo, rows []utils.JSON, err error) {
	var (
		modifiedSQL string
		args        []interface{}
	)

	dbt := base.StringToDXDatabaseType(tx.DriverName())

	// First, convert named parameters to positional parameters (? placeholders)
	modifiedSQL, args, err = sqlx.Named(sqlStatement, sqlArguments)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to convert named parameters")
	}

	// Then handle databases-specific parameter styles
	switch dbt {
	case base.DXDatabaseTypePostgreSQL:
		// PostgreSQL uses $1, $2, etc.
		modifiedSQL = tx.Rebind(modifiedSQL)

	case base.DXDatabaseTypeOracle:
		// For go-ora, we need to use sql.Named for each parameter
		// Keep the original SQL with :name parameters (no modification needed)

		// Convert JSON arguments to sql.Named arguments
		args = make([]interface{}, 0, len(sqlArguments))
		for name, value := range sqlArguments {
			args = append(args, sql.Named(name, value))
		}

	case base.DXDatabaseTypeMariaDB:
		// MariaDB uses ? placeholders
		// Convert to question mark format if needed for IN clauses
		modifiedSQL, args, err = sqlx.In(modifiedSQL, args...)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to convert to MariaDB parameter format")
		}
		modifiedSQL = tx.Rebind(modifiedSQL)

	case base.DXDatabaseTypeSQLServer:
		// SQL Server uses @p1, @p2, etc.
		modifiedSQL = tx.Rebind(modifiedSQL)

	default:
		return nil, nil, errors.Errorf("unsupported databases driver: %s", tx.DriverName())
	}

	// Call the original RawQueryRows function with the modified SQL and arguments
	return RawTxQueryRows(tx, fieldTypeMapping, modifiedSQL, args)
}
