package db

import (
	"database/sql"
	"fmt"

	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

func RawQueryRows(db *sqlx.DB, fieldTypeMapping DXDatabaseTableFieldTypeMapping, query string, arg []any) (rowsInfo *DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	r = []utils.JSON{}
	dbt := StringToDXDatabaseType(db.DriverName())
	err = CheckAll(dbt, query, arg)
	if err != nil {
		return nil, r, errors.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %w", err)
	}

	rows, err := db.Queryx(query, arg...)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	rowsInfo = &DXDatabaseTableRowsInfo{}
	rowsInfo.Columns, err = rows.Columns()
	if err != nil {
		return rowsInfo, r, err
	}
	//rowsInfo.ColumnTypes, err = rows.ColumnTypes()
	/*	if err != nil {
		return rowsInfo, r, err
	}*/
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			return nil, nil, err
		}
		rowJSON, err = DeformatKeys(rowJSON, db.DriverName(), fieldTypeMapping)
		if err != nil {
			return nil, nil, err
		}
		r = append(r, rowJSON)
	}
	return rowsInfo, r, nil
}

func RawTxQueryRows(tx *sqlx.Tx, fieldTypeMapping DXDatabaseTableFieldTypeMapping, query string, arg []any) (rowsInfo *DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	r = []utils.JSON{}

	dbt := StringToDXDatabaseType(tx.DriverName())
	err = CheckAll(dbt, query, arg)
	if err != nil {
		return nil, nil, errors.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %w=%s +%v", err, query, arg)
	}

	rows, err := tx.Queryx(query, arg...)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	rowsInfo = &DXDatabaseTableRowsInfo{}
	rowsInfo.Columns, err = rows.Columns()
	if err != nil {
		return rowsInfo, r, err
	}
	//rowsInfo.ColumnTypes, err = rows.ColumnTypes()
	/*	if err != nil {
		return rowsInfo, r, err
	}*/
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			return nil, nil, err
		}
		rowJSON, err = DeformatKeys(rowJSON, tx.DriverName(), fieldTypeMapping)
		if err != nil {
			return nil, nil, err
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
	dbt := StringToDXDatabaseType(db.DriverName())

	// First, convert named parameters to positional parameters (? placeholders)
	modifiedSQL, args, err = sqlx.Named(sqlStatement, sqlArguments)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to convert named parameters")
	}

	// Then handle database-specific parameter styles
	switch dbt {
	case PostgreSQL:
		// PostgreSQL uses $1, $2, etc.
		modifiedSQL = db.Rebind(modifiedSQL)

	case Oracle:
		// For go-ora, we need to use sql.Named for each parameter
		// Keep the original SQL with :name parameters (no modification needed)

		// Convert JSON arguments to sql.Named arguments
		args = make([]interface{}, 0, len(sqlArguments))
		for name, value := range sqlArguments {
			args = append(args, sql.Named(name, value))
		}

	case MariaDB:
		// MariaDB uses ? placeholders
		// Convert to question mark format if needed for IN clauses
		modifiedSQL, args, err = sqlx.In(modifiedSQL, args...)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to convert to MariaDB parameter format")
		}
		modifiedSQL = db.Rebind(modifiedSQL)

	case SQLServer:
		// SQL Server uses @p1, @p2, etc.
		modifiedSQL = db.Rebind(modifiedSQL)

	default:
		return nil, nil, errors.Errorf("unsupported database driver: %s", db.DriverName())
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
	dbt := StringToDXDatabaseType(db.DriverName())

	magicVariableName := "dx_internal_rowcount_x58f2"
	s := fmt.Sprintf("select count(*) as %s %s", magicVariableName, fromWhereJoinPartSqlStatement)

	// First, convert named parameters to positional parameters (? placeholders)
	modifiedSQL, args, err = sqlx.Named(s, sqlArguments)
	if err != nil {
		return 0, errors.Wrap(err, "failed to convert named parameters")
	}

	// Then handle database-specific parameter styles
	switch dbt {
	case PostgreSQL:
		// PostgreSQL uses $1, $2, etc.
		modifiedSQL = db.Rebind(modifiedSQL)

	case Oracle:
		// For go-ora, we need to use sql.Named for each parameter
		// Keep the original SQL with :name parameters (no modification needed)

		// Convert JSON arguments to sql.Named arguments
		args = make([]interface{}, 0, len(sqlArguments))
		for name, value := range sqlArguments {
			args = append(args, sql.Named(name, value))
		}

	case MariaDB:
		// MariaDB uses ? placeholders
		// Convert to question mark format if needed for IN clauses
		modifiedSQL, args, err = sqlx.In(modifiedSQL, args...)
		if err != nil {
			return 0, errors.Wrap(err, "failed to convert to MariaDB parameter format")
		}
		modifiedSQL = db.Rebind(modifiedSQL)

	case SQLServer:
		// SQL Server uses @p1, @p2, etc.
		modifiedSQL = db.Rebind(modifiedSQL)

	default:
		return 0, errors.Errorf("unsupported database driver: %s", db.DriverName())
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

	dbt := StringToDXDatabaseType(tx.DriverName())

	// First, convert named parameters to positional parameters (? placeholders)
	modifiedSQL, args, err = sqlx.Named(sqlStatement, sqlArguments)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to convert named parameters")
	}

	// Then handle database-specific parameter styles
	switch dbt {
	case PostgreSQL:
		// PostgreSQL uses $1, $2, etc.
		modifiedSQL = tx.Rebind(modifiedSQL)

	case Oracle:
		// For go-ora, we need to use sql.Named for each parameter
		// Keep the original SQL with :name parameters (no modification needed)

		// Convert JSON arguments to sql.Named arguments
		args = make([]interface{}, 0, len(sqlArguments))
		for name, value := range sqlArguments {
			args = append(args, sql.Named(name, value))
		}

	case MariaDB:
		// MariaDB uses ? placeholders
		// Convert to question mark format if needed for IN clauses
		modifiedSQL, args, err = sqlx.In(modifiedSQL, args...)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to convert to MariaDB parameter format")
		}
		modifiedSQL = tx.Rebind(modifiedSQL)

	case SQLServer:
		// SQL Server uses @p1, @p2, etc.
		modifiedSQL = tx.Rebind(modifiedSQL)

	default:
		return nil, nil, errors.Errorf("unsupported database driver: %s", tx.DriverName())
	}

	// Call the original RawQueryRows function with the modified SQL and arguments
	return RawTxQueryRows(tx, fieldTypeMapping, modifiedSQL, args)
}
