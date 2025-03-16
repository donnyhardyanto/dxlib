package raw

import (
	"database/sql"
	"github.com/donnyhardyanto/dxlib/database2/database_type"
	utils2 "github.com/donnyhardyanto/dxlib/database2/db/utils"
	"github.com/donnyhardyanto/dxlib/database2/sqlchecker"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"strings"
)

func RawQueryRows(db *sqlx.DB, fieldTypeMapping utils2.FieldTypeMapping, query string, arg []any) (rowsInfo *database_type.RowsInfo, r []utils.JSON, err error) {
	r = []utils.JSON{}

	err = sqlchecker.CheckAll(db.DriverName(), query, arg)
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
	rowsInfo = &database_type.RowsInfo{}
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
		rowJSON, err = utils2.DeformatKeys(rowJSON, db.DriverName(), fieldTypeMapping)
		if err != nil {
			return nil, nil, err
		}
		r = append(r, rowJSON)
	}
	return rowsInfo, r, nil
}

func RawTxQueryRows(tx *sqlx.Tx, fieldTypeMapping utils2.FieldTypeMapping, query string, arg []any) (rowsInfo *database_type.RowsInfo, r []utils.JSON, err error) {
	r = []utils.JSON{}

	err = sqlchecker.CheckAll(tx.DriverName(), query, arg)
	if err != nil {
		return nil, r, errors.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %w", err)
	}

	rows, err := tx.Queryx(query, arg...)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	rowsInfo = &database_type.RowsInfo{}
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
		rowJSON, err = utils2.DeformatKeys(rowJSON, tx.DriverName(), fieldTypeMapping)
		if err != nil {
			return nil, nil, err
		}
		r = append(r, rowJSON)
	}
	return rowsInfo, r, nil
}

func QueryRows(
	db *sqlx.DB,
	fieldTypeMapping utils2.FieldTypeMapping,
	sqlStatement string,
	sqlArguments utils.JSON,
) (*database_type.RowsInfo, []utils.JSON, error) {
	var (
		modifiedSQL string
		args        []interface{}
		err         error
	)

	// Get the driver name from the db connection
	dbDriverName := strings.ToLower(db.DriverName())

	// First, convert named parameters to positional parameters (? placeholders)
	modifiedSQL, args, err = sqlx.Named(sqlStatement, sqlArguments)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to convert named parameters")
	}

	// Then handle database-specific parameter styles
	switch dbDriverName {
	case "postgres":
		// PostgreSQL uses $1, $2, etc.
		modifiedSQL = db.Rebind(modifiedSQL)

	case "oracle":
		// For go-ora, we need to use sql.Named for each parameter
		// Keep the original SQL with :name parameters (no modification needed)

		// Convert JSON arguments to sql.Named arguments
		args = make([]interface{}, 0, len(sqlArguments))
		for name, value := range sqlArguments {
			args = append(args, sql.Named(name, value))
		}

	case "mysql":
		// MySQL uses ? placeholders
		// Convert to question mark format if needed for IN clauses
		modifiedSQL, args, err = sqlx.In(modifiedSQL, args...)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to convert to MySQL parameter format")
		}
		modifiedSQL = db.Rebind(modifiedSQL)

	case "sqlserver", "mssql":
		// SQL Server uses @p1, @p2, etc.
		modifiedSQL = db.Rebind(modifiedSQL)

	default:
		return nil, nil, errors.Errorf("unsupported database driver: %s", dbDriverName)
	}

	// Call the original RawQueryRows function with the modified SQL and arguments
	return RawQueryRows(db, fieldTypeMapping, modifiedSQL, args)
}

func TxQueryRows(
	tx *sqlx.Tx,
	fieldTypeMapping utils2.FieldTypeMapping,
	sqlStatement string,
	sqlArguments utils.JSON,
) (*database_type.RowsInfo, []utils.JSON, error) {
	var (
		modifiedSQL string
		args        []interface{}
		err         error
	)

	// Get the driver name from the db connection
	dbDriverName := strings.ToLower(tx.DriverName())

	// First, convert named parameters to positional parameters (? placeholders)
	modifiedSQL, args, err = sqlx.Named(sqlStatement, sqlArguments)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to convert named parameters")
	}

	// Then handle database-specific parameter styles
	switch dbDriverName {
	case "postgres":
		// PostgreSQL uses $1, $2, etc.
		modifiedSQL = tx.Rebind(modifiedSQL)

	case "oracle":
		// For go-ora, we need to use sql.Named for each parameter
		// Keep the original SQL with :name parameters (no modification needed)

		// Convert JSON arguments to sql.Named arguments
		args = make([]interface{}, 0, len(sqlArguments))
		for name, value := range sqlArguments {
			args = append(args, sql.Named(name, value))
		}

	case "mysql":
		// MySQL uses ? placeholders
		// Convert to question mark format if needed for IN clauses
		modifiedSQL, args, err = sqlx.In(modifiedSQL, args...)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to convert to MySQL parameter format")
		}
		modifiedSQL = tx.Rebind(modifiedSQL)

	case "sqlserver", "mssql":
		// SQL Server uses @p1, @p2, etc.
		modifiedSQL = tx.Rebind(modifiedSQL)

	default:
		return nil, nil, errors.Errorf("unsupported database driver: %s", dbDriverName)
	}

	// Call the original RawQueryRows function with the modified SQL and arguments
	return RawTxQueryRows(tx, fieldTypeMapping, modifiedSQL, args)
}
