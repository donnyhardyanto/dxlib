package raw

import (
	"database/sql"
	"github.com/donnyhardyanto/dxlib/database/sqlchecker"
	"github.com/donnyhardyanto/dxlib/database2/database_type"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"strings"
)

func RawExec(db *sqlx.DB, query string, arg []any) (r *database_type.ExecResult, err error) {
	err = sqlchecker.CheckAll(db.DriverName(), query, arg)
	if err != nil {
		return nil, errors.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %w", err)
	}

	result, err := db.Exec(query, arg...)
	if err != nil {
		return nil, err
	}

	lastInsertId, err := result.LastInsertId()
	if err != nil {
		// Some databases or operations might not support LastInsertId
		// Just ignore the error and return 0
		lastInsertId = 0
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		// This should rarely happen, but handle it anyway
		rowsAffected = 0
	}

	return &database_type.ExecResult{
		LastInsertId: lastInsertId,
		RowsAffected: rowsAffected,
	}, nil
}

func RawTxExec(tx *sqlx.Tx, query string, arg []any) (*database_type.ExecResult, error) {
	err := sqlchecker.CheckAll(tx.DriverName(), query, arg)
	if err != nil {
		return nil, errors.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %w", err)
	}

	result, err := tx.Exec(query, arg...)
	if err != nil {
		return nil, err
	}

	lastInsertId, err := result.LastInsertId()
	if err != nil {
		// Some databases or operations might not support LastInsertId
		// Just ignore the error and return 0
		lastInsertId = 0
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		// This should rarely happen, but handle it anyway
		rowsAffected = 0
	}

	return &database_type.ExecResult{
		LastInsertId: lastInsertId,
		RowsAffected: rowsAffected,
	}, nil
}

func Exec(db *sqlx.DB, sqlStatement string, sqlArguments utils.JSON) (r *database_type.ExecResult, err error) {
	var (
		modifiedSQL string
		args        []interface{}
	)

	// Get the driver name from the db connection
	dbDriverName := strings.ToLower(db.DriverName())

	// First, convert named parameters to positional parameters (? placeholders)
	modifiedSQL, args, err = sqlx.Named(sqlStatement, sqlArguments)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert named parameters")
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
			return nil, errors.Wrap(err, "failed to convert to MySQL parameter format")
		}
		modifiedSQL = db.Rebind(modifiedSQL)

	case "sqlserver", "mssql":
		// SQL Server uses @p1, @p2, etc.
		modifiedSQL = db.Rebind(modifiedSQL)

	default:
		return nil, errors.Errorf("unsupported database driver: %s", dbDriverName)
	}

	// Call the RawExec function with the modified SQL and arguments
	return RawExec(db, modifiedSQL, args)
}

func TxExec(
	tx *sqlx.Tx,
	sqlStatement string,
	sqlArguments utils.JSON,
) (*database_type.ExecResult, error) {
	var (
		modifiedSQL string
		args        []interface{}
		err         error
	)

	// Get the driver name from the tx connection
	dbDriverName := strings.ToLower(tx.DriverName())

	// First, convert named parameters to positional parameters (? placeholders)
	modifiedSQL, args, err = sqlx.Named(sqlStatement, sqlArguments)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert named parameters")
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
			return nil, errors.Wrap(err, "failed to convert to MySQL parameter format")
		}
		modifiedSQL = tx.Rebind(modifiedSQL)

	case "sqlserver", "mssql":
		// SQL Server uses @p1, @p2, etc.
		modifiedSQL = tx.Rebind(modifiedSQL)

	default:
		return nil, errors.Errorf("unsupported database driver: %s", dbDriverName)
	}

	// Call the RawTxExec function with the modified SQL and arguments
	return RawTxExec(tx, modifiedSQL, args)
}
