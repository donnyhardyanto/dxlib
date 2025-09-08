package raw

import (
	"database/sql"

	"github.com/donnyhardyanto/dxlib/database2/database_type"
	"github.com/donnyhardyanto/dxlib/database2/sqlchecker"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

func RawExec(db *sqlx.DB, query string, arg []any) (result sql.Result, err error) {
	dbt := database_type.StringToDXDatabaseType(db.DriverName())
	err = sqlchecker.CheckAll(dbt, query, arg)
	if err != nil {
		return nil, errors.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %w", err)
	}

	result, err = db.Exec(query, arg...)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func RawTxExec(tx *sqlx.Tx, query string, arg []any) (result sql.Result, err error) {
	dbt := database_type.StringToDXDatabaseType(tx.DriverName())
	err = sqlchecker.CheckAll(dbt, query, arg)
	if err != nil {
		return nil, errors.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %w", err)
	}

	result, err = tx.Exec(query, arg...)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func Exec(db *sqlx.DB, sqlStatement string, sqlArguments utils.JSON) (result sql.Result, err error) {
	var (
		modifiedSQL string
		args        []interface{}
	)

	dbt := database_type.StringToDXDatabaseType(db.DriverName())

	// First, convert named parameters to positional parameters (? placeholders)
	modifiedSQL, args, err = sqlx.Named(sqlStatement, sqlArguments)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert named parameters")
	}

	// Then handle database-specific parameter styles
	switch dbt {
	case database_type.PostgreSQL:
		// PostgreSQL uses $1, $2, etc.
		modifiedSQL = db.Rebind(modifiedSQL)

	case database_type.Oracle:
		// For go-ora, we need to use sql.Named for each parameter
		// Keep the original SQL with :name parameters (no modification needed)

		// Convert JSON arguments to sql.Named arguments
		args = make([]interface{}, 0, len(sqlArguments))
		for name, value := range sqlArguments {
			args = append(args, sql.Named(name, value))
		}

	case database_type.MariaDb:
		// MariaDb uses ? placeholders
		// Convert to question mark format if needed for IN clauses
		modifiedSQL, args, err = sqlx.In(modifiedSQL, args...)
		if err != nil {
			return nil, errors.Wrap(err, "failed to convert to MariaDb parameter format")
		}
		modifiedSQL = db.Rebind(modifiedSQL)

	case database_type.SQLServer:
		// SQL Server uses @p1, @p2, etc.
		modifiedSQL = db.Rebind(modifiedSQL)

	default:
		return nil, errors.Errorf("unsupported database driver: %s", db.DriverName())
	}

	// Call the RawExec function with the modified SQL and arguments
	return RawExec(db, modifiedSQL, args)
}

func TxExec(
	tx *sqlx.Tx,
	sqlStatement string,
	sqlArguments utils.JSON,
) (result sql.Result, err error) {
	var (
		modifiedSQL string
		args        []interface{}
	)

	dbt := database_type.StringToDXDatabaseType(tx.DriverName())

	// First, convert named parameters to positional parameters (? placeholders)
	modifiedSQL, args, err = sqlx.Named(sqlStatement, sqlArguments)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert named parameters")
	}

	// Then handle database-specific parameter styles
	switch dbt {
	case database_type.PostgreSQL:
		// PostgreSQL uses $1, $2, etc.
		modifiedSQL = tx.Rebind(modifiedSQL)

	case database_type.Oracle:
		// For go-ora, we need to use sql.Named for each parameter
		// Keep the original SQL with :name parameters (no modification needed)

		// Convert JSON arguments to sql.Named arguments
		args = make([]interface{}, 0, len(sqlArguments))
		for name, value := range sqlArguments {
			args = append(args, sql.Named(name, value))
		}

	case database_type.MariaDb:
		// MariaDb uses ? placeholders
		// Convert to question mark format if needed for IN clauses
		modifiedSQL, args, err = sqlx.In(modifiedSQL, args...)
		if err != nil {
			return nil, errors.Wrap(err, "failed to convert to MariaDb parameter format")
		}
		modifiedSQL = tx.Rebind(modifiedSQL)

	case database_type.SQLServer:
		// SQL Server uses @p1, @p2, etc.
		modifiedSQL = tx.Rebind(modifiedSQL)

	default:
		return nil, errors.Errorf("unsupported database driver: %s", tx.DriverName())
	}

	// Call the RawTxExec function with the modified SQL and arguments
	return RawTxExec(tx, modifiedSQL, args)
}
