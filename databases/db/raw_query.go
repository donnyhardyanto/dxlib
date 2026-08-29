package db

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
)

func RawQueryRows(ctx context.Context, db *sqlx.DB, fieldTypeMapping DXDatabaseTableFieldTypeMapping, query string, arg []any) (rowsInfo *DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	ctx, endOtel := DbOtelStart(ctx, "db.SELECT", query, 3)
	defer func() { endOtel(err, int64(len(r))) }()

	r = []utils.JSON{}
	rows, err := db.QueryxContext(ctx, query, arg...)
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
	// Give every driver's values the same Go type. This must run on the row as
	// MapScan produced it — the plan is keyed by the driver's own column names,
	// which DeformatKeys is about to rewrite. nil on PostgreSQL and whenever no
	// column needs work.
	normalizeRow := NewRowNormalizer(db.DriverName(), rows)
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to scan row")
		}
		if normalizeRow != nil {
			normalizeRow(rowJSON)
		}
		rowJSON, err = DeformatKeys(rowJSON, db.DriverName(), fieldTypeMapping)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to deformat keys")
		}
		r = append(r, rowJSON)
	}
	return rowsInfo, r, nil
}

func RawTxQueryRows(ctx context.Context, tx *sqlx.Tx, fieldTypeMapping DXDatabaseTableFieldTypeMapping, query string, arg []any) (rowsInfo *DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	ctx, endOtel := DbOtelStart(ctx, "db.TX_SELECT", query, 3)
	defer func() { endOtel(err, int64(len(r))) }()

	r = []utils.JSON{}
	rows, err := tx.QueryxContext(ctx, query, arg...)
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
	// See RawQueryRows: normalise before the keys are deformatted.
	normalizeRow := NewRowNormalizer(tx.DriverName(), rows)
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to scan row")
		}
		if normalizeRow != nil {
			normalizeRow(rowJSON)
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
	ctx context.Context,
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

	// Convert named parameters to positional (? placeholders) for the engines
	// that consume sqlx's output. Oracle is skipped deliberately: its branch
	// below re-derives from the ORIGINAL SQL because go-ora binds by name, so
	// running this could only ever fail -- and it does. A SQLExpression reaches
	// Oracle with its colons intact (they must not be doubled, since nothing
	// un-doubles them there), and sqlx reads ':30' inside a literal as a named
	// parameter it cannot find.
	if dbt != base.DXDatabaseTypeOracle {
		modifiedSQL, args, err = sqlx.Named(sqlStatement, sqlArguments)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to convert named parameters")
		}
	}

	// Then handle databases-specific parameter styles
	switch dbt {
	case base.DXDatabaseTypePostgreSQL:
		// PostgreSQL uses $1, $2, etc.
		modifiedSQL = db.Rebind(modifiedSQL)

	case base.DXDatabaseTypeOracle:
		// go-ora binds by :name — start from the ORIGINAL SQL (sqlx.Named above
		// rewrote :name to `?`), then rewrite each bind to a reserved-word-safe
		// ":p_<name>" paired with matching sql.Named args (ORA-01745 otherwise).
		modifiedSQL, args = OracleSafeBindNames(sqlStatement, sqlArguments)

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
	return RawQueryRows(ctx, db, fieldTypeMapping, modifiedSQL, args)
}

func RawCount(
	ctx context.Context,
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

	// Convert named parameters to positional (? placeholders) for the engines
	// that consume sqlx's output. Oracle is skipped deliberately: its branch
	// below re-derives from the ORIGINAL SQL because go-ora binds by name, so
	// running this could only ever fail -- and it does. A SQLExpression reaches
	// Oracle with its colons intact (they must not be doubled, since nothing
	// un-doubles them there), and sqlx reads ':30' inside a literal as a named
	// parameter it cannot find.
	if dbt != base.DXDatabaseTypeOracle {
		modifiedSQL, args, err = sqlx.Named(s, sqlArguments)
		if err != nil {
			return 0, errors.Wrap(err, "failed to convert named parameters")
		}
	}

	// Then handle databases-specific parameter styles
	switch dbt {
	case base.DXDatabaseTypePostgreSQL:
		// PostgreSQL uses $1, $2, etc.
		modifiedSQL = db.Rebind(modifiedSQL)

	case base.DXDatabaseTypeOracle:
		// go-ora binds by :name — start from the ORIGINAL SQL (sqlx.Named above
		// rewrote :name to `?`), then rewrite each bind to a reserved-word-safe
		// ":p_<name>" paired with matching sql.Named args (ORA-01745 otherwise).
		modifiedSQL, args = OracleSafeBindNames(s, sqlArguments)

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
	_, r, err := RawQueryRows(ctx, db, nil, modifiedSQL, args)
	if err != nil {
		return 0, errors.Wrapf(err, "error executing count query %s with args %+v", modifiedSQL, args)
	}

	if len(r) != 1 {
		return 0, errors.New("unexpected number of rows returned from count query")
	}
	// Handle potential type conversion for different databases. Oracle's
	// count(*) is a bare NUMBER (scale marker 0xFF), which normalises to
	// float64, so that arm is load-bearing rather than defensive; the []byte and
	// string arms cover a driver whose metadata was not usable.
	switch v := r[0][magicVariableName].(type) {
	case int64:
		count = v
	case int:
		count = int64(v)
	case float64:
		count = int64(v)
	case []byte:
		count, err = strconv.ParseInt(strings.TrimSpace(string(v)), 10, 64)
		if err != nil {
			return 0, errors.Wrapf(err, "unexpected count result %q", string(v))
		}
	case string:
		count, err = strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return 0, errors.Wrapf(err, "unexpected count result %q", v)
		}
	default:
		return 0, errors.Errorf("unexpected type for count result: %T", v)
	}
	return count, nil
}

func TxQueryRows(
	ctx context.Context,
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

	// Convert named parameters to positional (? placeholders) for the engines
	// that consume sqlx's output. Oracle is skipped deliberately: its branch
	// below re-derives from the ORIGINAL SQL because go-ora binds by name, so
	// running this could only ever fail -- and it does. A SQLExpression reaches
	// Oracle with its colons intact (they must not be doubled, since nothing
	// un-doubles them there), and sqlx reads ':30' inside a literal as a named
	// parameter it cannot find.
	if dbt != base.DXDatabaseTypeOracle {
		modifiedSQL, args, err = sqlx.Named(sqlStatement, sqlArguments)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to convert named parameters")
		}
	}

	// Then handle databases-specific parameter styles
	switch dbt {
	case base.DXDatabaseTypePostgreSQL:
		// PostgreSQL uses $1, $2, etc.
		modifiedSQL = tx.Rebind(modifiedSQL)

	case base.DXDatabaseTypeOracle:
		// go-ora binds by :name — start from the ORIGINAL SQL (sqlx.Named above
		// rewrote :name to `?`), then rewrite each bind to a reserved-word-safe
		// ":p_<name>" paired with matching sql.Named args (ORA-01745 otherwise).
		modifiedSQL, args = OracleSafeBindNames(sqlStatement, sqlArguments)

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
	return RawTxQueryRows(ctx, tx, fieldTypeMapping, modifiedSQL, args)
}
