package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/core"
	"github.com/donnyhardyanto/dxlib/errors"
	dxlibOtel "github.com/donnyhardyanto/dxlib/otel"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

func RawQueryRows(ctx context.Context, db *sqlx.DB, fieldTypeMapping DXDatabaseTableFieldTypeMapping, query string, arg []any) (rowsInfo *DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	if core.IsOtelEnabled {
		var span trace.Span
		ctx, span = otel.Tracer("dxlib.db").Start(ctx, "db.SELECT")
		start := time.Now()
		defer func() {
			attrs := metric.WithAttributes(attribute.String("db.system", "postgresql"), attribute.String("db.operation", "SELECT"))
			dxlibOtel.DBQueryDuration.Record(ctx, time.Since(start).Seconds(), attrs)
			dxlibOtel.DBQueryCount.Add(ctx, 1, attrs)
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
			}
			span.End()
		}()
	}

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

func RawTxQueryRows(ctx context.Context, tx *sqlx.Tx, fieldTypeMapping DXDatabaseTableFieldTypeMapping, query string, arg []any) (rowsInfo *DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	if core.IsOtelEnabled {
		var span trace.Span
		ctx, span = otel.Tracer("dxlib.db").Start(ctx, "db.TX_SELECT")
		start := time.Now()
		defer func() {
			attrs := metric.WithAttributes(attribute.String("db.system", "postgresql"), attribute.String("db.operation", "TX_SELECT"))
			dxlibOtel.DBQueryDuration.Record(ctx, time.Since(start).Seconds(), attrs)
			dxlibOtel.DBQueryCount.Add(ctx, 1, attrs)
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
			}
			span.End()
		}()
	}

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
	_, r, err := RawQueryRows(ctx, db, nil, modifiedSQL, args)
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
	return RawTxQueryRows(ctx, tx, fieldTypeMapping, modifiedSQL, args)
}
