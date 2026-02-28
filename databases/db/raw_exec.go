package db

import (
	"context"
	"database/sql"
	"time"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/core"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	dxlibOtel "github.com/donnyhardyanto/dxlib/otel"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

func dbOtelExecStart(ctx context.Context, opName string) (context.Context, func(error)) {
	if !core.IsOtelEnabled {
		return ctx, func(error) {}
	}
	var span trace.Span
	ctx, span = otel.Tracer("dxlib.db").Start(ctx, "db."+opName)
	start := time.Now()
	attrs := metric.WithAttributes(attribute.String("db.system", "postgresql"), attribute.String("db.operation", opName))
	return ctx, func(err error) {
		dxlibOtel.DBQueryDuration.Record(ctx, time.Since(start).Seconds(), attrs)
		dxlibOtel.DBQueryCount.Add(ctx, 1, attrs)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

func RawExec(ctx context.Context, db *sqlx.DB, query string, arg []any) (result sql.Result, err error) {
	ctx, endOtel := dbOtelExecStart(ctx, "EXEC")
	defer func() { endOtel(err) }()

	result, err = db.ExecContext(ctx, query, arg...)
	if err != nil {
		return nil, errors.Wrapf(err, "DB_EXEC_ERROR sql=%s", query)
	}

	log.Log.Debugf("DB_RAW_EXEC sql=%s", query)
	return result, nil
}

func RawTxExec(ctx context.Context, tx *sqlx.Tx, query string, arg []any) (result sql.Result, err error) {
	ctx, endOtel := dbOtelExecStart(ctx, "TX_EXEC")
	defer func() { endOtel(err) }()

	result, err = tx.ExecContext(ctx, query, arg...)
	if err != nil {
		return nil, errors.Wrapf(err, "DB_TX_EXEC_ERROR sql=%s", query)
	}

	log.Log.Debugf("DB_RAW_TX_EXEC sql=%s", query)
	return result, nil
}

func Exec(ctx context.Context, db *sqlx.DB, sqlStatement string, sqlArguments utils.JSON) (result sql.Result, err error) {
	var (
		modifiedSQL string
		args        []interface{}
	)

	dbt := base.StringToDXDatabaseType(db.DriverName())

	// First, convert named parameters to positional parameters (? placeholders)
	modifiedSQL, args, err = sqlx.Named(sqlStatement, sqlArguments)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert named parameters")
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
			return nil, errors.Wrap(err, "failed to convert to MariaDB parameter format")
		}
		modifiedSQL = db.Rebind(modifiedSQL)

	case base.DXDatabaseTypeSQLServer:
		// SQL Server uses @p1, @p2, etc.
		modifiedSQL = db.Rebind(modifiedSQL)

	default:
		return nil, errors.Errorf("unsupported databases driver: %s", db.DriverName())
	}

	// Call the RawExec function with the modified SQL and arguments
	return RawExec(ctx, db, modifiedSQL, args)
}

func TxExec(
	ctx context.Context,
	tx *sqlx.Tx,
	sqlStatement string,
	sqlArguments utils.JSON,
) (result sql.Result, err error) {
	var (
		modifiedSQL string
		args        []interface{}
	)

	dbt := base.StringToDXDatabaseType(tx.DriverName())

	// First, convert named parameters to positional parameters (? placeholders)
	modifiedSQL, args, err = sqlx.Named(sqlStatement, sqlArguments)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert named parameters")
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
			return nil, errors.Wrap(err, "failed to convert to MariaDB parameter format")
		}
		modifiedSQL = tx.Rebind(modifiedSQL)

	case base.DXDatabaseTypeSQLServer:
		// SQL Server uses @p1, @p2, etc.
		modifiedSQL = tx.Rebind(modifiedSQL)

	default:
		return nil, errors.Errorf("unsupported databases driver: %s", tx.DriverName())
	}

	// Call the RawTxExec function with the modified SQL and arguments
	return RawTxExec(ctx, tx, modifiedSQL, args)
}
