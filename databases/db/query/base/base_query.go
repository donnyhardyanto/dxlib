package base

import (
	"context"
	"time"

	"github.com/donnyhardyanto/dxlib/core"
	databaseDb "github.com/donnyhardyanto/dxlib/databases/db"
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

// BaseQueryRows2 executes a named query and returns all matching rows
// It supports both named parameters (map/struct) and positional parameters (slice)
// If fieldTypeMapping is provided, applies type conversion to the results
func BaseQueryRows2(ctx context.Context, db *sqlx.DB, query string, arg any, fieldTypeMapping databaseDb.DXDatabaseTableFieldTypeMapping) (rowsInfo *databaseDb.DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
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
	if arg == nil {
		arg = utils.JSON{}
	}

	//dbt := base.StringToDXDatabaseType(db.DriverName())
	/*	err = databaseDb.CheckAll(dbt, query, arg)
		if err != nil {
			return nil, nil, errors.Errorf("SQL_INJECTION_DETECTED:QUERY_VALIDATION_FAILED: %+v=%s +%v", err, query, arg)
		}
	*/
	// Check if arg is a slice (positional parameters) or map/struct (named parameters)
	var rows *sqlx.Rows
	switch v := arg.(type) {
	case []any:
		// Positional parameters - use QueryxContext
		if len(v) == 0 {
			rows, err = db.QueryxContext(ctx, query)
		} else {
			rows, err = db.QueryxContext(ctx, query, v...)
		}
	default:
		// Named parameters - use NamedQueryContext
		rows, err = sqlx.NamedQueryContext(ctx, db, query, arg)
	}
	if err != nil {
		return nil, nil, errors.Wrapf(err, "NAMED_QUERY_ROWS_ERROR:QUERY=%s", query)
	}
	defer func() {
		_ = rows.Close()
	}()

	rowsInfo = &databaseDb.DXDatabaseTableRowsInfo{}
	rowsInfo.Columns, err = rows.Columns()
	if err != nil {
		return nil, r, errors.Wrapf(err, "NAMED_QUERY_ROWS_COLUMNS_ERROR:QUERY=%s", query)
	}

	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "NAMED_QUERY_ROWS_SCAN_ERROR:QUERY=%s", query)
		}
		r = append(r, rowJSON)
	}

	// Apply field type conversion if fieldTypeMapping is provided
	if fieldTypeMapping != nil && len(fieldTypeMapping) > 0 {
		for i, row := range r {
			convertedRow, err := databaseDb.DeformatKeys(row, db.DriverName(), fieldTypeMapping)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "FIELD_TYPE_CONVERSION_ERROR:QUERY=%s", query)
			}
			r[i] = convertedRow
		}
	}

	return rowsInfo, r, nil
}

// BaseQueryRow2 executes a named query and returns a single row
func BaseQueryRow2(ctx context.Context, db *sqlx.DB, query string, arg any, fieldTypeMapping databaseDb.DXDatabaseTableFieldTypeMapping) (rowsInfo *databaseDb.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, rows, err := BaseQueryRows2(ctx, db, query, arg, fieldTypeMapping)
	if err != nil {
		return rowsInfo, nil, err
	}
	if len(rows) == 0 {
		return rowsInfo, nil, nil
	}
	return rowsInfo, rows[0], nil
}
