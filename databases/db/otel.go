package db

import (
	"context"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/core"
	dxlibOtel "github.com/donnyhardyanto/dxlib/otel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const dbOtelMaxStatementLen = 2048

var tableNameRegexp = regexp.MustCompile(`(?i)(?:FROM|INTO|UPDATE|JOIN)\s+(["\w]+\.["\w]+|["\w]+)`)

func dbOtelParseOperation(query string) string {
	q := strings.TrimSpace(query)
	if len(q) < 6 {
		return "UNKNOWN"
	}
	switch strings.ToUpper(q[:6]) {
	case "SELECT":
		return "SELECT"
	case "INSERT":
		return "INSERT"
	case "UPDATE":
		return "UPDATE"
	case "DELETE":
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

func dbOtelParseTableName(query string) string {
	matches := tableNameRegexp.FindStringSubmatch(query)
	if len(matches) >= 2 {
		return strings.ReplaceAll(matches[1], `"`, "")
	}
	return ""
}

func dbOtelTruncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

func dbOtelCallerInfo(skip int) (string, string, int) {
	pc, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "", "", 0
	}
	fn := runtime.FuncForPC(pc)
	funcName := ""
	if fn != nil {
		funcName = fn.Name()
		if idx := strings.LastIndex(funcName, "."); idx >= 0 {
			funcName = funcName[idx+1:]
		}
	}
	if idx := strings.LastIndex(file, "/"); idx >= 0 {
		file = file[idx+1:]
	}
	return funcName, file, line
}

// DbOtelStart creates a span with rich DB attributes. Returns the modified context
// and a finish function. The finish function accepts an error and an optional rows-affected count.
// callerSkip controls how many stack frames to skip for code.* attributes (use 2 from raw functions, 3+ from wrappers).
func DbOtelStart(ctx context.Context, spanName string, query string, callerSkip int) (context.Context, func(err error, rowsAffected int64)) {
	if !core.IsOtelEnabled {
		return ctx, func(error, int64) {}
	}

	op := dbOtelParseOperation(query)
	tableName := dbOtelParseTableName(query)
	funcName, fileName, lineNo := dbOtelCallerInfo(callerSkip)

	spanAttrs := []attribute.KeyValue{
		attribute.String("db.system", "postgresql"),
		attribute.String("db.operation", op),
		attribute.String("db.statement", dbOtelTruncate(query, dbOtelMaxStatementLen)),
		attribute.String("peer.service", "postgresql"),
	}
	if tableName != "" {
		spanAttrs = append(spanAttrs, attribute.String("db.sql.table", tableName))
	}
	if funcName != "" {
		spanAttrs = append(spanAttrs, attribute.String("code.function", funcName))
	}
	if fileName != "" {
		spanAttrs = append(spanAttrs, attribute.String("code.filepath", fileName))
		spanAttrs = append(spanAttrs, attribute.Int("code.lineno", lineNo))
	}

	var span trace.Span
	ctx, span = otel.Tracer("dxlib.db").Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(spanAttrs...),
	)
	start := time.Now()
	metricAttrs := metric.WithAttributes(
		attribute.String("db.system", "postgresql"),
		attribute.String("db.operation", op),
	)

	return ctx, func(err error, rowsAffected int64) {
		dxlibOtel.DBQueryDuration.Record(ctx, time.Since(start).Seconds(), metricAttrs)
		dxlibOtel.DBQueryCount.Add(ctx, 1, metricAttrs)
		if rowsAffected >= 0 {
			span.SetAttributes(attribute.Int64("db.rows_affected", rowsAffected))
		}
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}
