package query

import (
	"context"
	"database/sql"
	"strings"

	databaseDb "github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/databases/db/query/builder"
	"github.com/donnyhardyanto/dxlib/databases/db/query/named"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
)

// buildInsertSQL builds INSERT SQL and args from InsertQueryBuilder.
// Returns the SQL string and combined args map.
func buildInsertSQL(driverName string, qb *builder.InsertQueryBuilder) (string, utils.JSON, error) {
	if qb.SourceName == "" {
		return "", nil, errors.New("QUERY_BUILDER_SOURCE_NAME_NOT_SET")
	}
	if len(qb.SetFields) == 0 {
		return "", nil, errors.New("NO_FIELDS_TO_INSERT")
	}

	var columns []string
	var valuePlaceholders []string
	args := utils.JSON{}

	for fieldName, value := range qb.SetFields {
		columns = append(columns, fieldName)
		if expr, ok := value.(databaseDb.SQLExpression); ok {
			valuePlaceholders = append(valuePlaceholders, expr.String())
		} else {
			valuePlaceholders = append(valuePlaceholders, ":"+fieldName)
			args[fieldName] = value
		}
	}

	// Build: INSERT INTO table (col1, col2) VALUES (:col1, :col2)
	query := "INSERT INTO " + qb.SourceName +
		" (" + strings.Join(columns, ", ") + ")" +
		" VALUES (" + strings.Join(valuePlaceholders, ", ") + ")"

	// Handle RETURNING/OUTPUT
	if len(qb.OutFields) > 0 {
		switch driverName {
		case "postgres", "mysql":
			returningClause, err := qb.BuildReturningClause()
			if err != nil {
				return "", nil, err
			}
			query += " " + returningClause
		case "sqlserver", "mssql":
			outputClause, err := qb.BuildOutputClause()
			if err != nil {
				return "", nil, err
			}
			// SQL Server: INSERT INTO table (cols) OUTPUT INSERTED.field VALUES (vals)
			// Need to insert OUTPUT before VALUES
			valuesIdx := strings.LastIndex(query, " VALUES ")
			if valuesIdx >= 0 {
				query = query[:valuesIdx] + " " + outputClause + query[valuesIdx:]
			}
		case "oracle":
			// Oracle RETURNING INTO is handled in the execution function, not in SQL build
		default:
			return "", nil, errors.Errorf("RETURNING_NOT_SUPPORTED_FOR_DRIVER:%s", driverName)
		}
	}

	return query, args, nil
}

// InsertWithInsertQueryBuilder2 executes an INSERT using InsertQueryBuilder.
// If RETURNING fields are specified, returns the inserted row.
// Otherwise returns sql.Result info.
func InsertWithInsertQueryBuilder2(ctx context.Context, db *sqlx.DB, qb *builder.InsertQueryBuilder) (result sql.Result, returningRow utils.JSON, err error) {
	if qb.Error != nil {
		return nil, nil, qb.Error
	}

	driverName := db.DriverName()

	// Oracle: use RETURNING INTO with sql.Out binds
	if driverName == "oracle" && len(qb.OutFields) > 0 {
		return oracleInsertWithReturningInto(ctx, db, qb)
	}

	query, args, err := buildInsertSQL(driverName, qb)
	if err != nil {
		return nil, nil, err
	}

	if len(qb.OutFields) > 0 {
		_, row, err := named.NamedQueryRow2(ctx, db, query, args, nil)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "INSERT_WITH_RETURNING_ERROR")
		}
		return nil, row, nil
	}

	result, err = named.NamedExec2(ctx, db, query, args)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "INSERT_ERROR")
	}
	return result, nil, nil
}
