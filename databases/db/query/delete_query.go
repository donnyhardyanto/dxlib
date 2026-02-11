package query

import (
	"database/sql"
	"strings"

	"github.com/donnyhardyanto/dxlib/databases/db/query/builder"
	"github.com/donnyhardyanto/dxlib/databases/db/query/named"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
)

// buildDeleteSQL builds DELETE SQL and args from DeleteQueryBuilder.
func buildDeleteSQL(driverName string, qb *builder.DeleteQueryBuilder) (string, utils.JSON, error) {
	if qb.SourceName == "" {
		return "", nil, errors.New("QUERY_BUILDER_SOURCE_NAME_NOT_SET")
	}

	// Build WHERE clause
	whereClause, _, err := qb.BuildWhereClause()
	if err != nil {
		return "", nil, err
	}

	args := utils.JSON{}
	for k, v := range qb.Args {
		args[k] = v
	}

	// Build: DELETE FROM table WHERE cond
	query := "DELETE FROM" + " " + qb.SourceName

	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	// Handle RETURNING/OUTPUT
	if len(qb.OutFields) > 0 {
		switch driverName {
		case "postgres", "mariadb":
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
			// SQL Server: DELETE FROM table OUTPUT DELETED.field WHERE ...
			// Need to insert OUTPUT between FROM table and WHERE
			whereIdx := strings.Index(query, " WHERE ")
			if whereIdx >= 0 {
				query = query[:whereIdx] + " " + outputClause + query[whereIdx:]
			} else {
				query += " " + outputClause
			}
		}
	}

	return query, args, nil
}

// DeleteWithDeleteQueryBuilder2 executes a DELETE using DeleteQueryBuilder.
// If RETURNING fields are specified, returns the deleted rows.
// Otherwise, returns sql.Result info.
func DeleteWithDeleteQueryBuilder2(db *sqlx.DB, qb *builder.DeleteQueryBuilder) (result sql.Result, returningRows []utils.JSON, err error) {
	if qb.Error != nil {
		return nil, nil, qb.Error
	}

	driverName := db.DriverName()
	query, args, err := buildDeleteSQL(driverName, qb)
	if err != nil {
		return nil, nil, err
	}

	if len(qb.OutFields) > 0 {
		_, rows, err := named.NamedQueryRows2(db, query, args, nil)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "DELETE_WITH_RETURNING_ERROR")
		}
		return nil, rows, nil
	}

	result, err = named.NamedExec2(db, query, args)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "DELETE_ERROR")
	}
	return result, nil, nil
}
