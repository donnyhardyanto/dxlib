package query

import (
	"database/sql"
	"strings"

	databaseDb "github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/databases/db/query/builder"
	"github.com/donnyhardyanto/dxlib/databases/db/query/named"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
)

// buildUpdateSQL builds UPDATE SQL and combined args from UpdateQueryBuilder.
func buildUpdateSQL(driverName string, qb *builder.UpdateQueryBuilder) (string, utils.JSON, error) {
	if qb.SourceName == "" {
		return "", nil, errors.New("QUERY_BUILDER_SOURCE_NAME_NOT_SET")
	}
	if len(qb.SetFields) == 0 {
		return "", nil, errors.New("NO_FIELDS_TO_UPDATE")
	}

	// Build SET clause
	var setParts []string
	args := utils.JSON{}
	for fieldName, value := range qb.SetFields {
		if expr, ok := value.(databaseDb.SQLExpression); ok {
			setParts = append(setParts, fieldName+"="+expr.String())
		} else {
			setParts = append(setParts, fieldName+"=:"+fieldName)
			args[fieldName] = value
		}
	}

	// Merge WHERE args
	for k, v := range qb.Args {
		args[k] = v
	}

	// Build WHERE clause
	whereClause, _, err := qb.BuildWhereClause()
	if err != nil {
		return "", nil, err
	}

	// Build: UPDATE table SET col1=:col1, col2=:col2 WHERE cond
	query := "UPDATE " + qb.SourceName + " SET " + strings.Join(setParts, ", ")

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
			outputClause, err := qb.BuildOutputClause("INSERTED")
			if err != nil {
				return "", nil, err
			}
			// SQL Server: UPDATE table SET ... OUTPUT INSERTED.field WHERE ...
			// Need to insert OUTPUT between SET and WHERE
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

// UpdateWithUpdateQueryBuilder2 executes an UPDATE using UpdateQueryBuilder.
// If RETURNING fields are specified, returns the affected rows.
// Otherwise, returns sql.Result info.
func UpdateWithUpdateQueryBuilder2(db *sqlx.DB, qb *builder.UpdateQueryBuilder) (result sql.Result, returningRows []utils.JSON, err error) {
	if qb.Error != nil {
		return nil, nil, qb.Error
	}

	driverName := db.DriverName()
	query, args, err := buildUpdateSQL(driverName, qb)
	if err != nil {
		return nil, nil, err
	}

	if len(qb.OutFields) > 0 {
		_, rows, err := named.NamedQueryRows2(db, query, args, nil)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "UPDATE_WITH_RETURNING_ERROR")
		}
		return nil, rows, nil
	}

	result, err = named.NamedExec2(db, query, args)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "UPDATE_ERROR")
	}
	return result, nil, nil
}
