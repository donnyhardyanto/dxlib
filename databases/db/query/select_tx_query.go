package query

import (
	"strconv"
	"strings"

	"github.com/donnyhardyanto/dxlib/databases"
	databaseDb "github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/databases/db/query/builder"
	"github.com/donnyhardyanto/dxlib/databases/db/query/named"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// TxSelectWithSelectQueryBuilder2 executes a query within a transaction using SelectQueryBuilder and returns all matching rows.
// Builds SELECT query from SelectQueryBuilder (SourceName, OutFields, WHERE, JOIN, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET) and calls TxNamedQueryRows2.
func TxSelectWithSelectQueryBuilder2(dtx *databases.DXDatabaseTx, qb *builder.SelectQueryBuilder) (rowsInfo *databaseDb.DXDatabaseTableRowsInfo, r []utils.JSON, err error) {

	// Check for errors accumulated in SelectQueryBuilder
	if qb.Error != nil {
		return nil, nil, qb.Error
	}

	// Validate SourceName is set
	if qb.SourceName == "" {
		return nil, nil, errors.New("QUERY_BUILDER_SOURCE_NAME_NOT_SET")
	}

	// Build SELECT fields from qb.OutFields, default to "*"
	selectFieldsPart := "*"
	if len(qb.OutFields) > 0 {
		selectFieldsPart = strings.Join(qb.OutFields, ", ")
	}

	driverName := dtx.Tx.DriverName()

	// Build WHERE clause
	whereClause, args, err := qb.Build()
	if err != nil {
		return nil, nil, err
	}

	// Build JOIN clause
	joinClause, err := qb.BuildJoinClause()
	if err != nil {
		return nil, nil, err
	}

	// Build GROUP BY clause
	groupByClause, err := qb.BuildGroupByClause()
	if err != nil {
		return nil, nil, err
	}

	// Build HAVING clause and merge args
	havingClause, havingArgs, err := qb.BuildHavingClause()
	if err != nil {
		return nil, nil, err
	}
	for k, v := range havingArgs {
		args[k] = v
	}

	// Build ORDER BY clause
	orderByClause, err := qb.BuildOrderByClause()
	if err != nil {
		return nil, nil, err
	}

	// Build full query
	query := "SELECT " + selectFieldsPart + " FROM " + qb.SourceName

	if joinClause != "" {
		query += " " + joinClause
	}
	if whereClause != "" {
		query += " WHERE " + whereClause
	}
	if groupByClause != "" {
		query += " " + groupByClause
	}
	if havingClause != "" {
		query += " " + havingClause
	}
	if orderByClause != "" {
		query += " ORDER BY " + orderByClause
	}

	// Add LIMIT/OFFSET clause if specified (database-specific)
	if qb.LimitValue > 0 || qb.OffsetValue > 0 {
		switch driverName {
		case "postgres", "mysql", "mariadb":
			if qb.LimitValue > 0 {
				query += " LIMIT " + strconv.FormatInt(qb.LimitValue, 10)
			}
			if qb.OffsetValue > 0 {
				query += " OFFSET " + strconv.FormatInt(qb.OffsetValue, 10)
			}
		case "sqlserver":
			// SQL Server requires ORDER BY for OFFSET-FETCH
			if orderByClause == "" {
				query += " ORDER BY (SELECT NULL)"
			}
			offset := qb.OffsetValue
			if offset < 0 {
				offset = 0
			}
			query += " OFFSET " + strconv.FormatInt(offset, 10) + " ROWS"
			if qb.LimitValue > 0 {
				query += " FETCH NEXT " + strconv.FormatInt(qb.LimitValue, 10) + " ROWS ONLY"
			}
		case "oracle":
			if qb.OffsetValue > 0 {
				query += " OFFSET " + strconv.FormatInt(qb.OffsetValue, 10) + " ROWS"
			}
			if qb.LimitValue > 0 {
				query += " FETCH NEXT " + strconv.FormatInt(qb.LimitValue, 10) + " ROWS ONLY"
			}
		}
	}

	// Add FOR UPDATE if specified
	if qb.ForUpdatePart != nil {
		if s, ok := qb.ForUpdatePart.(string); ok && s != "" {
			query += " " + s
		}
	}

	return named.TxNamedQueryRows2(dtx, query, args)
}

// TxShouldSelectWithSelectQueryBuilder2 executes a query within a transaction using SelectQueryBuilder and returns all matching rows,
// erroring if no rows found.
func TxShouldSelectWithSelectQueryBuilder2(dtx *databases.DXDatabaseTx, qb *builder.SelectQueryBuilder) (rowsInfo *databaseDb.DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	rowsInfo, r, err = TxSelectWithSelectQueryBuilder2(dtx, qb)
	if err != nil {
		return rowsInfo, r, err
	}
	if len(r) == 0 {
		err = errors.New("ROWS_MUST_EXIST:TX_QUERY_BUILDER_QUERY")
		return rowsInfo, r, err
	}
	return rowsInfo, r, nil
}

// TxSelectOneWithSelectQueryBuilder2 executes a query within a transaction using SelectQueryBuilder and returns a single row.
// Sets LimitValue to 1 automatically.
func TxSelectOneWithSelectQueryBuilder2(dtx *databases.DXDatabaseTx, qb *builder.SelectQueryBuilder) (rowsInfo *databaseDb.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	qb.LimitValue = 1
	rowsInfo, rows, err := TxSelectWithSelectQueryBuilder2(dtx, qb)
	if err != nil {
		return rowsInfo, nil, err
	}
	if len(rows) == 0 {
		return rowsInfo, nil, nil
	}
	return rowsInfo, rows[0], nil
}

// TxShouldSelectOneWithSelectQueryBuilder2 executes a query within a transaction using SelectQueryBuilder and returns a single row,
// erroring if no row found.
func TxShouldSelectOneWithSelectQueryBuilder2(dtx *databases.DXDatabaseTx, qb *builder.SelectQueryBuilder) (rowsInfo *databaseDb.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = TxSelectOneWithSelectQueryBuilder2(dtx, qb)
	if err != nil {
		return rowsInfo, r, err
	}
	if r == nil {
		err = errors.New("ROW_MUST_EXIST:TX_QUERY_BUILDER_QUERY")
		return rowsInfo, r, err
	}
	return rowsInfo, r, nil
}

// TxCountWithSelectQueryBuilder2 executes a COUNT query within a transaction using SelectQueryBuilder and returns the count.
// Builds SELECT COUNT(*) query from SelectQueryBuilder (SourceName, WHERE, JOIN, GROUP BY, HAVING).
func TxCountWithSelectQueryBuilder2(dtx *databases.DXDatabaseTx, qb *builder.SelectQueryBuilder) (count int64, err error) {

	// Check for errors accumulated in SelectQueryBuilder
	if qb.Error != nil {
		return 0, qb.Error
	}

	// Validate SourceName is set
	if qb.SourceName == "" {
		return 0, errors.New("QUERY_BUILDER_SOURCE_NAME_NOT_SET")
	}

	// Build WHERE clause
	whereClause, args, err := qb.Build()
	if err != nil {
		return 0, err
	}

	// Build JOIN clause
	joinClause, err := qb.BuildJoinClause()
	if err != nil {
		return 0, err
	}

	// Build GROUP BY clause
	groupByClause, err := qb.BuildGroupByClause()
	if err != nil {
		return 0, err
	}

	// Build HAVING clause and merge args
	havingClause, havingArgs, err := qb.BuildHavingClause()
	if err != nil {
		return 0, err
	}
	for k, v := range havingArgs {
		args[k] = v
	}

	// Build full COUNT query
	query := "SELECT COUNT(*) AS count FROM " + qb.SourceName

	if joinClause != "" {
		query += " " + joinClause
	}
	if whereClause != "" {
		query += " WHERE " + whereClause
	}
	if groupByClause != "" {
		query += " " + groupByClause
	}
	if havingClause != "" {
		query += " " + havingClause
	}

	_, row, err := named.TxNamedQueryRow2(dtx, query, args)
	if err != nil {
		return 0, err
	}
	if row == nil {
		return 0, nil
	}

	// Extract count from result
	countVal, ok := row["count"]
	if !ok {
		return 0, errors.New("COUNT_FIELD_NOT_FOUND_IN_RESULT")
	}

	switch v := countVal.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	default:
		return 0, errors.Errorf("UNEXPECTED_COUNT_TYPE:%T", countVal)
	}
}
