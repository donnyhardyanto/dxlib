package builder

import (
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	utils2 "github.com/donnyhardyanto/dxlib/databases/db/query/utils"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DeleteQueryBuilder builds DELETE SQL statements with fluent API
type DeleteQueryBuilder struct {
	SourceName string              // Table name for DELETE FROM
	DbType     base.DXDatabaseType // Database type for syntax differences
	Error      error               // Accumulated error
	Conditions []string            // WHERE conditions
	Args       utils.JSON          // WHERE clause arguments
	OutFields  []string            // RETURNING/OUTPUT fields
}

// NewDeleteQueryBuilder creates a new DeleteQueryBuilder
func NewDeleteQueryBuilder(dbType base.DXDatabaseType) *DeleteQueryBuilder {
	return &DeleteQueryBuilder{
		DbType:     dbType,
		Conditions: []string{},
		Args:       utils.JSON{},
	}
}

// NewDeleteQueryBuilderWithSource creates a new DeleteQueryBuilder with table name
func NewDeleteQueryBuilderWithSource(dbType base.DXDatabaseType, tableName string) *DeleteQueryBuilder {
	return &DeleteQueryBuilder{
		SourceName: tableName,
		DbType:     dbType,
		Conditions: []string{},
		Args:       utils.JSON{},
	}
}

// From sets the table name for DELETE
func (qb *DeleteQueryBuilder) From(tableName string) *DeleteQueryBuilder {
	if qb.Error != nil {
		return qb
	}
	qb.SourceName = tableName
	return qb
}

// And adds a raw WHERE condition
func (qb *DeleteQueryBuilder) And(condition string) *DeleteQueryBuilder {
	if qb.Error != nil {
		return qb
	}
	if condition != "" {
		qb.Conditions = append(qb.Conditions, condition)
	}
	return qb
}

// Where adds a field = value WHERE condition
func (qb *DeleteQueryBuilder) Where(fieldName string, value any) *DeleteQueryBuilder {
	if qb.Error != nil {
		return qb
	}
	if !utils2.IsValidIdentifier(fieldName) {
		qb.Error = errors.Errorf("INVALID_WHERE_FIELD_NAME:%s", fieldName)
		return qb
	}
	paramName := "w_" + fieldName
	qb.Conditions = append(qb.Conditions, qb.QuoteIdentifier(fieldName)+" = :"+paramName)
	qb.Args[paramName] = value
	return qb
}

// Returning specifies fields for RETURNING clause
func (qb *DeleteQueryBuilder) Returning(fields ...string) *DeleteQueryBuilder {
	if qb.Error != nil {
		return qb
	}
	qb.OutFields = append(qb.OutFields, fields...)
	return qb
}

// QuoteIdentifier quotes a SQL identifier based on database type
func (qb *DeleteQueryBuilder) QuoteIdentifier(identifier string) string {
	return utils2.QuoteIdentifierByDbType(qb.DbType, identifier)
}

// BuildWhereClause returns the WHERE clause string and Args
func (qb *DeleteQueryBuilder) BuildWhereClause() (string, utils.JSON, error) {
	if qb.Error != nil {
		return "", nil, qb.Error
	}
	if len(qb.Conditions) == 0 {
		return "", qb.Args, nil
	}
	return strings.Join(qb.Conditions, " AND "), qb.Args, nil
}

// BuildReturningClause returns the RETURNING clause string for PostgreSQL/Oracle
func (qb *DeleteQueryBuilder) BuildReturningClause() (string, error) {
	if qb.Error != nil {
		return "", qb.Error
	}

	if len(qb.OutFields) == 0 {
		return "", nil
	}

	var quotedFields []string
	for _, field := range qb.OutFields {
		quotedFields = append(quotedFields, qb.QuoteIdentifier(field))
	}

	return "RETURNING " + strings.Join(quotedFields, ", "), nil
}

// BuildOutputClause returns the OUTPUT clause string for SQL Server
func (qb *DeleteQueryBuilder) BuildOutputClause() (string, error) {
	if qb.Error != nil {
		return "", qb.Error
	}

	if len(qb.OutFields) == 0 {
		return "", nil
	}

	var quotedFields []string
	for _, field := range qb.OutFields {
		quotedFields = append(quotedFields, "DELETED."+qb.QuoteIdentifier(field))
	}

	return "OUTPUT " + strings.Join(quotedFields, ", "), nil
}
