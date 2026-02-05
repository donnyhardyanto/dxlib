package builder

import (
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	utils2 "github.com/donnyhardyanto/dxlib/databases/db/query/utils"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// InsertQueryBuilder builds INSERT SQL statements with fluent API
type InsertQueryBuilder struct {
	SourceName string              // Table name for INSERT INTO
	DbType     base.DXDatabaseType // Database type for syntax differences
	Error      error               // Accumulated error
	SetFields  utils.JSON          // Fields to insert (column -> value)
	OutFields  []string            // RETURNING/OUTPUT fields
}

// NewInsertQueryBuilder creates a new InsertQueryBuilder
func NewInsertQueryBuilder(dbType base.DXDatabaseType) *InsertQueryBuilder {
	return &InsertQueryBuilder{
		DbType:    dbType,
		SetFields: utils.JSON{},
	}
}

// NewInsertQueryBuilderWithSource creates a new InsertQueryBuilder with table name
func NewInsertQueryBuilderWithSource(dbType base.DXDatabaseType, tableName string) *InsertQueryBuilder {
	return &InsertQueryBuilder{
		SourceName: tableName,
		DbType:     dbType,
		SetFields:  utils.JSON{},
	}
}

// Into sets the table name for INSERT
func (qb *InsertQueryBuilder) Into(tableName string) *InsertQueryBuilder {
	if qb.Error != nil {
		return qb
	}
	qb.SourceName = tableName
	return qb
}

// Set adds a field-value pair to insert
func (qb *InsertQueryBuilder) Set(fieldName string, value any) *InsertQueryBuilder {
	if qb.Error != nil {
		return qb
	}
	if !utils2.IsValidIdentifier(fieldName) {
		qb.Error = errors.Errorf("INVALID_INSERT_FIELD_NAME:%s", fieldName)
		return qb
	}
	qb.SetFields[fieldName] = value
	return qb
}

// SetAll adds multiple field-value pairs to insert
func (qb *InsertQueryBuilder) SetAll(fields utils.JSON) *InsertQueryBuilder {
	if qb.Error != nil {
		return qb
	}
	for k, v := range fields {
		qb.Set(k, v)
		if qb.Error != nil {
			return qb
		}
	}
	return qb
}

// Returning specifies fields for RETURNING clause
func (qb *InsertQueryBuilder) Returning(fields ...string) *InsertQueryBuilder {
	if qb.Error != nil {
		return qb
	}
	qb.OutFields = append(qb.OutFields, fields...)
	return qb
}

// QuoteIdentifier quotes a SQL identifier based on database type
func (qb *InsertQueryBuilder) QuoteIdentifier(identifier string) string {
	return utils2.QuoteIdentifierByDbType(qb.DbType, identifier)
}

// BuildReturningClause returns the RETURNING clause string for PostgreSQL/Oracle
func (qb *InsertQueryBuilder) BuildReturningClause() (string, error) {
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
func (qb *InsertQueryBuilder) BuildOutputClause() (string, error) {
	if qb.Error != nil {
		return "", qb.Error
	}

	if len(qb.OutFields) == 0 {
		return "", nil
	}

	var quotedFields []string
	for _, field := range qb.OutFields {
		quotedFields = append(quotedFields, "INSERTED."+qb.QuoteIdentifier(field))
	}

	return "OUTPUT " + strings.Join(quotedFields, ", "), nil
}
