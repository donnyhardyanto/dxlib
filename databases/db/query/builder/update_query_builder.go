package builder

import (
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	utils2 "github.com/donnyhardyanto/dxlib/databases/db/query/utils"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// UpdateQueryBuilder builds UPDATE SQL statements with fluent API
type UpdateQueryBuilder struct {
	SourceName string              // Table name for UPDATE
	DbType     base.DXDatabaseType // Database type for syntax differences
	Error      error               // Accumulated error
	SetFields  utils.JSON          // Fields to update (column -> value)
	Conditions []string            // WHERE conditions
	Args       utils.JSON          // WHERE clause arguments
	OutFields  []string            // RETURNING/OUTPUT fields
}

// NewUpdateQueryBuilder creates a new UpdateQueryBuilder
func NewUpdateQueryBuilder(dbType base.DXDatabaseType) *UpdateQueryBuilder {
	return &UpdateQueryBuilder{
		DbType:     dbType,
		SetFields:  utils.JSON{},
		Conditions: []string{},
		Args:       utils.JSON{},
	}
}

// NewUpdateQueryBuilderWithSource creates a new UpdateQueryBuilder with table name
func NewUpdateQueryBuilderWithSource(dbType base.DXDatabaseType, tableName string) *UpdateQueryBuilder {
	return &UpdateQueryBuilder{
		SourceName: tableName,
		DbType:     dbType,
		SetFields:  utils.JSON{},
		Conditions: []string{},
		Args:       utils.JSON{},
	}
}

// Table sets the table name for UPDATE
func (qb *UpdateQueryBuilder) Table(tableName string) *UpdateQueryBuilder {
	if qb.Error != nil {
		return qb
	}
	qb.SourceName = tableName
	return qb
}

// Set adds a field-value pair to update
func (qb *UpdateQueryBuilder) Set(fieldName string, value any) *UpdateQueryBuilder {
	if qb.Error != nil {
		return qb
	}
	if !utils2.IsValidIdentifier(fieldName) {
		qb.Error = errors.Errorf("INVALID_UPDATE_FIELD_NAME:%s", fieldName)
		return qb
	}
	qb.SetFields[fieldName] = value
	return qb
}

// SetAll adds multiple field-value pairs to update
func (qb *UpdateQueryBuilder) SetAll(fields utils.JSON) *UpdateQueryBuilder {
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

// And adds a raw WHERE condition
func (qb *UpdateQueryBuilder) And(condition string) *UpdateQueryBuilder {
	if qb.Error != nil {
		return qb
	}
	if condition != "" {
		qb.Conditions = append(qb.Conditions, condition)
	}
	return qb
}

// Where adds a field = value WHERE condition
func (qb *UpdateQueryBuilder) Where(fieldName string, value any) *UpdateQueryBuilder {
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
func (qb *UpdateQueryBuilder) Returning(fields ...string) *UpdateQueryBuilder {
	if qb.Error != nil {
		return qb
	}
	qb.OutFields = append(qb.OutFields, fields...)
	return qb
}

// QuoteIdentifier quotes a SQL identifier based on database type
func (qb *UpdateQueryBuilder) QuoteIdentifier(identifier string) string {
	return utils2.QuoteIdentifierByDbType(qb.DbType, identifier)
}

// BuildWhereClause returns the WHERE clause string and Args
func (qb *UpdateQueryBuilder) BuildWhereClause() (string, utils.JSON, error) {
	if qb.Error != nil {
		return "", nil, qb.Error
	}
	if len(qb.Conditions) == 0 {
		return "", qb.Args, nil
	}
	return strings.Join(qb.Conditions, " AND "), qb.Args, nil
}

// BuildReturningClause returns the RETURNING clause string for PostgreSQL/Oracle
func (qb *UpdateQueryBuilder) BuildReturningClause() (string, error) {
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
// prefix should be "INSERTED" for new values or "DELETED" for old values
func (qb *UpdateQueryBuilder) BuildOutputClause(prefix string) (string, error) {
	if qb.Error != nil {
		return "", qb.Error
	}

	if len(qb.OutFields) == 0 {
		return "", nil
	}

	// Validate prefix
	upperPrefix := strings.ToUpper(prefix)
	if upperPrefix != "INSERTED" && upperPrefix != "DELETED" {
		return "", errors.Errorf("INVALID_OUTPUT_PREFIX:%s", prefix)
	}

	var quotedFields []string
	for _, field := range qb.OutFields {
		quotedFields = append(quotedFields, upperPrefix+"."+qb.QuoteIdentifier(field))
	}

	return "OUTPUT " + strings.Join(quotedFields, ", "), nil
}
