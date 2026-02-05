package query_builder

import (
	"fmt"
	"slices"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/databases/db/query/builder"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// TableInsertQueryBuilder wraps builder.InsertQueryBuilder with table-specific field validation
type TableInsertQueryBuilder struct {
	*builder.InsertQueryBuilder
	TableInterface TableInterface
}

// NewTableInsertQueryBuilder creates a new TableInsertQueryBuilder with table interface for validation
func NewTableInsertQueryBuilder(dbType base.DXDatabaseType, tableInterface TableInterface) *TableInsertQueryBuilder {
	return &TableInsertQueryBuilder{
		InsertQueryBuilder: builder.NewInsertQueryBuilder(dbType),
		TableInterface:     tableInterface,
	}
}

// NewTableInsertQueryBuilderWithSource creates a new TableInsertQueryBuilder with table name
func NewTableInsertQueryBuilderWithSource(dbType base.DXDatabaseType, tableName string, tableInterface TableInterface) *TableInsertQueryBuilder {
	return &TableInsertQueryBuilder{
		InsertQueryBuilder: builder.NewInsertQueryBuilderWithSource(dbType, tableName),
		TableInterface:     tableInterface,
	}
}

// CheckFieldExist validates field exists and sets error if not
func (tqb *TableInsertQueryBuilder) CheckFieldExist(fieldName string) *TableInsertQueryBuilder {
	if tqb.TableInterface == nil {
		tqb.Error = errors.New(fmt.Sprintf("SHOULD_NOT_HAPPEN:TABLE_NOT_SET:%s", fieldName))
		return tqb
	}
	searchFieldNames := tqb.TableInterface.GetSearchTextFieldNames()
	if !slices.Contains(searchFieldNames, fieldName) {
		tqb.Error = errors.New(fmt.Sprintf("SHOULD_NOT_HAPPEN:INVALID_FIELD_NAME_IN_TABLE:%s:%s", tqb.TableInterface.GetFullTableName(), fieldName))
		return tqb
	}
	return tqb
}

// Set adds a field-value pair with field validation
func (tqb *TableInsertQueryBuilder) Set(fieldName string, value any) *TableInsertQueryBuilder {
	tqb.CheckFieldExist(fieldName)
	if tqb.Error != nil {
		return tqb
	}
	tqb.InsertQueryBuilder.Set(fieldName, value)
	return tqb
}

// SetAll adds multiple field-value pairs with field validation
func (tqb *TableInsertQueryBuilder) SetAll(fields utils.JSON) *TableInsertQueryBuilder {
	for k, v := range fields {
		tqb.Set(k, v)
		if tqb.Error != nil {
			return tqb
		}
	}
	return tqb
}

// Into sets the table name for INSERT
func (tqb *TableInsertQueryBuilder) Into(tableName string) *TableInsertQueryBuilder {
	tqb.InsertQueryBuilder.Into(tableName)
	return tqb
}

// Returning specifies fields for RETURNING clause
func (tqb *TableInsertQueryBuilder) Returning(fields ...string) *TableInsertQueryBuilder {
	tqb.InsertQueryBuilder.Returning(fields...)
	return tqb
}
