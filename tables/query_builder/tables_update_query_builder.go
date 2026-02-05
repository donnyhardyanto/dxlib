package query_builder

import (
	"fmt"
	"slices"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/databases/db/query/builder"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// TableUpdateQueryBuilder wraps builder.UpdateQueryBuilder with table-specific field validation
type TableUpdateQueryBuilder struct {
	*builder.UpdateQueryBuilder
	TableInterface TableInterface
}

// NewTableUpdateQueryBuilder creates a new TableUpdateQueryBuilder with table interface for validation
func NewTableUpdateQueryBuilder(dbType base.DXDatabaseType, tableInterface TableInterface) *TableUpdateQueryBuilder {
	return &TableUpdateQueryBuilder{
		UpdateQueryBuilder: builder.NewUpdateQueryBuilder(dbType),
		TableInterface:     tableInterface,
	}
}

// NewTableUpdateQueryBuilderWithSource creates a new TableUpdateQueryBuilder with table name
func NewTableUpdateQueryBuilderWithSource(dbType base.DXDatabaseType, tableName string, tableInterface TableInterface) *TableUpdateQueryBuilder {
	return &TableUpdateQueryBuilder{
		UpdateQueryBuilder: builder.NewUpdateQueryBuilderWithSource(dbType, tableName),
		TableInterface:     tableInterface,
	}
}

// CheckFieldExist validates field exists and sets error if not
func (tqb *TableUpdateQueryBuilder) CheckFieldExist(fieldName string) *TableUpdateQueryBuilder {
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

// Set adds a field-value pair to update with field validation
func (tqb *TableUpdateQueryBuilder) Set(fieldName string, value any) *TableUpdateQueryBuilder {
	tqb.CheckFieldExist(fieldName)
	if tqb.Error != nil {
		return tqb
	}
	tqb.UpdateQueryBuilder.Set(fieldName, value)
	return tqb
}

// SetAll adds multiple field-value pairs to update with field validation
func (tqb *TableUpdateQueryBuilder) SetAll(fields utils.JSON) *TableUpdateQueryBuilder {
	for k, v := range fields {
		tqb.Set(k, v)
		if tqb.Error != nil {
			return tqb
		}
	}
	return tqb
}

// Eq adds WHERE field = value condition with field validation
func (tqb *TableUpdateQueryBuilder) Eq(fieldName string, value any) *TableUpdateQueryBuilder {
	tqb.CheckFieldExist(fieldName)
	if tqb.Error != nil {
		return tqb
	}
	tqb.UpdateQueryBuilder.Where(fieldName, value)
	return tqb
}

// Ne adds WHERE field != value condition with field validation
func (tqb *TableUpdateQueryBuilder) Ne(fieldName string, value any) *TableUpdateQueryBuilder {
	tqb.CheckFieldExist(fieldName)
	if tqb.Error != nil {
		return tqb
	}
	paramName := "w_" + fieldName
	tqb.Conditions = append(tqb.Conditions, tqb.QuoteIdentifier(fieldName)+" != :"+paramName)
	tqb.Args[paramName] = value
	return tqb
}

// NotDeleted adds is_deleted = false WHERE condition (database-aware)
func (tqb *TableUpdateQueryBuilder) NotDeleted() *TableUpdateQueryBuilder {
	switch tqb.DbType {
	case base.DXDatabaseTypeSQLServer:
		tqb.Conditions = append(tqb.Conditions, "is_deleted = 0")
	default:
		tqb.Conditions = append(tqb.Conditions, "is_deleted = false")
	}
	return tqb
}

// Table sets the table name for UPDATE
func (tqb *TableUpdateQueryBuilder) Table(tableName string) *TableUpdateQueryBuilder {
	tqb.UpdateQueryBuilder.Table(tableName)
	return tqb
}

// Returning specifies fields for RETURNING clause
func (tqb *TableUpdateQueryBuilder) Returning(fields ...string) *TableUpdateQueryBuilder {
	tqb.UpdateQueryBuilder.Returning(fields...)
	return tqb
}
