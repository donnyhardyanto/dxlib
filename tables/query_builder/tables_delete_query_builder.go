package query_builder

import (
	"fmt"
	"slices"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/databases/db/query/builder"
	"github.com/donnyhardyanto/dxlib/errors"
)

// TableDeleteQueryBuilder wraps builder.DeleteQueryBuilder with table-specific field validation
type TableDeleteQueryBuilder struct {
	*builder.DeleteQueryBuilder
	TableInterface TableInterface
}

// NewTableDeleteQueryBuilder creates a new TableDeleteQueryBuilder with table interface for validation
func NewTableDeleteQueryBuilder(dbType base.DXDatabaseType, tableInterface TableInterface) *TableDeleteQueryBuilder {
	return &TableDeleteQueryBuilder{
		DeleteQueryBuilder: builder.NewDeleteQueryBuilder(dbType),
		TableInterface:     tableInterface,
	}
}

// NewTableDeleteQueryBuilderWithSource creates a new TableDeleteQueryBuilder with table name
func NewTableDeleteQueryBuilderWithSource(dbType base.DXDatabaseType, tableName string, tableInterface TableInterface) *TableDeleteQueryBuilder {
	return &TableDeleteQueryBuilder{
		DeleteQueryBuilder: builder.NewDeleteQueryBuilderWithSource(dbType, tableName),
		TableInterface:     tableInterface,
	}
}

// CheckFieldExist validates field exists and sets error if not
func (tqb *TableDeleteQueryBuilder) CheckFieldExist(fieldName string) *TableDeleteQueryBuilder {
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

// Eq adds WHERE field = value condition with field validation
func (tqb *TableDeleteQueryBuilder) Eq(fieldName string, value any) *TableDeleteQueryBuilder {
	tqb.CheckFieldExist(fieldName)
	if tqb.Error != nil {
		return tqb
	}
	tqb.DeleteQueryBuilder.Where(fieldName, value)
	return tqb
}

// Ne adds WHERE field != value condition with field validation
func (tqb *TableDeleteQueryBuilder) Ne(fieldName string, value any) *TableDeleteQueryBuilder {
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
func (tqb *TableDeleteQueryBuilder) NotDeleted() *TableDeleteQueryBuilder {
	switch tqb.DbType {
	case base.DXDatabaseTypeSQLServer:
		tqb.Conditions = append(tqb.Conditions, "is_deleted = 0")
	default:
		tqb.Conditions = append(tqb.Conditions, "is_deleted = false")
	}
	return tqb
}

// From sets the table name for DELETE
func (tqb *TableDeleteQueryBuilder) From(tableName string) *TableDeleteQueryBuilder {
	tqb.DeleteQueryBuilder.From(tableName)
	return tqb
}

// Returning specifies fields for RETURNING clause
func (tqb *TableDeleteQueryBuilder) Returning(fields ...string) *TableDeleteQueryBuilder {
	tqb.DeleteQueryBuilder.Returning(fields...)
	return tqb
}
