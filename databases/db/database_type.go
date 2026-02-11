package db

import "github.com/donnyhardyanto/dxlib/types"

type DXDatabaseTableRowsInfo struct {
	Columns []string
	//	ColumnTypes []*sql.ColumnType
}

type DXDatabaseTableFieldsOrderBy map[string]string
type DXDatabaseTableFieldTypeMapping map[string]types.APIParameterType
