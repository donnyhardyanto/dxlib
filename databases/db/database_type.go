package db

type DXDatabaseTableRowsInfo struct {
	Columns []string
	//	ColumnTypes []*sql.ColumnType
}

type DXDatabaseTableFieldsOrderBy map[string]string
type DXDatabaseTableFieldTypeMapping map[string]string
