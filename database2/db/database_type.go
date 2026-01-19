package db

/*
type DXDatabaseType int64

const (

	UnknownDatabaseType DXDatabaseType = iota
	PostgreSQL
	MariaDB
	Oracle
	SQLServer
	DeprecatedMysql

)
*/
type DXDatabaseTableRowsInfo struct {
	Columns []string
	//	ColumnTypes []*sql.ColumnType
}

type DXDatabaseTableFieldsOrderBy map[string]string
type DXDatabaseTableFieldTypeMapping map[string]string
