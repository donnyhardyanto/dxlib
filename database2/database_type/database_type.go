package database_type

type DXDatabaseType int64

const (
	UnknownDatabaseType DXDatabaseType = iota
	PostgreSQL
	MariaDB
	Oracle
	SQLServer
)

type RowsInfo struct {
	Columns []string
	//	ColumnTypes []*sql.ColumnType
}

func (t DXDatabaseType) String() string {
	switch t {
	case PostgreSQL:
		return "postgres"
	case Oracle:
		return "oracle"
	case SQLServer:
		return "sqlserver"
	case MariaDB:
		return "mariadb" // User-facing type remains "mariadb"
	default:
		return "unknown"
	}
}

// MariaDB uses the MySQL driver since they share the same wire protocol
// The Go ecosystem only provides github.com/go-sql-driver/mysql for both
func (t DXDatabaseType) Driver() string {
	switch t {
	case PostgreSQL:
		return "postgres"
	case Oracle:
		return "oracle"
	case SQLServer:
		return "sqlserver"
	case MariaDB:
		return "mysql" // Required: Go's database/sql only recognizes "mysql" driver for MariaDB
	default:
		return "unknown"
	}
}

func StringToDXDatabaseType(v string) DXDatabaseType {
	switch v {
	case "postgres", "postgresql":
		return PostgreSQL
	case "mysql":
		return MariaDB
	case "mariadb":
		return MariaDB
	case "oracle":
		return Oracle
	case "sqlserver":
		return SQLServer
	default:

		return UnknownDatabaseType
	}
}
