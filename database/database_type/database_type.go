package database_type

type DXDatabaseType int64

const (
	UnknownDatabaseType DXDatabaseType = iota
	PostgreSQL
	MySQL
	Oracle
	SQLServer
)

func (t DXDatabaseType) String() string {
	switch t {
	case PostgreSQL:
		return "postgres"
	case MySQL:
		return "mysql"
	case Oracle:
		return "oracle"
	case SQLServer:
		return "sqlserver"
	default:

		return "unknown"
	}
}

func (t DXDatabaseType) Driver() string {
	switch t {
	case PostgreSQL:
		return "postgres"
	case MySQL:
		return "mysql"
	case Oracle:
		return "godror"
	case SQLServer:
		return "sqlserver"
	default:

		return "unknown"
	}
}
func StringToDXDatabaseType(v string) DXDatabaseType {
	switch v {
	case "postgres", "postgresql":
		return PostgreSQL
	case "mariadb", "mysql":
		return MySQL
	case "oracle":
		return Oracle
	case "sqlserver":
		return SQLServer
	default:

		return UnknownDatabaseType
	}
}
