package base

import (
	"fmt"

	database1Type "github.com/donnyhardyanto/dxlib/database/database_type"
)

type DXDatabaseType int64

const (
	UnknownDatabaseType DXDatabaseType = iota
	DXDatabaseTypePostgreSQL
	DXDatabaseTypeMariaDB
	DXDatabaseTypeOracle
	DXDatabaseTypeSQLServer
)

func (t DXDatabaseType) String() string {
	switch t {
	case DXDatabaseTypePostgreSQL:
		return "postgres"
	case DXDatabaseTypeOracle:
		return "oracle"
	case DXDatabaseTypeSQLServer:
		return "sqlserver"
	case DXDatabaseTypeMariaDB:
		return "mariadb"
	default:
		// This helps you see if the value was 999 or 0 or -1
		return fmt.Sprintf("unknown(%d)", t)
	}
}

func (t DXDatabaseType) IsValid() bool {
	return t > UnknownDatabaseType && t <= DXDatabaseTypeSQLServer
}

func (t DXDatabaseType) Driver() string {
	switch t {
	case DXDatabaseTypePostgreSQL:
		return "postgres"
	case DXDatabaseTypeOracle:
		return "oracle"
	case DXDatabaseTypeSQLServer:
		return "sqlserver"
	case DXDatabaseTypeMariaDB:
		return "mysql"
	default:
		return "unknown"
	}
}

func StringToDXDatabaseType(v string) DXDatabaseType {
	switch v {
	case "postgres", "postgresql":
		return DXDatabaseTypePostgreSQL
	case "mysql":
		return DXDatabaseTypeMariaDB
	case "mariadb":
		return DXDatabaseTypeMariaDB
	case "oracle":
		return DXDatabaseTypeOracle
	case "sqlserver":
		return DXDatabaseTypeSQLServer
	default:
		return UnknownDatabaseType
	}
}

func Database1DXDatabaseTypeToDXDatabaseType(dbType database1Type.DXDatabaseType) DXDatabaseType {
	switch dbType {
	case database1Type.PostgreSQL:
		return DXDatabaseTypePostgreSQL
	case database1Type.MariaDB:
		return DXDatabaseTypeMariaDB
	case database1Type.Oracle:
		return DXDatabaseTypeOracle
	case database1Type.SQLServer:
		return DXDatabaseTypeSQLServer
	default:
		return UnknownDatabaseType
	}
}
