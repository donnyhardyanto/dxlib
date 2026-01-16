package database3

import (
	"fmt"
	"strings"

	database1Type "github.com/donnyhardyanto/dxlib/database/database_type"
)

type DXDatabaseType int64

const (
	UnknownDatabaseType DXDatabaseType = iota
	DXDatabaseTypePostgreSQL
	DXDatabaseTypeMariaDB
	DXDatabaseTypeOracle
	DXDatabaseTypeSQLServer
	DXDatabaseTypeDeprecatedMysql
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
		return "unknown"
	}
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

// DB represents a database with extensions and schemas
type DB struct {
	Name       string
	Extensions map[DXDatabaseType][]string // Database-specific extensions/features
	Schemas    []*DBSchema
}

// NewDB creates a new database with optional extensions
func NewDB(name string, extensions map[DXDatabaseType][]string) *DB {
	if extensions == nil {
		extensions = make(map[DXDatabaseType][]string)
	}
	return &DB{
		Name:       name,
		Extensions: extensions,
		Schemas:    []*DBSchema{},
	}
}

// AddExtensions adds extensions for a specific database type
func (d *DB) AddExtensions(dbType DXDatabaseType, extensions ...string) {
	if d.Extensions == nil {
		d.Extensions = make(map[DXDatabaseType][]string)
	}
	d.Extensions[dbType] = append(d.Extensions[dbType], extensions...)
}

// SetExtensions sets extensions for a specific database type (replaces existing)
func (d *DB) SetExtensions(dbType DXDatabaseType, extensions []string) {
	if d.Extensions == nil {
		d.Extensions = make(map[DXDatabaseType][]string)
	}
	d.Extensions[dbType] = extensions
}

// GetExtensions returns extensions for a specific database type
func (d *DB) GetExtensions(dbType DXDatabaseType) []string {
	if d.Extensions == nil {
		return nil
	}
	return d.Extensions[dbType]
}

// CreateDDL generates DDL script for the database including extensions and all schemas
func (d *DB) CreateDDL(dbType DXDatabaseType) string {
	var sb strings.Builder

	// Get extensions for the specific database type
	extensions := d.GetExtensions(dbType)
	if len(extensions) > 0 {
		switch dbType {
		case DXDatabaseTypePostgreSQL:
			// PostgreSQL: CREATE EXTENSION
			for _, ext := range extensions {
				sb.WriteString(fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s;\n", ext))
			}
			sb.WriteString("\n")
		case DXDatabaseTypeSQLServer:
			// SQL Server: Enable features/configurations
			for _, feature := range extensions {
				sb.WriteString(fmt.Sprintf("-- Enable SQL Server feature: %s\n", feature))
				// Example: sp_configure or ALTER DATABASE for specific features
				sb.WriteString(fmt.Sprintf("EXEC sp_configure '%s', 1;\nRECONFIGURE;\n", feature))
			}
			sb.WriteString("\n")
		case DXDatabaseTypeMariaDB:
			// MySQL/MariaDB: Install plugins or enable features
			for _, plugin := range extensions {
				sb.WriteString(fmt.Sprintf("-- Install MariaDB/MySQL plugin: %s\n", plugin))
				sb.WriteString(fmt.Sprintf("INSTALL PLUGIN IF NOT EXISTS %s;\n", plugin))
			}
			sb.WriteString("\n")
		case DXDatabaseTypeOracle:
			// Oracle: Grant privileges or enable features (typically done by DBA)
			for _, feature := range extensions {
				sb.WriteString(fmt.Sprintf("-- Oracle feature/package: %s (ensure enabled by DBA)\n", feature))
			}
			sb.WriteString("\n")
		default:
			panic("unhandled default case")
		}
	}

	// Create all schemas
	for _, schema := range d.Schemas {
		sb.WriteString(schema.CreateDDL(dbType))
		sb.WriteString("\n")
	}

	return sb.String()
}
