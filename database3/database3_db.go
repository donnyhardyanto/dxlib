package database3

import (
	"fmt"
	"sort"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
)

// DB represents a database with extensions and schemas
type DB struct {
	Name       string
	Extensions map[base.DXDatabaseType][]string // Database-specific extensions/features
	Schemas    []*DBSchema
}

// NewDB creates a new database with optional extensions
func NewDB(name string, extensions map[base.DXDatabaseType][]string) *DB {
	if extensions == nil {
		extensions = make(map[base.DXDatabaseType][]string)
	}
	return &DB{
		Name:       name,
		Extensions: extensions,
		Schemas:    []*DBSchema{},
	}
}

// AddExtensions adds extensions for a specific database type
func (d *DB) AddExtensions(dbType base.DXDatabaseType, extensions ...string) {
	if d.Extensions == nil {
		d.Extensions = make(map[base.DXDatabaseType][]string)
	}
	d.Extensions[dbType] = append(d.Extensions[dbType], extensions...)
}

// SetExtensions sets extensions for a specific database type (replaces existing)
func (d *DB) SetExtensions(dbType base.DXDatabaseType, extensions []string) {
	if d.Extensions == nil {
		d.Extensions = make(map[base.DXDatabaseType][]string)
	}
	d.Extensions[dbType] = extensions
}

// GetExtensions returns extensions for a specific database type
func (d *DB) GetExtensions(dbType base.DXDatabaseType) []string {
	if d.Extensions == nil {
		return nil
	}
	return d.Extensions[dbType]
}

// CreateDDL generates DDL script for the database including extensions and all schemas
func (d *DB) CreateDDL(dbType base.DXDatabaseType) (string, error) {
	var sb strings.Builder

	// Get extensions for the specific database type
	extensions := d.GetExtensions(dbType)
	if len(extensions) > 0 {
		switch dbType {
		case base.DXDatabaseTypePostgreSQL:
			// PostgreSQL: CREATE EXTENSION
			for _, ext := range extensions {
				sb.WriteString(fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s;\n", ext))
			}
			sb.WriteString("\n")
		case base.DXDatabaseTypeSQLServer:
			// SQL Server: Enable features/configurations
			for _, feature := range extensions {
				sb.WriteString(fmt.Sprintf("-- Enable SQL Server feature: %s\n", feature))
				// Example: sp_configure or ALTER DATABASE for specific features
				sb.WriteString(fmt.Sprintf("EXEC sp_configure '%s', 1;\nRECONFIGURE;\n", feature))
			}
			sb.WriteString("\n")
		case base.DXDatabaseTypeMariaDB:
			// MySQL/MariaDB: Install plugins or enable features
			for _, plugin := range extensions {
				sb.WriteString(fmt.Sprintf("-- Install MariaDB/MySQL plugin: %s\n", plugin))
				sb.WriteString(fmt.Sprintf("INSTALL PLUGIN IF NOT EXISTS %s;\n", plugin))
			}
			sb.WriteString("\n")
		case base.DXDatabaseTypeOracle:
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
	orderedSchemas := make([]*DBSchema, len(d.Schemas))
	copy(orderedSchemas, d.Schemas)
	sort.SliceStable(orderedSchemas, func(i, j int) bool {
		return orderedSchemas[i].Order < orderedSchemas[j].Order
	})

	for _, schema := range orderedSchemas {
		s, err := schema.CreateDDL(dbType)
		if err != nil {
			return "", err
		}
		sb.WriteString(s)
		sb.WriteString("\n")
	}

	return sb.String(), nil
}
