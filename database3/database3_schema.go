package database3

import (
	"fmt"
	"sort"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
)

type DBSchema struct {
	Name   string
	Order  int
	DB     *DB
	Tables []*DBTable
	Views  []*DBView
}

// NewDBSchema creates a new database schema and registers it with the DB
func NewDBSchema(db *DB, name string, order int) *DBSchema {
	schema := &DBSchema{
		Name:   name,
		Order:  order,
		DB:     db,
		Tables: []*DBTable{},
		Views:  []*DBView{},
	}
	if db != nil {
		db.Schemas = append(db.Schemas, schema)
	}
	return schema
}

// CreateDDL generates DDL script for the schema and all its entities
func (s *DBSchema) CreateDDL(dbType base.DXDatabaseType) (string, error) {
	var sb strings.Builder

	// Create schema statement
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		sb.WriteString(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;\n\n", s.Name))
	case base.DXDatabaseTypeSQLServer:
		sb.WriteString(fmt.Sprintf("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '%s')\nBEGIN\n    EXEC('CREATE SCHEMA %s')\nEND;\n\n", s.Name, s.Name))
	case base.DXDatabaseTypeMariaDB:
		// MySQL/MariaDB uses "database" instead of schema
		// language=text
		sb.WriteString(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;\nUSE %s;\n\n", s.Name, s.Name))
	case base.DXDatabaseTypeOracle:
		// Oracle uses users as schemas, typically created by DBA
		sb.WriteString(fmt.Sprintf("-- Oracle: Schema %s should be created by DBA\n\n", s.Name))
	default:
		panic("unhandled default case")
	}

	// Add pgcrypto extension for PostgreSQL if any table has encrypted fields
	if dbType == base.DXDatabaseTypePostgreSQL {
		for _, table := range s.Tables {
			if table.HasEncryptedFields() {
				sb.WriteString("CREATE EXTENSION IF NOT EXISTS pgcrypto;\n\n")
				break
			}
		}
	}

	// Create DDL for all tables
	orderedTables := make([]*DBTable, len(s.Tables))
	copy(orderedTables, s.Tables)
	sort.SliceStable(orderedTables, func(i, j int) bool {
		return orderedTables[i].Order < orderedTables[j].Order
	})

	for _, table := range orderedTables {
		ddl, err := table.createTableDDL(dbType)
		if err != nil {
			return "", err
		}
		sb.WriteString(ddl)
		sb.WriteString("\n")
	}

	// Create views for tables with encrypted fields (after all tables are created)
	for _, table := range orderedTables {
		if table.HasEncryptedFields() {
			sb.WriteString(table.createViewDDL(dbType))
			sb.WriteString("\n")
		}
	}

	// Create DDL for all explicit views
	orderedViews := make([]*DBView, len(s.Views))
	copy(orderedViews, s.Views)
	sort.SliceStable(orderedViews, func(i, j int) bool {
		return orderedViews[i].Order < orderedViews[j].Order
	})

	for _, view := range orderedViews {
		ddl, err := view.CreateDDL(dbType)
		if err != nil {
			return "", err
		}
		sb.WriteString(ddl)
		sb.WriteString("\n")
	}

	return sb.String(), nil
}
