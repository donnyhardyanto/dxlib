package database3

import (
	"fmt"
	"strings"
)

type DBSchema struct {
	Name     string
	DB       *DB
	Entities []*DBEntity
}

// NewDBSchema creates a new database schema and registers it with the DB
func NewDBSchema(db *DB, name string) *DBSchema {
	schema := &DBSchema{
		Name:     name,
		DB:       db,
		Entities: []*DBEntity{},
	}
	if db != nil {
		db.Schemas = append(db.Schemas, schema)
	}
	return schema
}

// CreateDDL generates DDL script for the schema and all its entities
func (s *DBSchema) CreateDDL(dbType DXDatabaseType) string {
	var sb strings.Builder

	// Create schema statement
	switch dbType {
	case DXDatabaseTypePostgreSQL:
		sb.WriteString(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;\n\n", s.Name))
	case DXDatabaseTypeSQLServer:
		sb.WriteString(fmt.Sprintf("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '%s')\nBEGIN\n    EXEC('CREATE SCHEMA %s')\nEND;\n\n", s.Name, s.Name))
	case DXDatabaseTypeMariaDB:
		// MySQL/MariaDB uses "database" instead of schema
		// language=text
		sb.WriteString(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;\nUSE %s;\n\n", s.Name, s.Name))
	case DXDatabaseTypeOracle:
		// Oracle uses users as schemas, typically created by DBA
		sb.WriteString(fmt.Sprintf("-- Oracle: Schema %s should be created by DBA\n\n", s.Name))
	default:
		panic("unhandled default case")
	}

	// Add pgcrypto extension for PostgreSQL if any entity has encrypted fields
	if dbType == DXDatabaseTypePostgreSQL {
		for _, entity := range s.Entities {
			if entity.HasEncryptedFields() {
				sb.WriteString("CREATE EXTENSION IF NOT EXISTS pgcrypto;\n\n")
				break
			}
		}
	}

	// Create DDL for all entities
	for _, entity := range s.Entities {
		sb.WriteString(entity.createTableDDL(dbType))
		sb.WriteString("\n")
	}

	// Create views for entities with encrypted fields (after all tables are created)
	for _, entity := range s.Entities {
		if entity.HasEncryptedFields() {
			sb.WriteString(entity.createViewDDL(dbType))
			sb.WriteString("\n")
		}
	}

	return sb.String()
}
