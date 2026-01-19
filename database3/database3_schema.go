package database3

import (
	"fmt"
	"sort"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
)

type DBSchema struct {
	Name     string
	Order    int
	DB       *DB
	Entities []*DBEntity
}

// NewDBSchema creates a new database schema and registers it with the DB
func NewDBSchema(db *DB, name string, order int) *DBSchema {
	schema := &DBSchema{
		Name:     name,
		Order:    order,
		DB:       db,
		Entities: []*DBEntity{},
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

	// Add pgcrypto extension for PostgreSQL if any entity has encrypted fields
	if dbType == base.DXDatabaseTypePostgreSQL {
		for _, entity := range s.Entities {
			if entity.HasEncryptedFields() {
				sb.WriteString("CREATE EXTENSION IF NOT EXISTS pgcrypto;\n\n")
				break
			}
		}
	}

	// Create DDL for all entities
	orderedEntities := make([]*DBEntity, len(s.Entities))
	copy(orderedEntities, s.Entities)
	sort.SliceStable(orderedEntities, func(i, j int) bool {
		return orderedEntities[i].Order < orderedEntities[j].Order
	})

	for _, entity := range orderedEntities {
		s, err := entity.createTableDDL(dbType)
		if err != nil {
			return "", err
		}
		sb.WriteString(s)
		sb.WriteString("\n")
	}

	// Create views for entities with encrypted fields (after all tables are created)
	for _, entity := range orderedEntities {
		if entity.HasEncryptedFields() {
			sb.WriteString(entity.createViewDDL(dbType))
			sb.WriteString("\n")
		}
	}

	return sb.String(), nil
}
