package database3

import (
	"fmt"
	"sort"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
)

type DBSchema struct {
	Name              string
	Order             int
	DB                *DB
	Functions         []*DBFunction
	Tables            []*DBTable
	Views             []*DBView
	MaterializedViews []*DBMaterializedView
	Triggers          []*DBTrigger // Pointer list to all triggers in this schema (owned by tables)
}

// NewDBSchema creates a new database schema and registers it with the DB
func NewDBSchema(db *DB, name string, order int) *DBSchema {
	schema := &DBSchema{
		Name:              name,
		Order:             order,
		DB:                db,
		Functions:         []*DBFunction{},
		Tables:            []*DBTable{},
		Views:             []*DBView{},
		MaterializedViews: []*DBMaterializedView{},
		Triggers:          []*DBTrigger{},
	}
	if db != nil {
		db.Schemas = append(db.Schemas, schema)
	}
	return schema
}

// CreateDDL generates DDL script for the schema and all its entities
// Order: function -> table -> (index table) -> trigger table -> view -> materialized view -> index materialized view
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

	// 1. Create DDL for all functions (before tables, as triggers may reference them)
	orderedFunctions := make([]*DBFunction, len(s.Functions))
	copy(orderedFunctions, s.Functions)
	sort.SliceStable(orderedFunctions, func(i, j int) bool {
		return orderedFunctions[i].Order < orderedFunctions[j].Order
	})

	for _, fn := range orderedFunctions {
		ddl, err := fn.CreateDDL(dbType)
		if err != nil {
			return "", err
		}
		sb.WriteString(ddl)
		sb.WriteString("\n")
	}

	// 2. Create DDL for all tables
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

	// 3. Create indexes for tables
	for _, table := range orderedTables {
		orderedIndexes := make([]*DBIndex, len(table.Indexes))
		copy(orderedIndexes, table.Indexes)
		sort.SliceStable(orderedIndexes, func(i, j int) bool {
			return orderedIndexes[i].Order < orderedIndexes[j].Order
		})

		for _, idx := range orderedIndexes {
			ddl, err := idx.CreateDDL(dbType)
			if err != nil {
				return "", err
			}
			sb.WriteString(ddl)
		}
		if len(orderedIndexes) > 0 {
			sb.WriteString("\n")
		}
	}

	// 4. Create triggers for tables
	for _, table := range orderedTables {
		orderedTriggers := make([]*DBTrigger, len(table.Triggers))
		copy(orderedTriggers, table.Triggers)
		sort.SliceStable(orderedTriggers, func(i, j int) bool {
			return orderedTriggers[i].Order < orderedTriggers[j].Order
		})

		for _, trigger := range orderedTriggers {
			ddl, err := trigger.CreateDDL(dbType)
			if err != nil {
				return "", err
			}
			sb.WriteString(ddl)
		}
		if len(orderedTriggers) > 0 {
			sb.WriteString("\n")
		}
	}

	// 5. Create views for tables with encrypted fields (after all tables are created)
	for _, table := range orderedTables {
		if table.HasEncryptedFields() {
			sb.WriteString(table.createViewDDL(dbType))
			sb.WriteString("\n")
		}
	}

	// 6. Create DDL for all explicit views
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

	// 7. Create DDL for all materialized views (after views, since MVs may depend on views)
	orderedMVs := make([]*DBMaterializedView, len(s.MaterializedViews))
	copy(orderedMVs, s.MaterializedViews)
	sort.SliceStable(orderedMVs, func(i, j int) bool {
		return orderedMVs[i].Order < orderedMVs[j].Order
	})

	for _, mv := range orderedMVs {
		ddl, err := mv.CreateDDL(dbType)
		if err != nil {
			return "", err
		}
		sb.WriteString(ddl)
		sb.WriteString("\n")
	}

	// 8. Create indexes for materialized views
	for _, mv := range orderedMVs {
		orderedIndexes := make([]*DBIndex, len(mv.Indexes))
		copy(orderedIndexes, mv.Indexes)
		sort.SliceStable(orderedIndexes, func(i, j int) bool {
			return orderedIndexes[i].Order < orderedIndexes[j].Order
		})

		for _, idx := range orderedIndexes {
			ddl, err := idx.CreateDDL(dbType)
			if err != nil {
				return "", err
			}
			sb.WriteString(ddl)
		}
		if len(orderedIndexes) > 0 {
			sb.WriteString("\n")
		}
	}

	return sb.String(), nil
}
