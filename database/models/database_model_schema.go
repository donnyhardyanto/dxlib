package models

import (
	"fmt"
	"sort"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
)

type ModelDBSchema struct {
	Name              string
	Order             int
	DB                *ModelDB
	Functions         []*ModelDBFunction
	Tables            []*ModelDBTable
	Views             []*ModelDBView
	MaterializedViews []*ModelDBMaterializedView
	Triggers          []*ModelDBTrigger // Pointer list to all triggers in this schema (owned by tables)
}

// NewModelDBSchema creates a new database schema and registers it with the ModelDB
func NewModelDBSchema(db *ModelDB, name string, order int) *ModelDBSchema {
	schema := &ModelDBSchema{
		Name:              name,
		Order:             order,
		DB:                db,
		Functions:         []*ModelDBFunction{},
		Tables:            []*ModelDBTable{},
		Views:             []*ModelDBView{},
		MaterializedViews: []*ModelDBMaterializedView{},
		Triggers:          []*ModelDBTrigger{},
	}
	if db != nil {
		db.Schemas = append(db.Schemas, schema)
	}
	return schema
}

// CreateDDL generates DDL script for the schema and all its entities
// Order: function -> table -> (index table) -> trigger table -> view -> materialized view -> index materialized view
func (s *ModelDBSchema) CreateDDL(dbType base.DXDatabaseType) (string, error) {
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

	// 1. Create DDL for all functions (before tables, as triggers may reference them)
	orderedFunctions := make([]*ModelDBFunction, len(s.Functions))
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
	orderedTables := make([]*ModelDBTable, len(s.Tables))
	copy(orderedTables, s.Tables)
	sort.SliceStable(orderedTables, func(i, j int) bool {
		return orderedTables[i].Order < orderedTables[j].Order
	})

	for _, table := range orderedTables {
		ddl, err := table.CreateDDL(dbType)
		if err != nil {
			return "", err
		}
		sb.WriteString(ddl)
		sb.WriteString("\n")
	}

	// 3. Create indexes for tables
	for _, table := range orderedTables {
		orderedIndexes := make([]*ModelDBIndex, len(table.Indexes))
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
		orderedTriggers := make([]*ModelDBTrigger, len(table.Triggers))
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

	// 5. Create DDL for all explicit views
	orderedViews := make([]*ModelDBView, len(s.Views))
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

	// 6. Create DDL for all materialized views (after views, since MVs may depend on views)
	orderedMVs := make([]*ModelDBMaterializedView, len(s.MaterializedViews))
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

	// 7. Create indexes for materialized views
	for _, mv := range orderedMVs {
		orderedIndexes := make([]*ModelDBIndex, len(mv.Indexes))
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
