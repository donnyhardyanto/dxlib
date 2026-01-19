package database3

import (
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
)

// DBMaterializedView represents a materialized view in the database
// Uses RawSQL only (Low IQ Tax Code Principle - keep it simple)
type DBMaterializedView struct {
	DBEntity                    // Embedded base entity (Name, Type, Order, Schema)
	RawSQL             string   // Raw SQL SELECT query
	UseTDE             bool     // If true, use USING tde_heap (PostgreSQL specific)
	UniqueIndexColumns []string // Columns for unique index (required for CONCURRENTLY refresh)
}

// NewDBMaterializedView creates a new materialized view and registers it with the schema
func NewDBMaterializedView(schema *DBSchema, name string, rawSQL string, useTDE bool, uniqueIndexColumns []string) *DBMaterializedView {
	mv := &DBMaterializedView{
		DBEntity: DBEntity{
			Name:   name,
			Type:   DBEntityTypeMaterializedView,
			Order:  0,
			Schema: schema,
		},
		RawSQL:             rawSQL,
		UseTDE:             useTDE,
		UniqueIndexColumns: uniqueIndexColumns,
	}
	if schema != nil {
		schema.MaterializedViews = append(schema.MaterializedViews, mv)
	}
	return mv
}

// SetOrder sets the view Order (for global view creation ordering)
func (mv *DBMaterializedView) SetOrder(order int) *DBMaterializedView {
	mv.Order = order
	return mv
}

// FullMaterializedViewName returns the materialized view name with schema prefix
func (mv *DBMaterializedView) FullMaterializedViewName() string {
	if mv.Schema != nil && mv.Schema.Name != "" {
		return mv.Schema.Name + "." + mv.Name
	}
	return mv.Name
}

// CreateDDL generates the CREATE MATERIALIZED VIEW DDL statement
func (mv *DBMaterializedView) CreateDDL(dbType base.DXDatabaseType) (string, error) {
	var sb strings.Builder

	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		// CREATE MATERIALIZED VIEW schema.name [USING tde_heap] AS SELECT...
		sb.WriteString(fmt.Sprintf("CREATE MATERIALIZED VIEW %s", mv.FullMaterializedViewName()))
		if mv.UseTDE {
			sb.WriteString(" USING tde_heap")
		}
		sb.WriteString(" AS\n")
		sb.WriteString(mv.RawSQL)
		sb.WriteString(";\n")

		// Create unique index if columns specified (required for CONCURRENTLY refresh)
		if len(mv.UniqueIndexColumns) > 0 {
			indexName := fmt.Sprintf("idx_%s_pk", mv.Name)
			sb.WriteString(fmt.Sprintf("\nCREATE UNIQUE INDEX %s ON %s (%s);\n",
				indexName,
				mv.FullMaterializedViewName(),
				strings.Join(mv.UniqueIndexColumns, ", ")))
		}

	case base.DXDatabaseTypeSQLServer:
		// SQL Server uses indexed views instead of materialized views
		sb.WriteString(fmt.Sprintf("-- SQL Server: Create indexed view %s\n", mv.FullMaterializedViewName()))
		sb.WriteString(fmt.Sprintf("CREATE VIEW %s WITH SCHEMABINDING AS\n", mv.FullMaterializedViewName()))
		sb.WriteString(mv.RawSQL)
		sb.WriteString(";\n")
		if len(mv.UniqueIndexColumns) > 0 {
			indexName := fmt.Sprintf("idx_%s_pk", mv.Name)
			sb.WriteString(fmt.Sprintf("\nCREATE UNIQUE CLUSTERED INDEX %s ON %s (%s);\n",
				indexName,
				mv.FullMaterializedViewName(),
				strings.Join(mv.UniqueIndexColumns, ", ")))
		}

	case base.DXDatabaseTypeOracle:
		// Oracle uses materialized views
		sb.WriteString(fmt.Sprintf("CREATE MATERIALIZED VIEW %s AS\n", mv.FullMaterializedViewName()))
		sb.WriteString(mv.RawSQL)
		sb.WriteString(";\n")
		if len(mv.UniqueIndexColumns) > 0 {
			indexName := fmt.Sprintf("idx_%s_pk", mv.Name)
			sb.WriteString(fmt.Sprintf("\nCREATE UNIQUE INDEX %s ON %s (%s);\n",
				indexName,
				mv.FullMaterializedViewName(),
				strings.Join(mv.UniqueIndexColumns, ", ")))
		}

	case base.DXDatabaseTypeMariaDB:
		// MariaDB doesn't have native materialized views, use a table with a view
		sb.WriteString(fmt.Sprintf("-- MariaDB: Materialized view emulated as table %s\n", mv.FullMaterializedViewName()))
		sb.WriteString(fmt.Sprintf("CREATE TABLE %s AS\n", mv.FullMaterializedViewName()))
		sb.WriteString(mv.RawSQL)
		sb.WriteString(";\n")
		if len(mv.UniqueIndexColumns) > 0 {
			indexName := fmt.Sprintf("idx_%s_pk", mv.Name)
			sb.WriteString(fmt.Sprintf("\nCREATE UNIQUE INDEX %s ON %s (%s);\n",
				indexName,
				mv.FullMaterializedViewName(),
				strings.Join(mv.UniqueIndexColumns, ", ")))
		}

	default:
		return "", fmt.Errorf("unsupported database type for materialized view: %v", dbType)
	}

	return sb.String(), nil
}

// DropDDL generates the DROP MATERIALIZED VIEW DDL statement
func (mv *DBMaterializedView) DropDDL(dbType base.DXDatabaseType) string {
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return fmt.Sprintf("DROP MATERIALIZED VIEW IF EXISTS %s;\n", mv.FullMaterializedViewName())
	case base.DXDatabaseTypeSQLServer:
		return fmt.Sprintf("IF OBJECT_ID('%s', 'V') IS NOT NULL DROP VIEW %s;\n", mv.FullMaterializedViewName(), mv.FullMaterializedViewName())
	case base.DXDatabaseTypeOracle:
		return fmt.Sprintf("DROP MATERIALIZED VIEW %s;\n", mv.FullMaterializedViewName())
	case base.DXDatabaseTypeMariaDB:
		return fmt.Sprintf("DROP TABLE IF EXISTS %s;\n", mv.FullMaterializedViewName())
	default:
		return fmt.Sprintf("DROP MATERIALIZED VIEW IF EXISTS %s;\n", mv.FullMaterializedViewName())
	}
}

// RefreshDDL generates the REFRESH MATERIALIZED VIEW DDL statement
func (mv *DBMaterializedView) RefreshDDL(dbType base.DXDatabaseType, concurrently bool) string {
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		if concurrently && len(mv.UniqueIndexColumns) > 0 {
			return fmt.Sprintf("REFRESH MATERIALIZED VIEW CONCURRENTLY %s;\n", mv.FullMaterializedViewName())
		}
		return fmt.Sprintf("REFRESH MATERIALIZED VIEW %s;\n", mv.FullMaterializedViewName())
	case base.DXDatabaseTypeOracle:
		return fmt.Sprintf("BEGIN\n    DBMS_MVIEW.REFRESH('%s');\nEND;\n/\n", mv.FullMaterializedViewName())
	case base.DXDatabaseTypeMariaDB:
		// MariaDB: truncate and repopulate
		return fmt.Sprintf("TRUNCATE TABLE %s;\nINSERT INTO %s %s;\n", mv.FullMaterializedViewName(), mv.FullMaterializedViewName(), mv.RawSQL)
	default:
		return fmt.Sprintf("-- Refresh materialized view %s\n", mv.FullMaterializedViewName())
	}
}
