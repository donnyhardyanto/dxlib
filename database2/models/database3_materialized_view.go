package models

import (
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
)

// DBMaterializedView represents a materialized view in the database
// Supports both builder pattern (for simple queries) and RawSQL (for complex queries)
// Low IQ Tax Code Principle: Simple queries use builder, complex queries use RawSQL
type DBMaterializedView struct {
	DBEntity                    // Embedded base entity (Name, Type, Order, Schema)
	UseTDE             bool     // If true, use USING tde_heap (PostgreSQL specific)
	UniqueIndexColumns []string // Columns for unique index (required for CONCURRENTLY refresh)

	// Builder pattern fields - Source table/view (use ONE of these)
	FromTable   *DBTable   // Main table to select from (when using DBTable)
	FromViewRef *DBViewRef // Main view/table to select from (when using view reference by name)

	// Builder pattern fields - Columns and clauses
	Columns     []DBViewColumn // Columns to select (for DBTable fields)
	ColumnExprs []string       // Column expressions as strings (for view references)
	Joins       []DBJoin       // Join clauses (for DBTable)
	JoinsByName []DBJoinByName // Join clauses using view/table names
	Where       string         // WHERE clause (without the "WHERE" keyword)
	GroupBy     []string       // GROUP BY columns (field names or expressions)
	Having      string         // HAVING clause (without the "HAVING" keyword)
	OrderBy     []DBOrderBy    // ORDER BY clause
	Distinct    bool           // SELECT DISTINCT

	// RawSQL for complex queries (CTEs, RECURSIVE, window functions, etc.)
	// When set, bypasses builder pattern entirely
	RawSQL string

	// Indexes on this materialized view
	Indexes []*DBIndex
}

// NewDBMaterializedView creates a materialized view using builder pattern
func NewDBMaterializedView(schema *DBSchema, name string, fromTable *DBTable, useTDE bool, uniqueIndexColumns []string) *DBMaterializedView {
	mv := &DBMaterializedView{
		DBEntity: DBEntity{
			Name:   name,
			Type:   DBEntityTypeMaterializedView,
			Order:  0,
			Schema: schema,
		},
		FromTable:          fromTable,
		Columns:            []DBViewColumn{},
		Joins:              []DBJoin{},
		GroupBy:            []string{},
		OrderBy:            []DBOrderBy{},
		UseTDE:             useTDE,
		UniqueIndexColumns: uniqueIndexColumns,
	}
	if schema != nil {
		schema.MaterializedViews = append(schema.MaterializedViews, mv)
	}
	return mv
}

// NewDBMaterializedViewFromViewRef creates a materialized view from a view reference
// Use this when the source is a view (not a DBTable)
func NewDBMaterializedViewFromViewRef(schema *DBSchema, name string, fromViewRef *DBViewRef, useTDE bool, uniqueIndexColumns []string) *DBMaterializedView {
	mv := &DBMaterializedView{
		DBEntity: DBEntity{
			Name:   name,
			Type:   DBEntityTypeMaterializedView,
			Order:  0,
			Schema: schema,
		},
		FromViewRef:        fromViewRef,
		ColumnExprs:        []string{},
		JoinsByName:        []DBJoinByName{},
		GroupBy:            []string{},
		OrderBy:            []DBOrderBy{},
		UseTDE:             useTDE,
		UniqueIndexColumns: uniqueIndexColumns,
	}
	if schema != nil {
		schema.MaterializedViews = append(schema.MaterializedViews, mv)
	}
	return mv
}

// NewDBMaterializedViewRawSQL creates a materialized view with raw SQL
// Use this for complex queries (CTEs, RECURSIVE, window functions, etc.)
func NewDBMaterializedViewRawSQL(schema *DBSchema, name string, rawSQL string, useTDE bool, uniqueIndexColumns []string) *DBMaterializedView {
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

// ================== BUILDER METHODS (for chaining) ==================

// AddColumn adds a simple column from the main table
func (mv *DBMaterializedView) AddColumn(field *Field, alias string) *DBMaterializedView {
	mv.Columns = append(mv.Columns, DBViewColumn{
		SourceTable: nil,
		SourceField: field,
		Alias:       alias,
	})
	return mv
}

// AddColumnFromTable adds a column from a joined table
func (mv *DBMaterializedView) AddColumnFromTable(table *DBTable, field *Field, alias string) *DBMaterializedView {
	mv.Columns = append(mv.Columns, DBViewColumn{
		SourceTable: table,
		SourceField: field,
		Alias:       alias,
	})
	return mv
}

// AddExpression adds a raw SQL expression (like COUNT(*), SUM(amount))
func (mv *DBMaterializedView) AddExpression(expr string, alias string) *DBMaterializedView {
	mv.Columns = append(mv.Columns, DBViewColumn{
		Expression: expr,
		Alias:      alias,
	})
	return mv
}

// AddJoin adds a join to another table
func (mv *DBMaterializedView) AddJoin(joinType JoinType, targetTable *DBTable, fromField *Field, toField *Field) *DBMaterializedView {
	mv.Joins = append(mv.Joins, DBJoin{
		JoinType:       joinType,
		TargetTable:    targetTable,
		FromLocalField: fromField,
		ToTargetField:  toField,
	})
	return mv
}

// SetWhere sets the WHERE clause
func (mv *DBMaterializedView) SetWhere(where string) *DBMaterializedView {
	mv.Where = where
	return mv
}

// AddGroupBy adds a GROUP BY column
func (mv *DBMaterializedView) AddGroupBy(columnExpr string) *DBMaterializedView {
	mv.GroupBy = append(mv.GroupBy, columnExpr)
	return mv
}

// SetHaving sets the HAVING clause
func (mv *DBMaterializedView) SetHaving(having string) *DBMaterializedView {
	mv.Having = having
	return mv
}

// AddOrderBy adds an ORDER BY column
func (mv *DBMaterializedView) AddOrderBy(columnExpr string, orderType DBOrderByType) *DBMaterializedView {
	mv.OrderBy = append(mv.OrderBy, DBOrderBy{
		ColumnExpr:  columnExpr,
		OrderByType: orderType,
	})
	return mv
}

// SetDistinct enables SELECT DISTINCT
func (mv *DBMaterializedView) SetDistinct(distinct bool) *DBMaterializedView {
	mv.Distinct = distinct
	return mv
}

// ================== BUILDER METHODS FOR VIEW REFERENCES ==================

// AddColumnExpr adds a column expression string (for use with FromViewRef)
// Example: "vfe.id AS user_role_membership_id" or "COUNT(*) as total"
func (mv *DBMaterializedView) AddColumnExpr(expr string) *DBMaterializedView {
	mv.ColumnExprs = append(mv.ColumnExprs, expr)
	return mv
}

// AddJoinByName adds a join to a view/table by name
// fromFieldExpr: field from source table (e.g., "vfe.id")
// toFieldExpr: field from target (e.g., "fee.user_role_membership_id")
func (mv *DBMaterializedView) AddJoinByName(joinType JoinType, targetViewRef *DBViewRef, fromFieldExpr, toFieldExpr string) *DBMaterializedView {
	mv.JoinsByName = append(mv.JoinsByName, DBJoinByName{
		JoinType:      joinType,
		TargetViewRef: targetViewRef,
		FromFieldExpr: fromFieldExpr,
		ToFieldExpr:   toFieldExpr,
	})
	return mv
}

// AddJoinByNameWithCondition adds a join with a custom ON condition
// onCondition: full ON condition (e.g., "a.id = b.id AND a.type = b.type")
func (mv *DBMaterializedView) AddJoinByNameWithCondition(joinType JoinType, targetViewRef *DBViewRef, onCondition string) *DBMaterializedView {
	mv.JoinsByName = append(mv.JoinsByName, DBJoinByName{
		JoinType:      joinType,
		TargetViewRef: targetViewRef,
		OnCondition:   onCondition,
	})
	return mv
}

// ================== DDL GENERATION ==================

// FullMaterializedViewName returns the materialized view name with schema prefix
func (mv *DBMaterializedView) FullMaterializedViewName() string {
	if mv.Schema != nil && mv.Schema.Name != "" {
		return mv.Schema.Name + "." + mv.Name
	}
	return mv.Name
}

// CreateDDL generates the CREATE MATERIALIZED VIEW DDL statement
func (mv *DBMaterializedView) CreateDDL(dbType base.DXDatabaseType) (string, error) {
	// Get the SELECT SQL - either from RawSQL or build from fields
	selectSQL, err := mv.buildSelectSQL(dbType)
	if err != nil {
		return "", err
	}

	var sb strings.Builder

	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		// CREATE MATERIALIZED VIEW schema.name [USING tde_heap] AS SELECT...
		fmt.Fprintf(&sb, "CREATE MATERIALIZED VIEW %s", mv.FullMaterializedViewName())
		if mv.UseTDE {
			sb.WriteString(" USING tde_heap")
		}
		sb.WriteString(" AS\n")
		sb.WriteString(selectSQL)
		sb.WriteString(";\n")

		// Create unique index if columns specified (required for CONCURRENTLY refresh)
		if len(mv.UniqueIndexColumns) > 0 {
			indexName := fmt.Sprintf("idx_%s_pk", mv.Name)
			fmt.Fprintf(&sb, "\nCREATE UNIQUE INDEX %s ON %s (%s);\n",
				indexName,
				mv.FullMaterializedViewName(),
				strings.Join(mv.UniqueIndexColumns, ", "))
		}

	case base.DXDatabaseTypeSQLServer:
		// SQL Server uses indexed views instead of materialized views
		fmt.Fprintf(&sb, "-- SQL Server: Create indexed view %s\n", mv.FullMaterializedViewName())
		fmt.Fprintf(&sb, "CREATE VIEW %s WITH SCHEMABINDING AS\n", mv.FullMaterializedViewName())
		sb.WriteString(selectSQL)
		sb.WriteString(";\n")
		if len(mv.UniqueIndexColumns) > 0 {
			indexName := fmt.Sprintf("idx_%s_pk", mv.Name)
			fmt.Fprintf(&sb, "\nCREATE UNIQUE CLUSTERED INDEX %s ON %s (%s);\n",
				indexName,
				mv.FullMaterializedViewName(),
				strings.Join(mv.UniqueIndexColumns, ", "))
		}

	case base.DXDatabaseTypeOracle:
		// Oracle uses materialized views
		fmt.Fprintf(&sb, "CREATE MATERIALIZED VIEW %s AS\n", mv.FullMaterializedViewName())
		sb.WriteString(selectSQL)
		sb.WriteString(";\n")
		if len(mv.UniqueIndexColumns) > 0 {
			indexName := fmt.Sprintf("idx_%s_pk", mv.Name)
			fmt.Fprintf(&sb, "\nCREATE UNIQUE INDEX %s ON %s (%s);\n",
				indexName,
				mv.FullMaterializedViewName(),
				strings.Join(mv.UniqueIndexColumns, ", "))
		}

	case base.DXDatabaseTypeMariaDB:
		// MariaDB doesn't have native materialized views, use a table with a view
		fmt.Fprintf(&sb, "-- MariaDB: Materialized view emulated as table %s\n", mv.FullMaterializedViewName())
		fmt.Fprintf(&sb, "CREATE TABLE %s AS\n", mv.FullMaterializedViewName())
		sb.WriteString(selectSQL)
		sb.WriteString(";\n")
		if len(mv.UniqueIndexColumns) > 0 {
			indexName := fmt.Sprintf("idx_%s_pk", mv.Name)
			fmt.Fprintf(&sb, "\nCREATE UNIQUE INDEX %s ON %s (%s);\n",
				indexName,
				mv.FullMaterializedViewName(),
				strings.Join(mv.UniqueIndexColumns, ", "))
		}

	default:
		return "", fmt.Errorf("unsupported database type for materialized view: %v", dbType)
	}

	return sb.String(), nil
}

// buildSelectSQL returns the SELECT SQL - either from RawSQL or builds from fields
func (mv *DBMaterializedView) buildSelectSQL(dbType base.DXDatabaseType) (string, error) {
	// If RawSQL is set, use it directly (for complex queries)
	if mv.RawSQL != "" {
		return mv.RawSQL, nil
	}

	// Determine source: FromTable or FromViewRef
	hasFromTable := mv.FromTable != nil
	hasFromViewRef := mv.FromViewRef != nil

	if !hasFromTable && !hasFromViewRef {
		return "", fmt.Errorf("materialized view %s: must have RawSQL, FromTable, or FromViewRef", mv.Name)
	}

	var sb strings.Builder

	// SELECT
	if mv.Distinct {
		sb.WriteString("SELECT DISTINCT\n")
	} else {
		sb.WriteString("SELECT\n")
	}

	// Columns - use ColumnExprs if available (for view references), otherwise use Columns
	if len(mv.ColumnExprs) > 0 {
		// Using string column expressions
		for i, expr := range mv.ColumnExprs {
			if i > 0 {
				sb.WriteString(",\n")
			}
			sb.WriteString("    ")
			sb.WriteString(expr)
		}
	} else if len(mv.Columns) > 0 {
		// Using DBViewColumn (requires FromTable)
		columns, err := mv.buildSelectColumns(dbType)
		if err != nil {
			return "", err
		}
		sb.WriteString(columns)
	} else {
		// Default to SELECT *
		sb.WriteString("    *")
	}

	// FROM
	if hasFromViewRef {
		// Using view reference
		fromClause := mv.FromViewRef.FullName()
		if mv.FromViewRef.Alias != "" {
			fromClause += " " + mv.FromViewRef.Alias
		}
		fmt.Fprintf(&sb, "\nFROM %s", fromClause)
	} else {
		// Using DBTable
		fmt.Fprintf(&sb, "\nFROM %s", mv.FromTable.FullTableName())
	}

	// JOINs - handle both Joins and JoinsByName
	for _, join := range mv.Joins {
		joinClause, err := mv.buildJoinClause(join)
		if err != nil {
			return "", err
		}
		sb.WriteString("\n")
		sb.WriteString(joinClause)
	}

	for _, join := range mv.JoinsByName {
		joinClause := mv.buildJoinByNameClause(join)
		sb.WriteString("\n")
		sb.WriteString(joinClause)
	}

	// WHERE
	if mv.Where != "" {
		fmt.Fprintf(&sb, "\nWHERE %s", mv.Where)
	}

	// GROUP BY
	if len(mv.GroupBy) > 0 {
		fmt.Fprintf(&sb, "\nGROUP BY %s", strings.Join(mv.GroupBy, ", "))
	}

	// HAVING
	if mv.Having != "" {
		fmt.Fprintf(&sb, "\nHAVING %s", mv.Having)
	}

	// ORDER BY
	if len(mv.OrderBy) > 0 {
		var orderParts []string
		for _, ob := range mv.OrderBy {
			orderParts = append(orderParts, fmt.Sprintf("%s %s", ob.ColumnExpr, ob.OrderByType.String()))
		}
		fmt.Fprintf(&sb, "\nORDER BY %s", strings.Join(orderParts, ", "))
	}

	return sb.String(), nil
}

// buildSelectColumns builds the SELECT column list
func (mv *DBMaterializedView) buildSelectColumns(dbType base.DXDatabaseType) (string, error) {
	if len(mv.Columns) == 0 {
		return "    *", nil
	}

	var cols []string
	for _, col := range mv.Columns {
		colStr, err := mv.buildColumnExpr(col)
		if err != nil {
			return "", err
		}
		cols = append(cols, "    "+colStr)
	}

	return strings.Join(cols, ",\n"), nil
}

// buildColumnExpr builds a single column expression
func (mv *DBMaterializedView) buildColumnExpr(col DBViewColumn) (string, error) {
	var expr string

	if col.Expression != "" {
		// Raw expression like COUNT(*), SUM(amount)
		expr = col.Expression
	} else if col.SourceField != nil {
		// Field reference
		fieldName := col.SourceField.GetName()
		if fieldName == "" {
			return "", fmt.Errorf("field has no name (not attached to table)")
		}

		if col.SourceTable != nil {
			// Field from a joined table: table.field_name
			expr = col.SourceTable.FullTableName() + "." + fieldName
		} else {
			// Field from the main table: main_table.field_name
			expr = mv.FromTable.FullTableName() + "." + fieldName
		}
	} else {
		return "", fmt.Errorf("column must have either Expression or SourceField")
	}

	// Add alias if specified
	if col.Alias != "" {
		expr = expr + " AS " + col.Alias
	}

	return expr, nil
}

// buildJoinClause builds a single JOIN clause
func (mv *DBMaterializedView) buildJoinClause(join DBJoin) (string, error) {
	// Determine target table name
	var targetTableName string
	if join.TargetTableName != "" {
		targetTableName = join.TargetTableName
	} else if join.TargetTable != nil {
		targetTableName = join.TargetTable.FullTableName()
	} else {
		return "", fmt.Errorf("join must have either TargetTable or TargetTableName")
	}

	// Get field names
	fromFieldName := join.FromLocalField.GetName()
	toFieldName := join.ToTargetField.GetName()

	if fromFieldName == "" || toFieldName == "" {
		return "", fmt.Errorf("join fields must have names")
	}

	// Determine the source table for the ON clause
	var fromTableName string
	if join.FromLocalField.Owner == mv.FromTable {
		fromTableName = mv.FromTable.FullTableName()
	} else {
		ownerTable := join.FromLocalField.Owner
		fromTableName = ownerTable.FullTableName()
	}

	// Build: INNER JOIN target_table ON from_table.from_field = target_table.to_field
	return fmt.Sprintf("%s %s ON %s.%s = %s.%s",
		join.JoinType.String(),
		targetTableName,
		fromTableName,
		fromFieldName,
		targetTableName,
		toFieldName,
	), nil
}

// buildJoinByNameClause builds a JOIN clause using view/table name references
func (mv *DBMaterializedView) buildJoinByNameClause(join DBJoinByName) string {
	// Build target reference with optional alias
	targetRef := join.TargetViewRef.FullName()
	if join.TargetViewRef.Alias != "" {
		targetRef += " " + join.TargetViewRef.Alias
	}

	// Build ON condition
	var onCondition string
	if join.OnCondition != "" {
		// Use custom ON condition
		onCondition = join.OnCondition
	} else {
		// Build from field expressions: fromFieldExpr = toFieldExpr
		onCondition = fmt.Sprintf("%s = %s", join.FromFieldExpr, join.ToFieldExpr)
	}

	return fmt.Sprintf("%s %s ON %s", join.JoinType.String(), targetRef, onCondition)
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
func (mv *DBMaterializedView) RefreshDDL(dbType base.DXDatabaseType, concurrently bool) (string, error) {
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		if concurrently && len(mv.UniqueIndexColumns) > 0 {
			return fmt.Sprintf("REFRESH MATERIALIZED VIEW CONCURRENTLY %s;\n", mv.FullMaterializedViewName()), nil
		}
		return fmt.Sprintf("REFRESH MATERIALIZED VIEW %s;\n", mv.FullMaterializedViewName()), nil
	case base.DXDatabaseTypeOracle:
		return fmt.Sprintf("BEGIN\n    DBMS_MVIEW.REFRESH('%s');\nEND;\n/\n", mv.FullMaterializedViewName()), nil
	case base.DXDatabaseTypeMariaDB:
		// MariaDB: truncate and repopulate - need SELECT SQL
		selectSQL, err := mv.buildSelectSQL(dbType)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("TRUNCATE TABLE %s;\nINSERT INTO %s %s;\n", mv.FullMaterializedViewName(), mv.FullMaterializedViewName(), selectSQL), nil
	default:
		return fmt.Sprintf("-- Refresh materialized view %s\n", mv.FullMaterializedViewName()), nil
	}
}
