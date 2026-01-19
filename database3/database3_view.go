package database3

import (
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/types"
)

// ================== JOIN TYPES ==================

type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
	JoinTypeRight
	JoinTypeFull
)

func (j JoinType) String() string {
	switch j {
	case JoinTypeInner:
		return "INNER JOIN"
	case JoinTypeLeft:
		return "LEFT JOIN"
	case JoinTypeRight:
		return "RIGHT JOIN"
	case JoinTypeFull:
		return "FULL OUTER JOIN"
	default:
		return "INNER JOIN"
	}
}

// DBJoin represents a join between two entities
type DBJoin struct {
	JoinType        JoinType
	TargetEntity    *DBEntity    // The entity to join TO
	FromLocalField  *types.Field // Field from the source/left entity
	ToTargetField   *types.Field // Field from the target/right entity
	TargetTableName string       // Optional: override target table name (for self-joins or subqueries)
}

// ================== ORDER BY ==================

type DBOrderByType int

const (
	DBOrderByTypeAsc DBOrderByType = iota
	DBOrderByTypeDesc
)

func (o DBOrderByType) String() string {
	if o == DBOrderByTypeDesc {
		return "DESC"
	}
	return "ASC"
}

type DBOrderBy struct {
	ColumnExpr  string // Can be field name or expression like "COUNT(*)"
	OrderByType DBOrderByType
}

// ================== VIEW COLUMN ==================

// DBViewColumn represents a column in the SELECT clause
type DBViewColumn struct {
	SourceEntity *DBEntity    // Which entity this column comes from (nil = from main table)
	SourceField  *types.Field // The field reference (nil if using Expression)
	Expression   string       // Raw SQL expression like "COUNT(*)", "SUM(amount)", etc.
	Alias        string       // AS alias_name (required if Expression is used)
}

// ================== AGGREGATE FUNCTIONS ==================

type DBAggregateType int

const (
	DBAggregateCount DBAggregateType = iota
	DBAggregateSum
	DBAggregateAvg
	DBAggregateMin
	DBAggregateMax
)

func (a DBAggregateType) String() string {
	switch a {
	case DBAggregateCount:
		return "COUNT"
	case DBAggregateSum:
		return "SUM"
	case DBAggregateAvg:
		return "AVG"
	case DBAggregateMin:
		return "MIN"
	case DBAggregateMax:
		return "MAX"
	default:
		return "COUNT"
	}
}

// ================== DB VIEW ==================

type DBView struct {
	Name       string
	Schema     *DBSchema
	FromEntity *DBEntity      // Main entity to select from
	Columns    []DBViewColumn // Columns to select
	Joins      []DBJoin       // Join clauses
	Where      string         // WHERE clause (without the "WHERE" keyword)
	GroupBy    []string       // GROUP BY columns (field names or expressions)
	Having     string         // HAVING clause (without the "HAVING" keyword)
	OrderBy    []DBOrderBy    // ORDER BY clause
	Distinct   bool           // SELECT DISTINCT
}

// NewDBView creates a new database view and registers it with the schema
func NewDBView(schema *DBSchema, name string, fromEntity *DBEntity) *DBView {
	view := &DBView{
		Name:       name,
		Schema:     schema,
		FromEntity: fromEntity,
		Columns:    []DBViewColumn{},
		Joins:      []DBJoin{},
		GroupBy:    []string{},
		OrderBy:    []DBOrderBy{},
	}
	return view
}

// ================== BUILDER METHODS (for chaining) ==================

// AddColumn adds a simple column from the main entity
func (v *DBView) AddColumn(field *types.Field, alias string) *DBView {
	v.Columns = append(v.Columns, DBViewColumn{
		SourceEntity: nil,
		SourceField:  field,
		Alias:        alias,
	})
	return v
}

// AddColumnFromEntity adds a column from a joined entity
func (v *DBView) AddColumnFromEntity(entity *DBEntity, field *types.Field, alias string) *DBView {
	v.Columns = append(v.Columns, DBViewColumn{
		SourceEntity: entity,
		SourceField:  field,
		Alias:        alias,
	})
	return v
}

// AddExpression adds a raw SQL expression (like COUNT(*), SUM(amount))
func (v *DBView) AddExpression(expr string, alias string) *DBView {
	v.Columns = append(v.Columns, DBViewColumn{
		Expression: expr,
		Alias:      alias,
	})
	return v
}

// AddJoin adds a join to another entity
func (v *DBView) AddJoin(joinType JoinType, targetEntity *DBEntity, fromField *types.Field, toField *types.Field) *DBView {
	v.Joins = append(v.Joins, DBJoin{
		JoinType:       joinType,
		TargetEntity:   targetEntity,
		FromLocalField: fromField,
		ToTargetField:  toField,
	})
	return v
}

// SetWhere sets the WHERE clause
func (v *DBView) SetWhere(where string) *DBView {
	v.Where = where
	return v
}

// AddGroupBy adds a GROUP BY column
func (v *DBView) AddGroupBy(columnExpr string) *DBView {
	v.GroupBy = append(v.GroupBy, columnExpr)
	return v
}

// SetHaving sets the HAVING clause
func (v *DBView) SetHaving(having string) *DBView {
	v.Having = having
	return v
}

// AddOrderBy adds an ORDER BY column
func (v *DBView) AddOrderBy(columnExpr string, orderType DBOrderByType) *DBView {
	v.OrderBy = append(v.OrderBy, DBOrderBy{
		ColumnExpr:  columnExpr,
		OrderByType: orderType,
	})
	return v
}

// SetDistinct enables SELECT DISTINCT
func (v *DBView) SetDistinct(distinct bool) *DBView {
	v.Distinct = distinct
	return v
}

// ================== DDL GENERATION ==================

// FullViewName returns the view name with a schema prefix
func (v *DBView) FullViewName() string {
	if v.Schema != nil && v.Schema.Name != "" {
		return v.Schema.Name + "." + v.Name
	}
	return v.Name
}

// CreateDDL generates the CREATE VIEW DDL statement
func (v *DBView) CreateDDL(dbType base.DXDatabaseType) (string, error) {
	var sb strings.Builder

	// CREATE VIEW
	sb.WriteString(fmt.Sprintf("CREATE VIEW %s AS\n", v.FullViewName()))

	// SELECT
	if v.Distinct {
		sb.WriteString("SELECT DISTINCT\n")
	} else {
		sb.WriteString("SELECT\n")
	}

	// Columns
	columns, err := v.buildSelectColumns(dbType)
	if err != nil {
		return "", err
	}
	sb.WriteString(columns)

	// FROM
	sb.WriteString(fmt.Sprintf("\nFROM %s", v.FromEntity.FullTableName()))

	// JOINs
	for _, join := range v.Joins {
		joinClause, err := v.buildJoinClause(join, dbType)
		if err != nil {
			return "", err
		}
		sb.WriteString("\n")
		sb.WriteString(joinClause)
	}

	// WHERE
	if v.Where != "" {
		sb.WriteString(fmt.Sprintf("\nWHERE %s", v.Where))
	}

	// GROUP BY
	if len(v.GroupBy) > 0 {
		sb.WriteString(fmt.Sprintf("\nGROUP BY %s", strings.Join(v.GroupBy, ", ")))
	}

	// HAVING
	if v.Having != "" {
		sb.WriteString(fmt.Sprintf("\nHAVING %s", v.Having))
	}

	// ORDER BY
	if len(v.OrderBy) > 0 {
		var orderParts []string
		for _, ob := range v.OrderBy {
			orderParts = append(orderParts, fmt.Sprintf("%s %s", ob.ColumnExpr, ob.OrderByType.String()))
		}
		sb.WriteString(fmt.Sprintf("\nORDER BY %s", strings.Join(orderParts, ", ")))
	}

	sb.WriteString(";\n")

	return sb.String(), nil
}

// buildSelectColumns builds the SELECT column list
func (v *DBView) buildSelectColumns(dbType base.DXDatabaseType) (string, error) {
	if len(v.Columns) == 0 {
		return "    *", nil
	}

	var cols []string
	for _, col := range v.Columns {
		colStr, err := v.buildColumnExpr(col, dbType)
		if err != nil {
			return "", err
		}
		cols = append(cols, "    "+colStr)
	}

	return strings.Join(cols, ",\n"), nil
}

// buildColumnExpr builds a single column expression
func (v *DBView) buildColumnExpr(col DBViewColumn, dbType base.DXDatabaseType) (string, error) {
	var expr string

	if col.Expression != "" {
		// Raw expression like COUNT(*), SUM(amount)
		expr = col.Expression
	} else if col.SourceField != nil {
		// Field reference
		fieldName := col.SourceField.GetName()
		if fieldName == "" {
			return "", fmt.Errorf("field has no name (not attached to entity)")
		}

		if col.SourceEntity != nil {
			// Field from a joined entity: entity_table.field_name
			expr = col.SourceEntity.FullTableName() + "." + fieldName
		} else {
			// Field from the main entity: main_table.field_name
			expr = v.FromEntity.FullTableName() + "." + fieldName
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
func (v *DBView) buildJoinClause(join DBJoin, dbType base.DXDatabaseType) (string, error) {
	// Determine a target table name
	var targetTable string
	if join.TargetTableName != "" {
		targetTable = join.TargetTableName
	} else if join.TargetEntity != nil {
		targetTable = join.TargetEntity.FullTableName()
	} else {
		return "", fmt.Errorf("join must have either TargetEntity or TargetTableName")
	}

	// Get field names
	fromFieldName := join.FromLocalField.GetName()
	toFieldName := join.ToTargetField.GetName()

	if fromFieldName == "" || toFieldName == "" {
		return "", fmt.Errorf("join fields must have names")
	}

	// Determine the source table for the ON clause
	var fromTable string
	// Check if FromLocalField belongs to FromEntity or one of the already joined entities
	if join.FromLocalField.Owner == v.FromEntity {
		fromTable = v.FromEntity.FullTableName()
	} else if ownerEntity, ok := join.FromLocalField.Owner.(*DBEntity); ok {
		fromTable = ownerEntity.FullTableName()
	} else {
		fromTable = v.FromEntity.FullTableName()
	}

	// Build: INNER JOIN target_table ON from_table.from_field = target_table.to_field
	return fmt.Sprintf("%s %s ON %s.%s = %s.%s",
		join.JoinType.String(),
		targetTable,
		fromTable,
		fromFieldName,
		targetTable,
		toFieldName,
	), nil
}

// DropDDL generates the DROP VIEW DDL statement
func (v *DBView) DropDDL(dbType base.DXDatabaseType) string {
	switch dbType {
	case base.DXDatabaseTypeSQLServer:
		return fmt.Sprintf("IF OBJECT_ID('%s', 'V') IS NOT NULL DROP VIEW %s;\n", v.FullViewName(), v.FullViewName())
	case base.DXDatabaseTypeOracle:
		return fmt.Sprintf("DROP VIEW %s;\n", v.FullViewName())
	default: // PostgreSQL, MariaDB
		return fmt.Sprintf("DROP VIEW IF EXISTS %s;\n", v.FullViewName())
	}
}

// CreateOrReplaceDDL generates CREATE OR REPLACE VIEW (where supported)
func (v *DBView) CreateOrReplaceDDL(dbType base.DXDatabaseType) (string, error) {
	switch dbType {
	case base.DXDatabaseTypeSQLServer:
		// SQL Server doesn't support CREATE OR REPLACE, use DROP + CREATE
		drop := v.DropDDL(dbType)
		create, err := v.CreateDDL(dbType)
		if err != nil {
			return "", err
		}
		return drop + create, nil
	default:
		// PostgreSQL, MariaDB, Oracle support CREATE OR REPLACE
		ddl, err := v.CreateDDL(dbType)
		if err != nil {
			return "", err
		}
		return strings.Replace(ddl, "CREATE VIEW", "CREATE OR REPLACE VIEW", 1), nil
	}
}
