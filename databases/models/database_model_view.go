package models

import (
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
)

// ================== VIEW/TABLE REFERENCE ==================

// DBViewRef represents a reference to a view or table by name (for use in FROM/JOIN)
// Use this when you need to reference a view or table that is not defined as ModelDBTable
type ModelDBViewRef struct {
	Schema *ModelDBSchema // Schema pointer (e.g., PartnerManagementSchema)
	Name   string         // View/table name (e.g., "v_field_executor")
	Alias  string         // Optional alias for use in queries (e.g., "vfe")
}

// NewDBViewRef creates a new view reference using schema pointer and name string
// Use this when the view/table is not defined as a Go variable
func NewModelDBViewRef(schema *ModelDBSchema, name, alias string) *ModelDBViewRef {
	return &ModelDBViewRef{
		Schema: schema,
		Name:   name,
		Alias:  alias,
	}
}

// NewDBViewRefFromTable creates a view reference from an existing ModelDBTable
// Use this when referencing a table that is defined as *ModelDBTable in Go
func NewModelDBViewRefFromTable(table *ModelDBTable, alias string) *ModelDBViewRef {
	return &ModelDBViewRef{
		Schema: table.Schema,
		Name:   table.Name,
		Alias:  alias,
	}
}

// NewDBViewRefFromView creates a view reference from an existing ModelDBView
// Use this when referencing a view that is defined as *ModelDBView in Go
func NewModelDBViewRefFromView(view *ModelDBView, alias string) *ModelDBViewRef {
	return &ModelDBViewRef{
		Schema: view.Schema,
		Name:   view.Name,
		Alias:  alias,
	}
}

// FullName returns schema.name
func (v *ModelDBViewRef) FullName() string {
	if v.Schema != nil && v.Schema.Name != "" {
		return v.Schema.Name + "." + v.Name
	}
	return v.Name
}

// RefName returns the alias if set, otherwise the full name (for use in SELECT/JOIN ON)
func (v *ModelDBViewRef) RefName() string {
	if v.Alias != "" {
		return v.Alias
	}
	return v.FullName()
}

// ================== JOIN BY NAME ==================

// ModelDBJoinByName represents a join using string-based table/field names
// Use this when joining to views or when *ModelDBField references are not available
type ModelDBJoinByName struct {
	JoinType      ModelDBJoinType
	TargetViewRef *ModelDBViewRef // The view/table to join TO
	FromFieldExpr string          // ModelDBField expression from source (e.g., "vfe.id" or just "id")
	ToFieldExpr   string          // ModelDBField expression from target (e.g., "fee.user_role_membership_id")
	OnCondition   string          // Optional: full ON condition override (e.g., "a.id = b.id AND a.type = b.type")
}

// ================== JOIN TYPES ==================

type ModelDBJoinType int

const (
	ModelDBJoinTypeInner ModelDBJoinType = iota
	ModelDBJoinTypeLeft
	ModelDBJoinTypeRight
	ModelDBJoinTypeFull
)

func (j ModelDBJoinType) String() string {
	switch j {
	case ModelDBJoinTypeInner:
		return "INNER JOIN"
	case ModelDBJoinTypeLeft:
		return "LEFT JOIN"
	case ModelDBJoinTypeRight:
		return "RIGHT JOIN"
	case ModelDBJoinTypeFull:
		return "FULL OUTER JOIN"
	default:
		return "INNER JOIN"
	}
}

// ModelDBJoin represents a join between two tables or views
type ModelDBJoin struct {
	JoinType        ModelDBJoinType
	TargetTable     *ModelDBEntity // The table or view to join TO (changed from *ModelDBTable to support views)
	TargetAlias     string         // Optional: alias for the target table/view (e.g., "st", "st2" for multiple joins)
	FromLocalField  *ModelDBField  // ModelDBField from the source/left table
	ToTargetField   *ModelDBField  // ModelDBField from the target/right table
	TargetTableName string         // Optional: override target table name (for self-joins or subqueries)
}

// ================== ORDER BY ==================

type ModelDBOrderByType int

const (
	ModelDBOrderByTypeAsc ModelDBOrderByType = iota
	ModelDBOrderByTypeDesc
)

func (o ModelDBOrderByType) String() string {
	if o == ModelDBOrderByTypeDesc {
		return "DESC"
	}
	return "ASC"
}

type ModelDBOrderBy struct {
	ColumnExpr  string // Can be field name or expression like "COUNT(*)"
	OrderByType ModelDBOrderByType
}

// ================== VIEW COLUMN ==================

// DBViewColumn represents a column in the SELECT clause
type ModelDBViewColumn struct {
	SourceTable *ModelDBEntity // Which table this column comes from (nil = from main table)
	SourceField *ModelDBField  // The field reference (nil if using Expression)
	Expression  string         // Raw SQL expression like "COUNT(*)", "SUM(amount)", etc.
	Alias       string         // AS alias_name (required if Expression is used)
}

// ================== AGGREGATE FUNCTIONS ==================

type ModelDBAggregateType int

const (
	ModelDBAggregateCount ModelDBAggregateType = iota
	ModelDBAggregateSum
	ModelDBAggregateAvg
	ModelDBAggregateMin
	ModelDBAggregateMax
)

func (a ModelDBAggregateType) String() string {
	switch a {
	case ModelDBAggregateCount:
		return "COUNT"
	case ModelDBAggregateSum:
		return "SUM"
	case ModelDBAggregateAvg:
		return "AVG"
	case ModelDBAggregateMin:
		return "MIN"
	case ModelDBAggregateMax:
		return "MAX"
	default:
		return "COUNT"
	}
}

// ================== ModelDB VIEW ==================

type ModelDBView struct {
	ModelDBEntity                     // Embedded base entity (Name, Type, Order, Schema)
	FromTable     *ModelDBTable       // Main table to select from
	Columns       []ModelDBViewColumn // Columns to select
	Joins         []ModelDBJoin       // Join clauses
	Where         string              // WHERE clause (without the "WHERE" keyword)
	GroupBy       []string            // GROUP BY columns (field names or expressions)
	Having        string              // HAVING clause (without the "HAVING" keyword)
	OrderBy       []ModelDBOrderBy    // ORDER BY clause
	Distinct      bool                // SELECT DISTINCT
	RawSQL        string              // Raw SQL for complex views (bypasses builder when set)
}

// NewDBView creates a new databases view and registers it with the schema
func NewModelDBView(schema *ModelDBSchema, name string, fromTable *ModelDBTable) *ModelDBView {
	view := &ModelDBView{
		ModelDBEntity: ModelDBEntity{
			Name:   name,
			Type:   ModelDBEntityTypeView,
			Order:  0,
			Schema: schema,
		},
		FromTable: fromTable,
		Columns:   []ModelDBViewColumn{},
		Joins:     []ModelDBJoin{},
		GroupBy:   []string{},
		OrderBy:   []ModelDBOrderBy{},
	}
	if schema != nil {
		schema.Views = append(schema.Views, view)
	}
	return view
}

// NewDBViewRawSQL creates a databases view with raw SQL definition
// Use this for complex views that are difficult to express with the builder pattern
func NewModelDBViewRawSQL(schema *ModelDBSchema, name string, rawSQL string) *ModelDBView {
	view := &ModelDBView{
		ModelDBEntity: ModelDBEntity{
			Name:   name,
			Type:   ModelDBEntityTypeView,
			Order:  0,
			Schema: schema,
		},
		RawSQL: rawSQL,
	}
	if schema != nil {
		schema.Views = append(schema.Views, view)
	}
	return view
}

// ================== BUILDER METHODS (for chaining) ==================

// AddColumn adds a simple column from the main table
func (v *ModelDBView) AddColumn(field *ModelDBField, alias string) *ModelDBView {
	v.Columns = append(v.Columns, ModelDBViewColumn{
		SourceTable: nil,
		SourceField: field,
		Alias:       alias,
	})
	return v
}

// AddColumnFromTable adds a column from a joined table
func (v *ModelDBView) AddColumnFromTable(table *ModelDBEntity, field *ModelDBField, alias string) *ModelDBView {
	v.Columns = append(v.Columns, ModelDBViewColumn{
		SourceTable: table,
		SourceField: field,
		Alias:       alias,
	})
	return v
}

// AddExpression adds a raw SQL expression (like COUNT(*), SUM(amount))
func (v *ModelDBView) AddExpression(expr string, alias string) *ModelDBView {
	v.Columns = append(v.Columns, ModelDBViewColumn{
		Expression: expr,
		Alias:      alias,
	})
	return v
}

// AddJoin adds a join to another table
func (v *ModelDBView) AddJoin(joinType ModelDBJoinType, targetTable *ModelDBEntity, fromField *ModelDBField, toField *ModelDBField) *ModelDBView {
	v.Joins = append(v.Joins, ModelDBJoin{
		JoinType:       joinType,
		TargetTable:    targetTable,
		FromLocalField: fromField,
		ToTargetField:  toField,
	})
	return v
}

// SetWhere sets the WHERE clause
func (v *ModelDBView) SetWhere(where string) *ModelDBView {
	v.Where = where
	return v
}

// SetOrder sets the view Order (for global view creation ordering)
func (v *ModelDBView) SetOrder(order int) *ModelDBView {
	v.Order = order
	return v
}

// AddGroupBy adds a GROUP BY column
func (v *ModelDBView) AddGroupBy(columnExpr string) *ModelDBView {
	v.GroupBy = append(v.GroupBy, columnExpr)
	return v
}

// SetHaving sets the HAVING clause
func (v *ModelDBView) SetHaving(having string) *ModelDBView {
	v.Having = having
	return v
}

// AddOrderBy adds an ORDER BY column
func (v *ModelDBView) AddOrderBy(columnExpr string, orderType ModelDBOrderByType) *ModelDBView {
	v.OrderBy = append(v.OrderBy, ModelDBOrderBy{
		ColumnExpr:  columnExpr,
		OrderByType: orderType,
	})
	return v
}

// SetDistinct enables SELECT DISTINCT
func (v *ModelDBView) SetDistinct(distinct bool) *ModelDBView {
	v.Distinct = distinct
	return v
}

// AddDecryptedColumn adds a column that decrypts an encrypted physical column.
// encryptedColumn: the physical column name (e.g., "fullname_encrypted")
// alias: the name to expose in the view (e.g., "fullname")
// keyConfigName: the session config key for the encryption key (e.g., "app.encryption_key")
func (v *ModelDBView) AddDecryptedColumn(encryptedColumn string, alias string, keyConfigName string) *ModelDBView {
	// Store the decryption expression - will be built in CreateDDL based on dbType
	expr := fmt.Sprintf("DECRYPT(%s, %s)", encryptedColumn, keyConfigName)
	v.Columns = append(v.Columns, ModelDBViewColumn{
		Expression: expr,
		Alias:      alias,
	})
	return v
}

// ================== DDL GENERATION ==================

// FullViewName returns the view name with a schema prefix
func (v *ModelDBView) FullViewName() string {
	if v.Schema != nil && v.Schema.Name != "" {
		return v.Schema.Name + "." + v.Name
	}
	return v.Name
}

// CreateDDL generates the CREATE VIEW DDL statement
func (v *ModelDBView) CreateDDL(dbType base.DXDatabaseType) (string, error) {
	// If RawSQL is set, use it directly
	if v.RawSQL != "" {
		return fmt.Sprintf("CREATE VIEW %s AS\n%s;\n", v.FullViewName(), v.RawSQL), nil
	}

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
	sb.WriteString(fmt.Sprintf("\nFROM %s", v.FromTable.FullTableName()))

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
func (v *ModelDBView) buildSelectColumns(dbType base.DXDatabaseType) (string, error) {
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
func (v *ModelDBView) buildColumnExpr(col ModelDBViewColumn, dbType base.DXDatabaseType) (string, error) {
	var expr string

	if col.Expression != "" {
		// Check if it's a DECRYPT placeholder expression
		if strings.HasPrefix(col.Expression, "DECRYPT(") {
			decryptExpr, err := v.buildDecryptExpr(col.Expression, dbType)
			if err != nil {
				return "", err
			}
			expr = decryptExpr
		} else {
			// Raw expression like COUNT(*), SUM(amount)
			expr = col.Expression
		}
	} else if col.SourceField != nil {
		// ModelDBField reference
		fieldName := col.SourceField.GetName()
		if fieldName == "" {
			return "", fmt.Errorf("field has no name (not attached to table)")
		}

		if col.SourceTable != nil {
			// ModelDBField from a joined table: table.field_name
			expr = col.SourceTable.FullName() + "." + fieldName
		} else {
			// ModelDBField from the main table: main_table.field_name
			expr = v.FromTable.FullTableName() + "." + fieldName
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

// buildDecryptExpr converts DECRYPT(column, keyConfig) placeholder to databases-specific decryption
func (v *ModelDBView) buildDecryptExpr(placeholder string, dbType base.DXDatabaseType) (string, error) {
	// Parse DECRYPT(column, keyConfig)
	inner := strings.TrimPrefix(placeholder, "DECRYPT(")
	inner = strings.TrimSuffix(inner, ")")
	parts := strings.SplitN(inner, ", ", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid DECRYPT expression: %s", placeholder)
	}
	encColumn := parts[0]
	keyConfigName := parts[1]

	keyExpr := BuildGetSessionConfigExpr(dbType, keyConfigName)
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return fmt.Sprintf("pgp_sym_decrypt(%s, %s)", encColumn, keyExpr), nil
	case base.DXDatabaseTypeSQLServer:
		return fmt.Sprintf("CONVERT(VARCHAR(MAX), DecryptByPassPhrase(%s, %s))", keyExpr, encColumn), nil
	case base.DXDatabaseTypeMariaDB:
		return fmt.Sprintf("AES_DECRYPT(%s, %s)", encColumn, keyExpr), nil
	case base.DXDatabaseTypeOracle:
		return fmt.Sprintf("UTL_RAW.CAST_TO_VARCHAR2(DBMS_CRYPTO.DECRYPT(%s, DBMS_CRYPTO.ENCRYPT_AES256 + DBMS_CRYPTO.CHAIN_CBC + DBMS_CRYPTO.PAD_PKCS5, UTL_RAW.CAST_TO_RAW(%s)))", encColumn, keyExpr), nil
	default:
		return encColumn, nil
	}
}

// buildJoinClause builds a single JOIN clause
func (v *ModelDBView) buildJoinClause(join ModelDBJoin, dbType base.DXDatabaseType) (string, error) {
	// Determine a target table name
	var targetTableName string
	if join.TargetTableName != "" {
		targetTableName = join.TargetTableName
	} else if join.TargetTable != nil {
		targetTableName = join.TargetTable.FullName()
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
	// Check if FromLocalField belongs to FromTable or one of the already joined tables
	if join.FromLocalField.Owner == v.FromTable {
		fromTableName = v.FromTable.FullTableName()
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

// DropDDL generates the DROP VIEW DDL statement
func (v *ModelDBView) DropDDL(dbType base.DXDatabaseType) string {
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
func (v *ModelDBView) CreateOrReplaceDDL(dbType base.DXDatabaseType) (string, error) {
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
