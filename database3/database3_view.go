package database3

import (
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
)

// ================== VIEW/TABLE REFERENCE ==================

// DBViewRef represents a reference to a view or table by name (for use in FROM/JOIN)
// Use this when you need to reference a view or table that is not defined as DBTable
type DBViewRef struct {
	Schema *DBSchema // Schema pointer (e.g., PartnerManagementSchema)
	Name   string    // View/table name (e.g., "v_field_executor")
	Alias  string    // Optional alias for use in queries (e.g., "vfe")
}

// NewDBViewRef creates a new view reference using schema pointer and name string
// Use this when the view/table is not defined as a Go variable
func NewDBViewRef(schema *DBSchema, name, alias string) *DBViewRef {
	return &DBViewRef{
		Schema: schema,
		Name:   name,
		Alias:  alias,
	}
}

// NewDBViewRefFromTable creates a view reference from an existing DBTable
// Use this when referencing a table that is defined as *DBTable in Go
func NewDBViewRefFromTable(table *DBTable, alias string) *DBViewRef {
	return &DBViewRef{
		Schema: table.Schema,
		Name:   table.Name,
		Alias:  alias,
	}
}

// NewDBViewRefFromView creates a view reference from an existing DBView
// Use this when referencing a view that is defined as *DBView in Go
func NewDBViewRefFromView(view *DBView, alias string) *DBViewRef {
	return &DBViewRef{
		Schema: view.Schema,
		Name:   view.Name,
		Alias:  alias,
	}
}

// FullName returns schema.name
func (v *DBViewRef) FullName() string {
	if v.Schema != nil && v.Schema.Name != "" {
		return v.Schema.Name + "." + v.Name
	}
	return v.Name
}

// RefName returns the alias if set, otherwise the full name (for use in SELECT/JOIN ON)
func (v *DBViewRef) RefName() string {
	if v.Alias != "" {
		return v.Alias
	}
	return v.FullName()
}

// ================== JOIN BY NAME ==================

// DBJoinByName represents a join using string-based table/field names
// Use this when joining to views or when *Field references are not available
type DBJoinByName struct {
	JoinType      JoinType
	TargetViewRef *DBViewRef // The view/table to join TO
	FromFieldExpr string     // Field expression from source (e.g., "vfe.id" or just "id")
	ToFieldExpr   string     // Field expression from target (e.g., "fee.user_role_membership_id")
	OnCondition   string     // Optional: full ON condition override (e.g., "a.id = b.id AND a.type = b.type")
}

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

// DBJoin represents a join between two tables
type DBJoin struct {
	JoinType        JoinType
	TargetTable     *DBTable // The table to join TO
	FromLocalField  *Field   // Field from the source/left table
	ToTargetField   *Field   // Field from the target/right table
	TargetTableName string   // Optional: override target table name (for self-joins or subqueries)
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
	SourceTable *DBTable // Which table this column comes from (nil = from main table)
	SourceField *Field   // The field reference (nil if using Expression)
	Expression  string   // Raw SQL expression like "COUNT(*)", "SUM(amount)", etc.
	Alias       string   // AS alias_name (required if Expression is used)
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
	DBEntity                 // Embedded base entity (Name, Type, Order, Schema)
	FromTable *DBTable       // Main table to select from
	Columns   []DBViewColumn // Columns to select
	Joins     []DBJoin       // Join clauses
	Where     string         // WHERE clause (without the "WHERE" keyword)
	GroupBy   []string       // GROUP BY columns (field names or expressions)
	Having    string         // HAVING clause (without the "HAVING" keyword)
	OrderBy   []DBOrderBy    // ORDER BY clause
	Distinct  bool           // SELECT DISTINCT
	RawSQL    string         // Raw SQL for complex views (bypasses builder when set)
}

// NewDBView creates a new database view and registers it with the schema
func NewDBView(schema *DBSchema, name string, fromTable *DBTable) *DBView {
	view := &DBView{
		DBEntity: DBEntity{
			Name:   name,
			Type:   DBEntityTypeView,
			Order:  0,
			Schema: schema,
		},
		FromTable: fromTable,
		Columns:   []DBViewColumn{},
		Joins:     []DBJoin{},
		GroupBy:   []string{},
		OrderBy:   []DBOrderBy{},
	}
	if schema != nil {
		schema.Views = append(schema.Views, view)
	}
	return view
}

// NewDBViewRawSQL creates a database view with raw SQL definition
// Use this for complex views that are difficult to express with the builder pattern
func NewDBViewRawSQL(schema *DBSchema, name string, rawSQL string) *DBView {
	view := &DBView{
		DBEntity: DBEntity{
			Name:   name,
			Type:   DBEntityTypeView,
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
func (v *DBView) AddColumn(field *Field, alias string) *DBView {
	v.Columns = append(v.Columns, DBViewColumn{
		SourceTable: nil,
		SourceField: field,
		Alias:       alias,
	})
	return v
}

// AddColumnFromTable adds a column from a joined table
func (v *DBView) AddColumnFromTable(table *DBTable, field *Field, alias string) *DBView {
	v.Columns = append(v.Columns, DBViewColumn{
		SourceTable: table,
		SourceField: field,
		Alias:       alias,
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

// AddJoin adds a join to another table
func (v *DBView) AddJoin(joinType JoinType, targetTable *DBTable, fromField *Field, toField *Field) *DBView {
	v.Joins = append(v.Joins, DBJoin{
		JoinType:       joinType,
		TargetTable:    targetTable,
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

// SetOrder sets the view Order (for global view creation ordering)
func (v *DBView) SetOrder(order int) *DBView {
	v.Order = order
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

// AddDecryptedColumn adds a column that decrypts an encrypted physical column.
// encryptedColumn: the physical column name (e.g., "fullname_encrypted")
// alias: the name to expose in the view (e.g., "fullname")
// keyConfigName: the session config key for the encryption key (e.g., "app.encryption_key")
func (v *DBView) AddDecryptedColumn(encryptedColumn string, alias string, keyConfigName string) *DBView {
	// Store the decryption expression - will be built in CreateDDL based on dbType
	expr := fmt.Sprintf("DECRYPT(%s, %s)", encryptedColumn, keyConfigName)
	v.Columns = append(v.Columns, DBViewColumn{
		Expression: expr,
		Alias:      alias,
	})
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

// buildDecryptExpr converts DECRYPT(column, keyConfig) placeholder to database-specific decryption
func (v *DBView) buildDecryptExpr(placeholder string, dbType base.DXDatabaseType) (string, error) {
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
func (v *DBView) buildJoinClause(join DBJoin, dbType base.DXDatabaseType) (string, error) {
	// Determine a target table name
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
