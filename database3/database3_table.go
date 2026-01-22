package database3

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/types"
	"github.com/donnyhardyanto/dxlib/utils"
)

// ============================================================================
// DBTable - Table entity (embeds DBEntity)
// ============================================================================

type KeySource int

const (
	KeySourceRaw KeySource = iota
	KeySourceEnv
	KeySourceConfig
	KeySourceDbSessionCurrentSetting
)

type Field struct {
	Owner                  *DBTable
	Order                  int
	Type                   types.DataType
	IsPrimaryKey           bool
	IsAutoIncrement        bool
	IsNotNull              bool
	IsUnique               bool
	DefaultValue           any                         // SQL expression for DEFAULT clause (used when DefaultValueByDBType not specified)
	DefaultValueByDBType   map[base.DXDatabaseType]any // Database-specific default values. Keys: "postgresql", "sqlserver", "oracle", "mariadb"
	References             string                      // Foreign key reference in format "schema.table.field"
	ResolvedReferenceField *Field                      // Resolved reference field pointer (set by NewDBTable)

	// for encrypted field only
	DecryptedFieldName  string
	EncryptionKeySource KeySource // "config" (PostgreSQL current_setting), "literal", "env"
	EncryptionKeyValue  string    // e.g., "app.encryption_key" for config, or literal key value
	HashFieldName       string    // e.g., "fullname_hashed" - companion hash field name
	HashSaltKeySource   KeySource // "config", "literal", "env"
	HashSaltKeyValue    string    // salt value or config name
}

func (f *Field) GetName() string {
	if f.Owner == nil {
		return ""
	}
	for k, v := range f.Owner.Fields {
		if v == f {
			return k
		}
	}
	return ""
}

type DBTable struct {
	DBEntity
	Fields            map[string]*Field
	TDE               TDEConfig // Database-specific TDE configuration
	UseTableSuffix    bool
	PhysicalTableName string
	Indexes           []*DBIndex   // Indexes on this table
	Triggers          []*DBTrigger // Triggers on this table
}

// NewDBTable creates a new database table and registers it with the schema.
// Note: Field References are resolved lazily by DB.Init() using the Order field.
func NewDBTable(schema *DBSchema, name string, order int, fields map[string]*Field, tde TDEConfig) *DBTable {
	dbTable := &DBTable{
		DBEntity: DBEntity{
			Name:   name,
			Type:   DBEntityTypeTable,
			Order:  order,
			Schema: schema,
		},
		Fields: fields,
		TDE:    tde,
	}
	if schema != nil {
		schema.Tables = append(schema.Tables, dbTable)
	}
	for _, field := range dbTable.Fields {
		field.Owner = dbTable
	}
	return dbTable
}

// ============================================================================
// DBTable Methods
// ============================================================================

// TableName returns the physical table name (without a schema prefix)
func (t *DBTable) TableName() string {
	if t.PhysicalTableName != "" {
		return t.PhysicalTableName
	}
	if t.UseTableSuffix {
		return t.Name + "_t"
	}
	return t.Name
}

// FullTableName returns the table name with schema prefix if schema is set
func (t *DBTable) FullTableName() string {
	if t.Schema != nil && t.Schema.Name != "" {
		return t.Schema.Name + "." + t.TableName()
	}
	return t.TableName()
}

// getOrderedFields returns field names sorted by Order
func (t *DBTable) getOrderedFields() []string {
	type fieldOrder struct {
		name  string
		order int
	}
	var fields []fieldOrder
	for name, field := range t.Fields {
		fields = append(fields, fieldOrder{name: name, order: field.Order})
	}
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].order < fields[j].order
	})
	var names []string
	for _, f := range fields {
		names = append(names, f.name)
	}
	return names
}

// CreateDDL generates a DDL script for the table based on database type
func (t *DBTable) CreateDDL(dbType base.DXDatabaseType) (string, error) {
	var sb strings.Builder
	tableName := t.FullTableName()

	sb.WriteString(fmt.Sprintf("CREATE TABLE"+" %s (\n", tableName))

	var columns []string
	for _, fieldName := range t.getOrderedFields() {
		field := t.Fields[fieldName]
		colDef := t.fieldToDDL(fieldName, *field, dbType)
		columns = append(columns, colDef)
	}

	sb.WriteString("    " + strings.Join(columns, ",\n    "))
	sb.WriteString("\n)")

	// Add database-specific TDE options
	sb.WriteString(t.buildTDEClause(dbType))

	sb.WriteString(";\n")

	return sb.String(), nil
}

// buildTDEClause generates the database-specific TDE clause for CREATE TABLE
func (t *DBTable) buildTDEClause(dbType base.DXDatabaseType) string {
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		// PostgreSQL: Use table access method for TDE (t.g., "tde_heap" with pg_tde extension)
		if t.TDE.PostgreSQLAccessMethod != "" {
			return fmt.Sprintf(" USING %s", t.TDE.PostgreSQLAccessMethod)
		}
	case base.DXDatabaseTypeOracle:
		// Oracle: Specify tablespace for encrypted storage
		if t.TDE.OracleTablespace != "" {
			return fmt.Sprintf(" TABLESPACE %s", t.TDE.OracleTablespace)
		}
	case base.DXDatabaseTypeSQLServer:
		// SQL Server: TDE is database-level, no per-table syntax
		// Add a comment to indicate TDE expectation if enabled
		if t.TDE.SQLServerTDEEnabled {
			return " /* TDE enabled at database level */"
		}
	case base.DXDatabaseTypeMariaDB:
		// MariaDB/MySQL: Use an ENCRYPTION table option for InnoDB
		if t.TDE.MariaDBEncryption == "Y" {
			return " ENCRYPTION='Y'"
		}
	default:
		// No TDE support for unknown database types
	}
	return ""
}

func (t *DBTable) fieldToDDL(fieldName string, field Field, dbType base.DXDatabaseType) string {

	var sb strings.Builder

	// Get the SQL data type for this database type
	sqlType := ""
	if field.Type.TypeByDatabaseType != nil {
		sqlType = field.Type.TypeByDatabaseType[dbType]
	}
	if sqlType == "" {
		sqlType = "TEXT" // Fallback if no type mapping exists
	}

	sb.WriteString(fmt.Sprintf("%s %s", fieldName, sqlType))

	// Add PRIMARY KEY constraint
	if field.IsPrimaryKey {
		sb.WriteString(" PRIMARY KEY")
	}

	// Add NOT NULL constraint
	if field.IsNotNull && !field.IsPrimaryKey { // PRIMARY KEY implies NOT NULL
		sb.WriteString(" NOT NULL")
	}

	// Add UNIQUE constraint
	if field.IsUnique && !field.IsPrimaryKey { // PRIMARY KEY implies UNIQUE
		sb.WriteString(" UNIQUE")
	}

	// Add DEFAULT value - check database-specific default first
	defaultValue := t.getDefaultValueForDBType(field, dbType)
	if defaultValue != "" {
		sb.WriteString(fmt.Sprintf(" DEFAULT %s", defaultValue))
	}

	// Add REFERENCES constraint for foreign keys
	if field.References != "" {
		// References format: "schema.table.field"
		parts := strings.Split(field.References, ".")
		if len(parts) == 3 {
			sb.WriteString(fmt.Sprintf(" REFERENCES %s.%s (%s)",
				parts[0], parts[1], parts[2]))
		}
	}

	return sb.String()
}

// isStringFieldType checks if the field type is a string type that should be quoted in SQL
func isStringFieldType(field Field) bool {
	switch field.Type.GoType {
	case types.GoTypeString, types.GoTypeStringPointer:
		return true
	default:
		return false
	}
}

// getDefaultValueForDBType returns the appropriate default value for the given database type
// Priority: 1. Field.DefaultValueByDBType, 2. Field.DefaultValue, 3. Field.Type.DefaultValueByDatabaseType
// For string fields, the value will be automatically quoted with SQL single quotes
func (t *DBTable) getDefaultValueForDBType(field Field, dbType base.DXDatabaseType) string {

	// 1. Check if the field has a database-specific default
	if field.DefaultValueByDBType != nil {
		if dbDefault, ok := field.DefaultValueByDBType[dbType]; ok && dbDefault != nil {
			return valueToSQLLiteral(field, dbDefault)
		}
	}

	// 2. Check field's generic default value
	if field.DefaultValue != nil {
		return valueToSQLLiteral(field, field.DefaultValue)
	}

	// 3. Check DataType's database-specific default (t.g., DataTypeUID) - only if IsAutoIncrement is true
	if field.IsAutoIncrement && field.Type.DefaultValueByDatabaseType != nil {
		if dbDefault, ok := field.Type.DefaultValueByDatabaseType[dbType]; ok && dbDefault != "" {
			return dbDefault
		}
	}

	return ""
}

// valueToSQLLiteral converts a value to SQL literal format based on field type
// For string fields, the value is wrapped with SQL single quotes
// For other types, the value is converted to string as-is
func valueToSQLLiteral(field Field, v any) string {
	if v == nil {
		return ""
	}

	// For string field types, wrap with SQL single quotes
	if isStringFieldType(field) {
		if strVal, ok := v.(string); ok {
			// Escape single quotes by doubling them
			escaped := strings.ReplaceAll(strVal, "'", "''")
			return fmt.Sprintf("'%s'", escaped)
		}
	}

	// For non-string types, use anyToString
	return anyToString(v)
}

// anyToString converts any value to string for DDL generation
func anyToString(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32, float64:
		return fmt.Sprintf("%v", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", val)
	}
}

// SelectOne selects a single row from the table
func (t *DBTable) SelectOne(db *sql.DB, dbType base.DXDatabaseType, where string, args ...any) (utils.JSON, error) {
	columns := t.buildSelectColumns()
	tableName := t.FullTableName()

	var query string
	if dbType == base.DXDatabaseTypeSQLServer {
		// language=text
		query = fmt.Sprintf("SELECT TOP 1 %s FROM %s WHERE %s", columns, tableName, where)
	} else {
		// language=text
		query = fmt.Sprintf("SELECT %s FROM %s WHERE %s LIMIT 1", columns, tableName, where)
	}
	row := db.QueryRow(query, args...)
	return t.scanRow(row)
}

// SelectMany selects multiple rows from the table
func (t *DBTable) SelectMany(db *sql.DB, dbType base.DXDatabaseType, where string, args ...any) ([]utils.JSON, error) {
	columns := t.buildSelectColumns()
	tableName := t.FullTableName()

	// language=text
	query := fmt.Sprintf("SELECT %s FROM %s", columns, tableName)
	if where != "" {
		query += " WHERE " + where
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			// suppress it
			return
		}
	}(rows)

	var results []utils.JSON
	for rows.Next() {
		result, err := t.scanRows(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

// Insert inserts a new row into the table
func (t *DBTable) Insert(db *sql.DB, dbType base.DXDatabaseType, data utils.JSON) error {
	columns, values, args, err := t.buildInsertData(dbType, data)
	if err != nil {
		return err
	}

	tableName := t.FullTableName()
	// language=text
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, values)
	_, err = db.Exec(query, args...)
	return err
}

// Update updates existing rows in the table
func (t *DBTable) Update(db *sql.DB, dbType base.DXDatabaseType, data utils.JSON, where string, whereArgs ...any) error {
	setClause, args, err := t.buildUpdateData(dbType, data)
	if err != nil {
		return err
	}

	args = append(args, whereArgs...)
	tableName := t.FullTableName()
	// language=text
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tableName, setClause, where)
	_, err = db.Exec(query, args...)
	return err
}

func (t *DBTable) Delete(db *sql.DB, where string, args ...any) error {
	tableName := t.FullTableName()
	// language=text
	query := fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, where)
	_, err := db.Exec(query, args...)
	return err
}

// buildSelectColumns returns column names for SELECT
func (t *DBTable) buildSelectColumns() string {
	return strings.Join(t.getOrderedFields(), ", ")
}

func (t *DBTable) buildInsertData(dbType base.DXDatabaseType, data utils.JSON) (columns string, values string, args []any, err error) {
	var cols []string
	var vals []string
	argIndex := 1

	// Track which decrypted field names we've processed (to avoid duplicates)
	processedDecryptedFields := make(map[string]bool)

	for _, fieldName := range t.getOrderedFields() {
		field := t.Fields[fieldName]

		// Check if this is an encrypted field with DecryptedFieldName
		if field.DecryptedFieldName != "" && field.EncryptionKeyValue != "" {
			// Look for value using DecryptedFieldName (e.g., "fullname")
			val, ok := data[field.DecryptedFieldName]
			if !ok {
				continue
			}

			// Skip if already processed
			if processedDecryptedFields[field.DecryptedFieldName] {
				continue
			}
			processedDecryptedFields[field.DecryptedFieldName] = true

			// Add an encrypted column
			cols = append(cols, fieldName)
			vals = append(vals, t.encryptExpression(dbType, argIndex, field.EncryptionKeySource, field.EncryptionKeyValue))
			args = append(args, val)
			argIndex++

			// Add hash column if specified
			if field.HashFieldName != "" {
				cols = append(cols, field.HashFieldName)
				vals = append(vals, t.hashExpression(dbType, argIndex, field.HashSaltKeySource, field.HashSaltKeyValue))
				args = append(args, val)
				argIndex++
			}
			continue
		}

		// Regular field - check if value exists in data
		val, ok := data[fieldName]
		if !ok {
			continue
		}

		// Skip hash fields that are auto-generated (they will be added by encrypted field processing)
		if t.isAutoGeneratedHashField(fieldName) {
			continue
		}

		// Validate incoming value matches expected type
		if err := t.validateFieldValue(fieldName, field, val); err != nil {
			return "", "", nil, err
		}

		cols = append(cols, fieldName)
		vals = append(vals, t.placeholder(dbType, argIndex))
		args = append(args, val)
		argIndex++
	}

	return strings.Join(cols, ", "), strings.Join(vals, ", "), args, nil
}

// isAutoGeneratedHashField checks if a field is a hash field makauto-generated by an encrypted field
func (t *DBTable) isAutoGeneratedHashField(fieldName string) bool {
	for _, field := range t.Fields {
		if field.HashFieldName == fieldName {
			return true
		}
	}
	return false
}

// encryptExpression returns the database-specific encryption expression
func (t *DBTable) encryptExpression(dbType base.DXDatabaseType, argIndex int, keySource KeySource, keyValue string) string {
	placeholder := t.placeholder(dbType, argIndex)
	keyExpr := t.keyExpression(dbType, keySource, keyValue)

	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return fmt.Sprintf("pgp_sym_encrypt(%s, %s)", placeholder, keyExpr)
	case base.DXDatabaseTypeSQLServer:
		return fmt.Sprintf("ENCRYPTBYPASSPHRASE(%s, %s)", keyExpr, placeholder)
	case base.DXDatabaseTypeOracle:
		// Oracle requires DBMS_CRYPTO package - simplified version
		return fmt.Sprintf("UTL_RAW.CAST_TO_RAW(%s)", placeholder)
	case base.DXDatabaseTypeMariaDB:
		return fmt.Sprintf("AES_ENCRYPT(%s, %s)", placeholder, keyExpr)
	default:
		return placeholder
	}
}

// hashExpression returns the database-specific hash expression with optional salt
func (t *DBTable) hashExpression(dbType base.DXDatabaseType, argIndex int, saltSource KeySource, saltValue string) string {
	placeholder := t.placeholder(dbType, argIndex)

	// If salt is specified, concatenate salt with value
	valueExpr := placeholder
	if saltValue != "" {
		saltExpr := t.keyExpression(dbType, saltSource, saltValue)
		valueExpr = t.concatExpression(dbType, saltExpr, placeholder)
	}

	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return fmt.Sprintf("digest(%s, 'sha256')", valueExpr)
	case base.DXDatabaseTypeSQLServer:
		return fmt.Sprintf("HASHBYTES('SHA2_256', %s)", valueExpr)
	case base.DXDatabaseTypeOracle:
		return fmt.Sprintf("DBMS_CRYPTO.HASH(UTL_RAW.CAST_TO_RAW(%s), 4)", valueExpr) // 4 = SHA256
	case base.DXDatabaseTypeMariaDB:
		return fmt.Sprintf("SHA2(%s, 256)", valueExpr)
	default:
		return placeholder
	}
}

// keyExpression returns the database-specific key retrieval expression
func (t *DBTable) keyExpression(dbType base.DXDatabaseType, source KeySource, value string) string {
	if value == "" {
		return "''"
	}

	switch source {
	case KeySourceRaw:
		return fmt.Sprintf("'%s'", value)
	case KeySourceEnv:
		// Environment variables - database specific
		switch dbType {
		case base.DXDatabaseTypePostgreSQL:
			return fmt.Sprintf("current_setting('%s')", value)
		default:
			return fmt.Sprintf("'%s'", value)
		}
	case KeySourceConfig:
		switch dbType {
		case base.DXDatabaseTypePostgreSQL:
			return fmt.Sprintf("current_setting('%s')", value)
		case base.DXDatabaseTypeSQLServer:
			return fmt.Sprintf("'%s'", value)
		case base.DXDatabaseTypeOracle:
			return fmt.Sprintf("SYS_CONTEXT('USERENV', '%s')", value)
		case base.DXDatabaseTypeMariaDB:
			return fmt.Sprintf("@%s", value)
		default:
			return fmt.Sprintf("'%s'", value)
		}
	case KeySourceDbSessionCurrentSetting:
		switch dbType {
		case base.DXDatabaseTypePostgreSQL:
			return fmt.Sprintf("current_setting('%s')", value)
		case base.DXDatabaseTypeSQLServer:
			return fmt.Sprintf("SESSION_CONTEXT(N'%s')", value)
		case base.DXDatabaseTypeOracle:
			return fmt.Sprintf("SYS_CONTEXT('CLIENTCONTEXT', '%s')", value)
		case base.DXDatabaseTypeMariaDB:
			return fmt.Sprintf("@%s", value)
		default:
			return fmt.Sprintf("'%s'", value)
		}
	default:
		return fmt.Sprintf("'%s'", value)
	}
}

// concatExpression returns the database-specific string concatenation expression
func (t *DBTable) concatExpression(dbType base.DXDatabaseType, expr1, expr2 string) string {
	switch dbType {
	case base.DXDatabaseTypePostgreSQL, base.DXDatabaseTypeMariaDB:
		return fmt.Sprintf("CONCAT(%s, %s)", expr1, expr2)
	case base.DXDatabaseTypeSQLServer:
		return fmt.Sprintf("CONCAT(%s, %s)", expr1, expr2)
	case base.DXDatabaseTypeOracle:
		return fmt.Sprintf("(%s || %s)", expr1, expr2)
	default:
		return fmt.Sprintf("CONCAT(%s, %s)", expr1, expr2)
	}
}

func (t *DBTable) buildUpdateData(dbType base.DXDatabaseType, data utils.JSON) (setClause string, args []any, err error) {
	var sets []string
	argIndex := 1

	// Track which decrypted field names we've processed (to avoid duplicates)
	processedDecryptedFields := make(map[string]bool)

	for _, fieldName := range t.getOrderedFields() {
		field := t.Fields[fieldName]

		// Check if this is an encrypted field with DecryptedFieldName
		if field.DecryptedFieldName != "" && field.EncryptionKeyValue != "" {
			// Look for value using DecryptedFieldName (e.g., "fullname")
			val, ok := data[field.DecryptedFieldName]
			if !ok {
				continue
			}

			// Skip if already processed
			if processedDecryptedFields[field.DecryptedFieldName] {
				continue
			}
			processedDecryptedFields[field.DecryptedFieldName] = true

			// Add an encrypted column
			sets = append(sets, fmt.Sprintf("%s = %s", fieldName, t.encryptExpression(dbType, argIndex, field.EncryptionKeySource, field.EncryptionKeyValue)))
			args = append(args, val)
			argIndex++

			// Add hash column if specified
			if field.HashFieldName != "" {
				sets = append(sets, fmt.Sprintf("%s = %s", field.HashFieldName, t.hashExpression(dbType, argIndex, field.HashSaltKeySource, field.HashSaltKeyValue)))
				args = append(args, val)
				argIndex++
			}
			continue
		}

		// Regular field - check if value exists in data
		val, ok := data[fieldName]
		if !ok {
			continue
		}

		// Skip hash fields that are auto-generated
		if t.isAutoGeneratedHashField(fieldName) {
			continue
		}

		// Validate incoming value matches expected type
		if err := t.validateFieldValue(fieldName, field, val); err != nil {
			return "", nil, err
		}

		sets = append(sets, fmt.Sprintf("%s = %s", fieldName, t.placeholder(dbType, argIndex)))
		args = append(args, val)
		argIndex++
	}

	return strings.Join(sets, ", "), args, nil
}

func (t *DBTable) placeholder(dbType base.DXDatabaseType, index int) string {
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return fmt.Sprintf("$%d", index)
	case base.DXDatabaseTypeSQLServer:
		return fmt.Sprintf("@p%d", index)
	case base.DXDatabaseTypeOracle:
		return fmt.Sprintf(":p%d", index)
	default: // base.DXDatabaseTypeMariaDB/MySQL
		return "?"
	}
}

func (t *DBTable) validateFieldValue(fieldName string, field *Field, val any) error {
	if val == nil {
		return nil
	}

	switch field.Type.GoType {
	case types.GoTypeString, types.GoTypeStringPointer:
		if _, ok := val.(string); !ok {
			if _, ok := val.(*string); !ok {
				return fmt.Errorf("field %s expects string, got %T", fieldName, val)
			}
		}
	case types.GoTypeInt64, types.GoTypeInt64Pointer:
		switch val.(type) {
		case int, int32, int64, float64:
			// OK - JSON numbers come as float64
		default:
			return fmt.Errorf("field %s expects int64, got %T", fieldName, val)
		}
	case types.GoTypeFloat32:
		switch val.(type) {
		case float32, float64:
			// OK
		default:
			return fmt.Errorf("field %s expects float32, got %T", fieldName, val)
		}
	case types.GoTypeFloat64:
		if _, ok := val.(float64); !ok {
			return fmt.Errorf("field %s expects float64, got %T", fieldName, val)
		}
	case types.GoTypeBool:
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("field %s expects bool, got %T", fieldName, val)
		}
	}
	return nil
}

func (t *DBTable) scanRow(row *sql.Row) (utils.JSON, error) {
	result := make(utils.JSON)
	orderedFields := t.getOrderedFields()
	scanDest := make([]any, len(orderedFields))
	scanPtrs := make([]any, len(orderedFields))

	for i := range scanDest {
		scanPtrs[i] = &scanDest[i]
	}

	if err := row.Scan(scanPtrs...); err != nil {
		return nil, err
	}

	for i, fieldName := range orderedFields {
		result[fieldName] = scanDest[i]
	}
	return result, nil
}

func (t *DBTable) scanRows(rows *sql.Rows) (utils.JSON, error) {
	result := make(utils.JSON)
	orderedFields := t.getOrderedFields()
	scanDest := make([]any, len(orderedFields))
	scanPtrs := make([]any, len(orderedFields))

	for i := range scanDest {
		scanPtrs[i] = &scanDest[i]
	}

	if err := rows.Scan(scanPtrs...); err != nil {
		return nil, err
	}

	for i, fieldName := range orderedFields {
		result[fieldName] = scanDest[i]
	}
	return result, nil
}
