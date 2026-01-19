package database3

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/types"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/pkg/errors"
)

// ============================================================================
// DBTable - Table entity (embeds DBEntity)
// ============================================================================

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
	IsVaulted              bool
	IsHashed               bool
	PhysicalFieldName      string
	PhysicalDataType       types.DataType
	VaultConfigKeyId       string
	HashDataName           string
	HashDataType           types.DataType
	HashSaltConfigKeyId    string
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
	ViewOverTable     bool
	PhysicalTableName string
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

// getEncryptionKey retrieves an encryption key from configuration
func getEncryptionKey(keyID string) ([]byte, error) {
	cfg, ok := configuration.Manager.Configurations["system"]
	if !ok {
		return nil, fmt.Errorf("system configuration not found")
	}
	dbEncryption, ok := (*cfg.Data)["database_encryption"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("database_encryption configuration not found")
	}
	key, ok := dbEncryption[keyID]
	if !ok {
		return nil, fmt.Errorf("encryption key not found: %s", keyID)
	}
	switch v := key.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		return nil, fmt.Errorf("invalid encryption key type for: %s", keyID)
	}
}

// getHashSalt retrieves hash salt from configuration
func getHashSalt(saltID string) ([]byte, error) {
	cfg, ok := configuration.Manager.Configurations["system"]
	if !ok {
		return nil, fmt.Errorf("system configuration not found")
	}
	dbEncryption, ok := (*cfg.Data)["database_encryption"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("database_encryption configuration not found")
	}
	salt, ok := dbEncryption[saltID]
	if !ok {
		return nil, fmt.Errorf("hash salt not found: %s", saltID)
	}
	switch v := salt.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		return nil, fmt.Errorf("invalid hash salt type for: %s", saltID)
	}
}

// HasEncryptedFields returns true if an entity has any encrypted fields
func (t *DBTable) HasEncryptedFields() bool {
	for _, field := range t.Fields {
		if field.IsVaulted && field.PhysicalFieldName != "" {
			return true
		}
	}
	return false
}

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

// ViewName returns the view name (without a schema prefix)
func (t *DBTable) ViewName() string {
	if t.ViewOverTable {
		return t.Name
	}
	if t.HasEncryptedFields() {
		if t.UseTableSuffix {
			return t.Name + "_v"
		}
		return t.Name + "_view"
	}
	return t.TableName()
}

// FullTableName returns the table name with schema prefix if schema is set
func (t *DBTable) FullTableName() string {
	if t.Schema != nil && t.Schema.Name != "" {
		return t.Schema.Name + "." + t.TableName()
	}
	return t.TableName()
}

// FullViewName returns the view name with schema prefix if schema is set
func (t *DBTable) FullViewName() string {
	if t.Schema != nil && t.Schema.Name != "" {
		return t.Schema.Name + "." + t.ViewName()
	}
	return t.ViewName()
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

// CreateDDL generates a DDL script for the entity based on database type
// For entities with encrypted fields: creates table (with encrypted columns only) + view (with decrypted columns)
// For entities without encrypted fields: creates table only
func (t *DBTable) CreateDDL(dbType base.DXDatabaseType) (string, error) {
	var sb strings.Builder
	hasEncrypted := t.HasEncryptedFields()

	// Add pgcrypto extension for base.DXDatabaseTypePostgreSQL
	if dbType == base.DXDatabaseTypePostgreSQL && hasEncrypted {
		sb.WriteString("CREATE EXTENSION IF NOT EXISTS pgcrypto;\n\n")
	}

	// Create table
	s, err := t.createTableDDL(dbType)
	if err != nil {
		return "", err
	}
	sb.WriteString(s)

	// Create a view if there are encrypted fields
	if hasEncrypted {
		sb.WriteString("\n")
		sb.WriteString(t.createViewDDL(dbType))
	}

	return sb.String(), nil
}

// createTableDDL generates the CREATE TABLE DDL only
func (t *DBTable) createTableDDL(dbType base.DXDatabaseType) (string, error) {
	var sb strings.Builder
	tableName := t.FullTableName()

	// language=text
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", tableName))

	var columns []string
	for _, fieldName := range t.getOrderedFields() {
		field := t.Fields[fieldName]
		if field.IsVaulted && field.PhysicalFieldName != "" {
			// For encrypted fields: only add encrypted_data_name and hash_data_name to the table
			encColDef, err := t.encryptedFieldToDDL(field, dbType)
			if err != nil {
				return "", err
			}
			columns = append(columns, encColDef)

			if field.IsHashed && field.HashDataName != "" {
				hashColDef, err := t.hashedFieldToDDL(field, dbType)
				if err != nil {
					return "", err
				}
				columns = append(columns, hashColDef)
			}
		} else {
			// For non-encrypted fields: add the original field name
			colDef := t.fieldToDDL(fieldName, *field, dbType)
			columns = append(columns, colDef)
		}
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
		// MariaDB/MySQL: Use ENCRYPTION table option for InnoDB
		if t.TDE.MariaDBEncryption == "Y" {
			return " ENCRYPTION='Y'"
		}
	default:
		// No TDE support for unknown database types
	}
	return ""
}

// createViewDDL generates the VIEW DDL with decrypted columns
func (t *DBTable) createViewDDL(dbType base.DXDatabaseType) string {
	var sb strings.Builder
	viewName := t.FullViewName()
	tableName := t.FullTableName()

	sb.WriteString(fmt.Sprintf("CREATE VIEW %s AS\nSELECT\n", viewName))

	var viewCols []string
	for _, fieldName := range t.getOrderedFields() {
		field := t.Fields[fieldName]
		if field.IsVaulted && field.PhysicalFieldName != "" {
			// Decrypt and alias to the original field name
			decryptExpr := t.buildDecryptExpr(fieldName, field, dbType)
			viewCols = append(viewCols, "    "+decryptExpr)

			// Also include encrypted column for reference
			viewCols = append(viewCols, "    "+field.PhysicalFieldName)

			// Include hash column if exists
			if field.IsHashed && field.HashDataName != "" {
				viewCols = append(viewCols, "    "+field.HashDataName)
			}
		} else {
			viewCols = append(viewCols, "    "+fieldName)
		}
	}

	sb.WriteString(strings.Join(viewCols, ",\n"))
	sb.WriteString(fmt.Sprintf("\nFROM %s;\n", tableName))

	return sb.String()
}

func (t *DBTable) fieldToDDL(fieldName string, field Field, dbType base.DXDatabaseType) string {

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%s %s", fieldName, dbType.String()))

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

// getDefaultValueForDBType returns the appropriate default value for the given database type
// Priority: 1. Field.DefaultValueByDBType, 2. Field.DefaultValue, 3. Field.Type.DefaultValueByDatabaseType
func (t *DBTable) getDefaultValueForDBType(field Field, dbType base.DXDatabaseType) string {

	// 1. Check if field has database-specific default
	if field.DefaultValueByDBType != nil {
		if dbDefault, ok := field.DefaultValueByDBType[dbType]; ok && dbDefault != nil {
			return anyToString(dbDefault)
		}
	}

	// 2. Check field's generic default value
	if field.DefaultValue != nil {
		return anyToString(field.DefaultValue)
	}

	// 3. Check DataType's database-specific default (t.g., DataTypeUID) - only if IsAutoIncrement is true
	if field.IsAutoIncrement && field.Type.DefaultValueByDatabaseType != nil {
		if dbDefault, ok := field.Type.DefaultValueByDatabaseType[dbType]; ok && dbDefault != "" {
			return dbDefault
		}
	}

	return ""
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

func (t *DBTable) encryptedFieldToDDL(field *Field, dbType base.DXDatabaseType) (string, error) {
	dbTypeStr, ok := field.PhysicalDataType.DbType[dbType]
	if !ok {
		return "", errors.Errorf("entity: %s, field: %s - unknown database type: %v",
			t.Name, field.GetName(), dbType)
	}
	return fmt.Sprintf("%s %s", field.PhysicalFieldName, dbTypeStr), nil
}

func (t *DBTable) hashedFieldToDDL(field *Field, dbType base.DXDatabaseType) (string, error) {

	dbTypeStr, ok := field.HashDataType.DbType[dbType]
	if !ok {
		return "", errors.Errorf("entity: %s, field: %s - unknown database type: %v",
			t.Name, field.GetName(), dbType)
	}
	return fmt.Sprintf("%s %s", field.HashDataName, dbTypeStr), nil
}

// SelectOne selects a single row from the view (decrypted data)
func (t *DBTable) SelectOne(db *sql.DB, dbType base.DXDatabaseType, where string, args ...any) (utils.JSON, error) {
	columns := t.buildSelectColumns()
	viewName := t.FullViewName()

	var query string
	if dbType == base.DXDatabaseTypeSQLServer {
		// language=text
		query = fmt.Sprintf("SELECT TOP 1 %s FROM %s WHERE %s", columns, viewName, where)
	} else {
		// language=text
		query = fmt.Sprintf("SELECT %s FROM %s WHERE %s LIMIT 1", columns, viewName, where)
	}
	row := db.QueryRow(query, args...)
	return t.scanRow(row)
}

// SelectMany selects multiple rows from the view (decrypted data)
func (t *DBTable) SelectMany(db *sql.DB, dbType base.DXDatabaseType, where string, args ...any) ([]utils.JSON, error) {
	columns := t.buildSelectColumns()
	viewName := t.FullViewName()

	// language=text
	query := fmt.Sprintf("SELECT %s FROM %s", columns, viewName)
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

// buildSelectColumns returns column names for SELECT from view.
// View already has decrypted columns, so we just select by field name (map key)
func (t *DBTable) buildSelectColumns() string {
	return strings.Join(t.getOrderedFields(), ", ")
}

func (t *DBTable) buildDecryptExpr(fieldName string, field *Field, dbType base.DXDatabaseType) string {
	encCol := field.PhysicalFieldName
	keyExpr := BuildGetSessionConfigExpr(dbType, "app.encryption_key")
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		// pgp_sym_decrypt(encrypted_col, key) AS original_name
		return fmt.Sprintf("pgp_sym_decrypt(%s, %s) AS %s", encCol, keyExpr, fieldName)
	case base.DXDatabaseTypeSQLServer:
		// DecryptByPassPhrase(key, encrypted_col) AS original_name
		return fmt.Sprintf("CONVERT(VARCHAR(MAX), DecryptByPassPhrase(%s, %s)) AS %s", keyExpr, encCol, fieldName)
	case base.DXDatabaseTypeMariaDB:
		// AES_DECRYPT(encrypted_col, key) AS original_name
		return fmt.Sprintf("AES_DECRYPT(%s, %s) AS %s", encCol, keyExpr, fieldName)
	case base.DXDatabaseTypeOracle:
		// DBMS_CRYPTO.DECRYPT using session context for a key
		return fmt.Sprintf("UTL_RAW.CAST_TO_VARCHAR2(DBMS_CRYPTO.DECRYPT(%s, DBMS_CRYPTO.ENCRYPT_AES256 + DBMS_CRYPTO.CHAIN_CBC + DBMS_CRYPTO.PAD_PKCS5, UTL_RAW.CAST_TO_RAW(%s))) AS %s", encCol, keyExpr, fieldName)
	default:
		return fieldName
	}
}

func (t *DBTable) buildInsertData(dbType base.DXDatabaseType, data utils.JSON) (columns string, values string, args []any, err error) {
	var cols []string
	var vals []string
	argIndex := 1

	for _, fieldName := range t.getOrderedFields() {
		field := t.Fields[fieldName]
		val, ok := data[fieldName]
		if !ok {
			continue
		}

		// Validate incoming value matches expected type
		if err := t.validateFieldValue(fieldName, field, val); err != nil {
			return "", "", nil, err
		}

		if field.IsVaulted && field.PhysicalFieldName != "" {
			// Add an encrypted column
			cols = append(cols, field.PhysicalFieldName)
			encExpr := t.buildEncryptExpr(dbType, argIndex)
			vals = append(vals, encExpr)
			args = append(args, val)
			argIndex++

			// Add a hash column if applicable
			if field.IsHashed && field.HashDataName != "" {
				cols = append(cols, field.HashDataName)
				hashExpr := t.buildHashExpr(dbType, argIndex)
				vals = append(vals, hashExpr)
				args = append(args, val) // hash the same value
				argIndex++
			}
		} else {
			cols = append(cols, fieldName)
			vals = append(vals, t.placeholder(dbType, argIndex))
			args = append(args, val)
			argIndex++
		}
	}

	return strings.Join(cols, ", "), strings.Join(vals, ", "), args, nil
}

func (t *DBTable) buildUpdateData(dbType base.DXDatabaseType, data utils.JSON) (setClause string, args []any, err error) {
	var sets []string
	argIndex := 1

	for _, fieldName := range t.getOrderedFields() {
		field := t.Fields[fieldName]
		val, ok := data[fieldName]
		if !ok {
			continue
		}

		// Validate incoming value matches expected type
		if err := t.validateFieldValue(fieldName, field, val); err != nil {
			return "", nil, err
		}

		if field.IsVaulted && field.PhysicalFieldName != "" {
			// Update encrypted column
			encExpr := t.buildEncryptExpr(dbType, argIndex)
			sets = append(sets, fmt.Sprintf("%s = %s", field.PhysicalFieldName, encExpr))
			args = append(args, val)
			argIndex++

			// Update the hash column if applicable
			if field.IsHashed && field.HashDataName != "" {
				hashExpr := t.buildHashExpr(dbType, argIndex)
				sets = append(sets, fmt.Sprintf("%s = %s", field.HashDataName, hashExpr))
				args = append(args, val) // hash the same value
				argIndex++
			}
		} else {
			sets = append(sets, fmt.Sprintf("%s = %s", fieldName, t.placeholder(dbType, argIndex)))
			args = append(args, val)
			argIndex++
		}
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

func (t *DBTable) buildEncryptExpr(dbType base.DXDatabaseType, argIndex int) string {
	placeholder := t.placeholder(dbType, argIndex)
	keyExpr := BuildGetSessionConfigExpr(dbType, "app.encryption_key")
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return fmt.Sprintf("pgp_sym_encrypt(%s::text, %s)", placeholder, keyExpr)
	case base.DXDatabaseTypeSQLServer:
		return fmt.Sprintf("EncryptByPassPhrase(%s, %s)", keyExpr, placeholder)
	case base.DXDatabaseTypeMariaDB:
		return fmt.Sprintf("AES_ENCRYPT(%s, %s)", placeholder, keyExpr)
	case base.DXDatabaseTypeOracle:
		return fmt.Sprintf("DBMS_CRYPTO.ENCRYPT(UTL_RAW.CAST_TO_RAW(%s), DBMS_CRYPTO.ENCRYPT_AES256 + DBMS_CRYPTO.CHAIN_CBC + DBMS_CRYPTO.PAD_PKCS5, UTL_RAW.CAST_TO_RAW(%s))", placeholder, keyExpr)
	default:
		return placeholder
	}
}

func (t *DBTable) buildHashExpr(dbType base.DXDatabaseType, argIndex int) string {
	placeholder := t.placeholder(dbType, argIndex)
	saltExpr := BuildGetSessionConfigExpr(dbType, "app.hash_salt")
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return fmt.Sprintf("digest(%s || %s, 'sha256')", placeholder, saltExpr)
	case base.DXDatabaseTypeSQLServer:
		return fmt.Sprintf("HASHBYTES('SHA2_256', CONCAT(%s, %s))", placeholder, saltExpr)
	case base.DXDatabaseTypeMariaDB:
		return fmt.Sprintf("SHA2(CONCAT(%s, %s), 256)", placeholder, saltExpr)
	case base.DXDatabaseTypeOracle:
		return fmt.Sprintf("DBMS_CRYPTO.HASH(UTL_RAW.CAST_TO_RAW(%s || %s), DBMS_CRYPTO.HASH_SH256)", placeholder, saltExpr)
	default:
		return placeholder
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
