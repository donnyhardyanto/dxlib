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

/*func dbTypeToKey(dbType base.DXDatabaseType) string {
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return types.DBTypeKeyPostgreSQL
	case base.DXDatabaseTypeSQLServer:
		return types.DBTypeKeySQLServer
	case base.DXDatabaseTypeOracle:
		return types.DBTypeKeyOracle
	case base.DXDatabaseTypeMariaDB:
		return types.DBTypeKeyMariaDB
	default:
		return types.DBTypeKeyPostgreSQL
	}
}*/

type DBEntityType int

const (
	DBEntityTypeTable DBEntityType = iota
	DBEntityTypeView
	DBEntityTypeMaterializedView
)

type DBEntity struct {
	types.Entity
	Type                      DBEntityType
	Order                     int
	Schema                    *DBSchema
	TDE                       TDEConfig // Database-specific TDE configuration
	UseTableSuffix            bool
	IsCompositeEncryptedTable bool // If true, adds "_t" suffix to the table name and "_v" suffix to view name
}

// NewDBEntity creates a new database entity and registers it with the schema
func NewDBEntity(schema *DBSchema, order int, entity types.Entity, tde TDEConfig) *DBEntity {
	dbEntity := &DBEntity{Entity: entity, Order: order, Schema: schema, TDE: tde}
	if schema != nil {
		schema.Entities = append(schema.Entities, dbEntity)
	}
	for _, field := range entity.Fields {
		field.Owner = dbEntity
		if field.IsReferences && field.References == nil {
			panic("references field without references")
		}
	}
	return dbEntity
}

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
func (e *DBEntity) HasEncryptedFields() bool {
	for _, field := range e.Fields {
		if field.IsVaulted && field.PhysicalFieldName != "" {
			return true
		}
	}
	return false
}

// TableName returns the physical table name (without a schema prefix)
func (e *DBEntity) TableName() string {
	if e.PhysicalTableName != "" {
		return e.PhysicalTableName
	}
	if e.UseTableSuffix {
		return e.Name + "_t"
	}
	return e.Name
}

// ViewName returns the view name (without a schema prefix)
func (e *DBEntity) ViewName() string {
	if e.ViewOverTable {
		return e.Name
	}
	if e.HasEncryptedFields() {
		if e.UseTableSuffix {
			return e.Name + "_v"
		}
		return e.Name + "_view"
	}
	return e.TableName()
}

// FullTableName returns the table name with schema prefix if schema is set
func (e *DBEntity) FullTableName() string {
	if e.Schema != nil && e.Schema.Name != "" {
		return e.Schema.Name + "." + e.TableName()
	}
	return e.TableName()
}

// FullViewName returns the view name with schema prefix if schema is set
func (e *DBEntity) FullViewName() string {
	if e.Schema != nil && e.Schema.Name != "" {
		return e.Schema.Name + "." + e.ViewName()
	}
	return e.ViewName()
}

// getOrderedFields returns field names sorted by Order
func (e *DBEntity) getOrderedFields() []string {
	type fieldOrder struct {
		name  string
		order int
	}
	var fields []fieldOrder
	for name, field := range e.Fields {
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
func (e *DBEntity) CreateDDL(dbType base.DXDatabaseType) (string, error) {
	var sb strings.Builder
	hasEncrypted := e.HasEncryptedFields()

	// Add pgcrypto extension for base.DXDatabaseTypePostgreSQL
	if dbType == base.DXDatabaseTypePostgreSQL && hasEncrypted {
		sb.WriteString("CREATE EXTENSION IF NOT EXISTS pgcrypto;\n\n")
	}

	// Create table
	s, err := e.createTableDDL(dbType)
	if err != nil {
		return "", err
	}
	sb.WriteString(s)

	// Create a view if there are encrypted fields
	if hasEncrypted {
		sb.WriteString("\n")
		sb.WriteString(e.createViewDDL(dbType))
	}

	return sb.String(), nil
}

// createTableDDL generates the CREATE TABLE DDL only
func (e *DBEntity) createTableDDL(dbType base.DXDatabaseType) (string, error) {
	var sb strings.Builder
	tableName := e.FullTableName()

	// language=text
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", tableName))

	var columns []string
	for _, fieldName := range e.getOrderedFields() {
		field := e.Fields[fieldName]
		if field.IsVaulted && field.PhysicalFieldName != "" {
			// For encrypted fields: only add encrypted_data_name and hash_data_name to the table
			encColDef, err := e.encryptedFieldToDDL(field, dbType)
			if err != nil {
				return "", err
			}
			columns = append(columns, encColDef)

			if field.IsHashed && field.HashDataName != "" {
				hashColDef, err := e.hashedFieldToDDL(field, dbType)
				if err != nil {
					return "", err
				}
				columns = append(columns, hashColDef)
			}
		} else {
			// For non-encrypted fields: add the original field name
			colDef := e.fieldToDDL(fieldName, *field, dbType)
			columns = append(columns, colDef)
		}
	}

	sb.WriteString("    " + strings.Join(columns, ",\n    "))
	sb.WriteString("\n)")

	// Add database-specific TDE options
	sb.WriteString(e.buildTDEClause(dbType))

	sb.WriteString(";\n")

	return sb.String(), nil
}

// buildTDEClause generates the database-specific TDE clause for CREATE TABLE
func (e *DBEntity) buildTDEClause(dbType base.DXDatabaseType) string {
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		// PostgreSQL: Use table access method for TDE (e.g., "tde_heap" with pg_tde extension)
		if e.TDE.PostgreSQLAccessMethod != "" {
			return fmt.Sprintf(" USING %s", e.TDE.PostgreSQLAccessMethod)
		}
	case base.DXDatabaseTypeOracle:
		// Oracle: Specify tablespace for encrypted storage
		if e.TDE.OracleTablespace != "" {
			return fmt.Sprintf(" TABLESPACE %s", e.TDE.OracleTablespace)
		}
	case base.DXDatabaseTypeSQLServer:
		// SQL Server: TDE is database-level, no per-table syntax
		// Add a comment to indicate TDE expectation if enabled
		if e.TDE.SQLServerTDEEnabled {
			return " /* TDE enabled at database level */"
		}
	case base.DXDatabaseTypeMariaDB:
		// MariaDB/MySQL: Use ENCRYPTION table option for InnoDB
		if e.TDE.MariaDBEncryption == "Y" {
			return " ENCRYPTION='Y'"
		}
	default:
		// No TDE support for unknown database types
	}
	return ""
}

// createViewDDL generates the VIEW DDL with decrypted columns
func (e *DBEntity) createViewDDL(dbType base.DXDatabaseType) string {
	var sb strings.Builder
	viewName := e.FullViewName()
	tableName := e.FullTableName()

	sb.WriteString(fmt.Sprintf("CREATE VIEW %s AS\nSELECT\n", viewName))

	var viewCols []string
	for _, fieldName := range e.getOrderedFields() {
		field := e.Fields[fieldName]
		if field.IsVaulted && field.PhysicalFieldName != "" {
			// Decrypt and alias to the original field name
			decryptExpr := e.buildDecryptExpr(fieldName, field, dbType)
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

func (e *DBEntity) fieldToDDL(fieldName string, field types.Field, dbType base.DXDatabaseType) string {

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
	defaultValue := e.getDefaultValueForDBType(field, dbType)
	if defaultValue != "" {
		sb.WriteString(fmt.Sprintf(" DEFAULT %s", defaultValue))
	}

	// Add REFERENCES constraint for foreign keys
	if field.References != nil {
		if refOwner, ok := field.References.Owner.(*DBEntity); ok {
			// Find the field name in the referenced entity
			refFieldName := ""
			for name, f := range refOwner.Fields {
				if f == field.References {
					refFieldName = name
					break
				}
			}
			if refFieldName != "" {
				sb.WriteString(fmt.Sprintf(" REFERENCES %s.%s (%s)",
					refOwner.Schema.Name, refOwner.Name, refFieldName))
			}
		}
	}

	return sb.String()
}

// getDefaultValueForDBType returns the appropriate default value for the given database type
// Priority: 1. Field.DefaultValueByDBType, 2. Field.DefaultValue, 3. Field.Type.DefaultValueByDBType
func (e *DBEntity) getDefaultValueForDBType(field types.Field, dbType base.DXDatabaseType) string {

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

	// 3. Check DataType's database-specific default (e.g., DataTypeUID) - only if IsAutoIncrement is true
	if field.IsAutoIncrement && field.Type.DefaultValueByDBType != nil {
		if dbDefault, ok := field.Type.DefaultValueByDBType[dbType]; ok && dbDefault != "" {
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

func (e *DBEntity) encryptedFieldToDDL(field *types.Field, dbType base.DXDatabaseType) (string, error) {
	dbTypeStr, ok := field.PhysicalDataType.DbType[dbType]
	if !ok {
		return "", errors.Errorf("entity: %s, field: %s - unknown database type: %v",
			e.Name, field.GetName(), dbType)
	}
	return fmt.Sprintf("%s %s", field.PhysicalFieldName, dbTypeStr), nil
}

func (e *DBEntity) hashedFieldToDDL(field *types.Field, dbType base.DXDatabaseType) (string, error) {

	dbTypeStr, ok := field.HashDataType.DbType[dbType]
	if !ok {
		return "", errors.Errorf("entity: %s, field: %s - unknown database type: %v",
			e.Name, field.GetName(), dbType)
	}
	return fmt.Sprintf("%s %s", field.HashDataName, dbTypeStr), nil
}

// SelectOne selects a single row from the view (decrypted data)
func (e *DBEntity) SelectOne(db *sql.DB, dbType base.DXDatabaseType, where string, args ...any) (utils.JSON, error) {
	columns := e.buildSelectColumns()
	viewName := e.FullViewName()

	var query string
	if dbType == base.DXDatabaseTypeSQLServer {
		// language=text
		query = fmt.Sprintf("SELECT TOP 1 %s FROM %s WHERE %s", columns, viewName, where)
	} else {
		// language=text
		query = fmt.Sprintf("SELECT %s FROM %s WHERE %s LIMIT 1", columns, viewName, where)
	}
	row := db.QueryRow(query, args...)
	return e.scanRow(row)
}

// SelectMany selects multiple rows from the view (decrypted data)
func (e *DBEntity) SelectMany(db *sql.DB, dbType base.DXDatabaseType, where string, args ...any) ([]utils.JSON, error) {
	columns := e.buildSelectColumns()
	viewName := e.FullViewName()

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
		result, err := e.scanRows(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

// Insert inserts a new row into the table
func (e *DBEntity) Insert(db *sql.DB, dbType base.DXDatabaseType, data utils.JSON) error {
	columns, values, args, err := e.buildInsertData(dbType, data)
	if err != nil {
		return err
	}

	tableName := e.FullTableName()
	// language=text
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, values)
	_, err = db.Exec(query, args...)
	return err
}

// Update updates existing rows in the table
func (e *DBEntity) Update(db *sql.DB, dbType base.DXDatabaseType, data utils.JSON, where string, whereArgs ...any) error {
	setClause, args, err := e.buildUpdateData(dbType, data)
	if err != nil {
		return err
	}

	args = append(args, whereArgs...)
	tableName := e.FullTableName()
	// language=text
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tableName, setClause, where)
	_, err = db.Exec(query, args...)
	return err
}

func (e *DBEntity) Delete(db *sql.DB, where string, args ...any) error {
	tableName := e.FullTableName()
	// language=text
	query := fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, where)
	_, err := db.Exec(query, args...)
	return err
}

// buildSelectColumns returns column names for SELECT from view.
// View already has decrypted columns, so we just select by field name (map key)
func (e *DBEntity) buildSelectColumns() string {
	return strings.Join(e.getOrderedFields(), ", ")
}

func (e *DBEntity) buildDecryptExpr(fieldName string, field *types.Field, dbType base.DXDatabaseType) string {
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

func (e *DBEntity) buildInsertData(dbType base.DXDatabaseType, data utils.JSON) (columns string, values string, args []any, err error) {
	var cols []string
	var vals []string
	argIndex := 1

	for _, fieldName := range e.getOrderedFields() {
		field := e.Fields[fieldName]
		val, ok := data[fieldName]
		if !ok {
			continue
		}

		// Validate incoming value matches expected type
		if err := e.validateFieldValue(fieldName, field, val); err != nil {
			return "", "", nil, err
		}

		if field.IsVaulted && field.PhysicalFieldName != "" {
			// Add an encrypted column
			cols = append(cols, field.PhysicalFieldName)
			encExpr := e.buildEncryptExpr(dbType, argIndex)
			vals = append(vals, encExpr)
			args = append(args, val)
			argIndex++

			// Add a hash column if applicable
			if field.IsHashed && field.HashDataName != "" {
				cols = append(cols, field.HashDataName)
				hashExpr := e.buildHashExpr(dbType, argIndex)
				vals = append(vals, hashExpr)
				args = append(args, val) // hash the same value
				argIndex++
			}
		} else {
			cols = append(cols, fieldName)
			vals = append(vals, e.placeholder(dbType, argIndex))
			args = append(args, val)
			argIndex++
		}
	}

	return strings.Join(cols, ", "), strings.Join(vals, ", "), args, nil
}

func (e *DBEntity) buildUpdateData(dbType base.DXDatabaseType, data utils.JSON) (setClause string, args []any, err error) {
	var sets []string
	argIndex := 1

	for _, fieldName := range e.getOrderedFields() {
		field := e.Fields[fieldName]
		val, ok := data[fieldName]
		if !ok {
			continue
		}

		// Validate incoming value matches expected type
		if err := e.validateFieldValue(fieldName, field, val); err != nil {
			return "", nil, err
		}

		if field.IsVaulted && field.PhysicalFieldName != "" {
			// Update encrypted column
			encExpr := e.buildEncryptExpr(dbType, argIndex)
			sets = append(sets, fmt.Sprintf("%s = %s", field.PhysicalFieldName, encExpr))
			args = append(args, val)
			argIndex++

			// Update the hash column if applicable
			if field.IsHashed && field.HashDataName != "" {
				hashExpr := e.buildHashExpr(dbType, argIndex)
				sets = append(sets, fmt.Sprintf("%s = %s", field.HashDataName, hashExpr))
				args = append(args, val) // hash the same value
				argIndex++
			}
		} else {
			sets = append(sets, fmt.Sprintf("%s = %s", fieldName, e.placeholder(dbType, argIndex)))
			args = append(args, val)
			argIndex++
		}
	}

	return strings.Join(sets, ", "), args, nil
}

func (e *DBEntity) placeholder(dbType base.DXDatabaseType, index int) string {
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

func (e *DBEntity) buildEncryptExpr(dbType base.DXDatabaseType, argIndex int) string {
	placeholder := e.placeholder(dbType, argIndex)
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

func (e *DBEntity) buildHashExpr(dbType base.DXDatabaseType, argIndex int) string {
	placeholder := e.placeholder(dbType, argIndex)
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

func (e *DBEntity) validateFieldValue(fieldName string, field *types.Field, val any) error {
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

func (e *DBEntity) scanRow(row *sql.Row) (utils.JSON, error) {
	result := make(utils.JSON)
	orderedFields := e.getOrderedFields()
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

func (e *DBEntity) scanRows(rows *sql.Rows) (utils.JSON, error) {
	result := make(utils.JSON)
	orderedFields := e.getOrderedFields()
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
