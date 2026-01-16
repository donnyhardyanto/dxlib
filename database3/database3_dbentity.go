package database3

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/types"
	"github.com/donnyhardyanto/dxlib/utils"
)

func dbTypeToKey(dbType DXDatabaseType) string {
	switch dbType {
	case DXDatabaseTypePostgreSQL:
		return types.DBTypeKeyPostgreSQL
	case DXDatabaseTypeSQLServer:
		return types.DBTypeKeySQLServer
	case DXDatabaseTypeOracle:
		return types.DBTypeKeyOracle
	case DXDatabaseTypeMariaDB:
		return types.DBTypeKeyMariaDB
	default:
		return types.DBTypeKeyPostgreSQL
	}
}

type DBEntity struct {
	types.Entity
	Schema         *DBSchema
	TDE            types.TDEConfig // Database-specific TDE configuration
	UseTableSuffix bool            // If true, adds "_t" suffix to the table name and "_v" suffix to view name
}

// NewDBEntity creates a new database entity and registers it with the schema
func NewDBEntity(schema *DBSchema, entity types.Entity, tde types.TDEConfig) *DBEntity {
	dbEntity := &DBEntity{Entity: entity, Schema: schema, TDE: tde}
	if schema != nil {
		schema.Entities = append(schema.Entities, dbEntity)
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
		if field.IsEncrypted && field.EncryptedDataName != "" {
			return true
		}
	}
	return false
}

// TableName returns the physical table name (without a schema prefix)
func (e *DBEntity) TableName() string {
	if e.UseTableSuffix {
		return e.Name + "_t"
	}
	return e.Name
}

// ViewName returns the view name (without a schema prefix)
func (e *DBEntity) ViewName() string {
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

// CreateDDL generates a DDL script for the entity based on database type
// For entities with encrypted fields: creates table (with encrypted columns only) + view (with decrypted columns)
// For entities without encrypted fields: creates table only
func (e *DBEntity) CreateDDL(dbType DXDatabaseType) string {
	var sb strings.Builder
	hasEncrypted := e.HasEncryptedFields()

	// Add pgcrypto extension for DXDatabaseTypePostgreSQL
	if dbType == DXDatabaseTypePostgreSQL && hasEncrypted {
		sb.WriteString("CREATE EXTENSION IF NOT EXISTS pgcrypto;\n\n")
	}

	// Create table
	sb.WriteString(e.createTableDDL(dbType))

	// Create a view if there are encrypted fields
	if hasEncrypted {
		sb.WriteString("\n")
		sb.WriteString(e.createViewDDL(dbType))
	}

	return sb.String()
}

// createTableDDL generates the CREATE TABLE DDL only
func (e *DBEntity) createTableDDL(dbType DXDatabaseType) string {
	var sb strings.Builder
	tableName := e.FullTableName()

	// language=text
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", tableName))

	var columns []string
	for _, field := range e.Fields {
		if field.IsEncrypted && field.EncryptedDataName != "" {
			// For encrypted fields: only add encrypted_data_name and hash_data_name to the table
			encColDef := e.encryptedFieldToDDL(field, dbType)
			columns = append(columns, encColDef)

			if field.IsHashed && field.HashDataName != "" {
				hashColDef := e.hashedFieldToDDL(field, dbType)
				columns = append(columns, hashColDef)
			}
		} else {
			// For non-encrypted fields: add the original field name
			colDef := e.fieldToDDL(field, dbType)
			columns = append(columns, colDef)
		}
	}

	sb.WriteString("    " + strings.Join(columns, ",\n    "))
	sb.WriteString("\n)")

	// Add database-specific TDE options
	sb.WriteString(e.buildTDEClause(dbType))

	sb.WriteString(";\n")

	return sb.String()
}

// buildTDEClause generates the database-specific TDE clause for CREATE TABLE
func (e *DBEntity) buildTDEClause(dbType DXDatabaseType) string {
	switch dbType {
	case DXDatabaseTypePostgreSQL:
		// PostgreSQL: Use table access method for TDE (e.g., "tde_heap" with pg_tde extension)
		if e.TDE.PostgreSQLAccessMethod != "" {
			return fmt.Sprintf(" USING %s", e.TDE.PostgreSQLAccessMethod)
		}
	case DXDatabaseTypeOracle:
		// Oracle: Specify tablespace for encrypted storage
		if e.TDE.OracleTablespace != "" {
			return fmt.Sprintf(" TABLESPACE %s", e.TDE.OracleTablespace)
		}
	case DXDatabaseTypeSQLServer:
		// SQL Server: TDE is database-level, no per-table syntax
		// Add a comment to indicate TDE expectation if enabled
		if e.TDE.SQLServerTDEEnabled {
			return " /* TDE enabled at database level */"
		}
	case DXDatabaseTypeMariaDB:
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
func (e *DBEntity) createViewDDL(dbType DXDatabaseType) string {
	var sb strings.Builder
	viewName := e.FullViewName()
	tableName := e.FullTableName()

	sb.WriteString(fmt.Sprintf("CREATE VIEW %s AS\nSELECT\n", viewName))

	var viewCols []string
	for _, field := range e.Fields {
		if field.IsEncrypted && field.EncryptedDataName != "" {
			// Decrypt and alias to the original field name
			decryptExpr := e.buildDecryptExpr(field, dbType)
			viewCols = append(viewCols, "    "+decryptExpr)

			// Also include encrypted column for reference
			viewCols = append(viewCols, "    "+field.EncryptedDataName)

			// Include hash column if exists
			if field.IsHashed && field.HashDataName != "" {
				viewCols = append(viewCols, "    "+field.HashDataName)
			}
		} else {
			viewCols = append(viewCols, "    "+field.Name)
		}
	}

	sb.WriteString(strings.Join(viewCols, ",\n"))
	sb.WriteString(fmt.Sprintf("\nFROM %s;\n", tableName))

	return sb.String()
}

func (e *DBEntity) fieldToDDL(field types.Field, dbType DXDatabaseType) string {
	key := dbTypeToKey(dbType)
	dbTypeStr := field.Type.GetDbType(key)

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%s %s", field.Name, dbTypeStr))

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

	return sb.String()
}

// getDefaultValueForDBType returns the appropriate default value for the given database type
// Priority: 1. Field.DefaultValueByDBType, 2. Field.DefaultValue, 3. Field.Type.DefaultValueByDBType
func (e *DBEntity) getDefaultValueForDBType(field types.Field, dbType DXDatabaseType) string {
	key := dbTypeToKey(dbType)

	// 1. Check if field has database-specific default
	if field.DefaultValueByDBType != nil {
		if dbDefault, ok := field.DefaultValueByDBType[key]; ok && dbDefault != "" {
			return dbDefault
		}
	}

	// 2. Check field's generic default value
	if field.DefaultValue != "" {
		return field.DefaultValue
	}

	// 3. Check DataType's database-specific default (e.g., DataTypeUID)
	if field.Type.DefaultValueByDBType != nil {
		if dbDefault, ok := field.Type.DefaultValueByDBType[key]; ok && dbDefault != "" {
			return dbDefault
		}
	}

	return ""
}

func (e *DBEntity) encryptedFieldToDDL(field types.Field, dbType DXDatabaseType) string {
	key := dbTypeToKey(dbType)
	dbTypeStr := field.EncryptedDataType.GetDbType(key)
	return fmt.Sprintf("%s %s", field.EncryptedDataName, dbTypeStr)
}

func (e *DBEntity) hashedFieldToDDL(field types.Field, dbType DXDatabaseType) string {
	key := dbTypeToKey(dbType)
	dbTypeStr := field.HashDataType.GetDbType(key)
	return fmt.Sprintf("%s %s", field.HashDataName, dbTypeStr)
}

// SelectOne selects a single row from the view (decrypted data)
func (e *DBEntity) SelectOne(db *sql.DB, dbType DXDatabaseType, where string, args ...any) (utils.JSON, error) {
	columns := e.buildSelectColumns()
	viewName := e.FullViewName()

	var query string
	if dbType == DXDatabaseTypeSQLServer {
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
func (e *DBEntity) SelectMany(db *sql.DB, dbType DXDatabaseType, where string, args ...any) ([]utils.JSON, error) {
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
func (e *DBEntity) Insert(db *sql.DB, dbType DXDatabaseType, data utils.JSON) error {
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
func (e *DBEntity) Update(db *sql.DB, dbType DXDatabaseType, data utils.JSON, where string, whereArgs ...any) error {
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
// View already has decrypted columns, so we just select by field.Name
func (e *DBEntity) buildSelectColumns() string {
	var cols []string
	for _, field := range e.Fields {
		cols = append(cols, field.Name)
	}
	return strings.Join(cols, ", ")
}

func (e *DBEntity) buildDecryptExpr(field types.Field, dbType DXDatabaseType) string {
	encCol := field.EncryptedDataName
	keyExpr := BuildGetSessionConfigExpr(dbType, "app.encryption_key")
	switch dbType {
	case DXDatabaseTypePostgreSQL:
		// pgp_sym_decrypt(encrypted_col, key) AS original_name
		return fmt.Sprintf("pgp_sym_decrypt(%s, %s) AS %s", encCol, keyExpr, field.Name)
	case DXDatabaseTypeSQLServer:
		// DecryptByPassPhrase(key, encrypted_col) AS original_name
		return fmt.Sprintf("CONVERT(VARCHAR(MAX), DecryptByPassPhrase(%s, %s)) AS %s", keyExpr, encCol, field.Name)
	case DXDatabaseTypeMariaDB:
		// AES_DECRYPT(encrypted_col, key) AS original_name
		return fmt.Sprintf("AES_DECRYPT(%s, %s) AS %s", encCol, keyExpr, field.Name)
	case DXDatabaseTypeOracle:
		// DBMS_CRYPTO.DECRYPT using session context for a key
		return fmt.Sprintf("UTL_RAW.CAST_TO_VARCHAR2(DBMS_CRYPTO.DECRYPT(%s, DBMS_CRYPTO.ENCRYPT_AES256 + DBMS_CRYPTO.CHAIN_CBC + DBMS_CRYPTO.PAD_PKCS5, UTL_RAW.CAST_TO_RAW(%s))) AS %s", encCol, keyExpr, field.Name)
	default:
		return field.Name
	}
}

func (e *DBEntity) buildInsertData(dbType DXDatabaseType, data utils.JSON) (columns string, values string, args []any, err error) {
	var cols []string
	var vals []string
	argIndex := 1

	for _, field := range e.Fields {
		val, ok := data[field.Name]
		if !ok {
			continue
		}

		// Validate incoming value matches expected type
		if err := e.validateFieldValue(field, val); err != nil {
			return "", "", nil, err
		}

		if field.IsEncrypted && field.EncryptedDataName != "" {
			// Add an encrypted column
			cols = append(cols, field.EncryptedDataName)
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
			cols = append(cols, field.Name)
			vals = append(vals, e.placeholder(dbType, argIndex))
			args = append(args, val)
			argIndex++
		}
	}

	return strings.Join(cols, ", "), strings.Join(vals, ", "), args, nil
}

func (e *DBEntity) buildUpdateData(dbType DXDatabaseType, data utils.JSON) (setClause string, args []any, err error) {
	var sets []string
	argIndex := 1

	for _, field := range e.Fields {
		val, ok := data[field.Name]
		if !ok {
			continue
		}

		// Validate incoming value matches expected type
		if err := e.validateFieldValue(field, val); err != nil {
			return "", nil, err
		}

		if field.IsEncrypted && field.EncryptedDataName != "" {
			// Update encrypted column
			encExpr := e.buildEncryptExpr(dbType, argIndex)
			sets = append(sets, fmt.Sprintf("%s = %s", field.EncryptedDataName, encExpr))
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
			sets = append(sets, fmt.Sprintf("%s = %s", field.Name, e.placeholder(dbType, argIndex)))
			args = append(args, val)
			argIndex++
		}
	}

	return strings.Join(sets, ", "), args, nil
}

func (e *DBEntity) placeholder(dbType DXDatabaseType, index int) string {
	switch dbType {
	case DXDatabaseTypePostgreSQL:
		return fmt.Sprintf("$%d", index)
	case DXDatabaseTypeSQLServer:
		return fmt.Sprintf("@p%d", index)
	case DXDatabaseTypeOracle:
		return fmt.Sprintf(":p%d", index)
	default: // DXDatabaseTypeMariaDB/MySQL
		return "?"
	}
}

func (e *DBEntity) buildEncryptExpr(dbType DXDatabaseType, argIndex int) string {
	placeholder := e.placeholder(dbType, argIndex)
	keyExpr := BuildGetSessionConfigExpr(dbType, "app.encryption_key")
	switch dbType {
	case DXDatabaseTypePostgreSQL:
		return fmt.Sprintf("pgp_sym_encrypt(%s::text, %s)", placeholder, keyExpr)
	case DXDatabaseTypeSQLServer:
		return fmt.Sprintf("EncryptByPassPhrase(%s, %s)", keyExpr, placeholder)
	case DXDatabaseTypeMariaDB:
		return fmt.Sprintf("AES_ENCRYPT(%s, %s)", placeholder, keyExpr)
	case DXDatabaseTypeOracle:
		return fmt.Sprintf("DBMS_CRYPTO.ENCRYPT(UTL_RAW.CAST_TO_RAW(%s), DBMS_CRYPTO.ENCRYPT_AES256 + DBMS_CRYPTO.CHAIN_CBC + DBMS_CRYPTO.PAD_PKCS5, UTL_RAW.CAST_TO_RAW(%s))", placeholder, keyExpr)
	default:
		return placeholder
	}
}

func (e *DBEntity) buildHashExpr(dbType DXDatabaseType, argIndex int) string {
	placeholder := e.placeholder(dbType, argIndex)
	saltExpr := BuildGetSessionConfigExpr(dbType, "app.hash_salt")
	switch dbType {
	case DXDatabaseTypePostgreSQL:
		return fmt.Sprintf("digest(%s || %s, 'sha256')", placeholder, saltExpr)
	case DXDatabaseTypeSQLServer:
		return fmt.Sprintf("HASHBYTES('SHA2_256', CONCAT(%s, %s))", placeholder, saltExpr)
	case DXDatabaseTypeMariaDB:
		return fmt.Sprintf("SHA2(CONCAT(%s, %s), 256)", placeholder, saltExpr)
	case DXDatabaseTypeOracle:
		return fmt.Sprintf("DBMS_CRYPTO.HASH(UTL_RAW.CAST_TO_RAW(%s || %s), DBMS_CRYPTO.HASH_SH256)", placeholder, saltExpr)
	default:
		return placeholder
	}
}

func (e *DBEntity) validateFieldValue(field types.Field, val any) error {
	if val == nil {
		return nil
	}

	switch field.Type.GoType {
	case types.GoTypeString, types.GoTypeStringPointer:
		if _, ok := val.(string); !ok {
			if _, ok := val.(*string); !ok {
				return fmt.Errorf("field %s expects string, got %T", field.Name, val)
			}
		}
	case types.GoTypeInt64, types.GoTypeInt64Pointer:
		switch val.(type) {
		case int, int32, int64, float64:
			// OK - JSON numbers come as float64
		default:
			return fmt.Errorf("field %s expects int64, got %T", field.Name, val)
		}
	case types.GoTypeFloat32:
		switch val.(type) {
		case float32, float64:
			// OK
		default:
			return fmt.Errorf("field %s expects float32, got %T", field.Name, val)
		}
	case types.GoTypeFloat64:
		if _, ok := val.(float64); !ok {
			return fmt.Errorf("field %s expects float64, got %T", field.Name, val)
		}
	case types.GoTypeBool:
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("field %s expects bool, got %T", field.Name, val)
		}
	}
	return nil
}

func (e *DBEntity) scanRow(row *sql.Row) (utils.JSON, error) {
	result := make(utils.JSON)
	scanDest := make([]any, len(e.Fields))
	scanPtrs := make([]any, len(e.Fields))

	for i := range scanDest {
		scanPtrs[i] = &scanDest[i]
	}

	if err := row.Scan(scanPtrs...); err != nil {
		return nil, err
	}

	for i, field := range e.Fields {
		result[field.Name] = scanDest[i]
	}
	return result, nil
}

func (e *DBEntity) scanRows(rows *sql.Rows) (utils.JSON, error) {
	result := make(utils.JSON)
	scanDest := make([]any, len(e.Fields))
	scanPtrs := make([]any, len(e.Fields))

	for i := range scanDest {
		scanPtrs[i] = &scanDest[i]
	}

	if err := rows.Scan(scanPtrs...); err != nil {
		return nil, err
	}

	for i, field := range e.Fields {
		result[field.Name] = scanDest[i]
	}
	return result, nil
}
