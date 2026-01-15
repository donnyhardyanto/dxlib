package database3

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/configuration"
	database1Type "github.com/donnyhardyanto/dxlib/database/database_type"
	"github.com/donnyhardyanto/dxlib/types"
	"github.com/donnyhardyanto/dxlib/utils"
)

type DXDatabaseType int64

const (
	UnknownDatabaseType DXDatabaseType = iota
	DXDatabaseTypePostgreSQL
	DXDatabaseTypeMariaDB
	DXDatabaseTypeOracle
	DXDatabaseTypeSQLServer
	DXDatabaseTypeDeprecatedMysql
)

func (t DXDatabaseType) String() string {
	switch t {
	case DXDatabaseTypePostgreSQL:
		return "postgres"
	case DXDatabaseTypeOracle:
		return "oracle"
	case DXDatabaseTypeSQLServer:
		return "sqlserver"
	case DXDatabaseTypeMariaDB:
		return "mariadb"
	default:
		return "unknown"
	}
}

func (t DXDatabaseType) Driver() string {
	switch t {
	case DXDatabaseTypePostgreSQL:
		return "postgres"
	case DXDatabaseTypeOracle:
		return "oracle"
	case DXDatabaseTypeSQLServer:
		return "sqlserver"
	case DXDatabaseTypeMariaDB:
		return "mysql"
	default:
		return "unknown"
	}
}

func StringToDXDatabaseType(v string) DXDatabaseType {
	switch v {
	case "postgres", "postgresql":
		return DXDatabaseTypePostgreSQL
	case "mysql":
		return DXDatabaseTypeMariaDB
	case "mariadb":
		return DXDatabaseTypeMariaDB
	case "oracle":
		return DXDatabaseTypeOracle
	case "sqlserver":
		return DXDatabaseTypeSQLServer
	default:
		return UnknownDatabaseType
	}
}

func Database1DXDatabaseTypeToDXDatabaseType(dbType database1Type.DXDatabaseType) DXDatabaseType {
	switch dbType {
	case database1Type.PostgreSQL:
		return DXDatabaseTypePostgreSQL
	case database1Type.MariaDB:
		return DXDatabaseTypeMariaDB
	case database1Type.Oracle:
		return DXDatabaseTypeOracle
	case database1Type.SQLServer:
		return DXDatabaseTypeSQLServer
	default:
		return UnknownDatabaseType
	}
}

// DB represents a database with extensions and schemas
type DB struct {
	Name       string
	Extensions map[DXDatabaseType][]string // Database-specific extensions/features
	Schemas    []*DBSchema
}

// NewDB creates a new database
func NewDB(name string) *DB {
	return &DB{
		Name:       name,
		Extensions: make(map[DXDatabaseType][]string),
		Schemas:    []*DBSchema{},
	}
}

// AddExtensions adds extensions for a specific database type
func (d *DB) AddExtensions(dbType DXDatabaseType, extensions ...string) {
	if d.Extensions == nil {
		d.Extensions = make(map[DXDatabaseType][]string)
	}
	d.Extensions[dbType] = append(d.Extensions[dbType], extensions...)
}

// SetExtensions sets extensions for a specific database type (replaces existing)
func (d *DB) SetExtensions(dbType DXDatabaseType, extensions []string) {
	if d.Extensions == nil {
		d.Extensions = make(map[DXDatabaseType][]string)
	}
	d.Extensions[dbType] = extensions
}

// GetExtensions returns extensions for a specific database type
func (d *DB) GetExtensions(dbType DXDatabaseType) []string {
	if d.Extensions == nil {
		return nil
	}
	return d.Extensions[dbType]
}

// CreateDDL generates DDL script for the database including extensions and all schemas
func (d *DB) CreateDDL(dbType DXDatabaseType) string {
	var sb strings.Builder

	// Get extensions for the specific database type
	extensions := d.GetExtensions(dbType)
	if len(extensions) > 0 {
		switch dbType {
		case DXDatabaseTypePostgreSQL:
			// PostgreSQL: CREATE EXTENSION
			for _, ext := range extensions {
				sb.WriteString(fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s;\n", ext))
			}
			sb.WriteString("\n")
		case DXDatabaseTypeSQLServer:
			// SQL Server: Enable features/configurations
			for _, feature := range extensions {
				sb.WriteString(fmt.Sprintf("-- Enable SQL Server feature: %s\n", feature))
				// Example: sp_configure or ALTER DATABASE for specific features
				sb.WriteString(fmt.Sprintf("EXEC sp_configure '%s', 1;\nRECONFIGURE;\n", feature))
			}
			sb.WriteString("\n")
		case DXDatabaseTypeMariaDB:
			// MySQL/MariaDB: Install plugins or enable features
			for _, plugin := range extensions {
				sb.WriteString(fmt.Sprintf("-- Install MariaDB/MySQL plugin: %s\n", plugin))
				sb.WriteString(fmt.Sprintf("INSTALL PLUGIN IF NOT EXISTS %s;\n", plugin))
			}
			sb.WriteString("\n")
		case DXDatabaseTypeOracle:
			// Oracle: Grant privileges or enable features (typically done by DBA)
			for _, feature := range extensions {
				sb.WriteString(fmt.Sprintf("-- Oracle feature/package: %s (ensure enabled by DBA)\n", feature))
			}
			sb.WriteString("\n")
		default:
			panic("unhandled default case")
		}
	}

	// Create all schemas
	for _, schema := range d.Schemas {
		sb.WriteString(schema.CreateDDL(dbType))
		sb.WriteString("\n")
	}

	return sb.String()
}

// ============================================================================
// Session Configuration Functions
// ============================================================================

// ValidateSessionConfigKey validates that a session config key contains only safe characters
// Allowed: alphanumeric, dots, underscores
// Returns error if the key contains unsafe characters
func ValidateSessionConfigKey(key string) error {
	if key == "" {
		return fmt.Errorf("session config key cannot be empty")
	}
	for i, c := range key {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '_') {
			return fmt.Errorf("session config key contains invalid character '%c' at position %d: only alphanumeric, dots, and underscores are allowed", c, i)
		}
	}
	// Key must not start with a number
	if key[0] >= '0' && key[0] <= '9' {
		return fmt.Errorf("session config key cannot start with a number")
	}
	return nil
}

// parseOracleKey splits a key like "app.encryption_key" into namespace and attribute for Oracle context
func parseOracleKey(key string) (namespace string, attribute string) {
	parts := strings.SplitN(key, ".", 2)
	namespace = "APP_CTX"
	attribute = key
	if len(parts) == 2 {
		namespace = strings.ToUpper(parts[0]) + "_CTX"
		attribute = parts[1]
	}
	return
}

// BuildSetSessionConfigSQL generates SQL to set a session-level configuration variable
// NOTE: This function is for DDL/script generation only. For runtime execution, use SetSessionConfig()
// which uses parameterized queries to prevent SQL injection.
// PostgreSQL: SET app.key = 'value'
// SQL Server: EXEC sp_set_session_context @key = N'key', @value = N'value'
// Oracle: EXEC DBMS_SESSION.SET_CONTEXT('app_ctx', 'key', 'value')
// MariaDB/MySQL: SET @key = 'value'
func BuildSetSessionConfigSQL(dbType DXDatabaseType, key string, value string) string {
	switch dbType {
	case DXDatabaseTypePostgreSQL:
		// PostgreSQL uses SET for custom GUC variables
		// Key format: "namespace.variable" e.g., "app.encryption_key"
		return fmt.Sprintf("SET %s = '%s'", key, value)
	case DXDatabaseTypeSQLServer:
		// SQL Server 2016+ uses sp_set_session_context
		return fmt.Sprintf("EXEC sp_set_session_context @key = N'%s', @value = N'%s'", key, value)
	case DXDatabaseTypeOracle:
		// Oracle uses application context (requires context to be created first)
		namespace, attribute := parseOracleKey(key)
		return fmt.Sprintf("BEGIN DBMS_SESSION.SET_CONTEXT('%s', '%s', '%s'); END;", namespace, attribute, value)
	case DXDatabaseTypeMariaDB:
		// MySQL/MariaDB uses user-defined variables with @ prefix
		// Replace dots with underscores for variable name
		varName := strings.ReplaceAll(key, ".", "_")
		return fmt.Sprintf("SET @%s = '%s'", varName, value)
	default:
		return fmt.Sprintf("-- Unknown database type for setting: %s = %s", key, value)
	}
}

// BuildGetSessionConfigExpr returns the SQL expression to retrieve a session configuration value
// This expression can be used within SQL queries (e.g., in SELECT, WHERE, or function calls)
// NOTE: The key parameter should be validated with ValidateSessionConfigKey() before use
// PostgreSQL: current_setting('app.key')
// SQL Server: SESSION_CONTEXT(N'key')
// Oracle: SYS_CONTEXT('APP_CTX', 'key')
// MariaDB/MySQL: @key
func BuildGetSessionConfigExpr(dbType DXDatabaseType, key string) string {
	switch dbType {
	case DXDatabaseTypePostgreSQL:
		return fmt.Sprintf("current_setting('%s')", key)
	case DXDatabaseTypeSQLServer:
		return fmt.Sprintf("CAST(SESSION_CONTEXT(N'%s') AS NVARCHAR(MAX))", key)
	case DXDatabaseTypeOracle:
		namespace, attribute := parseOracleKey(key)
		return fmt.Sprintf("SYS_CONTEXT('%s', '%s')", namespace, attribute)
	case DXDatabaseTypeMariaDB:
		// MySQL/MariaDB uses user-defined variables
		varName := strings.ReplaceAll(key, ".", "_")
		return fmt.Sprintf("@%s", varName)
	default:
		return fmt.Sprintf("'%s'", key) // Fallback to literal
	}
}

// BuildGetSessionConfigSQL generates a complete SQL query to retrieve a session configuration value
// NOTE: This function is for DDL/script generation only. For runtime execution, use GetSessionConfig()
// which uses parameterized queries to prevent SQL injection.
// PostgreSQL: SELECT current_setting('app.key')
// SQL Server: SELECT SESSION_CONTEXT(N'key')
// Oracle: SELECT SYS_CONTEXT('APP_CTX', 'key') FROM DUAL
// MariaDB/MySQL: SELECT @key
func BuildGetSessionConfigSQL(dbType DXDatabaseType, key string) string {
	expr := BuildGetSessionConfigExpr(dbType, key)
	switch dbType {
	case DXDatabaseTypeOracle:
		return fmt.Sprintf("SELECT "+"%s FROM DUAL", expr)
	default:
		return fmt.Sprintf("SELECT %s", expr)
	}
}

// SetSessionConfig executes the SET command on a database connection using parameterized queries
// This is safe from SQL injection as it validates the key and uses parameterized queries for the value
func SetSessionConfig(db *sql.DB, dbType DXDatabaseType, key string, value string) error {
	// Validate key to prevent SQL injection
	if err := ValidateSessionConfigKey(key); err != nil {
		return fmt.Errorf("invalid session config key: %w", err)
	}

	switch dbType {
	case DXDatabaseTypePostgreSQL:
		// Use set_config() function which accepts parameters
		// set_config(setting_name, new_value, is_local) - is_local=false means session-level
		_, err := db.Exec("SELECT set_config($1, $2, false)", key, value)
		return err
	case DXDatabaseTypeSQLServer:
		// sp_set_session_context accepts parameters
		_, err := db.Exec("EXEC "+"sp_set_session_context @key = @p1, @value = @p2", key, value)
		return err
	case DXDatabaseTypeOracle:
		// Oracle: use bind variables in PL/SQL block
		namespace, attribute := parseOracleKey(key)
		// Validate namespace and attribute as well (they're derived from a key which is already validated)
		_, err := db.Exec("BEGIN "+"DBMS_SESSION.SET_CONTEXT(:1, :2, :3); END;", namespace, attribute, value)
		return err
	case DXDatabaseTypeMariaDB:
		// MySQL/MariaDB: use prepared statement
		// Note: User variable names cannot be parameterized, but the key is validated
		varName := strings.ReplaceAll(key, ".", "_")
		// Re-validate varName after transformation
		if err := ValidateSessionConfigKey(varName); err != nil {
			return fmt.Errorf("invalid transformed variable name: %w", err)
		}
		// For MariaDB, we need to use a different approach since SET @var =? doesn't work directly
		// We use a prepared statement with the value as a parameter
		query := fmt.Sprintf("SET @%s = ?", varName)
		_, err := db.Exec(query, value)
		return err
	default:
		return fmt.Errorf("unsupported database type for SetSessionConfig: %v", dbType)
	}
}

// GetSessionConfig retrieves a session configuration value from the database using parameterized queries
// This is safe from SQL injection as it validates the key and uses parameterized queries
func GetSessionConfig(db *sql.DB, dbType DXDatabaseType, key string) (string, error) {
	// Validate key to prevent SQL injection
	if err := ValidateSessionConfigKey(key); err != nil {
		return "", fmt.Errorf("invalid session config key: %w", err)
	}

	var value sql.NullString
	var err error

	switch dbType {
	case DXDatabaseTypePostgreSQL:
		// current_setting() accepts parameter
		err = db.QueryRow("SELECT current_setting($1)", key).Scan(&value)
	case DXDatabaseTypeSQLServer:
		// SESSION_CONTEXT accepts parameter
		err = db.QueryRow("SELECT "+"CAST(SESSION_CONTEXT(@p1) AS NVARCHAR(MAX))", key).Scan(&value)
	case DXDatabaseTypeOracle:
		// SYS_CONTEXT accepts parameters
		namespace, attribute := parseOracleKey(key)
		s := "SELECT " + "SYS_CONTEXT(:1, :2) FROM DUAL"
		err = db.QueryRow(s, namespace, attribute).Scan(&value)
	case DXDatabaseTypeMariaDB:
		// MySQL/MariaDB user variables - variable name cannot be parameterized but key is validated
		varName := strings.ReplaceAll(key, ".", "_")
		query := fmt.Sprintf("SELECT @%s", varName)
		err = db.QueryRow(query).Scan(&value)
	default:
		return "", fmt.Errorf("unsupported database type for GetSessionConfig: %v", dbType)
	}

	if err != nil {
		return "", err
	}
	if !value.Valid {
		return "", nil
	}
	return value.String, nil
}

// BuildCreateContextDDL generates DDL to create the application context (required for Oracle)
// For other databases, this returns an empty string or comment
func BuildCreateContextDDL(dbType DXDatabaseType, namespace string) string {
	switch dbType {
	case DXDatabaseTypeOracle:
		// Oracle requires creating a context before using it
		ctxName := strings.ToUpper(namespace) + "_CTX"
		return fmt.Sprintf("CREATE OR REPLACE CONTEXT %s USING %s_PKG ACCESSED GLOBALLY;\n", ctxName, ctxName)
	case DXDatabaseTypePostgreSQL:
		// PostgreSQL doesn't require pre-creation for custom GUC variables,
		// But you may need to add to postgresql.conf: custom_variable_classes = 'app'
		return fmt.Sprintf("-- PostgreSQL: Ensure '%s' namespace is allowed in postgresql.conf\n-- Add: custom_variable_classes = '%s'\n", namespace, namespace)
	default:
		return ""
	}
}

// ============================================================================
// UID/Unique ID Default Expression Functions
// ============================================================================

// BuildUIDDefaultExpr generates a database-specific default expression for unique ID generation
// Format: hex(timestamp_microseconds) + uuid
// PostgreSQL: CONCAT(to_hex((extract(epoch from now()) * 1000000)::bigint), gen_random_uuid()::text)
// SQL Server: CONCAT(CONVERT(VARCHAR(50), CAST(DATEDIFF_BIG(MICROSECOND, '1970-01-01', SYSUTCDATETIME()) AS VARBINARY(8)), 2), LOWER(REPLACE(CONVERT(VARCHAR(36), NEWID()), '-', ”)))
// Oracle: LOWER(TO_CHAR((CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE) - TO_DATE('1970-01-01','YYYY-MM-DD')) * 86400000000, 'XXXXXXXXXXXXXXXX')) || LOWER(RAWTOHEX(SYS_GUID()))
// MariaDB/MySQL: CONCAT(HEX(UNIX_TIMESTAMP(NOW(6)) * 1000000), REPLACE(UUID(), '-', ”))
func BuildUIDDefaultExpr(dbType DXDatabaseType) string {
	switch dbType {
	case DXDatabaseTypePostgreSQL:
		// PostgreSQL: hex timestamp (microseconds since epoch) + UUID
		return "CONCAT(to_hex((extract(epoch from now()) * 1000000)::bigint), gen_random_uuid()::text)"
	case DXDatabaseTypeSQLServer:
		// SQL Server: hex timestamp (microseconds since 1970) + UUID without dashes
		// DATEDIFF_BIG returns microseconds, convert to hex, then append NEWID
		return "CONCAT(CONVERT(VARCHAR(50), CAST(DATEDIFF_BIG(MICROSECOND, '1970-01-01', SYSUTCDATETIME()) AS VARBINARY(8)), 2), LOWER(REPLACE(CONVERT(VARCHAR(36), NEWID()), '-', '')))"
	case DXDatabaseTypeOracle:
		// Oracle: hex timestamp + SYS_GUID
		// Calculate microseconds since epoch, convert to hex, append SYS_GUID
		return "LOWER(TO_CHAR(ROUND((CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE) - TO_DATE('1970-01-01','YYYY-MM-DD')) * 86400000000), 'XXXXXXXXXXXXXXXX')) || LOWER(RAWTOHEX(SYS_GUID()))"
	case DXDatabaseTypeMariaDB:
		// MySQL/MariaDB: hex timestamp (microseconds) + UUID without dashes
		// NOW(6) gives microsecond precision, UNIX_TIMESTAMP converts to seconds with a fraction
		return "CONCAT(HEX(FLOOR(UNIX_TIMESTAMP(NOW(6)) * 1000000)), REPLACE(UUID(), '-', ''))"
	default:
		// Fallback to PostgreSQL syntax
		return "CONCAT(to_hex((extract(epoch from now()) * 1000000)::bigint), gen_random_uuid()::text)"
	}
}

// UIDDefaultExprPostgreSQL is a constant for PostgreSQL UID default expression
const UIDDefaultExprPostgreSQL = "CONCAT(to_hex((extract(epoch from now()) * 1000000)::bigint), gen_random_uuid()::text)"

// UIDDefaultExprSQLServer is a constant for SQL Server UID default expression
const UIDDefaultExprSQLServer = "CONCAT(CONVERT(VARCHAR(50), CAST(DATEDIFF_BIG(MICROSECOND, '1970-01-01', SYSUTCDATETIME()) AS VARBINARY(8)), 2), LOWER(REPLACE(CONVERT(VARCHAR(36), NEWID()), '-', '')))"

// UIDDefaultExprOracle is a constant for Oracle UID default expression
const UIDDefaultExprOracle = "LOWER(TO_CHAR(ROUND((CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE) - TO_DATE('1970-01-01','YYYY-MM-DD')) * 86400000000), 'XXXXXXXXXXXXXXXX')) || LOWER(RAWTOHEX(SYS_GUID()))"

// UIDDefaultExprMariaDB is a constant for MariaDB/MySQL UID default expression
const UIDDefaultExprMariaDB = "CONCAT(HEX(FLOOR(UNIX_TIMESTAMP(NOW(6)) * 1000000)), REPLACE(UUID(), '-', ''))"

type DBSchema struct {
	Name     string
	DB       *DB
	Entities []*DBEntity
}

// NewDBSchema creates a new database schema and registers it with the DB
func NewDBSchema(db *DB, name string) *DBSchema {
	schema := &DBSchema{
		Name:     name,
		DB:       db,
		Entities: []*DBEntity{},
	}
	if db != nil {
		db.Schemas = append(db.Schemas, schema)
	}
	return schema
}

// CreateDDL generates DDL script for the schema and all its entities
func (s *DBSchema) CreateDDL(dbType DXDatabaseType) string {
	var sb strings.Builder

	// Create schema statement
	switch dbType {
	case DXDatabaseTypePostgreSQL:
		sb.WriteString(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;\n\n", s.Name))
	case DXDatabaseTypeSQLServer:
		sb.WriteString(fmt.Sprintf("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '%s')\nBEGIN\n    EXEC('CREATE SCHEMA %s')\nEND;\n\n", s.Name, s.Name))
	case DXDatabaseTypeMariaDB:
		// MySQL/MariaDB uses "database" instead of schema
		// language=text
		sb.WriteString(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;\nUSE %s;\n\n", s.Name, s.Name))
	case DXDatabaseTypeOracle:
		// Oracle uses users as schemas, typically created by DBA
		sb.WriteString(fmt.Sprintf("-- Oracle: Schema %s should be created by DBA\n\n", s.Name))
	default:
		panic("unhandled default case")
	}

	// Add pgcrypto extension for PostgreSQL if any entity has encrypted fields
	if dbType == DXDatabaseTypePostgreSQL {
		for _, entity := range s.Entities {
			if entity.HasEncryptedFields() {
				sb.WriteString("CREATE EXTENSION IF NOT EXISTS pgcrypto;\n\n")
				break
			}
		}
	}

	// Create DDL for all entities
	for _, entity := range s.Entities {
		sb.WriteString(entity.createTableDDL(dbType))
		sb.WriteString("\n")
	}

	// Create views for entities with encrypted fields (after all tables are created)
	for _, entity := range s.Entities {
		if entity.HasEncryptedFields() {
			sb.WriteString(entity.createViewDDL(dbType))
			sb.WriteString("\n")
		}
	}

	return sb.String()
}

type DBEntity struct {
	types.Entity
	Schema            *DBSchema
	TableAccessMethod string // e.g., "tde_heap" for PostgreSQL transparent data encryption
	UseTableSuffix    bool   // If true, adds "_t" suffix to the table name and "_v" suffix to view name
}

// NewDBEntity creates a new database entity and registers it with the schema
func NewDBEntity(schema *DBSchema, entity types.Entity) *DBEntity {
	dbEntity := &DBEntity{Entity: entity, Schema: schema}
	if schema != nil {
		schema.Entities = append(schema.Entities, dbEntity)
	}
	return dbEntity
}

// NewDBEntityWithAccessMethod creates a new database entity with a table access method
func NewDBEntityWithAccessMethod(schema *DBSchema, entity types.Entity, tableAccessMethod string) *DBEntity {
	dbEntity := &DBEntity{Entity: entity, Schema: schema, TableAccessMethod: tableAccessMethod}
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

	// Add a table access method (PostgreSQL only)
	if e.TableAccessMethod != "" && dbType == DXDatabaseTypePostgreSQL {
		sb.WriteString(fmt.Sprintf(" USING %s", e.TableAccessMethod))
	}

	sb.WriteString(";\n")

	return sb.String()
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
	var dbTypeStr string
	switch dbType {
	case DXDatabaseTypePostgreSQL:
		dbTypeStr = field.Type.DbTypePostgreSQL
	case DXDatabaseTypeSQLServer:
		dbTypeStr = field.Type.DbTypeSqlserver
	case DXDatabaseTypeMariaDB:
		dbTypeStr = field.Type.DbTypeMysql
	case DXDatabaseTypeOracle:
		dbTypeStr = field.Type.DbTypeOracle
	default:
		dbTypeStr = field.Type.DbTypePostgreSQL
	}

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
// It first checks DefaultValueByDBType map, then falls back to DefaultValue
func (e *DBEntity) getDefaultValueForDBType(field types.Field, dbType DXDatabaseType) string {
	// Check if database-specific default is defined
	if field.DefaultValueByDBType != nil {
		var key string
		switch dbType {
		case DXDatabaseTypePostgreSQL:
			key = types.DBTypeKeyPostgreSQL
		case DXDatabaseTypeSQLServer:
			key = types.DBTypeKeySQLServer
		case DXDatabaseTypeOracle:
			key = types.DBTypeKeyOracle
		case DXDatabaseTypeMariaDB:
			key = types.DBTypeKeyMariaDB
		default:
			panic("unhandled default case")
		}
		if dbDefault, ok := field.DefaultValueByDBType[key]; ok && dbDefault != "" {
			return dbDefault
		}
	}
	// Fall back to generic default value
	return field.DefaultValue
}

func (e *DBEntity) encryptedFieldToDDL(field types.Field, dbType DXDatabaseType) string {
	var dbTypeStr string
	switch dbType {
	case DXDatabaseTypePostgreSQL:
		dbTypeStr = field.EncryptedDataType.DbTypePostgreSQL
	case DXDatabaseTypeSQLServer:
		dbTypeStr = field.EncryptedDataType.DbTypeSqlserver
	case DXDatabaseTypeMariaDB:
		dbTypeStr = field.EncryptedDataType.DbTypeMysql
	case DXDatabaseTypeOracle:
		dbTypeStr = field.EncryptedDataType.DbTypeOracle
	default:
		dbTypeStr = field.EncryptedDataType.DbTypePostgreSQL
	}
	return fmt.Sprintf("%s %s", field.EncryptedDataName, dbTypeStr)
}

func (e *DBEntity) hashedFieldToDDL(field types.Field, dbType DXDatabaseType) string {
	var dbTypeStr string
	switch dbType {
	case DXDatabaseTypePostgreSQL:
		dbTypeStr = field.HashDataType.DbTypePostgreSQL
	case DXDatabaseTypeSQLServer:
		dbTypeStr = field.HashDataType.DbTypeSqlserver
	case DXDatabaseTypeMariaDB:
		dbTypeStr = field.HashDataType.DbTypeMysql
	case DXDatabaseTypeOracle:
		dbTypeStr = field.HashDataType.DbTypeOracle
	default:
		dbTypeStr = field.HashDataType.DbTypePostgreSQL
	}
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
