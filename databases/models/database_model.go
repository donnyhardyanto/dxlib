package models

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/types"
)

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
func BuildSetSessionConfigSQL(dbType base.DXDatabaseType, key string, value string) string {
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		// PostgreSQL uses SET for custom GUC variables
		// Key format: "namespace.variable" e.g., "app.encryption_key"
		return fmt.Sprintf("SET %s = '%s'", key, value)
	case base.DXDatabaseTypeSQLServer:
		// SQL Server 2016+ uses sp_set_session_context
		return fmt.Sprintf("EXEC sp_set_session_context @key = N'%s', @value = N'%s'", key, value)
	case base.DXDatabaseTypeOracle:
		// Oracle uses application context (requires context to be created first)
		namespace, attribute := parseOracleKey(key)
		return fmt.Sprintf("BEGIN DBMS_SESSION.SET_CONTEXT('%s', '%s', '%s'); END;", namespace, attribute, value)
	case base.DXDatabaseTypeMariaDB:
		// MySQL/MariaDB uses user-defined variables with @ prefix
		// Replace dots with underscores for variable name
		varName := strings.ReplaceAll(key, ".", "_")
		return fmt.Sprintf("SET @%s = '%s'", varName, value)
	default:
		return fmt.Sprintf("-- Unknown databases type for setting: %s = %s", key, value)
	}
}

// BuildGetSessionConfigExpr returns the SQL expression to retrieve a session configuration value
// This expression can be used within SQL queries (e.g., in SELECT, WHERE, or function calls)
// NOTE: The key parameter should be validated with ValidateSessionConfigKey() before use
// PostgreSQL: current_setting('app.key')
// SQL Server: SESSION_CONTEXT(N'key')
// Oracle: SYS_CONTEXT('APP_CTX', 'key')
// MariaDB/MySQL: @key
func BuildGetSessionConfigExpr(dbType base.DXDatabaseType, key string) string {
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return fmt.Sprintf("current_setting('%s')", key)
	case base.DXDatabaseTypeSQLServer:
		return fmt.Sprintf("CAST(SESSION_CONTEXT(N'%s') AS NVARCHAR(MAX))", key)
	case base.DXDatabaseTypeOracle:
		namespace, attribute := parseOracleKey(key)
		return fmt.Sprintf("SYS_CONTEXT('%s', '%s')", namespace, attribute)
	case base.DXDatabaseTypeMariaDB:
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
func BuildGetSessionConfigSQL(dbType base.DXDatabaseType, key string) string {
	expr := BuildGetSessionConfigExpr(dbType, key)
	switch dbType {
	case base.DXDatabaseTypeOracle:
		return fmt.Sprintf("SELECT "+"%s FROM DUAL", expr)
	default:
		return fmt.Sprintf("SELECT %s", expr)
	}
}

// SetSessionConfig executes the SET command on a databases connection using parameterized queries
// This is safe from SQL injection as it validates the key and uses parameterized queries for the value
func SetSessionConfig(db *sql.DB, dbType base.DXDatabaseType, key string, value string) error {
	// Validate key to prevent SQL injection
	if err := ValidateSessionConfigKey(key); err != nil {
		return fmt.Errorf("invalid session config key: %w", err)
	}

	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		// Use set_config() function which accepts parameters
		// set_config(setting_name, new_value, is_local) - is_local=false means session-level
		_, err := db.Exec("SELECT set_config($1, $2, false)", key, value)
		return err
	case base.DXDatabaseTypeSQLServer:
		// sp_set_session_context accepts parameters
		_, err := db.Exec("EXEC "+"sp_set_session_context @key = @p1, @value = @p2", key, value)
		return err
	case base.DXDatabaseTypeOracle:
		// Oracle: use bind variables in PL/SQL block
		namespace, attribute := parseOracleKey(key)
		// Validate namespace and attribute as well (they're derived from a key which is already validated)
		_, err := db.Exec("BEGIN "+"DBMS_SESSION.SET_CONTEXT(:1, :2, :3); END;", namespace, attribute, value)
		return err
	case base.DXDatabaseTypeMariaDB:
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
		return fmt.Errorf("unsupported databases type for SetSessionConfig: %v", dbType)
	}
}

// GetSessionConfig retrieves a session configuration value from the databases using parameterized queries
// This is safe from SQL injection as it validates the key and uses parameterized queries
func GetSessionConfig(db *sql.DB, dbType base.DXDatabaseType, key string) (string, error) {
	// Validate key to prevent SQL injection
	if err := ValidateSessionConfigKey(key); err != nil {
		return "", fmt.Errorf("invalid session config key: %w", err)
	}

	var value sql.NullString
	var err error

	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		// current_setting() accepts parameter
		err = db.QueryRow("SELECT current_setting($1)", key).Scan(&value)
	case base.DXDatabaseTypeSQLServer:
		// SESSION_CONTEXT accepts parameter
		err = db.QueryRow("SELECT "+"CAST(SESSION_CONTEXT(@p1) AS NVARCHAR(MAX))", key).Scan(&value)
	case base.DXDatabaseTypeOracle:
		// SYS_CONTEXT accepts parameters
		namespace, attribute := parseOracleKey(key)
		s := "SELECT " + "SYS_CONTEXT(:1, :2) FROM DUAL"
		err = db.QueryRow(s, namespace, attribute).Scan(&value)
	case base.DXDatabaseTypeMariaDB:
		// MySQL/MariaDB user variables - variable name cannot be parameterized but key is validated
		varName := strings.ReplaceAll(key, ".", "_")
		query := fmt.Sprintf("SELECT @%s", varName)
		err = db.QueryRow(query).Scan(&value)
	default:
		return "", fmt.Errorf("unsupported databases type for GetSessionConfig: %v", dbType)
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
func BuildCreateContextDDL(dbType base.DXDatabaseType, namespace string) string {
	switch dbType {
	case base.DXDatabaseTypeOracle:
		// Oracle requires creating a context before using it
		ctxName := strings.ToUpper(namespace) + "_CTX"
		return fmt.Sprintf("CREATE OR REPLACE CONTEXT %s USING %s_PKG ACCESSED GLOBALLY;\n", ctxName, ctxName)
	case base.DXDatabaseTypePostgreSQL:
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

// BuildUIDDefaultExpr generates a databases-specific default expression for unique ID generation
// Format: hex(timestamp_microseconds) + uuid
func BuildUIDDefaultExpr(dbType base.DXDatabaseType) string {
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return types.UIDDefaultExprPostgreSQL
	case base.DXDatabaseTypeSQLServer:
		return types.UIDDefaultExprSQLServer
	case base.DXDatabaseTypeOracle:
		return types.UIDDefaultExprOracle
	case base.DXDatabaseTypeMariaDB:
		return types.UIDDefaultExprMariaDB
	default:
		return types.UIDDefaultExprPostgreSQL
	}
}
