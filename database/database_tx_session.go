package database

import (
	"fmt"
	"os"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/database/models"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/secure_memory"
)

// TxSetSessionKey sets a session-level configuration key in the transaction
//
// Parameters:
//   - dbType: database type (PostgreSQL, SQLServer, Oracle, MariaDB)
//   - sourceType: where to get the value from
//   - sourceValue: meaning depends on sourceType:
//   - ModelDBKeySourceRaw: the actual value to set
//   - ModelDBKeySourceEnv: environment variable name
//   - ModelDBKeySourceConfig: config key name (format: "configName.keyPath")
//   - ModelDBKeySourceSecureMemory: secure memory key
//   - sessionKey: the database session config key name (e.g., "app.encryption_key")
func (dtx *DXDatabaseTx) TxSetSessionKey(dbType base.DXDatabaseType, sourceType models.ModelDBKeySource, sourceValue string, sessionKey string) error {
	// Get the value based on source type
	value, err := resolveKeyValue(sourceType, sourceValue)
	if err != nil {
		return errors.Wrapf(err, "TX_SET_SESSION_KEY_RESOLVE_VALUE_ERROR:%s", sessionKey)
	}

	// Validate session key
	if err := models.ValidateSessionConfigKey(sessionKey); err != nil {
		return errors.Wrapf(err, "TX_SET_SESSION_KEY_INVALID_SESSION_KEY:%s", sessionKey)
	}

	// Set the session config on the transaction
	err = txSetSessionConfig(dtx, dbType, sessionKey, value)
	if err != nil {
		return errors.Wrapf(err, "TX_SET_SESSION_KEY_SET_CONFIG_ERROR:%s", sessionKey)
	}

	return nil
}

// txSetSessionConfig executes the SET command on a transaction
func txSetSessionConfig(dtx *DXDatabaseTx, dbType base.DXDatabaseType, key string, value string) error {
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		// Use set_config() function which accepts parameters
		_, err := dtx.Tx.Exec("SELECT set_config($1, $2, true)", key, value)
		return err
	case base.DXDatabaseTypeSQLServer:
		// sp_set_session_context accepts parameters
		_, err := dtx.Tx.Exec("EXEC sp_set_session_context @key = @p1, @value = @p2", key, value)
		return err
	case base.DXDatabaseTypeOracle:
		// Oracle: use bind variables in PL/SQL block
		namespace, attribute := parseOracleKey(key)
		_, err := dtx.Tx.Exec("BEGIN DBMS_SESSION.SET_CONTEXT(:1, :2, :3); END;", namespace, attribute, value)
		return err
	case base.DXDatabaseTypeMariaDB:
		// MySQL/MariaDB: use prepared statement
		varName := strings.ReplaceAll(key, ".", "_")
		if err := models.ValidateSessionConfigKey(varName); err != nil {
			return fmt.Errorf("invalid transformed variable name: %w", err)
		}
		query := fmt.Sprintf("SET @%s = ?", varName)
		_, err := dtx.Tx.Exec(query, value)
		return err
	default:
		return fmt.Errorf("unsupported database type for TxSetSessionKey: %v", dbType)
	}
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

// TxSetSessionKeyFromSecureMemory is a convenience method to set session key from secure memory
// This simplifies the common case of loading encryption keys from secure memory into DB session
// DatabaseType is obtained from dtx.Database.DatabaseType
//
// Parameters:
//   - secureMemoryKey: the key name in secure memory (previously stored via secure_memory.Manager.StoreEnclave)
//   - sessionKey: the database session config key name (e.g., "app.encryption_key")
func (dtx *DXDatabaseTx) TxSetSessionKeyFromSecureMemory(secureMemoryKey string, sessionKey string) error {
	return dtx.TxSetSessionKey(dtx.Database.DatabaseType, models.ModelDBKeySourceSecureMemory, secureMemoryKey, sessionKey)
}

// resolveKeyValue gets the actual value based on source type
func resolveKeyValue(sourceType models.ModelDBKeySource, sourceValue string) (string, error) {
	switch sourceType {
	case models.ModelDBKeySourceRaw:
		// Direct value
		return sourceValue, nil

	case models.ModelDBKeySourceEnv:
		// Get from environment variable
		value := os.Getenv(sourceValue)
		if value == "" {
			return "", errors.Errorf("ENVIRONMENT_VARIABLE_NOT_FOUND_OR_EMPTY:%s", sourceValue)
		}
		return value, nil

	case models.ModelDBKeySourceConfig:
		// Get from configuration
		// Format: "configName.keyPath" e.g., "encryption.db_key"
		parts := strings.SplitN(sourceValue, ".", 2)
		if len(parts) != 2 {
			return "", errors.Errorf("INVALID_CONFIG_KEY_FORMAT:%s (expected: configName.keyPath)", sourceValue)
		}
		configName := parts[0]
		keyPath := parts[1]

		config, ok := configuration.Manager.Configurations[configName]
		if !ok {
			return "", errors.Errorf("CONFIGURATION_NOT_FOUND:%s", configName)
		}

		// Get value from config data
		if config.Data == nil {
			return "", errors.Errorf("CONFIGURATION_DATA_IS_NIL:%s", configName)
		}

		value, ok := (*config.Data)[keyPath]
		if !ok {
			return "", errors.Errorf("CONFIGURATION_KEY_NOT_FOUND:%s.%s", configName, keyPath)
		}

		valueStr, ok := value.(string)
		if !ok {
			return "", errors.Errorf("CONFIGURATION_VALUE_NOT_STRING:%s.%s", configName, keyPath)
		}

		return valueStr, nil

	case models.ModelDBKeySourceSecureMemory:
		// Get from secure memory (stored from vault)
		data, err := secure_memory.Manager.Get(sourceValue)
		if err != nil {
			return "", errors.Wrapf(err, "SECURE_MEMORY_GET_ERROR:%s", sourceValue)
		}
		return string(data), nil

	case models.ModelDBKeySourceDbSessionCurrentSetting:
		// This source type is for reading from DB session, not for setting
		return "", errors.Errorf("INVALID_SOURCE_TYPE_FOR_SET_SESSION_KEY:DbSessionCurrentSetting")

	default:
		return "", errors.Errorf("UNKNOWN_KEY_SOURCE_TYPE:%d", sourceType)
	}
}
