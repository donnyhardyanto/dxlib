package tables

import (
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/errors"
)

// Encryption Column Definition

// EncryptionColumn defines encryption config for a single column with its value.
// Used for INSERT/UPDATE (encryption) and SELECT (decryption).
type EncryptionColumn struct {
	FieldName          string                     // actual DB column name (e.g., "fullname_encrypted")
	DataFieldName      string                     // field name in data JSON for INSERT/UPDATE (e.g., "fullname")
	AliasName          string                     // output alias for SELECT (e.g., "fullname")
	Value              any                        // plaintext value to encrypt (for INSERT/UPDATE only)
	EncryptionKeyDef   *database.EncryptionKeyDef // encryption key definition (must not be nil when used)
	HashFieldName      string                     // optional: hash field for searchable hash (e.g., "fullname_hash")
	HashSaltMemoryKey  string                     // optional: secure memory key for hash salt
	HashSaltSessionKey string                     // optional: DB session key for hash salt (e.g., "app.hash_salt")
	ViewHasDecrypt     bool                       // true = view already has pgp_sym_decrypt, just set session key and select AliasName
}

// Internal Shared Helper Functions

// setSessionKeysForEncryption sets all unique session keys from secure memory
func setSessionKeysForEncryption(dtx *database.DXDatabaseTx, encryptionColumns []EncryptionColumn) error {
	// Collect unique session keys to set
	sessionKeys := make(map[string]string) // sessionKey -> secureMemoryKey

	for _, col := range encryptionColumns {
		if col.EncryptionKeyDef != nil && col.EncryptionKeyDef.SecureMemoryKey != "" && col.EncryptionKeyDef.SessionKey != "" {
			sessionKeys[col.EncryptionKeyDef.SessionKey] = col.EncryptionKeyDef.SecureMemoryKey
		}
		if col.HashSaltMemoryKey != "" && col.HashSaltSessionKey != "" {
			sessionKeys[col.HashSaltSessionKey] = col.HashSaltMemoryKey
		}
	}

	// Set each session key
	for sessionKey, memoryKey := range sessionKeys {
		if err := dtx.TxSetSessionKeyFromSecureMemory(memoryKey, sessionKey); err != nil {
			return errors.Wrapf(err, "ENCRYPTED_INSERT_SET_SESSION_KEY_ERROR:%s", sessionKey)
		}
	}

	return nil
}

// placeholder returns database-specific placeholder
func placeholder(dbType base.DXDatabaseType, index int) string {
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return fmt.Sprintf("$%d", index)
	case base.DXDatabaseTypeSQLServer:
		return fmt.Sprintf("@p%d", index)
	case base.DXDatabaseTypeOracle:
		return fmt.Sprintf(":%d", index)
	default:
		return "?"
	}
}

// encryptExpression returns database-specific encryption SQL expression
func encryptExpression(dbType base.DXDatabaseType, argIndex int, sessionKey string) string {
	ph := placeholder(dbType, argIndex)
	keyExpr := sessionKeyExpression(dbType, sessionKey)

	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return fmt.Sprintf("pgp_sym_encrypt(%s, %s)", ph, keyExpr)
	case base.DXDatabaseTypeSQLServer:
		return fmt.Sprintf("ENCRYPTBYPASSPHRASE(%s, %s)", keyExpr, ph)
	case base.DXDatabaseTypeOracle:
		return fmt.Sprintf("UTL_RAW.CAST_TO_RAW(%s)", ph)
	case base.DXDatabaseTypeMariaDB:
		return fmt.Sprintf("AES_ENCRYPT(%s, %s)", ph, keyExpr)
	default:
		return ph
	}
}

// hashExpression returns database-specific hash SQL expression with optional salt
func hashExpression(dbType base.DXDatabaseType, argIndex int, saltSessionKey string) string {
	ph := placeholder(dbType, argIndex)

	valueExpr := ph
	if saltSessionKey != "" {
		saltExpr := sessionKeyExpression(dbType, saltSessionKey)
		valueExpr = concatExpression(dbType, saltExpr, ph)
	}

	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return fmt.Sprintf("digest(%s, 'sha256')", valueExpr)
	case base.DXDatabaseTypeSQLServer:
		return fmt.Sprintf("HASHBYTES('SHA2_256', %s)", valueExpr)
	case base.DXDatabaseTypeOracle:
		return fmt.Sprintf("DBMS_CRYPTO.HASH(UTL_RAW.CAST_TO_RAW(%s), 4)", valueExpr)
	case base.DXDatabaseTypeMariaDB:
		return fmt.Sprintf("SHA2(%s, 256)", valueExpr)
	default:
		return ph
	}
}

// sessionKeyExpression returns database-specific session key retrieval expression
func sessionKeyExpression(dbType base.DXDatabaseType, sessionKey string) string {
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return fmt.Sprintf("current_setting('%s')", sessionKey)
	case base.DXDatabaseTypeSQLServer:
		return fmt.Sprintf("SESSION_CONTEXT(N'%s')", sessionKey)
	case base.DXDatabaseTypeOracle:
		return fmt.Sprintf("SYS_CONTEXT('CLIENTCONTEXT', '%s')", sessionKey)
	case base.DXDatabaseTypeMariaDB:
		return fmt.Sprintf("@%s", strings.ReplaceAll(sessionKey, ".", "_"))
	default:
		return fmt.Sprintf("'%s'", sessionKey)
	}
}

// concatExpression returns database-specific string concatenation
func concatExpression(dbType base.DXDatabaseType, expr1, expr2 string) string {
	switch dbType {
	case base.DXDatabaseTypeOracle:
		return fmt.Sprintf("(%s || %s)", expr1, expr2)
	default:
		return fmt.Sprintf("CONCAT(%s, %s)", expr1, expr2)
	}
}
