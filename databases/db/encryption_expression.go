package db

import (
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
)

// SessionKeyExpression returns database-specific session key retrieval expression
func SessionKeyExpression(dbType base.DXDatabaseType, sessionKey string) string {
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

// DecryptExpression returns database-specific decryption SQL expression
func DecryptExpression(dbType base.DXDatabaseType, fieldName string, sessionKey string) string {
	keyExpr := SessionKeyExpression(dbType, sessionKey)

	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return fmt.Sprintf("pgp_sym_decrypt(%s, %s)", fieldName, keyExpr)
	case base.DXDatabaseTypeSQLServer:
		return fmt.Sprintf("CONVERT(VARCHAR(MAX), DECRYPTBYPASSPHRASE(%s, %s))", keyExpr, fieldName)
	case base.DXDatabaseTypeOracle:
		return fmt.Sprintf("UTL_RAW.CAST_TO_VARCHAR2(%s)", fieldName)
	case base.DXDatabaseTypeMariaDB:
		return fmt.Sprintf("AES_DECRYPT(%s, %s)", fieldName, keyExpr)
	default:
		return fieldName
	}
}
