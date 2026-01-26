package database

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// ============================================================================
// Encryption/Decryption Column Definitions
// ============================================================================

// EncryptedColumnDef defines encryption config for INSERT/UPDATE
type EncryptedColumnDef struct {
	FieldName          string // DB column name (e.g., "fullname_encrypted")
	DataFieldName      string // field name in data JSON (e.g., "fullname")
	SecureMemoryKey    string // key name in secure memory
	SessionKey         string // DB session key name (e.g., "app.encryption_key")
	HashFieldName      string // optional: hash field for searchable hash
	HashSaltMemoryKey  string // optional: secure memory key for hash salt
	HashSaltSessionKey string // optional: DB session key for hash salt
}

// DecryptedColumnDef defines decryption config for SELECT
type DecryptedColumnDef struct {
	FieldName       string // DB column name (e.g., "fullname_encrypted") - ignored if ViewHasDecrypt
	AliasName       string // output alias (e.g., "fullname")
	SecureMemoryKey string // key name in secure memory
	SessionKey      string // DB session key name
	ViewHasDecrypt  bool   // true = view already has pgp_sym_decrypt, just set session key
}

// ============================================================================
// DXDatabaseTx Encrypted Insert
// ============================================================================

// InsertWithEncryption inserts with encrypted columns
// Automatically sets session keys and builds encryption expressions
func (dtx *DXDatabaseTx) InsertWithEncryption(
	tableName string,
	data utils.JSON,
	encryptedDefs []EncryptedColumnDef,
	returningFieldNames []string,
) (sql.Result, utils.JSON, error) {
	dbType := dtx.Database.DatabaseType

	// Set session keys
	if err := dtx.setEncryptionSessionKeys(encryptedDefs); err != nil {
		return nil, nil, err
	}

	// Build INSERT
	return dtx.executeEncryptedInsert(tableName, dbType, data, encryptedDefs, returningFieldNames)
}

// ============================================================================
// DXDatabaseTx Encrypted Update
// ============================================================================

// UpdateWithEncryption updates with encrypted columns
func (dtx *DXDatabaseTx) UpdateWithEncryption(
	tableName string,
	data utils.JSON,
	encryptedDefs []EncryptedColumnDef,
	where utils.JSON,
	returningFieldNames []string,
) (sql.Result, []utils.JSON, error) {
	dbType := dtx.Database.DatabaseType

	// Set session keys
	if err := dtx.setEncryptionSessionKeys(encryptedDefs); err != nil {
		return nil, nil, err
	}

	// Build UPDATE
	return dtx.executeEncryptedUpdate(tableName, dbType, data, encryptedDefs, where, returningFieldNames)
}

// ============================================================================
// DXDatabaseTx Encrypted Select
// ============================================================================

// SelectWithEncryption selects with decrypted columns
func (dtx *DXDatabaseTx) SelectWithEncryption(
	tableName string,
	columns []string,
	decryptedDefs []DecryptedColumnDef,
	where utils.JSON,
	orderBy *string,
	limit *int,
) ([]utils.JSON, error) {
	dbType := dtx.Database.DatabaseType

	// Set session keys
	if err := dtx.setDecryptionSessionKeys(decryptedDefs); err != nil {
		return nil, err
	}

	// Build SELECT
	return dtx.executeEncryptedSelect(tableName, dbType, columns, decryptedDefs, where, orderBy, limit)
}

// SelectOneWithEncryption selects one row with decrypted columns
func (dtx *DXDatabaseTx) SelectOneWithEncryption(
	tableName string,
	columns []string,
	decryptedDefs []DecryptedColumnDef,
	where utils.JSON,
) (utils.JSON, error) {
	limit := 1
	rows, err := dtx.SelectWithEncryption(tableName, columns, decryptedDefs, where, nil, &limit)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows[0], nil
}

// ============================================================================
// Internal: Session Key Setup
// ============================================================================

func (dtx *DXDatabaseTx) setEncryptionSessionKeys(encryptedDefs []EncryptedColumnDef) error {
	sessionKeys := make(map[string]string)

	for _, def := range encryptedDefs {
		if def.SecureMemoryKey != "" && def.SessionKey != "" {
			sessionKeys[def.SessionKey] = def.SecureMemoryKey
		}
		if def.HashSaltMemoryKey != "" && def.HashSaltSessionKey != "" {
			sessionKeys[def.HashSaltSessionKey] = def.HashSaltMemoryKey
		}
	}

	for sessionKey, memoryKey := range sessionKeys {
		if err := dtx.TxSetSessionKeyFromSecureMemory(memoryKey, sessionKey); err != nil {
			return errors.Wrapf(err, "SET_ENCRYPTION_SESSION_KEY_ERROR:%s", sessionKey)
		}
	}

	return nil
}

func (dtx *DXDatabaseTx) setDecryptionSessionKeys(decryptedDefs []DecryptedColumnDef) error {
	sessionKeys := make(map[string]string)

	for _, def := range decryptedDefs {
		if def.SecureMemoryKey != "" && def.SessionKey != "" {
			sessionKeys[def.SessionKey] = def.SecureMemoryKey
		}
	}

	for sessionKey, memoryKey := range sessionKeys {
		if err := dtx.TxSetSessionKeyFromSecureMemory(memoryKey, sessionKey); err != nil {
			return errors.Wrapf(err, "SET_DECRYPTION_SESSION_KEY_ERROR:%s", sessionKey)
		}
	}

	return nil
}

// ============================================================================
// Internal: Execute Encrypted Insert
// ============================================================================

func (dtx *DXDatabaseTx) executeEncryptedInsert(
	tableName string,
	dbType base.DXDatabaseType,
	data utils.JSON,
	encryptedDefs []EncryptedColumnDef,
	returningFieldNames []string,
) (sql.Result, utils.JSON, error) {

	var columns []string
	var placeholders []string
	var args []any
	argIndex := 1

	// Build map of encrypted data field names for exclusion
	encryptedDataFields := make(map[string]bool)
	for _, def := range encryptedDefs {
		encryptedDataFields[def.DataFieldName] = true
	}

	// Add regular columns (excluding encrypted data fields)
	for fieldName, value := range data {
		if encryptedDataFields[fieldName] {
			continue // Skip - will be handled by encrypted defs
		}
		columns = append(columns, fieldName)
		placeholders = append(placeholders, placeholder(dbType, argIndex))
		args = append(args, value)
		argIndex++
	}

	// Add encrypted columns
	for _, def := range encryptedDefs {
		value, ok := data[def.DataFieldName]
		if !ok {
			continue // No value provided
		}

		// Encrypted field
		columns = append(columns, def.FieldName)
		placeholders = append(placeholders, encryptExpr(dbType, argIndex, def.SessionKey))
		args = append(args, value)
		argIndex++

		// Hash field (if specified)
		if def.HashFieldName != "" {
			columns = append(columns, def.HashFieldName)
			placeholders = append(placeholders, hashExpr(dbType, argIndex, def.HashSaltSessionKey))
			args = append(args, value)
			argIndex++
		}
	}

	// Build INSERT SQL
	sqlStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	// Add RETURNING clause
	if len(returningFieldNames) > 0 {
		sqlStr += " RETURNING " + strings.Join(returningFieldNames, ", ")
	}

	// Execute
	if len(returningFieldNames) > 0 {
		row := dtx.Tx.QueryRowx(sqlStr, args...)
		returningValues := make(map[string]any)
		if err := row.MapScan(returningValues); err != nil {
			return nil, nil, errors.Wrapf(err, "ENCRYPTED_INSERT_RETURNING_ERROR")
		}
		return nil, returningValues, nil
	}

	result, err := dtx.Tx.Exec(sqlStr, args...)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "ENCRYPTED_INSERT_EXEC_ERROR")
	}
	return result, nil, nil
}

// ============================================================================
// Internal: Execute Encrypted Update
// ============================================================================

func (dtx *DXDatabaseTx) executeEncryptedUpdate(
	tableName string,
	dbType base.DXDatabaseType,
	data utils.JSON,
	encryptedDefs []EncryptedColumnDef,
	where utils.JSON,
	returningFieldNames []string,
) (sql.Result, []utils.JSON, error) {

	var setClauses []string
	var args []any
	argIndex := 1

	// Build map of encrypted data field names for exclusion
	encryptedDataFields := make(map[string]bool)
	for _, def := range encryptedDefs {
		encryptedDataFields[def.DataFieldName] = true
	}

	// Add regular columns to SET (excluding encrypted data fields)
	for fieldName, value := range data {
		if encryptedDataFields[fieldName] {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", fieldName, placeholder(dbType, argIndex)))
		args = append(args, value)
		argIndex++
	}

	// Add encrypted columns to SET
	for _, def := range encryptedDefs {
		value, ok := data[def.DataFieldName]
		if !ok {
			continue
		}

		// Encrypted field
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", def.FieldName, encryptExpr(dbType, argIndex, def.SessionKey)))
		args = append(args, value)
		argIndex++

		// Hash field
		if def.HashFieldName != "" {
			setClauses = append(setClauses, fmt.Sprintf("%s = %s", def.HashFieldName, hashExpr(dbType, argIndex, def.HashSaltSessionKey)))
			args = append(args, value)
			argIndex++
		}
	}

	// Build WHERE clause
	var whereClauses []string
	for fieldName, value := range where {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", fieldName, placeholder(dbType, argIndex)))
		args = append(args, value)
		argIndex++
	}

	// Build UPDATE SQL
	sqlStr := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		tableName,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)

	// Add RETURNING clause
	if len(returningFieldNames) > 0 {
		sqlStr += " RETURNING " + strings.Join(returningFieldNames, ", ")
	}

	// Execute
	if len(returningFieldNames) > 0 {
		rows, err := dtx.Tx.Queryx(sqlStr, args...)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "ENCRYPTED_UPDATE_RETURNING_ERROR")
		}
		defer rows.Close()

		var results []utils.JSON
		for rows.Next() {
			row := make(map[string]any)
			if err := rows.MapScan(row); err != nil {
				return nil, nil, errors.Wrapf(err, "ENCRYPTED_UPDATE_SCAN_ERROR")
			}
			results = append(results, row)
		}
		return nil, results, nil
	}

	result, err := dtx.Tx.Exec(sqlStr, args...)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "ENCRYPTED_UPDATE_EXEC_ERROR")
	}
	return result, nil, nil
}

// ============================================================================
// Internal: Execute Encrypted Select
// ============================================================================

func (dtx *DXDatabaseTx) executeEncryptedSelect(
	tableName string,
	dbType base.DXDatabaseType,
	columns []string,
	decryptedDefs []DecryptedColumnDef,
	where utils.JSON,
	orderBy *string,
	limit *int,
) ([]utils.JSON, error) {

	selectCols := buildSelectCols(dbType, columns, decryptedDefs)

	// Build WHERE clause
	var whereClauses []string
	var args []any
	argIndex := 1

	for fieldName, value := range where {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", fieldName, placeholder(dbType, argIndex)))
		args = append(args, value)
		argIndex++
	}

	// Build SQL
	sqlStr := fmt.Sprintf("SELECT %s FROM %s", selectCols, tableName)

	if len(whereClauses) > 0 {
		sqlStr += " WHERE " + strings.Join(whereClauses, " AND ")
	}

	if orderBy != nil && *orderBy != "" {
		sqlStr += " ORDER BY " + *orderBy
	}

	if limit != nil && *limit > 0 {
		sqlStr += fmt.Sprintf(" LIMIT %d", *limit)
	}

	// Execute
	rows, err := dtx.Tx.Queryx(sqlStr, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "ENCRYPTED_SELECT_ERROR")
	}
	defer rows.Close()

	var results []utils.JSON
	for rows.Next() {
		row := make(map[string]any)
		if err := rows.MapScan(row); err != nil {
			return nil, errors.Wrapf(err, "ENCRYPTED_SELECT_SCAN_ERROR")
		}
		// Convert []byte to string for decrypted text fields
		for k, v := range row {
			if b, ok := v.([]byte); ok {
				row[k] = string(b)
			}
		}
		results = append(results, row)
	}

	return results, nil
}

// ============================================================================
// Internal: SQL Expression Helpers
// ============================================================================

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

func sessionKeyExpr(dbType base.DXDatabaseType, sessionKey string) string {
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

func encryptExpr(dbType base.DXDatabaseType, argIndex int, sessionKey string) string {
	ph := placeholder(dbType, argIndex)
	keyExpr := sessionKeyExpr(dbType, sessionKey)

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

func decryptExpr(dbType base.DXDatabaseType, fieldName string, sessionKey string) string {
	keyExpr := sessionKeyExpr(dbType, sessionKey)

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

func hashExpr(dbType base.DXDatabaseType, argIndex int, saltSessionKey string) string {
	ph := placeholder(dbType, argIndex)

	valueExpr := ph
	if saltSessionKey != "" {
		saltExpr := sessionKeyExpr(dbType, saltSessionKey)
		switch dbType {
		case base.DXDatabaseTypeOracle:
			valueExpr = fmt.Sprintf("(%s || %s)", saltExpr, ph)
		default:
			valueExpr = fmt.Sprintf("CONCAT(%s, %s)", saltExpr, ph)
		}
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

func buildSelectCols(dbType base.DXDatabaseType, columns []string, decryptedDefs []DecryptedColumnDef) string {
	var selectCols []string

	// Add regular columns - use * if no specific columns requested
	if len(columns) == 0 {
		// Check if all decrypted columns come from view (ViewHasDecrypt=true)
		// If so, * already includes them, so just return *
		allFromView := true
		for _, def := range decryptedDefs {
			if !def.ViewHasDecrypt {
				allFromView = false
				break
			}
		}
		if allFromView {
			return "*"
		}
		// Some decrypted columns need explicit decryption expression
		selectCols = append(selectCols, "*")
	} else {
		selectCols = append(selectCols, columns...)
	}

	// Add decrypted columns (only those not already in view)
	for _, def := range decryptedDefs {
		if def.ViewHasDecrypt {
			// Skip if we're using *, as view already has this column
			if len(columns) == 0 {
				continue
			}
			selectCols = append(selectCols, def.AliasName)
		} else {
			expr := decryptExpr(dbType, def.FieldName, def.SessionKey)
			selectCols = append(selectCols, fmt.Sprintf("%s AS %s", expr, def.AliasName))
		}
	}

	return strings.Join(selectCols, ", ")
}
