package table

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
)

// ============================================================================
// Encrypted Column Definitions
// ============================================================================

// EncryptedColumn defines an encrypted column for INSERT/UPDATE
type EncryptedColumn struct {
	FieldName          string // actual DB column name (e.g., "fullname_encrypted")
	Value              any    // plaintext value to encrypt
	SecureMemoryKey    string // key name in secure memory (e.g., "db-key")
	SessionKey         string // DB session key name (e.g., "app.encryption_key")
	HashFieldName      string // optional: hash field for searchable hash (e.g., "fullname_hash")
	HashSaltMemoryKey  string // optional: secure memory key for hash salt
	HashSaltSessionKey string // optional: DB session key for hash salt (e.g., "app.hash_salt")
}

// DecryptedColumn defines an encrypted column for SELECT (decryption)
type DecryptedColumn struct {
	FieldName       string // DB column name to decrypt (e.g., "fullname_encrypted") - ignored if ViewHasDecrypt=true
	AliasName       string // output alias (e.g., "fullname") - also used as column name if ViewHasDecrypt=true
	SecureMemoryKey string // key name in secure memory (e.g., "db-key")
	SessionKey      string // DB session key name (e.g., "app.encryption_key")
	ViewHasDecrypt  bool   // true = view already has pgp_sym_decrypt, just set session key and select AliasName
}

// ============================================================================
// DXRawTable Encrypted Select Methods
// ============================================================================

// TxSelectWithEncryption selects with decrypted columns within a transaction
func (t *DXRawTable) TxSelectWithEncryption(
	dtx *database.DXDatabaseTx,
	columns []string,
	decryptedColumns []DecryptedColumn,
	where utils.JSON,
	orderBy *string,
	limit *int,
) ([]utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, err
	}

	dbType := t.Database.DatabaseType

	// Set session keys from secure memory
	if err := setSessionKeysFromDecryptedColumns(dtx, decryptedColumns); err != nil {
		return nil, err
	}

	return executeEncryptedSelect(dtx, t.ListViewNameId, dbType, columns, decryptedColumns, where, orderBy, limit)
}

// SelectWithEncryption selects with decrypted columns (creates transaction internally)
func (t *DXRawTable) SelectWithEncryption(
	l *log.DXLog,
	columns []string,
	decryptedColumns []DecryptedColumn,
	where utils.JSON,
	orderBy *string,
	limit *int,
) ([]utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, err
	}

	dtx, err := t.Database.TransactionBegin(database.LevelReadCommitted)
	if err != nil {
		return nil, err
	}
	defer dtx.Finish(l, err)

	return t.TxSelectWithEncryption(dtx, columns, decryptedColumns, where, orderBy, limit)
}

// TxSelectOneWithEncryption selects one row with decrypted columns
func (t *DXRawTable) TxSelectOneWithEncryption(
	dtx *database.DXDatabaseTx,
	columns []string,
	decryptedColumns []DecryptedColumn,
	where utils.JSON,
) (utils.JSON, error) {
	limit := 1
	rows, err := t.TxSelectWithEncryption(dtx, columns, decryptedColumns, where, nil, &limit)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows[0], nil
}

// SelectOneWithEncryption selects one row with decrypted columns
func (t *DXRawTable) SelectOneWithEncryption(
	l *log.DXLog,
	columns []string,
	decryptedColumns []DecryptedColumn,
	where utils.JSON,
) (utils.JSON, error) {
	limit := 1
	rows, err := t.SelectWithEncryption(l, columns, decryptedColumns, where, nil, &limit)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows[0], nil
}

// TxSelectByIdWithEncryption selects by ID with decrypted columns
func (t *DXRawTable) TxSelectByIdWithEncryption(
	dtx *database.DXDatabaseTx,
	id int64,
	columns []string,
	decryptedColumns []DecryptedColumn,
) (utils.JSON, error) {
	return t.TxSelectOneWithEncryption(dtx, columns, decryptedColumns, utils.JSON{t.FieldNameForRowId: id})
}

// SelectByIdWithEncryption selects by ID with decrypted columns
func (t *DXRawTable) SelectByIdWithEncryption(
	l *log.DXLog,
	id int64,
	columns []string,
	decryptedColumns []DecryptedColumn,
) (utils.JSON, error) {
	return t.SelectOneWithEncryption(l, columns, decryptedColumns, utils.JSON{t.FieldNameForRowId: id})
}

// ============================================================================
// DXRawTable Encrypted Paging Methods
// ============================================================================

// TxPagingWithEncryption executes paging query with decrypted columns
func (t *DXRawTable) TxPagingWithEncryption(
	dtx *database.DXDatabaseTx,
	columns []string,
	decryptedColumns []DecryptedColumn,
	whereClause string,
	whereArgs utils.JSON,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, err
	}

	dbType := t.Database.DatabaseType

	// Set session keys from secure memory
	if err := setSessionKeysFromDecryptedColumns(dtx, decryptedColumns); err != nil {
		return nil, err
	}

	return executeEncryptedPaging(dtx, t.ListViewNameId, dbType, columns, decryptedColumns, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
}

// PagingWithEncryption executes paging query with decrypted columns
func (t *DXRawTable) PagingWithEncryption(
	l *log.DXLog,
	columns []string,
	decryptedColumns []DecryptedColumn,
	whereClause string,
	whereArgs utils.JSON,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, err
	}

	dtx, err := t.Database.TransactionBegin(database.LevelReadCommitted)
	if err != nil {
		return nil, err
	}
	defer dtx.Finish(l, err)

	return t.TxPagingWithEncryption(dtx, columns, decryptedColumns, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
}

// TxPagingWithEncryptionAndBuilder executes paging with QueryBuilder and decrypted columns
func (t *DXRawTable) TxPagingWithEncryptionAndBuilder(
	dtx *database.DXDatabaseTx,
	columns []string,
	decryptedColumns []DecryptedColumn,
	qb *QueryBuilder,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	whereClause, whereArgs := qb.Build()
	return t.TxPagingWithEncryption(dtx, columns, decryptedColumns, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
}

// PagingWithEncryptionAndBuilder executes paging with QueryBuilder and decrypted columns
func (t *DXRawTable) PagingWithEncryptionAndBuilder(
	l *log.DXLog,
	columns []string,
	decryptedColumns []DecryptedColumn,
	qb *QueryBuilder,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	whereClause, whereArgs := qb.Build()
	return t.PagingWithEncryption(l, columns, decryptedColumns, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
}

// ============================================================================
// DXTable Encrypted Select Methods (delegates to DXRawTable)
// ============================================================================

// TxSelectWithEncryption selects with decrypted columns within a transaction
func (t *DXTable) TxSelectWithEncryption(
	dtx *database.DXDatabaseTx,
	columns []string,
	decryptedColumns []DecryptedColumn,
	where utils.JSON,
	orderBy *string,
	limit *int,
) ([]utils.JSON, error) {
	return t.DXRawTable.TxSelectWithEncryption(dtx, columns, decryptedColumns, where, orderBy, limit)
}

// SelectWithEncryption selects with decrypted columns
func (t *DXTable) SelectWithEncryption(
	l *log.DXLog,
	columns []string,
	decryptedColumns []DecryptedColumn,
	where utils.JSON,
	orderBy *string,
	limit *int,
) ([]utils.JSON, error) {
	return t.DXRawTable.SelectWithEncryption(l, columns, decryptedColumns, where, orderBy, limit)
}

// TxSelectOneWithEncryption selects one row with decrypted columns
func (t *DXTable) TxSelectOneWithEncryption(
	dtx *database.DXDatabaseTx,
	columns []string,
	decryptedColumns []DecryptedColumn,
	where utils.JSON,
) (utils.JSON, error) {
	return t.DXRawTable.TxSelectOneWithEncryption(dtx, columns, decryptedColumns, where)
}

// SelectOneWithEncryption selects one row with decrypted columns
func (t *DXTable) SelectOneWithEncryption(
	l *log.DXLog,
	columns []string,
	decryptedColumns []DecryptedColumn,
	where utils.JSON,
) (utils.JSON, error) {
	return t.DXRawTable.SelectOneWithEncryption(l, columns, decryptedColumns, where)
}

// TxSelectByIdWithEncryption selects by ID with decrypted columns
func (t *DXTable) TxSelectByIdWithEncryption(
	dtx *database.DXDatabaseTx,
	id int64,
	columns []string,
	decryptedColumns []DecryptedColumn,
) (utils.JSON, error) {
	return t.DXRawTable.TxSelectByIdWithEncryption(dtx, id, columns, decryptedColumns)
}

// SelectByIdWithEncryption selects by ID with decrypted columns
func (t *DXTable) SelectByIdWithEncryption(
	l *log.DXLog,
	id int64,
	columns []string,
	decryptedColumns []DecryptedColumn,
) (utils.JSON, error) {
	return t.DXRawTable.SelectByIdWithEncryption(l, id, columns, decryptedColumns)
}

// ============================================================================
// DXTable Encrypted Paging Methods (delegates to DXRawTable)
// ============================================================================

// TxPagingWithEncryption executes paging query with decrypted columns
func (t *DXTable) TxPagingWithEncryption(
	dtx *database.DXDatabaseTx,
	columns []string,
	decryptedColumns []DecryptedColumn,
	whereClause string,
	whereArgs utils.JSON,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	return t.DXRawTable.TxPagingWithEncryption(dtx, columns, decryptedColumns, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
}

// PagingWithEncryption executes paging query with decrypted columns
func (t *DXTable) PagingWithEncryption(
	l *log.DXLog,
	columns []string,
	decryptedColumns []DecryptedColumn,
	whereClause string,
	whereArgs utils.JSON,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	return t.DXRawTable.PagingWithEncryption(l, columns, decryptedColumns, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
}

// TxPagingWithEncryptionAndBuilder executes paging with QueryBuilder and decrypted columns
func (t *DXTable) TxPagingWithEncryptionAndBuilder(
	dtx *database.DXDatabaseTx,
	columns []string,
	decryptedColumns []DecryptedColumn,
	qb *QueryBuilder,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	return t.DXRawTable.TxPagingWithEncryptionAndBuilder(dtx, columns, decryptedColumns, qb, orderBy, rowPerPage, pageIndex)
}

// PagingWithEncryptionAndBuilder executes paging with QueryBuilder and decrypted columns
func (t *DXTable) PagingWithEncryptionAndBuilder(
	l *log.DXLog,
	columns []string,
	decryptedColumns []DecryptedColumn,
	qb *QueryBuilder,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	return t.DXRawTable.PagingWithEncryptionAndBuilder(l, columns, decryptedColumns, qb, orderBy, rowPerPage, pageIndex)
}

// ============================================================================
// DXRawTable Encrypted Insert Methods
// ============================================================================

// TxInsertWithEncryption inserts with encrypted columns within a transaction
// Automatically sets session keys from secure memory before insert
func (t *DXRawTable) TxInsertWithEncryption(
	dtx *database.DXDatabaseTx,
	data utils.JSON,
	encryptedColumns []EncryptedColumn,
	returningFieldNames []string,
) (sql.Result, utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}

	dbType := t.Database.DatabaseType

	// Set session keys from secure memory
	if err := setSessionKeysFromEncryptedColumns(dtx, encryptedColumns); err != nil {
		return nil, nil, err
	}

	// Build and execute INSERT
	return executeEncryptedInsert(dtx, t.TableName(), dbType, data, encryptedColumns, returningFieldNames)
}

// InsertWithEncryption inserts with encrypted columns (creates transaction internally)
// Automatically sets session keys from secure memory before insert
func (t *DXRawTable) InsertWithEncryption(
	l *log.DXLog,
	data utils.JSON,
	encryptedColumns []EncryptedColumn,
	returningFieldNames []string,
) (sql.Result, utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}

	dtx, err := t.Database.TransactionBegin(database.LevelReadCommitted)
	if err != nil {
		return nil, nil, err
	}
	defer dtx.Finish(l, err)

	result, returning, err := t.TxInsertWithEncryption(dtx, data, encryptedColumns, returningFieldNames)
	return result, returning, err
}

// TxInsertWithEncryptionReturningId is a simplified version returning just the new ID
func (t *DXRawTable) TxInsertWithEncryptionReturningId(
	dtx *database.DXDatabaseTx,
	data utils.JSON,
	encryptedColumns []EncryptedColumn,
) (int64, error) {
	_, returningValues, err := t.TxInsertWithEncryption(dtx, data, encryptedColumns, []string{t.FieldNameForRowId})
	if err != nil {
		return 0, err
	}
	newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
	return newId, nil
}

// InsertWithEncryptionReturningId is a simplified version returning just the new ID
func (t *DXRawTable) InsertWithEncryptionReturningId(
	l *log.DXLog,
	data utils.JSON,
	encryptedColumns []EncryptedColumn,
) (int64, error) {
	_, returningValues, err := t.InsertWithEncryption(l, data, encryptedColumns, []string{t.FieldNameForRowId})
	if err != nil {
		return 0, err
	}
	newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
	return newId, nil
}

// ============================================================================
// DXTable Encrypted Insert Methods (with audit fields)
// ============================================================================

// TxInsertWithEncryption inserts with encrypted columns and audit fields
func (t *DXTable) TxInsertWithEncryption(
	dtx *database.DXDatabaseTx,
	data utils.JSON,
	encryptedColumns []EncryptedColumn,
	returningFieldNames []string,
) (sql.Result, utils.JSON, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.TxInsertWithEncryption(dtx, data, encryptedColumns, returningFieldNames)
}

// InsertWithEncryption inserts with encrypted columns and audit fields
func (t *DXTable) InsertWithEncryption(
	l *log.DXLog,
	data utils.JSON,
	encryptedColumns []EncryptedColumn,
	returningFieldNames []string,
) (sql.Result, utils.JSON, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.InsertWithEncryption(l, data, encryptedColumns, returningFieldNames)
}

// TxInsertWithEncryptionReturningId is a simplified version with audit fields
func (t *DXTable) TxInsertWithEncryptionReturningId(
	dtx *database.DXDatabaseTx,
	data utils.JSON,
	encryptedColumns []EncryptedColumn,
) (int64, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.TxInsertWithEncryptionReturningId(dtx, data, encryptedColumns)
}

// InsertWithEncryptionReturningId is a simplified version with audit fields
func (t *DXTable) InsertWithEncryptionReturningId(
	l *log.DXLog,
	data utils.JSON,
	encryptedColumns []EncryptedColumn,
) (int64, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.InsertWithEncryptionReturningId(l, data, encryptedColumns)
}

// ============================================================================
// DXRawTable Encrypted Update Methods
// ============================================================================

// TxUpdateWithEncryption updates with encrypted columns within a transaction
// Automatically sets session keys from secure memory before update
func (t *DXRawTable) TxUpdateWithEncryption(
	dtx *database.DXDatabaseTx,
	data utils.JSON,
	encryptedColumns []EncryptedColumn,
	where utils.JSON,
	returningFieldNames []string,
) (sql.Result, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}

	dbType := t.Database.DatabaseType

	// Set session keys from secure memory
	if err := setSessionKeysFromEncryptedColumns(dtx, encryptedColumns); err != nil {
		return nil, nil, err
	}

	// Build and execute UPDATE
	return executeEncryptedUpdate(dtx, t.TableName(), dbType, data, encryptedColumns, where, returningFieldNames)
}

// UpdateWithEncryption updates with encrypted columns (creates transaction internally)
// Automatically sets session keys from secure memory before update
func (t *DXRawTable) UpdateWithEncryption(
	l *log.DXLog,
	data utils.JSON,
	encryptedColumns []EncryptedColumn,
	where utils.JSON,
	returningFieldNames []string,
) (sql.Result, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}

	dtx, err := t.Database.TransactionBegin(database.LevelReadCommitted)
	if err != nil {
		return nil, nil, err
	}
	defer dtx.Finish(l, err)

	return t.TxUpdateWithEncryption(dtx, data, encryptedColumns, where, returningFieldNames)
}

// TxUpdateByIdWithEncryption updates by ID with encrypted columns
func (t *DXRawTable) TxUpdateByIdWithEncryption(
	dtx *database.DXDatabaseTx,
	id int64,
	data utils.JSON,
	encryptedColumns []EncryptedColumn,
) (sql.Result, error) {
	result, _, err := t.TxUpdateWithEncryption(dtx, data, encryptedColumns, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// UpdateByIdWithEncryption updates by ID with encrypted columns
func (t *DXRawTable) UpdateByIdWithEncryption(
	l *log.DXLog,
	id int64,
	data utils.JSON,
	encryptedColumns []EncryptedColumn,
) (sql.Result, error) {
	result, _, err := t.UpdateWithEncryption(l, data, encryptedColumns, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// ============================================================================
// DXTable Encrypted Update Methods (with audit fields)
// ============================================================================

// TxUpdateWithEncryption updates with encrypted columns and audit fields
func (t *DXTable) TxUpdateWithEncryption(
	dtx *database.DXDatabaseTx,
	data utils.JSON,
	encryptedColumns []EncryptedColumn,
	where utils.JSON,
	returningFieldNames []string,
) (sql.Result, []utils.JSON, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.TxUpdateWithEncryption(dtx, data, encryptedColumns, where, returningFieldNames)
}

// UpdateWithEncryption updates with encrypted columns and audit fields
func (t *DXTable) UpdateWithEncryption(
	l *log.DXLog,
	data utils.JSON,
	encryptedColumns []EncryptedColumn,
	where utils.JSON,
	returningFieldNames []string,
) (sql.Result, []utils.JSON, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.UpdateWithEncryption(l, data, encryptedColumns, where, returningFieldNames)
}

// TxUpdateByIdWithEncryption updates by ID with encrypted columns and audit fields
func (t *DXTable) TxUpdateByIdWithEncryption(
	dtx *database.DXDatabaseTx,
	id int64,
	data utils.JSON,
	encryptedColumns []EncryptedColumn,
) (sql.Result, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.TxUpdateByIdWithEncryption(dtx, id, data, encryptedColumns)
}

// UpdateByIdWithEncryption updates by ID with encrypted columns and audit fields
func (t *DXTable) UpdateByIdWithEncryption(
	l *log.DXLog,
	id int64,
	data utils.JSON,
	encryptedColumns []EncryptedColumn,
) (sql.Result, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.UpdateByIdWithEncryption(l, id, data, encryptedColumns)
}

// ============================================================================
// Internal Helper Functions
// ============================================================================

// setSessionKeysFromEncryptedColumns sets all unique session keys from secure memory
func setSessionKeysFromEncryptedColumns(dtx *database.DXDatabaseTx, encryptedColumns []EncryptedColumn) error {
	// Collect unique session keys to set
	sessionKeys := make(map[string]string) // sessionKey -> secureMemoryKey

	for _, col := range encryptedColumns {
		if col.SecureMemoryKey != "" && col.SessionKey != "" {
			sessionKeys[col.SessionKey] = col.SecureMemoryKey
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

// executeEncryptedInsert builds and executes INSERT with encrypted columns
func executeEncryptedInsert(
	dtx *database.DXDatabaseTx,
	tableName string,
	dbType base.DXDatabaseType,
	data utils.JSON,
	encryptedColumns []EncryptedColumn,
	returningFieldNames []string,
) (sql.Result, utils.JSON, error) {

	var columns []string
	var placeholders []string
	var args []any
	argIndex := 1

	// Add regular columns
	for fieldName, value := range data {
		columns = append(columns, fieldName)
		placeholders = append(placeholders, placeholder(dbType, argIndex))
		args = append(args, value)
		argIndex++
	}

	// Add encrypted columns
	for _, col := range encryptedColumns {
		// Encrypted field
		columns = append(columns, col.FieldName)
		placeholders = append(placeholders, encryptExpression(dbType, argIndex, col.SessionKey))
		args = append(args, col.Value)
		argIndex++

		// Hash field (if specified)
		if col.HashFieldName != "" {
			columns = append(columns, col.HashFieldName)
			placeholders = append(placeholders, hashExpression(dbType, argIndex, col.HashSaltSessionKey))
			args = append(args, col.Value)
			argIndex++
		}
	}

	// Build INSERT SQL
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	// Add RETURNING clause
	if len(returningFieldNames) > 0 {
		sql += " RETURNING " + strings.Join(returningFieldNames, ", ")
	}

	// Execute
	if len(returningFieldNames) > 0 {
		row := dtx.Tx.QueryRowx(sql, args...)
		returningValues := make(map[string]any)
		if err := row.MapScan(returningValues); err != nil {
			return nil, nil, errors.Wrapf(err, "ENCRYPTED_INSERT_RETURNING_ERROR")
		}
		return nil, returningValues, nil
	}

	result, err := dtx.Tx.Exec(sql, args...)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "ENCRYPTED_INSERT_EXEC_ERROR")
	}
	return result, nil, nil
}

// executeEncryptedUpdate builds and executes UPDATE with encrypted columns
func executeEncryptedUpdate(
	dtx *database.DXDatabaseTx,
	tableName string,
	dbType base.DXDatabaseType,
	data utils.JSON,
	encryptedColumns []EncryptedColumn,
	where utils.JSON,
	returningFieldNames []string,
) (sql.Result, []utils.JSON, error) {

	var setClauses []string
	var args []any
	argIndex := 1

	// Add regular columns to SET
	for fieldName, value := range data {
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", fieldName, placeholder(dbType, argIndex)))
		args = append(args, value)
		argIndex++
	}

	// Add encrypted columns to SET
	for _, col := range encryptedColumns {
		// Encrypted field
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", col.FieldName, encryptExpression(dbType, argIndex, col.SessionKey)))
		args = append(args, col.Value)
		argIndex++

		// Hash field (if specified)
		if col.HashFieldName != "" {
			setClauses = append(setClauses, fmt.Sprintf("%s = %s", col.HashFieldName, hashExpression(dbType, argIndex, col.HashSaltSessionKey)))
			args = append(args, col.Value)
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

// setSessionKeysFromDecryptedColumns sets all unique session keys from secure memory for SELECT
func setSessionKeysFromDecryptedColumns(dtx *database.DXDatabaseTx, decryptedColumns []DecryptedColumn) error {
	sessionKeys := make(map[string]string) // sessionKey -> secureMemoryKey

	for _, col := range decryptedColumns {
		if col.SecureMemoryKey != "" && col.SessionKey != "" {
			sessionKeys[col.SessionKey] = col.SecureMemoryKey
		}
	}

	for sessionKey, memoryKey := range sessionKeys {
		if err := dtx.TxSetSessionKeyFromSecureMemory(memoryKey, sessionKey); err != nil {
			return errors.Wrapf(err, "ENCRYPTED_SELECT_SET_SESSION_KEY_ERROR:%s", sessionKey)
		}
	}

	return nil
}

// decryptExpression returns database-specific decryption SQL expression
func decryptExpression(dbType base.DXDatabaseType, fieldName string, sessionKey string) string {
	keyExpr := sessionKeyExpression(dbType, sessionKey)

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

// buildSelectColumns builds SELECT column list with decryption expressions
func buildSelectColumns(dbType base.DXDatabaseType, columns []string, decryptedColumns []DecryptedColumn) string {
	var selectCols []string

	// Add regular columns
	for _, col := range columns {
		selectCols = append(selectCols, col)
	}

	// Add decrypted columns
	for _, col := range decryptedColumns {
		if col.ViewHasDecrypt {
			// View already has decryption, just select the alias
			selectCols = append(selectCols, col.AliasName)
		} else {
			// Build decrypt expression
			expr := decryptExpression(dbType, col.FieldName, col.SessionKey)
			selectCols = append(selectCols, fmt.Sprintf("%s AS %s", expr, col.AliasName))
		}
	}

	return strings.Join(selectCols, ", ")
}

// executeEncryptedSelect builds and executes SELECT with decrypted columns
func executeEncryptedSelect(
	dtx *database.DXDatabaseTx,
	tableName string,
	dbType base.DXDatabaseType,
	columns []string,
	decryptedColumns []DecryptedColumn,
	where utils.JSON,
	orderBy *string,
	limit *int,
) ([]utils.JSON, error) {

	selectCols := buildSelectColumns(dbType, columns, decryptedColumns)

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

// executeEncryptedPaging builds and executes paging query with decrypted columns
func executeEncryptedPaging(
	dtx *database.DXDatabaseTx,
	tableName string,
	dbType base.DXDatabaseType,
	columns []string,
	decryptedColumns []DecryptedColumn,
	whereClause string,
	whereArgs utils.JSON,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {

	selectCols := buildSelectColumns(dbType, columns, decryptedColumns)

	// Count total rows
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	if whereClause != "" {
		countSQL += " WHERE " + whereClause
	}

	var totalRows int64
	row := dtx.Tx.QueryRowx(countSQL)
	if err := row.Scan(&totalRows); err != nil {
		return nil, errors.Wrapf(err, "ENCRYPTED_PAGING_COUNT_ERROR")
	}

	// Calculate pagination
	offset := (pageIndex - 1) * rowPerPage
	if offset < 0 {
		offset = 0
	}

	totalPages := totalRows / rowPerPage
	if totalRows%rowPerPage > 0 {
		totalPages++
	}

	// Build SELECT with paging
	sqlStr := fmt.Sprintf("SELECT %s FROM %s", selectCols, tableName)

	if whereClause != "" {
		sqlStr += " WHERE " + whereClause
	}

	if orderBy != "" {
		sqlStr += " ORDER BY " + orderBy
	}

	sqlStr += fmt.Sprintf(" LIMIT %d OFFSET %d", rowPerPage, offset)

	// Execute
	rows, err := dtx.Tx.NamedQuery(sqlStr, whereArgs)
	if err != nil {
		return nil, errors.Wrapf(err, "ENCRYPTED_PAGING_QUERY_ERROR")
	}
	defer rows.Close()

	var results []utils.JSON
	for rows.Next() {
		row := make(map[string]any)
		if err := rows.MapScan(row); err != nil {
			return nil, errors.Wrapf(err, "ENCRYPTED_PAGING_SCAN_ERROR")
		}
		// Convert []byte to string for decrypted text fields
		for k, v := range row {
			if b, ok := v.([]byte); ok {
				row[k] = string(b)
			}
		}
		results = append(results, row)
	}

	return &PagingResult{
		Rows:       results,
		TotalRows:  totalRows,
		TotalPages: totalPages,
	}, nil
}
