package tables

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
// DXRawTable Encrypted Insert Methods
// ============================================================================

// TxInsertWithEncryption inserts with encrypted columns within a transaction
// Automatically sets session keys from secure memory before insert
func (t *DXRawTable) TxInsertWithEncryption(
	dtx *database.DXDatabaseTx,
	data utils.JSON,
	encryptionColumns []EncryptionColumn,
	returningFieldNames []string,
) (sql.Result, utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}

	dbType := t.Database.DatabaseType

	// Set session keys from secure memory
	if err := setSessionKeysForEncryption(dtx, encryptionColumns); err != nil {
		return nil, nil, err
	}

	// Build and execute INSERT
	return executeEncryptedInsert(dtx, t.TableName(), dbType, data, encryptionColumns, returningFieldNames)
}

// InsertWithEncryption inserts with encrypted columns (creates transaction internally)
// Automatically sets session keys from secure memory before insert
func (t *DXRawTable) InsertWithEncryption(
	l *log.DXLog,
	data utils.JSON,
	encryptionColumns []EncryptionColumn,
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

	result, returning, err := t.TxInsertWithEncryption(dtx, data, encryptionColumns, returningFieldNames)
	return result, returning, err
}

// TxInsertWithEncryptionReturningId is a simplified version returning just the new ID
func (t *DXRawTable) TxInsertWithEncryptionReturningId(
	dtx *database.DXDatabaseTx,
	data utils.JSON,
	encryptionColumns []EncryptionColumn,
) (int64, error) {
	_, returningValues, err := t.TxInsertWithEncryption(dtx, data, encryptionColumns, []string{t.FieldNameForRowId})
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
	encryptionColumns []EncryptionColumn,
) (int64, error) {
	_, returningValues, err := t.InsertWithEncryption(l, data, encryptionColumns, []string{t.FieldNameForRowId})
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
	encryptionColumns []EncryptionColumn,
	returningFieldNames []string,
) (sql.Result, utils.JSON, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.TxInsertWithEncryption(dtx, data, encryptionColumns, returningFieldNames)
}

// InsertWithEncryption inserts with encrypted columns and audit fields
func (t *DXTable) InsertWithEncryption(
	l *log.DXLog,
	data utils.JSON,
	encryptionColumns []EncryptionColumn,
	returningFieldNames []string,
) (sql.Result, utils.JSON, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.InsertWithEncryption(l, data, encryptionColumns, returningFieldNames)
}

// TxInsertWithEncryptionReturningId is a simplified version with audit fields
func (t *DXTable) TxInsertWithEncryptionReturningId(
	dtx *database.DXDatabaseTx,
	data utils.JSON,
	encryptionColumns []EncryptionColumn,
) (int64, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.TxInsertWithEncryptionReturningId(dtx, data, encryptionColumns)
}

// InsertWithEncryptionReturningId is a simplified version with audit fields
func (t *DXTable) InsertWithEncryptionReturningId(
	l *log.DXLog,
	data utils.JSON,
	encryptionColumns []EncryptionColumn,
) (int64, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.InsertWithEncryptionReturningId(l, data, encryptionColumns)
}

// ============================================================================
// Internal Insert Helper Function
// ============================================================================

// executeEncryptedInsert builds and executes INSERT with encrypted columns
func executeEncryptedInsert(
	dtx *database.DXDatabaseTx,
	tableName string,
	dbType base.DXDatabaseType,
	data utils.JSON,
	encryptionColumns []EncryptionColumn,
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
	for _, col := range encryptionColumns {
		// Encrypted field
		columns = append(columns, col.FieldName)
		placeholders = append(placeholders, encryptExpression(dbType, argIndex, col.EncryptionKeyDef.SessionKey))
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
	sqlStr := fmt.Sprintf("INSERT INTO"+" %s (%s) VALUES (%s)",
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
