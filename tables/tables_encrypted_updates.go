package tables

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/database/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
)

// DXRawTable Encrypted Update Methods

// TxUpdateWithEncryption updates with encrypted columns within a transaction
// Automatically sets session keys from secure memory before update
func (t *DXRawTable) TxUpdateWithEncryption(
	dtx *database.DXDatabaseTx,
	data utils.JSON,
	encryptionColumns []EncryptionColumn,
	where utils.JSON,
	returningFieldNames []string,
) (sql.Result, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}

	dbType := t.Database.DatabaseType

	// Set session keys from secure memory
	if err := setSessionKeysForEncryption(dtx, encryptionColumns); err != nil {
		return nil, nil, err
	}

	// Build and execute UPDATE
	return executeEncryptedUpdate(dtx, t.TableName(), dbType, data, encryptionColumns, where, returningFieldNames)
}

// UpdateWithEncryption updates with encrypted columns (creates transaction internally)
// Automatically sets session keys from secure memory before update
func (t *DXRawTable) UpdateWithEncryption(
	l *log.DXLog,
	data utils.JSON,
	encryptionColumns []EncryptionColumn,
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

	return t.TxUpdateWithEncryption(dtx, data, encryptionColumns, where, returningFieldNames)
}

// TxUpdateByIdWithEncryption updates by ID with encrypted columns
func (t *DXRawTable) TxUpdateByIdWithEncryption(
	dtx *database.DXDatabaseTx,
	id int64,
	data utils.JSON,
	encryptionColumns []EncryptionColumn,
) (sql.Result, error) {
	result, _, err := t.TxUpdateWithEncryption(dtx, data, encryptionColumns, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// UpdateByIdWithEncryption updates by ID with encrypted columns
func (t *DXRawTable) UpdateByIdWithEncryption(
	l *log.DXLog,
	id int64,
	data utils.JSON,
	encryptionColumns []EncryptionColumn,
) (sql.Result, error) {
	result, _, err := t.UpdateWithEncryption(l, data, encryptionColumns, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// DXTable Encrypted Update Methods (with audit fields)

// TxUpdateWithEncryption updates with encrypted columns and audit fields
func (t *DXTable) TxUpdateWithEncryption(
	dtx *database.DXDatabaseTx,
	data utils.JSON,
	encryptionColumns []EncryptionColumn,
	where utils.JSON,
	returningFieldNames []string,
) (sql.Result, []utils.JSON, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.TxUpdateWithEncryption(dtx, data, encryptionColumns, where, returningFieldNames)
}

// UpdateWithEncryption updates with encrypted columns and audit fields
func (t *DXTable) UpdateWithEncryption(
	l *log.DXLog,
	data utils.JSON,
	encryptionColumns []EncryptionColumn,
	where utils.JSON,
	returningFieldNames []string,
) (sql.Result, []utils.JSON, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.UpdateWithEncryption(l, data, encryptionColumns, where, returningFieldNames)
}

// TxUpdateByIdWithEncryption updates by ID with encrypted columns and audit fields
func (t *DXTable) TxUpdateByIdWithEncryption(
	dtx *database.DXDatabaseTx,
	id int64,
	data utils.JSON,
	encryptionColumns []EncryptionColumn,
) (sql.Result, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.TxUpdateByIdWithEncryption(dtx, id, data, encryptionColumns)
}

// UpdateByIdWithEncryption updates by ID with encrypted columns and audit fields
func (t *DXTable) UpdateByIdWithEncryption(
	l *log.DXLog,
	id int64,
	data utils.JSON,
	encryptionColumns []EncryptionColumn,
) (sql.Result, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.UpdateByIdWithEncryption(l, id, data, encryptionColumns)
}

// Internal Update Helper Function

// executeEncryptedUpdate builds and executes UPDATE with encrypted columns
func executeEncryptedUpdate(
	dtx *database.DXDatabaseTx,
	tableName string,
	dbType base.DXDatabaseType,
	data utils.JSON,
	encryptionColumns []EncryptionColumn,
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
	for _, col := range encryptionColumns {
		// Encrypted field
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", col.FieldName, encryptExpression(dbType, argIndex, col.EncryptionKeyDef.SessionKey)))
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
		if value == nil {
			whereClauses = append(whereClauses, fieldName+" IS NULL")
		} else if sqlExpr, ok := value.(db.SQLExpression); ok {
			whereClauses = append(whereClauses, sqlExpr.String())
		} else {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", fieldName, placeholder(dbType, argIndex)))
			args = append(args, value)
			argIndex++
		}
	}

	// Build UPDATE SQL
	sqlStr := fmt.Sprintf("UPDATE"+" %s SET %s WHERE %s",
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
		defer func(rows *sqlx.Rows) {
			err := rows.Close()
			if err != nil {
				log.Log.Error("ERROR_IN_ROWS_CLOSE", err)
			}
		}(rows)

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
