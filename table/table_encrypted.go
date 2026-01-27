package table

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
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
	"github.com/jmoiron/sqlx"
)

// ============================================================================
// Encryption Column Definition
// ============================================================================

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

// ============================================================================
// DXRawTable Encrypted Select Methods
// ============================================================================

// TxSelectWithEncryption selects with decrypted columns within a transaction
func (t *DXRawTable) TxSelectWithEncryption(dtx *database.DXDatabaseTx, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}

	// Use table name instead of view when FOR UPDATE is requested (views with outer joins don't support FOR UPDATE)
	tableName := t.ListViewNameId
	if forUpdatePart != nil && forUpdatePart == true {
		tableName = t.TableName()
	}

	dbType := t.Database.DatabaseType

	// Set session keys from secure memory
	if err := setSessionKeysForDecryption(dtx, encryptionColumns); err != nil {
		return nil, nil, err
	}

	return executeEncryptedSelect(dtx, tableName, t.FieldTypeMapping, dbType, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, limit, forUpdatePart)
}

// SelectWithEncryption selects with decrypted columns (creates transaction internally)
func (t *DXRawTable) SelectWithEncryption(l *log.DXLog, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}

	dtx, err := t.Database.TransactionBegin(database.LevelReadCommitted)
	if err != nil {
		return nil, nil, err
	}
	defer dtx.Finish(l, err)

	return t.TxSelectWithEncryption(dtx, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, limit, forUpdatePart)
}

// TxSelectOneWithEncryption selects one row with decrypted columns within a transaction
func (t *DXRawTable) TxSelectOneWithEncryption(dtx *database.DXDatabaseTx, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}

	// Use table name instead of view when FOR UPDATE is requested (views with outer joins don't support FOR UPDATE)
	tableName := t.ListViewNameId
	if forUpdatePart != nil && forUpdatePart == true {
		tableName = t.TableName()
	}

	dbType := t.Database.DatabaseType

	if err := setSessionKeysForDecryption(dtx, encryptionColumns); err != nil {
		return nil, nil, err
	}

	rowsInfo, rows, err := executeEncryptedSelect(dtx, tableName, t.FieldTypeMapping, dbType, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, 1, forUpdatePart)
	if err != nil {
		return rowsInfo, nil, err
	}
	if len(rows) == 0 {
		return rowsInfo, nil, nil
	}
	return rowsInfo, rows[0], nil
}

// SelectOneWithEncryption selects one row with decrypted columns
func (t *DXRawTable) SelectOneWithEncryption(l *log.DXLog, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	rowsInfo, rows, err := t.SelectWithEncryption(l, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, 1, nil)
	if err != nil {
		return rowsInfo, nil, err
	}
	if len(rows) == 0 {
		return rowsInfo, nil, nil
	}
	return rowsInfo, rows[0], nil
}

// TxShouldSelectOneWithEncryption selects one row or returns error if not found within a transaction
func (t *DXRawTable) TxShouldSelectOneWithEncryption(dtx *database.DXDatabaseTx, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	rowsInfo, row, err := t.TxSelectOneWithEncryption(dtx, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, forUpdatePart)
	if err != nil {
		return rowsInfo, nil, err
	}
	if row == nil {
		return rowsInfo, nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s", t.ListViewNameId)
	}
	return rowsInfo, row, nil
}

// ShouldSelectOneWithEncryption selects one row or returns error if not found
func (t *DXRawTable) ShouldSelectOneWithEncryption(l *log.DXLog, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	rowsInfo, row, err := t.SelectOneWithEncryption(l, nil, encryptionColumns, where, joinSQLPart, orderBy)
	if err != nil {
		return rowsInfo, nil, err
	}
	if row == nil {
		return rowsInfo, nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s", t.ListViewNameId)
	}
	return rowsInfo, row, nil
}

// TxSelectByIdWithEncryption selects by ID with decrypted columns within a transaction
func (t *DXRawTable) TxSelectByIdWithEncryption(dtx *database.DXDatabaseTx, id int64, fieldNames []string,
	encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxSelectOneWithEncryption(dtx, fieldNames, encryptionColumns, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// SelectByIdWithEncryption selects by ID with decrypted columns
func (t *DXRawTable) SelectByIdWithEncryption(l *log.DXLog, id int64, fieldNames []string,
	encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.SelectOneWithEncryption(l, fieldNames, encryptionColumns, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// GetByIdWithEncryption returns a row by ID with decrypted columns
func (t *DXRawTable) GetByIdWithEncryption(l *log.DXLog, id int64, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.SelectOneWithEncryption(l, nil, encryptionColumns, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// ShouldGetByIdWithEncryption returns a row by ID or error if not found
func (t *DXRawTable) ShouldGetByIdWithEncryption(l *log.DXLog, id int64, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldSelectOneWithEncryption(l, encryptionColumns, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// TxGetByIdWithEncryption returns a row by ID with decrypted columns within a transaction
func (t *DXRawTable) TxGetByIdWithEncryption(dtx *database.DXDatabaseTx, id int64, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxSelectOneWithEncryption(dtx, nil, encryptionColumns, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxShouldGetByIdWithEncryption returns a row by ID or error if not found within a transaction
func (t *DXRawTable) TxShouldGetByIdWithEncryption(dtx *database.DXDatabaseTx, id int64, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxShouldSelectOneWithEncryption(dtx, nil, encryptionColumns, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// GetByUidWithEncryption returns a row by UID with decrypted columns
func (t *DXRawTable) GetByUidWithEncryption(l *log.DXLog, uid string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.SelectOneWithEncryption(l, nil, encryptionColumns, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// ShouldGetByUidWithEncryption returns a row by UID or error if not found
func (t *DXRawTable) ShouldGetByUidWithEncryption(l *log.DXLog, uid string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.ShouldSelectOneWithEncryption(l, encryptionColumns, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// TxGetByUidWithEncryption returns a row by UID with decrypted columns within a transaction
func (t *DXRawTable) TxGetByUidWithEncryption(dtx *database.DXDatabaseTx, uid string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.TxSelectOneWithEncryption(dtx, nil, encryptionColumns, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxShouldGetByUidWithEncryption returns a row by UID or error if not found within a transaction
func (t *DXRawTable) TxShouldGetByUidWithEncryption(dtx *database.DXDatabaseTx, uid string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.TxShouldSelectOneWithEncryption(dtx, nil, encryptionColumns, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// GetByNameIdWithEncryption returns a row by NameId with decrypted columns
func (t *DXRawTable) GetByNameIdWithEncryption(l *log.DXLog, nameId string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.SelectOneWithEncryption(l, nil, encryptionColumns, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// ShouldGetByNameIdWithEncryption returns a row by NameId or error if not found
func (t *DXRawTable) ShouldGetByNameIdWithEncryption(l *log.DXLog, nameId string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.ShouldSelectOneWithEncryption(l, encryptionColumns, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// TxGetByNameIdWithEncryption returns a row by NameId with decrypted columns within a transaction
func (t *DXRawTable) TxGetByNameIdWithEncryption(dtx *database.DXDatabaseTx, nameId string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.TxSelectOneWithEncryption(dtx, nil, encryptionColumns, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// TxShouldGetByNameIdWithEncryption returns a row by NameId or error if not found within a transaction
func (t *DXRawTable) TxShouldGetByNameIdWithEncryption(dtx *database.DXDatabaseTx, nameId string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.TxShouldSelectOneWithEncryption(dtx, nil, encryptionColumns, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// GetByUtagWithEncryption returns a row by Utag with decrypted columns
func (t *DXRawTable) GetByUtagWithEncryption(l *log.DXLog, utag string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.SelectOneWithEncryption(l, nil, encryptionColumns, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil)
}

// ShouldGetByUtagWithEncryption returns a row by Utag or error if not found
func (t *DXRawTable) ShouldGetByUtagWithEncryption(l *log.DXLog, utag string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.ShouldSelectOneWithEncryption(l, encryptionColumns, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil)
}

// TxGetByUtagWithEncryption returns a row by Utag with decrypted columns within a transaction
func (t *DXRawTable) TxGetByUtagWithEncryption(dtx *database.DXDatabaseTx, utag string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.TxSelectOneWithEncryption(dtx, nil, encryptionColumns, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil, nil)
}

// TxShouldGetByUtagWithEncryption returns a row by Utag or error if not found within a transaction
func (t *DXRawTable) TxShouldGetByUtagWithEncryption(dtx *database.DXDatabaseTx, utag string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.TxShouldSelectOneWithEncryption(dtx, nil, encryptionColumns, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil, nil)
}

// ============================================================================
// DXRawTable Encrypted Paging Methods
// ============================================================================

// TxPagingWithEncryption executes paging query with decrypted columns
func (t *DXRawTable) TxPagingWithEncryption(
	dtx *database.DXDatabaseTx,
	columns []string,
	encryptionColumns []EncryptionColumn,
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
	if err := setSessionKeysForDecryption(dtx, encryptionColumns); err != nil {
		return nil, err
	}

	return executeEncryptedPaging(dtx, t.ListViewNameId, dbType, columns, encryptionColumns, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
}

// PagingWithEncryption executes paging query with decrypted columns
func (t *DXRawTable) PagingWithEncryption(
	l *log.DXLog,
	columns []string,
	encryptionColumns []EncryptionColumn,
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

	return t.TxPagingWithEncryption(dtx, columns, encryptionColumns, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
}

// TxPagingWithEncryptionAndBuilder executes paging with QueryBuilder and decrypted columns
func (t *DXRawTable) TxPagingWithEncryptionAndBuilder(
	dtx *database.DXDatabaseTx,
	columns []string,
	encryptionColumns []EncryptionColumn,
	qb *QueryBuilder,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	whereClause, whereArgs := qb.Build()
	return t.TxPagingWithEncryption(dtx, columns, encryptionColumns, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
}

// PagingWithEncryptionAndBuilder executes paging with QueryBuilder and decrypted columns
func (t *DXRawTable) PagingWithEncryptionAndBuilder(
	l *log.DXLog,
	columns []string,
	encryptionColumns []EncryptionColumn,
	qb *QueryBuilder,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	whereClause, whereArgs := qb.Build()
	return t.PagingWithEncryption(l, columns, encryptionColumns, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
}

// ============================================================================
// DXTable Encrypted Select Methods (delegates to DXRawTable)
// ============================================================================

// TxSelectWithEncryption selects with decrypted columns within a transaction
func (t *DXTable) TxSelectWithEncryption(dtx *database.DXDatabaseTx, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.DXRawTable.TxSelectWithEncryption(dtx, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, limit, forUpdatePart)
}

// SelectWithEncryption selects with decrypted columns
func (t *DXTable) SelectWithEncryption(l *log.DXLog, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.DXRawTable.SelectWithEncryption(l, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, limit, forUpdatePart)
}

// TxSelectOneWithEncryption selects one row with decrypted columns within a transaction
func (t *DXTable) TxSelectOneWithEncryption(dtx *database.DXDatabaseTx, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxSelectOneWithEncryption(dtx, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, forUpdatePart)
}

// SelectOneWithEncryption selects one row with decrypted columns
func (t *DXTable) SelectOneWithEncryption(l *log.DXLog, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.SelectOneWithEncryption(l, fieldNames, encryptionColumns, where, joinSQLPart, orderBy)
}

// TxShouldSelectOneWithEncryption selects one row or returns error if not found within a transaction
func (t *DXTable) TxShouldSelectOneWithEncryption(dtx *database.DXDatabaseTx, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxShouldSelectOneWithEncryption(dtx, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, forUpdatePart)
}

// ShouldSelectOneWithEncryption selects one row or returns error if not found
func (t *DXTable) ShouldSelectOneWithEncryption(l *log.DXLog, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.ShouldSelectOneWithEncryption(l, encryptionColumns, where, joinSQLPart, orderBy)
}

// TxSelectByIdWithEncryption selects by ID with decrypted columns within a transaction
func (t *DXTable) TxSelectByIdWithEncryption(dtx *database.DXDatabaseTx, id int64, fieldNames []string,
	encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxSelectByIdWithEncryption(dtx, id, fieldNames, encryptionColumns)
}

// SelectByIdWithEncryption selects by ID with decrypted columns
func (t *DXTable) SelectByIdWithEncryption(l *log.DXLog, id int64, fieldNames []string,
	encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.SelectByIdWithEncryption(l, id, fieldNames, encryptionColumns)
}

// GetByIdWithEncryption returns a row by ID with decrypted columns
func (t *DXTable) GetByIdWithEncryption(l *log.DXLog, id int64, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.GetByIdWithEncryption(l, id, encryptionColumns)
}

// ShouldGetByIdWithEncryption returns a row by ID or error if not found
func (t *DXTable) ShouldGetByIdWithEncryption(l *log.DXLog, id int64, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.ShouldGetByIdWithEncryption(l, id, encryptionColumns)
}

// TxGetByIdWithEncryption returns a row by ID with decrypted columns within a transaction
func (t *DXTable) TxGetByIdWithEncryption(dtx *database.DXDatabaseTx, id int64, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxGetByIdWithEncryption(dtx, id, encryptionColumns)
}

// TxShouldGetByIdWithEncryption returns a row by ID or error if not found within a transaction
func (t *DXTable) TxShouldGetByIdWithEncryption(dtx *database.DXDatabaseTx, id int64, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxShouldGetByIdWithEncryption(dtx, id, encryptionColumns)
}

// GetByUidWithEncryption returns a row by UID with decrypted columns
func (t *DXTable) GetByUidWithEncryption(l *log.DXLog, uid string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.GetByUidWithEncryption(l, uid, encryptionColumns)
}

// ShouldGetByUidWithEncryption returns a row by UID or error if not found
func (t *DXTable) ShouldGetByUidWithEncryption(l *log.DXLog, uid string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.ShouldGetByUidWithEncryption(l, uid, encryptionColumns)
}

// TxGetByUidWithEncryption returns a row by UID with decrypted columns within a transaction
func (t *DXTable) TxGetByUidWithEncryption(dtx *database.DXDatabaseTx, uid string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxGetByUidWithEncryption(dtx, uid, encryptionColumns)
}

// TxShouldGetByUidWithEncryption returns a row by UID or error if not found within a transaction
func (t *DXTable) TxShouldGetByUidWithEncryption(dtx *database.DXDatabaseTx, uid string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxShouldGetByUidWithEncryption(dtx, uid, encryptionColumns)
}

// GetByNameIdWithEncryption returns a row by NameId with decrypted columns
func (t *DXTable) GetByNameIdWithEncryption(l *log.DXLog, nameId string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.GetByNameIdWithEncryption(l, nameId, encryptionColumns)
}

// ShouldGetByNameIdWithEncryption returns a row by NameId or error if not found
func (t *DXTable) ShouldGetByNameIdWithEncryption(l *log.DXLog, nameId string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.ShouldGetByNameIdWithEncryption(l, nameId, encryptionColumns)
}

// TxGetByNameIdWithEncryption returns a row by NameId with decrypted columns within a transaction
func (t *DXTable) TxGetByNameIdWithEncryption(dtx *database.DXDatabaseTx, nameId string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxGetByNameIdWithEncryption(dtx, nameId, encryptionColumns)
}

// TxShouldGetByNameIdWithEncryption returns a row by NameId or error if not found within a transaction
func (t *DXTable) TxShouldGetByNameIdWithEncryption(dtx *database.DXDatabaseTx, nameId string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxShouldGetByNameIdWithEncryption(dtx, nameId, encryptionColumns)
}

// GetByUtagWithEncryption returns a row by Utag with decrypted columns
func (t *DXTable) GetByUtagWithEncryption(l *log.DXLog, utag string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.GetByUtagWithEncryption(l, utag, encryptionColumns)
}

// ShouldGetByUtagWithEncryption returns a row by Utag or error if not found
func (t *DXTable) ShouldGetByUtagWithEncryption(l *log.DXLog, utag string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.ShouldGetByUtagWithEncryption(l, utag, encryptionColumns)
}

// TxGetByUtagWithEncryption returns a row by Utag with decrypted columns within a transaction
func (t *DXTable) TxGetByUtagWithEncryption(dtx *database.DXDatabaseTx, utag string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxGetByUtagWithEncryption(dtx, utag, encryptionColumns)
}

// TxShouldGetByUtagWithEncryption returns a row by Utag or error if not found within a transaction
func (t *DXTable) TxShouldGetByUtagWithEncryption(dtx *database.DXDatabaseTx, utag string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxShouldGetByUtagWithEncryption(dtx, utag, encryptionColumns)
}

// ============================================================================
// DXTable Encrypted Paging Methods (delegates to DXRawTable)
// ============================================================================

// TxPagingWithEncryption executes paging query with decrypted columns
func (t *DXTable) TxPagingWithEncryption(
	dtx *database.DXDatabaseTx,
	columns []string,
	encryptionColumns []EncryptionColumn,
	whereClause string,
	whereArgs utils.JSON,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	return t.DXRawTable.TxPagingWithEncryption(dtx, columns, encryptionColumns, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
}

// PagingWithEncryption executes paging query with decrypted columns
func (t *DXTable) PagingWithEncryption(
	l *log.DXLog,
	columns []string,
	encryptionColumns []EncryptionColumn,
	whereClause string,
	whereArgs utils.JSON,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	return t.DXRawTable.PagingWithEncryption(l, columns, encryptionColumns, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
}

// TxPagingWithEncryptionAndBuilder executes paging with QueryBuilder and decrypted columns
func (t *DXTable) TxPagingWithEncryptionAndBuilder(
	dtx *database.DXDatabaseTx,
	columns []string,
	encryptionColumns []EncryptionColumn,
	qb *QueryBuilder,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	return t.DXRawTable.TxPagingWithEncryptionAndBuilder(dtx, columns, encryptionColumns, qb, orderBy, rowPerPage, pageIndex)
}

// PagingWithEncryptionAndBuilder executes paging with QueryBuilder and decrypted columns
func (t *DXTable) PagingWithEncryptionAndBuilder(
	l *log.DXLog,
	columns []string,
	encryptionColumns []EncryptionColumn,
	qb *QueryBuilder,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	return t.DXRawTable.PagingWithEncryptionAndBuilder(l, columns, encryptionColumns, qb, orderBy, rowPerPage, pageIndex)
}

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
// DXRawTable Encrypted Update Methods
// ============================================================================

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

// ============================================================================
// DXTable Encrypted Update Methods (with audit fields)
// ============================================================================

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

// ============================================================================
// Internal Helper Functions
// ============================================================================

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

// setSessionKeysForDecryption sets all unique session keys from secure memory for SELECT
func setSessionKeysForDecryption(dtx *database.DXDatabaseTx, encryptionColumns []EncryptionColumn) error {
	sessionKeys := make(map[string]string) // sessionKey -> secureMemoryKey

	for _, col := range encryptionColumns {
		if col.EncryptionKeyDef != nil && col.EncryptionKeyDef.SecureMemoryKey != "" && col.EncryptionKeyDef.SessionKey != "" {
			sessionKeys[col.EncryptionKeyDef.SessionKey] = col.EncryptionKeyDef.SecureMemoryKey
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
func buildSelectColumns(dbType base.DXDatabaseType, columns []string, encryptionColumns []EncryptionColumn) string {
	var selectCols []string

	// Add regular columns (or * if empty)
	if len(columns) == 0 {
		selectCols = append(selectCols, "*")
	} else {
		for _, col := range columns {
			selectCols = append(selectCols, col)
		}
	}

	// Add decrypted columns
	for _, col := range encryptionColumns {
		if col.ViewHasDecrypt {
			// View already has decryption, just select the alias
			selectCols = append(selectCols, col.AliasName)
		} else {
			// Build decrypt expression
			expr := decryptExpression(dbType, col.FieldName, col.EncryptionKeyDef.SessionKey)
			selectCols = append(selectCols, fmt.Sprintf("%s AS %s", expr, col.AliasName))
		}
	}

	return strings.Join(selectCols, ", ")
}

// orderByToString converts DXDatabaseTableFieldsOrderBy to string
func orderByToString(orderBy db.DXDatabaseTableFieldsOrderBy) string {
	if orderBy == nil || len(orderBy) == 0 {
		return ""
	}
	var parts []string
	for field, direction := range orderBy {
		parts = append(parts, fmt.Sprintf("%s %s", field, direction))
	}
	return strings.Join(parts, ", ")
}

// limitToInt converts limit any to int
func limitToInt(limit any) int {
	if limit == nil {
		return 0
	}
	switch v := limit.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case int32:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

// executeEncryptedSelect builds and executes SELECT with decrypted columns
func executeEncryptedSelect(
	dtx *database.DXDatabaseTx,
	tableName string,
	fieldTypeMapping db.DXDatabaseTableFieldTypeMapping,
	dbType base.DXDatabaseType,
	fieldNames []string,
	encryptionColumns []EncryptionColumn,
	where utils.JSON,
	joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy,
	limit any,
	forUpdatePart any,
) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {

	selectCols := buildSelectColumns(dbType, fieldNames, encryptionColumns)

	// Build WHERE clause
	var whereClauses []string
	var args []any
	argIndex := 1

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

	// Build SQL
	sqlStr := fmt.Sprintf("SELECT"+" %s FROM %s", selectCols, tableName)

	// Add JOIN if specified
	if joinSQLPart != nil {
		if joinStr, ok := joinSQLPart.(string); ok && joinStr != "" {
			sqlStr += " " + joinStr
		}
	}

	if len(whereClauses) > 0 {
		sqlStr += " WHERE " + strings.Join(whereClauses, " AND ")
	}

	orderByStr := orderByToString(orderBy)
	if orderByStr != "" {
		sqlStr += " ORDER BY " + orderByStr
	}

	limitInt := limitToInt(limit)
	if limitInt > 0 {
		sqlStr += fmt.Sprintf(" LIMIT %d", limitInt)
	}

	// Add FOR UPDATE if specified
	if forUpdatePart != nil {
		if forUpdateStr, ok := forUpdatePart.(string); ok && forUpdateStr != "" {
			sqlStr += " " + forUpdateStr
		} else if forUpdateBool, ok := forUpdatePart.(bool); ok && forUpdateBool {
			sqlStr += " FOR UPDATE"
		}
	}

	// Execute
	rows, err := dtx.Tx.Queryx(sqlStr, args...)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "ENCRYPTED_SELECT_ERROR")
	}
	defer func(rows *sqlx.Rows) {
		err := rows.Close()
		if err != nil {
			log.Log.Error("ERROR_IN_ROWS_CLOSE", err)
		}
	}(rows)

	// Get column info
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, errors.Wrapf(err, "ENCRYPTED_SELECT_COLUMNS_ERROR")
	}

	rowsInfo := &db.DXDatabaseTableRowsInfo{
		Columns: columns,
	}

	var results []utils.JSON
	for rows.Next() {
		row := make(map[string]any)
		if err := rows.MapScan(row); err != nil {
			return rowsInfo, nil, errors.Wrapf(err, "ENCRYPTED_SELECT_SCAN_ERROR")
		}
		// Convert []byte to string for decrypted text fields
		for k, v := range row {
			if b, ok := v.([]byte); ok {
				row[k] = string(b)
			}
		}
		results = append(results, row)
	}

	return rowsInfo, results, nil
}

// executeEncryptedPaging builds and executes paging query with decrypted columns
func executeEncryptedPaging(
	dtx *database.DXDatabaseTx,
	tableName string,
	dbType base.DXDatabaseType,
	columns []string,
	encryptionColumns []EncryptionColumn,
	whereClause string,
	whereArgs utils.JSON,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {

	selectCols := buildSelectColumns(dbType, columns, encryptionColumns)

	// Count total rows
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM"+" %s", tableName)
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
	sqlStr := fmt.Sprintf("SELECT"+" %s FROM %s", selectCols, tableName)

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
