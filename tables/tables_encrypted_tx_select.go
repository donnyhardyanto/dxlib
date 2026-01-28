package tables

import (
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/database/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DXRawTable Encrypted Tx Select Methods

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

// TxSelectByIdWithEncryption selects by ID with decrypted columns within a transaction
func (t *DXRawTable) TxSelectByIdWithEncryption(dtx *database.DXDatabaseTx, id int64, fieldNames []string,
	encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxSelectOneWithEncryption(dtx, fieldNames, encryptionColumns, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxGetByIdWithEncryption returns a row by ID with decrypted columns within a transaction
func (t *DXRawTable) TxGetByIdWithEncryption(dtx *database.DXDatabaseTx, id int64, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxSelectOneWithEncryption(dtx, nil, encryptionColumns, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxShouldGetByIdWithEncryption returns a row by ID or error if not found within a transaction
func (t *DXRawTable) TxShouldGetByIdWithEncryption(dtx *database.DXDatabaseTx, id int64, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxShouldSelectOneWithEncryption(dtx, nil, encryptionColumns, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
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

// DXRawTable Encrypted Tx Paging Methods

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

// DXTable Encrypted Tx Select Methods (delegates to DXRawTable)

// TxSelectWithEncryption selects with decrypted columns within a transaction
func (t *DXTable) TxSelectWithEncryption(dtx *database.DXDatabaseTx, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.DXRawTable.TxSelectWithEncryption(dtx, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, limit, forUpdatePart)
}

// TxSelectOneWithEncryption selects one row with decrypted columns within a transaction
func (t *DXTable) TxSelectOneWithEncryption(dtx *database.DXDatabaseTx, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxSelectOneWithEncryption(dtx, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, forUpdatePart)
}

// TxShouldSelectOneWithEncryption selects one row or returns error if not found within a transaction
func (t *DXTable) TxShouldSelectOneWithEncryption(dtx *database.DXDatabaseTx, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxShouldSelectOneWithEncryption(dtx, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, forUpdatePart)
}

// TxSelectByIdWithEncryption selects by ID with decrypted columns within a transaction
func (t *DXTable) TxSelectByIdWithEncryption(dtx *database.DXDatabaseTx, id int64, fieldNames []string,
	encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxSelectByIdWithEncryption(dtx, id, fieldNames, encryptionColumns)
}

// TxGetByIdWithEncryption returns a row by ID with decrypted columns within a transaction
func (t *DXTable) TxGetByIdWithEncryption(dtx *database.DXDatabaseTx, id int64, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxGetByIdWithEncryption(dtx, id, encryptionColumns)
}

// TxShouldGetByIdWithEncryption returns a row by ID or error if not found within a transaction
func (t *DXTable) TxShouldGetByIdWithEncryption(dtx *database.DXDatabaseTx, id int64, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxShouldGetByIdWithEncryption(dtx, id, encryptionColumns)
}

// TxGetByUidWithEncryption returns a row by UID with decrypted columns within a transaction
func (t *DXTable) TxGetByUidWithEncryption(dtx *database.DXDatabaseTx, uid string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxGetByUidWithEncryption(dtx, uid, encryptionColumns)
}

// TxShouldGetByUidWithEncryption returns a row by UID or error if not found within a transaction
func (t *DXTable) TxShouldGetByUidWithEncryption(dtx *database.DXDatabaseTx, uid string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxShouldGetByUidWithEncryption(dtx, uid, encryptionColumns)
}

// TxGetByNameIdWithEncryption returns a row by NameId with decrypted columns within a transaction
func (t *DXTable) TxGetByNameIdWithEncryption(dtx *database.DXDatabaseTx, nameId string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxGetByNameIdWithEncryption(dtx, nameId, encryptionColumns)
}

// TxShouldGetByNameIdWithEncryption returns a row by NameId or error if not found within a transaction
func (t *DXTable) TxShouldGetByNameIdWithEncryption(dtx *database.DXDatabaseTx, nameId string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxShouldGetByNameIdWithEncryption(dtx, nameId, encryptionColumns)
}

// TxGetByUtagWithEncryption returns a row by Utag with decrypted columns within a transaction
func (t *DXTable) TxGetByUtagWithEncryption(dtx *database.DXDatabaseTx, utag string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxGetByUtagWithEncryption(dtx, utag, encryptionColumns)
}

// TxShouldGetByUtagWithEncryption returns a row by Utag or error if not found within a transaction
func (t *DXTable) TxShouldGetByUtagWithEncryption(dtx *database.DXDatabaseTx, utag string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxShouldGetByUtagWithEncryption(dtx, utag, encryptionColumns)
}

// DXTable Encrypted Tx Paging Methods (delegates to DXRawTable)

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
