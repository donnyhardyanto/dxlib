package tables

import (
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DXRawTable Auto Tx Select Methods

// TxSelectAuto selects using table's EncryptionColumnDefs and EncryptionKeyDefs
func (t *DXRawTable) TxSelectAuto(dtx *databases.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	if len(t.EncryptionColumnDefs) > 0 {
		// Has column-specific decryption, use encrypted select path
		encryptionColumns := t.convertEncryptionColumnDefsForSelect()
		return t.TxSelectWithEncryption(dtx, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, limit, forUpdatePart)
	}
	if len(t.EncryptionKeyDefs) > 0 {
		// Only session keys needed, set them then regular select
		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, nil, err
		}
	}
	return t.TxSelect(dtx, fieldNames, where, joinSQLPart, orderBy, limit, forUpdatePart)
}

// TxSelectOneAuto selects one row using table's EncryptionColumnDefs and EncryptionKeyDefs
func (t *DXRawTable) TxSelectOneAuto(dtx *databases.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if len(t.EncryptionColumnDefs) > 0 {
		// Has column-specific decryption, use encrypted select path
		encryptionColumns := t.convertEncryptionColumnDefsForSelect()
		return t.TxSelectOneWithEncryption(dtx, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, forUpdatePart)
	}
	if len(t.EncryptionKeyDefs) > 0 {
		// Only session keys needed, set them then regular select
		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, nil, err
		}
	}
	return t.TxSelectOne(dtx, fieldNames, where, joinSQLPart, orderBy, forUpdatePart)
}

// TxShouldSelectOneAuto selects one row or returns error if not found, using table's EncryptionColumnDefs and EncryptionKeyDefs
func (t *DXRawTable) TxShouldSelectOneAuto(dtx *databases.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if len(t.EncryptionColumnDefs) > 0 {
		// Has column-specific decryption, use encrypted select path
		encryptionColumns := t.convertEncryptionColumnDefsForSelect()
		return t.TxShouldSelectOneWithEncryption(dtx, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, forUpdatePart)
	}
	if len(t.EncryptionKeyDefs) > 0 {
		// Only session keys needed, set them then regular select
		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, nil, err
		}
	}
	return t.TxShouldSelectOne(dtx, fieldNames, where, joinSQLPart, orderBy, forUpdatePart)
}

// TxGetByIdAuto returns a row by ID using table's EncryptionColumnDefs within a transaction
func (t *DXRawTable) TxGetByIdAuto(dtx *databases.DXDatabaseTx, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxShouldGetByIdAuto returns a row by ID or error if not found, using table's EncryptionColumnDefs within a transaction
func (t *DXRawTable) TxShouldGetByIdAuto(dtx *databases.DXDatabaseTx, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxShouldSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxGetByUidAuto returns a row by UID using table's EncryptionColumnDefs within a transaction
func (t *DXRawTable) TxGetByUidAuto(dtx *databases.DXDatabaseTx, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxShouldGetByUidAuto returns a row by UID or error if not found, using table's EncryptionColumnDefs within a transaction
func (t *DXRawTable) TxShouldGetByUidAuto(dtx *databases.DXDatabaseTx, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxShouldSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxGetByUtagAuto returns a row by Utag using table's EncryptionColumnDefs within a transaction
func (t *DXRawTable) TxGetByUtagAuto(dtx *databases.DXDatabaseTx, utag string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil, nil)
}

// TxShouldGetByUtagAuto returns a row by Utag or error if not found, using table's EncryptionColumnDefs within a transaction
func (t *DXRawTable) TxShouldGetByUtagAuto(dtx *databases.DXDatabaseTx, utag string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxShouldSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil, nil)
}

// TxGetByNameIdAuto returns a row by NameId using table's EncryptionColumnDefs within a transaction
func (t *DXRawTable) TxGetByNameIdAuto(dtx *databases.DXDatabaseTx, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// TxShouldGetByNameIdAuto returns a row by NameId or error if not found, using table's EncryptionColumnDefs within a transaction
func (t *DXRawTable) TxShouldGetByNameIdAuto(dtx *databases.DXDatabaseTx, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxShouldSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// DXTable Auto Tx Select Methods (with audit fields)

// TxSelectAuto selects using table's EncryptionColumnDefs
func (t *DXTable) TxSelectAuto(dtx *databases.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.DXRawTable.TxSelectAuto(dtx, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, limit, forUpdatePart)
}

// TxSelectOneAuto selects one row using table's EncryptionColumnDefs
func (t *DXTable) TxSelectOneAuto(dtx *databases.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxSelectOneAuto(dtx, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, forUpdatePart)
}

// TxShouldSelectOneAuto selects one row or returns error if not found, using table's EncryptionColumnDefs
func (t *DXTable) TxShouldSelectOneAuto(dtx *databases.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxShouldSelectOneAuto(dtx, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, forUpdatePart)
}

// TxGetByIdAuto returns a row by ID using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxGetByIdAuto(dtx *databases.DXDatabaseTx, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxShouldGetByIdAuto returns a row by ID or error if not found, using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxShouldGetByIdAuto(dtx *databases.DXDatabaseTx, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxShouldSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxGetByIdNotDeletedAuto returns a non-deleted row by ID using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxGetByIdNotDeletedAuto(dtx *databases.DXDatabaseTx, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxShouldGetByIdNotDeletedAuto returns a non-deleted row by ID or error if not found within a transaction
func (t *DXTable) TxShouldGetByIdNotDeletedAuto(dtx *databases.DXDatabaseTx, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxShouldSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxGetByUidAuto returns a row by UID using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxGetByUidAuto(dtx *databases.DXDatabaseTx, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxShouldGetByUidAuto returns a row by UID or error if not found, using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxShouldGetByUidAuto(dtx *databases.DXDatabaseTx, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxShouldSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxGetByUidNotDeletedAuto returns a non-deleted row by UID using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxGetByUidNotDeletedAuto(dtx *databases.DXDatabaseTx, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxShouldGetByUidNotDeletedAuto returns a non-deleted row by UID or error if not found within a transaction
func (t *DXTable) TxShouldGetByUidNotDeletedAuto(dtx *databases.DXDatabaseTx, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxShouldSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxGetByUtagAuto returns a row by Utag using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxGetByUtagAuto(dtx *databases.DXDatabaseTx, utag string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil, nil)
}

// TxShouldGetByUtagAuto returns a row by Utag or error if not found, using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxShouldGetByUtagAuto(dtx *databases.DXDatabaseTx, utag string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxShouldSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil, nil)
}

// TxGetByNameIdAuto returns a row by NameId using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxGetByNameIdAuto(dtx *databases.DXDatabaseTx, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// TxShouldGetByNameIdAuto returns a row by NameId or error if not found, using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxShouldGetByNameIdAuto(dtx *databases.DXDatabaseTx, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxShouldSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// TxGetByNameIdNotDeletedAuto returns a non-deleted row by NameId using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxGetByNameIdNotDeletedAuto(dtx *databases.DXDatabaseTx, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// TxShouldGetByNameIdNotDeletedAuto returns a non-deleted row by NameId or error if not found within a transaction
func (t *DXTable) TxShouldGetByNameIdNotDeletedAuto(dtx *databases.DXDatabaseTx, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxShouldSelectOneAuto(dtx, fn, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}
