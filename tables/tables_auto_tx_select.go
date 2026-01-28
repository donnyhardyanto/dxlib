package tables

import (
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/database/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DXRawTable Auto Tx Select Methods

// TxSelectAuto selects using table's EncryptionColumnDefs and EncryptionKeyDefs
func (t *DXRawTable) TxSelectAuto(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
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
func (t *DXRawTable) TxSelectOneAuto(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
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
func (t *DXRawTable) TxShouldSelectOneAuto(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
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
func (t *DXRawTable) TxGetByIdAuto(dtx *database.DXDatabaseTx, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxShouldGetByIdAuto returns a row by ID or error if not found, using table's EncryptionColumnDefs within a transaction
func (t *DXRawTable) TxShouldGetByIdAuto(dtx *database.DXDatabaseTx, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxShouldSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxGetByUidAuto returns a row by UID using table's EncryptionColumnDefs within a transaction
func (t *DXRawTable) TxGetByUidAuto(dtx *database.DXDatabaseTx, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxShouldGetByUidAuto returns a row by UID or error if not found, using table's EncryptionColumnDefs within a transaction
func (t *DXRawTable) TxShouldGetByUidAuto(dtx *database.DXDatabaseTx, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.TxShouldSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxGetByUtagAuto returns a row by Utag using table's EncryptionColumnDefs within a transaction
func (t *DXRawTable) TxGetByUtagAuto(dtx *database.DXDatabaseTx, utag string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil, nil)
}

// TxShouldGetByUtagAuto returns a row by Utag or error if not found, using table's EncryptionColumnDefs within a transaction
func (t *DXRawTable) TxShouldGetByUtagAuto(dtx *database.DXDatabaseTx, utag string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.TxShouldSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil, nil)
}

// TxGetByNameIdAuto returns a row by NameId using table's EncryptionColumnDefs within a transaction
func (t *DXRawTable) TxGetByNameIdAuto(dtx *database.DXDatabaseTx, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// TxShouldGetByNameIdAuto returns a row by NameId or error if not found, using table's EncryptionColumnDefs within a transaction
func (t *DXRawTable) TxShouldGetByNameIdAuto(dtx *database.DXDatabaseTx, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.TxShouldSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// DXTable Auto Tx Select Methods (with audit fields)

// TxSelectAuto selects using table's EncryptionColumnDefs
func (t *DXTable) TxSelectAuto(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.DXRawTable.TxSelectAuto(dtx, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, limit, forUpdatePart)
}

// TxSelectOneAuto selects one row using table's EncryptionColumnDefs
func (t *DXTable) TxSelectOneAuto(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxSelectOneAuto(dtx, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, forUpdatePart)
}

// TxShouldSelectOneAuto selects one row or returns error if not found, using table's EncryptionColumnDefs
func (t *DXTable) TxShouldSelectOneAuto(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxShouldSelectOneAuto(dtx, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, forUpdatePart)
}

// TxGetByIdAuto returns a row by ID using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxGetByIdAuto(dtx *database.DXDatabaseTx, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxShouldGetByIdAuto returns a row by ID or error if not found, using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxShouldGetByIdAuto(dtx *database.DXDatabaseTx, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxShouldSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxGetByIdNotDeletedAuto returns a non-deleted row by ID using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxGetByIdNotDeletedAuto(dtx *database.DXDatabaseTx, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxShouldGetByIdNotDeletedAuto returns a non-deleted row by ID or error if not found within a transaction
func (t *DXTable) TxShouldGetByIdNotDeletedAuto(dtx *database.DXDatabaseTx, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxShouldSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxGetByUidAuto returns a row by UID using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxGetByUidAuto(dtx *database.DXDatabaseTx, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxShouldGetByUidAuto returns a row by UID or error if not found, using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxShouldGetByUidAuto(dtx *database.DXDatabaseTx, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.TxShouldSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxGetByUidNotDeletedAuto returns a non-deleted row by UID using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxGetByUidNotDeletedAuto(dtx *database.DXDatabaseTx, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxShouldGetByUidNotDeletedAuto returns a non-deleted row by UID or error if not found within a transaction
func (t *DXTable) TxShouldGetByUidNotDeletedAuto(dtx *database.DXDatabaseTx, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.TxShouldSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxGetByUtagAuto returns a row by Utag using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxGetByUtagAuto(dtx *database.DXDatabaseTx, utag string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil, nil)
}

// TxShouldGetByUtagAuto returns a row by Utag or error if not found, using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxShouldGetByUtagAuto(dtx *database.DXDatabaseTx, utag string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.TxShouldSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil, nil)
}

// TxGetByNameIdAuto returns a row by NameId using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxGetByNameIdAuto(dtx *database.DXDatabaseTx, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// TxShouldGetByNameIdAuto returns a row by NameId or error if not found, using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxShouldGetByNameIdAuto(dtx *database.DXDatabaseTx, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.TxShouldSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// TxGetByNameIdNotDeletedAuto returns a non-deleted row by NameId using table's EncryptionColumnDefs within a transaction
func (t *DXTable) TxGetByNameIdNotDeletedAuto(dtx *database.DXDatabaseTx, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// TxShouldGetByNameIdNotDeletedAuto returns a non-deleted row by NameId or error if not found within a transaction
func (t *DXTable) TxShouldGetByNameIdNotDeletedAuto(dtx *database.DXDatabaseTx, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.TxShouldSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}
