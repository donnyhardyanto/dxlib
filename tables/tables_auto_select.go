package tables

import (
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/database/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DXRawTable Auto Select Methods

// SelectAuto selects using table's EncryptionColumnDefs and EncryptionKeyDefs (creates transaction if needed)
func (t *DXRawTable) SelectAuto(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	if len(t.EncryptionColumnDefs) > 0 {
		// Has column-specific decryption, use an encrypted select path (creates transaction internally)
		encryptionColumns := t.convertEncryptionColumnDefsForSelect()
		return t.SelectWithEncryption(l, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, limit, forUpdatePart)
	}
	if len(t.EncryptionKeyDefs) > 0 {
		// Only session keys needed, create transaction, set keys, then regular select
		if err := t.EnsureDatabase(); err != nil {
			return nil, nil, err
		}
		dtx, err := t.Database.TransactionBegin(database.LevelReadCommitted)
		if err != nil {
			return nil, nil, err
		}
		defer dtx.Finish(l, err)

		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, nil, err
		}
		return t.TxSelect(dtx, fieldNames, where, joinSQLPart, orderBy, limit, forUpdatePart)
	}
	return t.Select(l, fieldNames, where, joinSQLPart, orderBy, limit, forUpdatePart)
}

// SelectOneAuto selects one row using table's EncryptionColumnDefs and EncryptionKeyDefs
func (t *DXRawTable) SelectOneAuto(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if len(t.EncryptionColumnDefs) > 0 {
		// Has column-specific decryption, use encrypted select path
		encryptionColumns := t.convertEncryptionColumnDefsForSelect()
		return t.SelectOneWithEncryption(l, fieldNames, encryptionColumns, where, joinSQLPart, orderBy)
	}
	if len(t.EncryptionKeyDefs) > 0 {
		// Only session keys needed, create transaction, set keys, then regular select
		if err := t.EnsureDatabase(); err != nil {
			return nil, nil, err
		}
		dtx, err := t.Database.TransactionBegin(database.LevelReadCommitted)
		if err != nil {
			return nil, nil, err
		}
		defer dtx.Finish(l, err)

		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, nil, err
		}
		return t.TxSelectOne(dtx, fieldNames, where, joinSQLPart, orderBy, nil)
	}
	return t.SelectOne(l, fieldNames, where, joinSQLPart, orderBy)
}

// ShouldSelectOneAuto selects one row or returns error if not found, using table's EncryptionColumnDefs and EncryptionKeyDefs
func (t *DXRawTable) ShouldSelectOneAuto(l *log.DXLog, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if len(t.EncryptionColumnDefs) > 0 {
		// Has column-specific decryption, use encrypted select path
		encryptionColumns := t.convertEncryptionColumnDefsForSelect()
		return t.ShouldSelectOneWithEncryption(l, encryptionColumns, where, joinSQLPart, orderBy)
	}
	if len(t.EncryptionKeyDefs) > 0 {
		// Only session keys needed, create transaction, set keys, then regular select
		if err := t.EnsureDatabase(); err != nil {
			return nil, nil, err
		}
		dtx, err := t.Database.TransactionBegin(database.LevelReadCommitted)
		if err != nil {
			return nil, nil, err
		}
		defer dtx.Finish(l, err)

		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, nil, err
		}
		return t.TxShouldSelectOne(dtx, nil, where, joinSQLPart, orderBy, nil)
	}
	return t.ShouldSelectOne(l, where, joinSQLPart, orderBy)
}

// GetByIdAuto returns a row by ID using table's EncryptionColumnDefs
func (t *DXRawTable) GetByIdAuto(l *log.DXLog, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// ShouldGetByIdAuto returns a row by ID or error if not found, using table's EncryptionColumnDefs
func (t *DXRawTable) ShouldGetByIdAuto(l *log.DXLog, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldSelectOneAuto(l, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// GetByUidAuto returns a row by UID using table's EncryptionColumnDefs
func (t *DXRawTable) GetByUidAuto(l *log.DXLog, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// ShouldGetByUidAuto returns a row by UID or error if not found, using table's EncryptionColumnDefs
func (t *DXRawTable) ShouldGetByUidAuto(l *log.DXLog, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.ShouldSelectOneAuto(l, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// GetByUtagAuto returns a row by Utag using table's EncryptionColumnDefs
func (t *DXRawTable) GetByUtagAuto(l *log.DXLog, utag string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil)
}

// ShouldGetByUtagAuto returns a row by Utag or error if not found, using table's EncryptionColumnDefs
func (t *DXRawTable) ShouldGetByUtagAuto(l *log.DXLog, utag string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.ShouldSelectOneAuto(l, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil)
}

// GetByNameIdAuto returns a row by NameId using table's EncryptionColumnDefs
func (t *DXRawTable) GetByNameIdAuto(l *log.DXLog, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// ShouldGetByNameIdAuto returns a row by NameId or error if not found, using table's EncryptionColumnDefs
func (t *DXRawTable) ShouldGetByNameIdAuto(l *log.DXLog, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.ShouldSelectOneAuto(l, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// DXTable Auto Select Methods (with audit fields)

// SelectAuto selects using table's EncryptionColumnDefs
func (t *DXTable) SelectAuto(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.DXRawTable.SelectAuto(l, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, limit, forUpdatePart)
}

// SelectOneAuto selects one row using table's EncryptionColumnDefs
func (t *DXTable) SelectOneAuto(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.SelectOneAuto(l, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy)
}

// ShouldSelectOneAuto selects one row or returns error if not found, using table's EncryptionColumnDefs
func (t *DXTable) ShouldSelectOneAuto(l *log.DXLog, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.ShouldSelectOneAuto(l, t.addNotDeletedFilter(where), joinSQLPart, orderBy)
}

// GetByIdAuto returns a row by ID using table's EncryptionColumnDefs
func (t *DXTable) GetByIdAuto(l *log.DXLog, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// ShouldGetByIdAuto returns a row by ID or error if not found, using table's EncryptionColumnDefs
func (t *DXTable) ShouldGetByIdAuto(l *log.DXLog, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldSelectOneAuto(l, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// GetByIdNotDeletedAuto returns a non-deleted row by ID using table's EncryptionColumnDefs
func (t *DXTable) GetByIdNotDeletedAuto(l *log.DXLog, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// ShouldGetByIdNotDeletedAuto returns a non-deleted row by ID or error if not found
func (t *DXTable) ShouldGetByIdNotDeletedAuto(l *log.DXLog, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldSelectOneAuto(l, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// GetByUidAuto returns a row by UID using table's EncryptionColumnDefs
func (t *DXTable) GetByUidAuto(l *log.DXLog, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// ShouldGetByUidAuto returns a row by UID or error if not found, using table's EncryptionColumnDefs
func (t *DXTable) ShouldGetByUidAuto(l *log.DXLog, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.ShouldSelectOneAuto(l, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// GetByUidNotDeletedAuto returns a non-deleted row by UID using table's EncryptionColumnDefs
func (t *DXTable) GetByUidNotDeletedAuto(l *log.DXLog, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// ShouldGetByUidNotDeletedAuto returns a non-deleted row by UID or error if not found
func (t *DXTable) ShouldGetByUidNotDeletedAuto(l *log.DXLog, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.ShouldSelectOneAuto(l, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// GetByUtagAuto returns a row by Utag using table's EncryptionColumnDefs
func (t *DXTable) GetByUtagAuto(l *log.DXLog, utag string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil)
}

// ShouldGetByUtagAuto returns a row by Utag or error if not found, using table's EncryptionColumnDefs
func (t *DXTable) ShouldGetByUtagAuto(l *log.DXLog, utag string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.ShouldSelectOneAuto(l, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil)
}

// GetByNameIdAuto returns a row by NameId using table's EncryptionColumnDefs
func (t *DXTable) GetByNameIdAuto(l *log.DXLog, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// ShouldGetByNameIdAuto returns a row by NameId or error if not found, using table's EncryptionColumnDefs
func (t *DXTable) ShouldGetByNameIdAuto(l *log.DXLog, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.ShouldSelectOneAuto(l, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// GetByNameIdNotDeletedAuto returns a non-deleted row by NameId using table's EncryptionColumnDefs
func (t *DXTable) GetByNameIdNotDeletedAuto(l *log.DXLog, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// ShouldGetByNameIdNotDeletedAuto returns a non-deleted row by NameId or error if not found
func (t *DXTable) ShouldGetByNameIdNotDeletedAuto(l *log.DXLog, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.ShouldSelectOneAuto(l, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// DXRawTable Paging Auto Methods

// PagingAuto executes paging query using table's EncryptionColumnDefs and EncryptionKeyDefs
func (t *DXRawTable) PagingAuto(
	l *log.DXLog,
	rowPerPage, pageIndex int64,
	whereClause string,
	whereArgs utils.JSON,
	orderBy string,
) (*PagingResult, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, err
	}

	if len(t.EncryptionColumnDefs) > 0 {
		// Has column-specific decryption, use encrypted paging path
		encryptionColumns := t.convertEncryptionColumnDefsForSelect()
		return t.PagingWithEncryption(l, nil, encryptionColumns, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
	}
	if len(t.EncryptionKeyDefs) > 0 {
		// Only session keys needed, create transaction, set keys, then paging within transaction
		dtx, err := t.Database.TransactionBegin(database.LevelReadCommitted)
		if err != nil {
			return nil, err
		}
		defer dtx.Finish(l, err)

		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, err
		}
		return executeEncryptedPaging(dtx, t.ListViewNameId, t.Database.DatabaseType, nil, nil, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
	}
	// No encryption, use regular paging
	return t.Paging(l, rowPerPage, pageIndex, whereClause, orderBy, whereArgs)
}

// DXTable Paging Auto Methods

// PagingAuto executes paging query using table's EncryptionColumnDefs
func (t *DXTable) PagingAuto(
	l *log.DXLog,
	rowPerPage, pageIndex int64,
	whereClause string,
	whereArgs utils.JSON,
	orderBy string,
) (*PagingResult, error) {
	return t.DXRawTable.PagingAuto(l, rowPerPage, pageIndex, t.addNotDeletedToWhere(whereClause), whereArgs, orderBy)
}
