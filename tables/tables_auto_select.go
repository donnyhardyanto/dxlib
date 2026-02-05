package tables

import (
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DXRawTable Auto Select Methods
// These methods now delegate to the base methods which are auto-encryption-aware.

// SelectAuto selects using table's EncryptionColumnDefs and EncryptionKeyDefs (creates transaction if needed)
func (t *DXRawTable) SelectAuto(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.Select(l, fieldNames, where, joinSQLPart, orderBy, limit, forUpdatePart)
}

// SelectOneAuto selects one row using table's EncryptionColumnDefs and EncryptionKeyDefs
func (t *DXRawTable) SelectOneAuto(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.SelectOne(l, fieldNames, where, joinSQLPart, orderBy)
}

// ShouldSelectOneAuto selects one row or returns error if not found, using table's EncryptionColumnDefs and EncryptionKeyDefs
func (t *DXRawTable) ShouldSelectOneAuto(l *log.DXLog, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldSelectOne(l, nil, where, joinSQLPart, orderBy)
}

// GetByIdAuto returns a row by ID using table's EncryptionColumnDefs
func (t *DXRawTable) GetByIdAuto(l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetById(l, id, fieldNames...)
}

// ShouldGetByIdAuto returns a row by ID or error if not found, using table's EncryptionColumnDefs
func (t *DXRawTable) ShouldGetByIdAuto(l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetById(l, id, fieldNames...)
}

// GetByUidAuto returns a row by UID using table's EncryptionColumnDefs
func (t *DXRawTable) GetByUidAuto(l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetByUid(l, uid, fieldNames...)
}

// ShouldGetByUidAuto returns a row by UID or error if not found, using table's EncryptionColumnDefs
func (t *DXRawTable) ShouldGetByUidAuto(l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetByUid(l, uid, fieldNames...)
}

// GetByUtagAuto returns a row by Utag using table's EncryptionColumnDefs
func (t *DXRawTable) GetByUtagAuto(l *log.DXLog, utag string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetByUtag(l, utag, fieldNames...)
}

// ShouldGetByUtagAuto returns a row by Utag or error if not found, using table's EncryptionColumnDefs
func (t *DXRawTable) ShouldGetByUtagAuto(l *log.DXLog, utag string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetByUtag(l, utag, fieldNames...)
}

// GetByNameIdAuto returns a row by NameId using table's EncryptionColumnDefs
func (t *DXRawTable) GetByNameIdAuto(l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetByNameId(l, nameId, fieldNames...)
}

// ShouldGetByNameIdAuto returns a row by NameId or error if not found, using table's EncryptionColumnDefs
func (t *DXRawTable) ShouldGetByNameIdAuto(l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetByNameId(l, nameId, fieldNames...)
}

// DXTable Auto Select Methods (with audit fields)
// These methods now delegate to the base DXTable methods which add is_deleted filter and are auto-encryption-aware.

// SelectAuto selects using table's EncryptionColumnDefs
func (t *DXTable) SelectAuto(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.Select(l, fieldNames, where, joinSQLPart, orderBy, limit, forUpdatePart)
}

// SelectOneAuto selects one row using table's EncryptionColumnDefs
func (t *DXTable) SelectOneAuto(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.SelectOne(l, fieldNames, where, joinSQLPart, orderBy)
}

// ShouldSelectOneAuto selects one row or returns error if not found, using table's EncryptionColumnDefs
func (t *DXTable) ShouldSelectOneAuto(l *log.DXLog, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldSelectOne(l, nil, where, joinSQLPart, orderBy)
}

// GetByIdAuto returns a row by ID using table's EncryptionColumnDefs
func (t *DXTable) GetByIdAuto(l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetByIdNotDeleted(l, id, fieldNames...)
}

// ShouldGetByIdAuto returns a row by ID or error if not found, using table's EncryptionColumnDefs
func (t *DXTable) ShouldGetByIdAuto(l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetByIdNotDeleted(l, id, fieldNames...)
}

// GetByIdNotDeletedAuto returns a non-deleted row by ID using table's EncryptionColumnDefs
func (t *DXTable) GetByIdNotDeletedAuto(l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetByIdNotDeleted(l, id, fieldNames...)
}

// ShouldGetByIdNotDeletedAuto returns a non-deleted row by ID or error if not found
func (t *DXTable) ShouldGetByIdNotDeletedAuto(l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetByIdNotDeleted(l, id, fieldNames...)
}

// GetByUidAuto returns a row by UID using table's EncryptionColumnDefs
func (t *DXTable) GetByUidAuto(l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetByUidNotDeleted(l, uid, fieldNames...)
}

// ShouldGetByUidAuto returns a row by UID or error if not found, using table's EncryptionColumnDefs
func (t *DXTable) ShouldGetByUidAuto(l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetByUidNotDeleted(l, uid, fieldNames...)
}

// GetByUidNotDeletedAuto returns a non-deleted row by UID using table's EncryptionColumnDefs
func (t *DXTable) GetByUidNotDeletedAuto(l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetByUidNotDeleted(l, uid, fieldNames...)
}

// ShouldGetByUidNotDeletedAuto returns a non-deleted row by UID or error if not found
func (t *DXTable) ShouldGetByUidNotDeletedAuto(l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetByUidNotDeleted(l, uid, fieldNames...)
}

// GetByUtagAuto returns a row by Utag using table's EncryptionColumnDefs
func (t *DXTable) GetByUtagAuto(l *log.DXLog, utag string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.SelectOne(l, fn, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil)
}

// ShouldGetByUtagAuto returns a row by Utag or error if not found, using table's EncryptionColumnDefs
func (t *DXTable) ShouldGetByUtagAuto(l *log.DXLog, utag string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.ShouldSelectOne(l, fn, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil)
}

// GetByNameIdAuto returns a row by NameId using table's EncryptionColumnDefs
func (t *DXTable) GetByNameIdAuto(l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetByNameIdNotDeleted(l, nameId, fieldNames...)
}

// ShouldGetByNameIdAuto returns a row by NameId or error if not found, using table's EncryptionColumnDefs
func (t *DXTable) ShouldGetByNameIdAuto(l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetByNameIdNotDeleted(l, nameId, fieldNames...)
}

// GetByNameIdNotDeletedAuto returns a non-deleted row by NameId using table's EncryptionColumnDefs
func (t *DXTable) GetByNameIdNotDeletedAuto(l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetByNameIdNotDeleted(l, nameId, fieldNames...)
}

// ShouldGetByNameIdNotDeletedAuto returns a non-deleted row by NameId or error if not found
func (t *DXTable) ShouldGetByNameIdNotDeletedAuto(l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetByNameIdNotDeleted(l, nameId, fieldNames...)
}

