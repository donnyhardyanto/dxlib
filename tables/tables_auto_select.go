package tables

import (
	"context"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DXRawTable Auto Select Methods
// These methods now delegate to the base methods which are auto-encryption-aware.

// SelectAuto selects using table's EncryptionColumnDefs and EncryptionKeyDefs (creates transaction if needed)
func (t *DXRawTable) SelectAuto(ctx context.Context, l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.Select(ctx, l, fieldNames, where, joinSQLPart, orderBy, limit, forUpdatePart)
}

// SelectOneAuto selects one row using table's EncryptionColumnDefs and EncryptionKeyDefs
func (t *DXRawTable) SelectOneAuto(ctx context.Context, l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.SelectOne(ctx, l, fieldNames, where, joinSQLPart, orderBy)
}

// ShouldSelectOneAuto selects one row or returns error if not found, using table's EncryptionColumnDefs and EncryptionKeyDefs
func (t *DXRawTable) ShouldSelectOneAuto(ctx context.Context, l *log.DXLog, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldSelectOne(ctx, l, nil, where, joinSQLPart, orderBy)
}

// GetByIdAuto returns a row by ID using table's EncryptionColumnDefs
func (t *DXRawTable) GetByIdAuto(ctx context.Context, l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetById(ctx, l, id, fieldNames...)
}

// ShouldGetByIdAuto returns a row by ID or error if not found, using table's EncryptionColumnDefs
func (t *DXRawTable) ShouldGetByIdAuto(ctx context.Context, l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetById(ctx, l, id, fieldNames...)
}

// GetByUidAuto returns a row by UID using table's EncryptionColumnDefs
func (t *DXRawTable) GetByUidAuto(ctx context.Context, l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetByUid(ctx, l, uid, fieldNames...)
}

// ShouldGetByUidAuto returns a row by UID or error if not found, using table's EncryptionColumnDefs
func (t *DXRawTable) ShouldGetByUidAuto(ctx context.Context, l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetByUid(ctx, l, uid, fieldNames...)
}

// GetByUtagAuto returns a row by Utag using table's EncryptionColumnDefs
func (t *DXRawTable) GetByUtagAuto(ctx context.Context, l *log.DXLog, utag string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetByUtag(ctx, l, utag, fieldNames...)
}

// ShouldGetByUtagAuto returns a row by Utag or error if not found, using table's EncryptionColumnDefs
func (t *DXRawTable) ShouldGetByUtagAuto(ctx context.Context, l *log.DXLog, utag string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetByUtag(ctx, l, utag, fieldNames...)
}

// GetByNameIdAuto returns a row by NameId using table's EncryptionColumnDefs
func (t *DXRawTable) GetByNameIdAuto(ctx context.Context, l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetByNameId(ctx, l, nameId, fieldNames...)
}

// ShouldGetByNameIdAuto returns a row by NameId or error if not found, using table's EncryptionColumnDefs
func (t *DXRawTable) ShouldGetByNameIdAuto(ctx context.Context, l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetByNameId(ctx, l, nameId, fieldNames...)
}

// DXTable Auto Select Methods (with audit fields)
// These methods now delegate to the base DXTable methods which add is_deleted filter and are auto-encryption-aware.

// SelectAuto selects using table's EncryptionColumnDefs
func (t *DXTable) SelectAuto(ctx context.Context, l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.Select(ctx, l, fieldNames, where, joinSQLPart, orderBy, limit, forUpdatePart)
}

// SelectOneAuto selects one row using table's EncryptionColumnDefs
func (t *DXTable) SelectOneAuto(ctx context.Context, l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.SelectOne(ctx, l, fieldNames, where, joinSQLPart, orderBy)
}

// ShouldSelectOneAuto selects one row or returns error if not found, using table's EncryptionColumnDefs
func (t *DXTable) ShouldSelectOneAuto(ctx context.Context, l *log.DXLog, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldSelectOne(ctx, l, nil, where, joinSQLPart, orderBy)
}

// GetByIdAuto returns a row by ID using table's EncryptionColumnDefs
func (t *DXTable) GetByIdAuto(ctx context.Context, l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetByIdNotDeleted(ctx, l, id, fieldNames...)
}

// ShouldGetByIdAuto returns a row by ID or error if not found, using table's EncryptionColumnDefs
func (t *DXTable) ShouldGetByIdAuto(ctx context.Context, l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetByIdNotDeleted(ctx, l, id, fieldNames...)
}

// GetByIdNotDeletedAuto returns a non-deleted row by ID using table's EncryptionColumnDefs
func (t *DXTable) GetByIdNotDeletedAuto(ctx context.Context, l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetByIdNotDeleted(ctx, l, id, fieldNames...)
}

// ShouldGetByIdNotDeletedAuto returns a non-deleted row by ID or error if not found
func (t *DXTable) ShouldGetByIdNotDeletedAuto(ctx context.Context, l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetByIdNotDeleted(ctx, l, id, fieldNames...)
}

// GetByUidAuto returns a row by UID using table's EncryptionColumnDefs
func (t *DXTable) GetByUidAuto(ctx context.Context, l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetByUidNotDeleted(ctx, l, uid, fieldNames...)
}

// ShouldGetByUidAuto returns a row by UID or error if not found, using table's EncryptionColumnDefs
func (t *DXTable) ShouldGetByUidAuto(ctx context.Context, l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetByUidNotDeleted(ctx, l, uid, fieldNames...)
}

// GetByUidNotDeletedAuto returns a non-deleted row by UID using table's EncryptionColumnDefs
func (t *DXTable) GetByUidNotDeletedAuto(ctx context.Context, l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetByUidNotDeleted(ctx, l, uid, fieldNames...)
}

// ShouldGetByUidNotDeletedAuto returns a non-deleted row by UID or error if not found
func (t *DXTable) ShouldGetByUidNotDeletedAuto(ctx context.Context, l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetByUidNotDeleted(ctx, l, uid, fieldNames...)
}

// GetByUtagAuto returns a row by Utag using table's EncryptionColumnDefs
func (t *DXTable) GetByUtagAuto(ctx context.Context, l *log.DXLog, utag string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.SelectOne(ctx, l, fn, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil)
}

// ShouldGetByUtagAuto returns a row by Utag or error if not found, using table's EncryptionColumnDefs
func (t *DXTable) ShouldGetByUtagAuto(ctx context.Context, l *log.DXLog, utag string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.ShouldSelectOne(ctx, l, fn, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil)
}

// GetByNameIdAuto returns a row by NameId using table's EncryptionColumnDefs
func (t *DXTable) GetByNameIdAuto(ctx context.Context, l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetByNameIdNotDeleted(ctx, l, nameId, fieldNames...)
}

// ShouldGetByNameIdAuto returns a row by NameId or error if not found, using table's EncryptionColumnDefs
func (t *DXTable) ShouldGetByNameIdAuto(ctx context.Context, l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetByNameIdNotDeleted(ctx, l, nameId, fieldNames...)
}

// GetByNameIdNotDeletedAuto returns a non-deleted row by NameId using table's EncryptionColumnDefs
func (t *DXTable) GetByNameIdNotDeletedAuto(ctx context.Context, l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.GetByNameIdNotDeleted(ctx, l, nameId, fieldNames...)
}

// ShouldGetByNameIdNotDeletedAuto returns a non-deleted row by NameId or error if not found
func (t *DXTable) ShouldGetByNameIdNotDeletedAuto(ctx context.Context, l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldGetByNameIdNotDeleted(ctx, l, nameId, fieldNames...)
}

