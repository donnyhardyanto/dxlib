package tables

import (
	"context"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

// Direct Select Methods — query the base TABLE directly (not the view).
// These bypass all encryption handling: no transaction, no session key, no pgp_sym_decrypt.
// Use these when the caller only needs non-encrypted columns.

// DirectSelect returns multiple rows from the base table
func (t *DXRawTable) DirectSelect(ctx context.Context, l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.Select(ctx, t.GetFullTableName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, limit, nil, forUpdatePart)
}

// DirectSelectOne returns a single row from the base table
func (t *DXRawTable) DirectSelectOne(ctx context.Context, l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.SelectOne(ctx, t.GetFullTableName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, nil, nil)
}

// DirectShouldSelectOne returns a single row from the base table or error if not found
func (t *DXRawTable) DirectShouldSelectOne(ctx context.Context, l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.ShouldSelectOne(ctx, t.GetFullTableName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, nil, nil)
}

// DirectGetById returns a row by ID from the base table
func (t *DXRawTable) DirectGetById(ctx context.Context, l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectSelectOne(ctx, l, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// DirectShouldGetById returns a row by ID from the base table or error if not found
func (t *DXRawTable) GetUidById(ctx context.Context, l *log.DXLog, id int64) (string, error) {
	_, row, err := t.DirectShouldSelectOne(ctx, l, []string{"uid"}, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
	if err != nil {
		return "", err
	}
	uid, err := utils.GetStringFromKV(row, "uid")
	if err != nil {
		return "", err
	}
	return uid, nil
}

// DirectShouldGetById returns a row by ID from the base table or error if not found
func (t *DXRawTable) DirectShouldGetById(ctx context.Context, l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectShouldSelectOne(ctx, l, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// DirectGetByUid returns a row by UID from the base table
func (t *DXRawTable) DirectGetByUid(ctx context.Context, l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectSelectOne(ctx, l, fn, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// DirectShouldGetByUid returns a row by UID from the base table or error if not found
func (t *DXRawTable) DirectShouldGetByUid(ctx context.Context, l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectShouldSelectOne(ctx, l, fn, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// DirectGetByNameId returns a row by NameId from the base table
func (t *DXRawTable) DirectGetByNameId(ctx context.Context, l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectSelectOne(ctx, l, fn, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// DirectShouldGetByNameId returns a row by NameId from the base table or error if not found
func (t *DXRawTable) DirectShouldGetByNameId(ctx context.Context, l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectShouldSelectOne(ctx, l, fn, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// DirectCount returns row count from the base table
func (t *DXRawTable) DirectCount(ctx context.Context, l *log.DXLog, where utils.JSON, joinSQLPart any) (int64, error) {
	if err := t.EnsureDatabase(); err != nil {
		return 0, err
	}
	return t.Database.Count(ctx, t.GetFullTableName(), where, joinSQLPart)
}
