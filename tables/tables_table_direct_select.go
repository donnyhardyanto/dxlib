package tables

import (
	"context"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DXTable Direct Select Methods — query the base TABLE directly (not the view),
// bypassing encryption, with is_deleted=false filter applied.

// DirectSelect returns multiple non-deleted rows from the base table
func (t *DXTable) DirectSelect(ctx context.Context, l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.DXRawTable.DirectSelect(ctx, l, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, limit, forUpdatePart)
}

// DirectSelectOne returns a single non-deleted row from the base table
func (t *DXTable) DirectSelectOne(ctx context.Context, l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.DirectSelectOne(ctx, l, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy)
}

// DirectShouldSelectOne returns a single non-deleted row from the base table or error if not found
func (t *DXTable) DirectShouldSelectOne(ctx context.Context, l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.DirectShouldSelectOne(ctx, l, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy)
}

// DirectGetById returns a non-deleted row by ID from the base table
func (t *DXTable) DirectGetById(ctx context.Context, l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectSelectOne(ctx, l, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// DirectShouldGetById returns a non-deleted row by ID from the base table or error if not found
func (t *DXTable) DirectShouldGetById(ctx context.Context, l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectShouldSelectOne(ctx, l, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// DirectGetByUid returns a non-deleted row by UID from the base table
func (t *DXTable) DirectGetByUid(ctx context.Context, l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectSelectOne(ctx, l, fn, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// DirectShouldGetByUid returns a non-deleted row by UID from the base table or error if not found
func (t *DXTable) DirectShouldGetByUid(ctx context.Context, l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectShouldSelectOne(ctx, l, fn, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// DirectGetByNameId returns a non-deleted row by NameId from the base table
func (t *DXTable) DirectGetByNameId(ctx context.Context, l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectSelectOne(ctx, l, fn, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// DirectShouldGetByNameId returns a non-deleted row by NameId from the base table or error if not found
func (t *DXTable) DirectShouldGetByNameId(ctx context.Context, l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectShouldSelectOne(ctx, l, fn, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// DirectCount returns non-deleted row count from the base table
func (t *DXTable) DirectCount(ctx context.Context, l *log.DXLog, where utils.JSON, joinSQLPart any) (int64, error) {
	return t.DXRawTable.DirectCount(ctx, l, t.addNotDeletedFilter(where), joinSQLPart)
}
