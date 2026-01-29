package tables

import (
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DXTable Direct Select Methods — query the base TABLE directly (not the view),
// bypassing encryption, with is_deleted=false filter applied.

// DirectSelect returns multiple non-deleted rows from the base table
func (t *DXTable) DirectSelect(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.DXRawTable.DirectSelect(l, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, limit, forUpdatePart)
}

// DirectSelectOne returns a single non-deleted row from the base table
func (t *DXTable) DirectSelectOne(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.DirectSelectOne(l, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy)
}

// DirectShouldSelectOne returns a single non-deleted row from the base table or error if not found
func (t *DXTable) DirectShouldSelectOne(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.DirectShouldSelectOne(l, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy)
}

// DirectGetById returns a non-deleted row by ID from the base table
func (t *DXTable) DirectGetById(l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectSelectOne(l, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// DirectShouldGetById returns a non-deleted row by ID from the base table or error if not found
func (t *DXTable) DirectShouldGetById(l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectShouldSelectOne(l, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// DirectGetByUid returns a non-deleted row by UID from the base table
func (t *DXTable) DirectGetByUid(l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectSelectOne(l, fn, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// DirectShouldGetByUid returns a non-deleted row by UID from the base table or error if not found
func (t *DXTable) DirectShouldGetByUid(l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectShouldSelectOne(l, fn, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// DirectGetByNameId returns a non-deleted row by NameId from the base table
func (t *DXTable) DirectGetByNameId(l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectSelectOne(l, fn, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// DirectShouldGetByNameId returns a non-deleted row by NameId from the base table or error if not found
func (t *DXTable) DirectShouldGetByNameId(l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectShouldSelectOne(l, fn, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// DirectCount returns non-deleted row count from the base table
func (t *DXTable) DirectCount(l *log.DXLog, where utils.JSON, joinSQLPart any) (int64, error) {
	return t.DXRawTable.DirectCount(l, t.addNotDeletedFilter(where), joinSQLPart)
}

// DirectPaging returns paginated non-deleted rows from the base table
func (t *DXTable) DirectPaging(l *log.DXLog, rowPerPage, pageIndex int64, whereClause, orderBy string, args utils.JSON) (*PagingResult, error) {
	return t.DXRawTable.DirectPaging(l, rowPerPage, pageIndex, t.addNotDeletedToWhere(whereClause), orderBy, args)
}
