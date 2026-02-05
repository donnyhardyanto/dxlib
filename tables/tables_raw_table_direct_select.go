package tables

import (
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

// Direct Select Methods — query the base TABLE directly (not the view).
// These bypass all encryption handling: no transaction, no session key, no pgp_sym_decrypt.
// Use these when the caller only needs non-encrypted columns.

// DirectSelect returns multiple rows from the base table
func (t *DXRawTable) DirectSelect(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.Select(t.GetFullTableName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, limit, nil, forUpdatePart)
}

// DirectSelectOne returns a single row from the base table
func (t *DXRawTable) DirectSelectOne(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.SelectOne(t.GetFullTableName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, nil, nil)
}

// DirectShouldSelectOne returns a single row from the base table or error if not found
func (t *DXRawTable) DirectShouldSelectOne(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.ShouldSelectOne(t.GetFullTableName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, nil, nil)
}

// DirectGetById returns a row by ID from the base table
func (t *DXRawTable) DirectGetById(l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectSelectOne(l, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// DirectShouldGetById returns a row by ID from the base table or error if not found
func (t *DXRawTable) GetUidById(l *log.DXLog, id int64) (string, error) {
	_, row, err := t.DirectShouldSelectOne(l, []string{"uid"}, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
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
func (t *DXRawTable) DirectShouldGetById(l *log.DXLog, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectShouldSelectOne(l, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// DirectGetByUid returns a row by UID from the base table
func (t *DXRawTable) DirectGetByUid(l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectSelectOne(l, fn, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// DirectShouldGetByUid returns a row by UID from the base table or error if not found
func (t *DXRawTable) DirectShouldGetByUid(l *log.DXLog, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectShouldSelectOne(l, fn, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// DirectGetByNameId returns a row by NameId from the base table
func (t *DXRawTable) DirectGetByNameId(l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectSelectOne(l, fn, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// DirectShouldGetByNameId returns a row by NameId from the base table or error if not found
func (t *DXRawTable) DirectShouldGetByNameId(l *log.DXLog, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.DirectShouldSelectOne(l, fn, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// DirectCount returns row count from the base table
func (t *DXRawTable) DirectCount(l *log.DXLog, where utils.JSON, joinSQLPart any) (int64, error) {
	if err := t.EnsureDatabase(); err != nil {
		return 0, err
	}
	return t.Database.Count(t.GetFullTableName(), where, joinSQLPart)
}
