package tables

import (
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/database/db"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DXTable Transaction Direct Select Methods — query the base TABLE within a transaction,
// bypassing encryption, with is_deleted=false filter applied.

// TxDirectSelect returns multiple non-deleted rows from the base table within a transaction
func (t *DXTable) TxDirectSelect(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.DXRawTable.TxDirectSelect(dtx, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, limit, forUpdatePart)
}

// TxDirectSelectOne returns a single non-deleted row from the base table within a transaction
func (t *DXTable) TxDirectSelectOne(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxDirectSelectOne(dtx, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, forUpdatePart)
}

// TxDirectShouldSelectOne returns a single non-deleted row from the base table or error if not found within a transaction
func (t *DXTable) TxDirectShouldSelectOne(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxDirectShouldSelectOne(dtx, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, forUpdatePart)
}

// TxDirectGetById returns a non-deleted row by ID from the base table within a transaction
func (t *DXTable) TxDirectGetById(dtx *database.DXDatabaseTx, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxDirectSelectOne(dtx, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxDirectShouldGetById returns a non-deleted row by ID from the base table or error if not found within a transaction
func (t *DXTable) TxDirectShouldGetById(dtx *database.DXDatabaseTx, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxDirectShouldSelectOne(dtx, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxDirectGetByUid returns a non-deleted row by UID from the base table within a transaction
func (t *DXTable) TxDirectGetByUid(dtx *database.DXDatabaseTx, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxDirectSelectOne(dtx, fn, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxDirectShouldGetByUid returns a non-deleted row by UID from the base table or error if not found within a transaction
func (t *DXTable) TxDirectShouldGetByUid(dtx *database.DXDatabaseTx, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxDirectShouldSelectOne(dtx, fn, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxDirectGetByNameId returns a non-deleted row by NameId from the base table within a transaction
func (t *DXTable) TxDirectGetByNameId(dtx *database.DXDatabaseTx, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxDirectSelectOne(dtx, fn, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// TxDirectShouldGetByNameId returns a non-deleted row by NameId from the base table or error if not found within a transaction
func (t *DXTable) TxDirectShouldGetByNameId(dtx *database.DXDatabaseTx, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxDirectShouldSelectOne(dtx, fn, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}
