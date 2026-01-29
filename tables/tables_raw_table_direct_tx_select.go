package tables

import (
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
)

// Transaction Direct Select Methods — query the base TABLE within a transaction,
// bypassing all encryption handling. No session key, no pgp_sym_decrypt.
// Use these when the caller only needs non-encrypted columns inside a transaction.

// TxDirectSelect returns multiple rows from the base table within a transaction
func (t *DXRawTable) TxDirectSelect(dtx *databases.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return dtx.Select(t.TableName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, limit, nil, forUpdatePart)
}

// TxDirectSelectOne returns a single row from the base table within a transaction
func (t *DXRawTable) TxDirectSelectOne(dtx *databases.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return dtx.SelectOne(t.TableName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, nil, forUpdatePart)
}

// TxDirectShouldSelectOne returns a single row from the base table or error if not found within a transaction
func (t *DXRawTable) TxDirectShouldSelectOne(dtx *databases.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return dtx.ShouldSelectOne(t.TableName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, nil, forUpdatePart)
}

// TxDirectGetById returns a row by ID from the base table within a transaction
func (t *DXRawTable) TxDirectGetById(dtx *databases.DXDatabaseTx, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxDirectSelectOne(dtx, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxDirectShouldGetById returns a row by ID from the base table or error if not found within a transaction
func (t *DXRawTable) TxDirectShouldGetById(dtx *databases.DXDatabaseTx, id int64, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxDirectShouldSelectOne(dtx, fn, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxDirectGetByUid returns a row by UID from the base table within a transaction
func (t *DXRawTable) TxDirectGetByUid(dtx *databases.DXDatabaseTx, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxDirectSelectOne(dtx, fn, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxDirectShouldGetByUid returns a row by UID from the base table or error if not found within a transaction
func (t *DXRawTable) TxDirectShouldGetByUid(dtx *databases.DXDatabaseTx, uid string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxDirectShouldSelectOne(dtx, fn, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxDirectGetByNameId returns a row by NameId from the base table within a transaction
func (t *DXRawTable) TxDirectGetByNameId(dtx *databases.DXDatabaseTx, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxDirectSelectOne(dtx, fn, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// TxDirectShouldGetByNameId returns a row by NameId from the base table or error if not found within a transaction
func (t *DXRawTable) TxDirectShouldGetByNameId(dtx *databases.DXDatabaseTx, nameId string, fieldNames ...string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	var fn []string
	if len(fieldNames) > 0 {
		fn = fieldNames
	}
	return t.TxDirectShouldSelectOne(dtx, fn, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}
