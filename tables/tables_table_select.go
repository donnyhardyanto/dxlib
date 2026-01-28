package tables

import (
	"net/http"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/database/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
)

// Select Operations (with is_deleted = false filter)

// addNotDeletedFilter adds is_deleted=false to where condition
func (t *DXTable) addNotDeletedFilter(where utils.JSON) utils.JSON {
	if where == nil {
		where = utils.JSON{}
	}
	where["is_deleted"] = false
	return where
}

// Select returns non-deleted rows
func (t *DXTable) Select(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.DXRawTable.Select(l, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, limit, forUpdatePart)
}

// SelectOne returns a single non-deleted row
func (t *DXTable) SelectOne(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.SelectOne(l, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy)
}

// ShouldSelectOne returns a single non-deleted row or error if not found
func (t *DXTable) ShouldSelectOne(l *log.DXLog, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.ShouldSelectOne(l, t.addNotDeletedFilter(where), joinSQLPart, orderBy)
}

// GetByIdNotDeleted returns a non-deleted row by ID
func (t *DXTable) GetByIdNotDeleted(l *log.DXLog, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// ShouldGetByIdNotDeleted returns a non-deleted row by ID or error if not found
func (t *DXTable) ShouldGetByIdNotDeleted(l *log.DXLog, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// GetByUidNotDeleted returns a non-deleted row by UID
func (t *DXTable) GetByUidNotDeleted(l *log.DXLog, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// ShouldGetByUidNotDeleted returns a non-deleted row by UID or error if not found
func (t *DXTable) ShouldGetByUidNotDeleted(l *log.DXLog, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// GetByNameIdNotDeleted returns a non-deleted row by NameId
func (t *DXTable) GetByNameIdNotDeleted(l *log.DXLog, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// ShouldGetByNameIdNotDeleted returns a non-deleted row by NameId or error if not found
func (t *DXTable) ShouldGetByNameIdNotDeleted(l *log.DXLog, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// Transaction Select Operations (with is_deleted = false filter)

// TxSelect returns non-deleted rows within a transaction
func (t *DXTable) TxSelect(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.DXRawTable.TxSelect(dtx, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, limit, forUpdatePart)
}

// TxSelectOne returns a single non-deleted row within a transaction
func (t *DXTable) TxSelectOne(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxSelectOne(dtx, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, forUpdatePart)
}

// TxShouldSelectOne returns a single non-deleted row or error if not found within a transaction
func (t *DXTable) TxShouldSelectOne(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.TxShouldSelectOne(dtx, fieldNames, t.addNotDeletedFilter(where), joinSQLPart, orderBy, forUpdatePart)
}

// TxGetByIdNotDeleted returns a non-deleted row by ID within a transaction
func (t *DXTable) TxGetByIdNotDeleted(dtx *database.DXDatabaseTx, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxShouldGetByIdNotDeleted returns a non-deleted row by ID or error if not found within a transaction
func (t *DXTable) TxShouldGetByIdNotDeleted(dtx *database.DXDatabaseTx, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxShouldSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxShouldGetByNameIdNotDeleted returns a non-deleted row by NameId within a transaction or error if not found
func (t *DXTable) TxShouldGetByNameIdNotDeleted(dtx *database.DXDatabaseTx, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.TxShouldSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// TxShouldGetByNameId is an alias for TxShouldGetByNameIdNotDeleted
func (t *DXTable) TxShouldGetByNameId(dtx *database.DXDatabaseTx, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxShouldGetByNameIdNotDeleted(dtx, nameId)
}

// Count Operations (with is_deleted = false filter)

// Count returns total non-deleted row count
func (t *DXTable) Count(l *log.DXLog, where utils.JSON, joinSQLPart any) (int64, error) {
	return t.DXRawTable.Count(l, t.addNotDeletedFilter(where), joinSQLPart)
}

// SelectAll returns all non-deleted rows from the table
func (t *DXTable) SelectAll(l *log.DXLog) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.Select(l, nil, nil, nil, nil, nil, nil)
}

// API Select Helpers

// RequestRead handles read by ID API requests (with is_deleted filter)
func (t *DXTable) RequestRead(aepr *api.DXAPIEndPointRequest) error {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	_, row, err := t.GetByIdNotDeleted(&aepr.Log, id)
	if err != nil {
		return err
	}
	if row == nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusNotFound, "", "RECORD_NOT_FOUND:%d", id)
	}

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.ResultObjectName: row,
	})
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
}

// RequestReadByUid handles read by UID API requests (with is_deleted filter)
func (t *DXTable) RequestReadByUid(aepr *api.DXAPIEndPointRequest) error {
	_, uid, err := aepr.GetParameterValueAsString(t.FieldNameForRowUid)
	if err != nil {
		return err
	}

	_, row, err := t.GetByUidNotDeleted(&aepr.Log, uid)
	if err != nil {
		return err
	}
	if row == nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusNotFound, "", "RECORD_NOT_FOUND:%s", uid)
	}

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.ResultObjectName: row,
	})
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
}

// RequestReadByNameId handles read by NameId API requests (with is_deleted filter)
func (t *DXTable) RequestReadByNameId(aepr *api.DXAPIEndPointRequest) error {
	_, nameId, err := aepr.GetParameterValueAsString(t.FieldNameForRowNameId)
	if err != nil {
		return err
	}

	_, row, err := t.GetByNameIdNotDeleted(&aepr.Log, nameId)
	if err != nil {
		return err
	}
	if row == nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusNotFound, "", "RECORD_NOT_FOUND:%s", nameId)
	}

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.ResultObjectName: row,
	})
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
}
