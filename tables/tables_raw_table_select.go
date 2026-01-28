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

// Select returns multiple rows matching where condition
func (t *DXRawTable) Select(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.Select(t.GetListViewName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, limit, nil, forUpdatePart)
}

// SelectOne returns a single row matching where condition
func (t *DXRawTable) SelectOne(l *log.DXLog, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.SelectOne(t.GetListViewName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, nil, nil)
}

// ShouldSelectOne returns a single row or error if not found
func (t *DXRawTable) ShouldSelectOne(l *log.DXLog, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.ShouldSelectOne(t.GetListViewName(), t.FieldTypeMapping, nil, where, joinSQLPart, nil, nil, orderBy, nil, nil)
}

// GetById returns a row by ID
func (t *DXRawTable) GetById(l *log.DXLog, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// ShouldGetById returns a row by ID or error if not found
func (t *DXRawTable) ShouldGetById(l *log.DXLog, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// GetByUid returns a row by UID
func (t *DXRawTable) GetByUid(l *log.DXLog, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// ShouldGetByUid returns a row by UID or error if not found
func (t *DXRawTable) ShouldGetByUid(l *log.DXLog, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// GetByUtag returns a row by Utag
func (t *DXRawTable) GetByUtag(l *log.DXLog, utag string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil)
}

// ShouldGetByUtag returns a row by Utag or error if not found
func (t *DXRawTable) ShouldGetByUtag(l *log.DXLog, utag string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil)
}

// GetByNameId returns a row by NameId
func (t *DXRawTable) GetByNameId(l *log.DXLog, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.SelectOne(l, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// ShouldGetByNameId returns a row by NameId or error if not found
func (t *DXRawTable) ShouldGetByNameId(l *log.DXLog, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.ShouldSelectOne(l, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// Transaction Select Operations

// TxSelect returns multiple rows within a transaction
func (t *DXRawTable) TxSelect(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return dtx.Select(t.GetListViewName(), t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, limit, nil, forUpdatePart)
}

// TxSelectOne returns a single row within a transaction
func (t *DXRawTable) TxSelectOne(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	// Use table name instead of view when FOR UPDATE is requested (views with outer joins don't support FOR UPDATE)
	tableName := t.GetListViewName()
	if forUpdatePart != nil && forUpdatePart != false && forUpdatePart != "" {
		tableName = t.TableName()
	}
	return dtx.SelectOne(tableName, t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, nil, forUpdatePart)
}

// TxShouldSelectOne returns a single row or error if not found within a transaction
func (t *DXRawTable) TxShouldSelectOne(dtx *database.DXDatabaseTx, fieldNames []string, where utils.JSON, joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	// Use table name instead of view when FOR UPDATE is requested (views with outer joins don't support FOR UPDATE)
	tableName := t.GetListViewName()
	if forUpdatePart != nil && forUpdatePart != false && forUpdatePart != "" {
		tableName = t.TableName()
	}
	return dtx.ShouldSelectOne(tableName, t.FieldTypeMapping, fieldNames, where, joinSQLPart, nil, nil, orderBy, nil, forUpdatePart)
}

// TxGetById returns a row by ID within a transaction
func (t *DXRawTable) TxGetById(dtx *database.DXDatabaseTx, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxShouldGetById returns a row by ID or error if not found within a transaction
func (t *DXRawTable) TxShouldGetById(dtx *database.DXDatabaseTx, id int64) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.TxShouldSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowId: id}, nil, nil, nil)
}

// TxGetByUid returns a row by UID within a transaction
func (t *DXRawTable) TxGetByUid(dtx *database.DXDatabaseTx, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.TxSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxShouldGetByUid returns a row by UID or error if not found within a transaction
func (t *DXRawTable) TxShouldGetByUid(dtx *database.DXDatabaseTx, uid string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.TxShouldSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil, nil)
}

// TxGetByUtag returns a row by Utag within a transaction
func (t *DXRawTable) TxGetByUtag(dtx *database.DXDatabaseTx, utag string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.TxSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil, nil)
}

// TxShouldGetByUtag returns a row by Utag or error if not found within a transaction
func (t *DXRawTable) TxShouldGetByUtag(dtx *database.DXDatabaseTx, utag string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.TxShouldSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil, nil)
}

// TxGetByNameId returns a row by NameId within a transaction
func (t *DXRawTable) TxGetByNameId(dtx *database.DXDatabaseTx, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.TxSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// TxShouldGetByNameId returns a row by NameId within a transaction or error if not found
func (t *DXRawTable) TxShouldGetByNameId(dtx *database.DXDatabaseTx, nameId string) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.TxShouldSelectOne(dtx, nil, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil, nil)
}

// Count Operations

// Count returns total row count
func (t *DXRawTable) Count(l *log.DXLog, where utils.JSON, joinSQLPart any) (int64, error) {
	if err := t.EnsureDatabase(); err != nil {
		return 0, err
	}
	return t.Database.Count(t.GetListViewName(), where, joinSQLPart)
}

// SelectAll returns all rows from the table
func (t *DXRawTable) SelectAll(l *log.DXLog) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.Select(l, nil, nil, nil, nil, nil, nil)
}

// API Select Helpers

// RequestRead handles read by ID API requests
func (t *DXRawTable) RequestRead(aepr *api.DXAPIEndPointRequest) error {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	_, row, err := t.GetById(&aepr.Log, id)
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

// RequestReadByUid handles read by UID API requests
func (t *DXRawTable) RequestReadByUid(aepr *api.DXAPIEndPointRequest) error {
	_, uid, err := aepr.GetParameterValueAsString(t.FieldNameForRowUid)
	if err != nil {
		return err
	}

	_, row, err := t.GetByUid(&aepr.Log, uid)
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

// RequestReadByUtag handles read by Utag API requests
func (t *DXRawTable) RequestReadByUtag(aepr *api.DXAPIEndPointRequest) error {
	_, utag, err := aepr.GetParameterValueAsString("utag")
	if err != nil {
		return err
	}

	rowsInfo, row, err := t.ShouldGetByUtag(&aepr.Log, utag)
	if err != nil {
		return err
	}

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.ResultObjectName: row,
		"rows_info":        rowsInfo,
	})
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
}

// RequestReadByNameId handles read by NameId API requests
func (t *DXRawTable) RequestReadByNameId(aepr *api.DXAPIEndPointRequest) error {
	_, nameId, err := aepr.GetParameterValueAsString(t.FieldNameForRowNameId)
	if err != nil {
		return err
	}

	_, row, err := t.GetByNameId(&aepr.Log, nameId)
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
