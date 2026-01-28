package tables

import (
	"database/sql"
	"net/http"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
)

// Update updates rows matching where condition
func (t *DXRawTable) Update(l *log.DXLog, data utils.JSON, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.Update(t.TableName(), data, where, returningFieldNames)
}

// TxUpdate updates within a transaction
func (t *DXRawTable) TxUpdate(dtx *database.DXDatabaseTx, data utils.JSON, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	return dtx.Update(t.TableName(), data, where, returningFieldNames)
}

// UpdateSimple is a simplified update that just takes data and where (backward compatible)
func (t *DXRawTable) UpdateSimple(data utils.JSON, where utils.JSON) (sql.Result, error) {
	result, _, err := t.Update(nil, data, where, nil)
	return result, err
}

// TxUpdateSimple is a simplified transaction update (backward compatible)
func (t *DXRawTable) TxUpdateSimple(dtx *database.DXDatabaseTx, data utils.JSON, where utils.JSON) (sql.Result, error) {
	result, _, err := t.TxUpdate(dtx, data, where, nil)
	return result, err
}

// UpdateById updates a single row by ID
func (t *DXRawTable) UpdateById(l *log.DXLog, id int64, data utils.JSON) (sql.Result, error) {
	result, _, err := t.Update(l, data, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// TxUpdateById updates a single row by ID within a transaction
func (t *DXRawTable) TxUpdateById(dtx *database.DXDatabaseTx, id int64, data utils.JSON) (sql.Result, error) {
	result, _, err := t.TxUpdate(dtx, data, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// DoUpdate is an API helper that updates and writes response
func (t *DXRawTable) DoUpdate(aepr *api.DXAPIEndPointRequest, id int64, data utils.JSON) error {
	_, row, err := t.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}
	if row == nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusNotFound, "", "RECORD_NOT_FOUND:%d", id)
	}

	for k, v := range data {
		if v == nil {
			delete(data, k)
		}
	}

	_, err = t.UpdateById(&aepr.Log, id, data)
	if err != nil {
		return err
	}

	// Re-fetch and return updated row
	_, updatedRow, err := t.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.ResultObjectName: updatedRow,
	})
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
}

// DoEdit is an alias for DoUpdate (backward compatibility)
func (t *DXRawTable) DoEdit(aepr *api.DXAPIEndPointRequest, id int64, data utils.JSON) error {
	return t.DoUpdate(aepr, id, data)
}

// RequestEdit handles edit by ID API requests
func (t *DXRawTable) RequestEdit(aepr *api.DXAPIEndPointRequest) error {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	_, newKeyValues, err := aepr.GetParameterValueAsJSON("new")
	if err != nil {
		return err
	}

	// Remove nil values
	for k, v := range newKeyValues {
		if v == nil {
			delete(newKeyValues, k)
		}
	}

	return t.DoUpdate(aepr, id, newKeyValues)
}
