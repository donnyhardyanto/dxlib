package tables

import (
	"database/sql"
	"net/http"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
)

// Update Operations (with audit fields)

// Update updates with audit fields
func (t *DXTable) Update(l *log.DXLog, data utils.JSON, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.Update(l, data, where, returningFieldNames)
}

// TxUpdate updates within a transaction with audit fields
func (t *DXTable) TxUpdate(dtx *databases.DXDatabaseTx, data utils.JSON, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.TxUpdate(dtx, data, where, returningFieldNames)
}

// UpdateById updates with audit fields
func (t *DXTable) UpdateById(l *log.DXLog, id int64, data utils.JSON) (sql.Result, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.UpdateById(l, id, data)
}

// TxUpdateById updates within a transaction with audit fields
func (t *DXTable) TxUpdateById(dtx *databases.DXDatabaseTx, id int64, data utils.JSON) (sql.Result, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.TxUpdateById(dtx, id, data)
}

// DoUpdate is an API helper with audit fields
func (t *DXTable) DoUpdate(aepr *api.DXAPIEndPointRequest, id int64, data utils.JSON) error {
	_, row, err := t.ShouldGetByIdNotDeletedAuto(&aepr.Log, id)
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

	t.SetUpdateAuditFields(aepr, data)

	_, err = t.DXRawTable.UpdateById(&aepr.Log, id, data)
	if err != nil {
		return err
	}

	// Re-fetch and return the updated row
	_, updatedRow, err := t.ShouldGetByIdNotDeletedAuto(&aepr.Log, id)
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
func (t *DXTable) DoEdit(aepr *api.DXAPIEndPointRequest, id int64, data utils.JSON) error {
	return t.DoUpdate(aepr, id, data)
}

// RequestEdit handles edit by ID API requests (with is_deleted filter and audit fields)
func (t *DXTable) RequestEdit(aepr *api.DXAPIEndPointRequest) error {
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

// RequestEditByUid handles edit by UID API requests
func (t *DXTable) RequestEditByUid(aepr *api.DXAPIEndPointRequest) error {
	_, uid, err := aepr.GetParameterValueAsString(t.FieldNameForRowUid)
	if err != nil {
		return err
	}

	_, row, err := t.ShouldGetByUidNotDeletedAuto(&aepr.Log, uid)
	if err != nil {
		return err
	}

	id, ok := row[t.FieldNameForRowId].(int64)
	if !ok {
		return aepr.WriteResponseAndNewErrorf(http.StatusInternalServerError, "", "CANNOT_GET_ID_FROM_ROW")
	}

	_, data, err := aepr.GetParameterValueAsJSON("new")
	if err != nil {
		return err
	}

	return t.DoEdit(aepr, id, data)
}

// DoUpdateWithValidation updates with unique field validation, merging current row data with new data for validation
func (t *DXTable) DoUpdateWithValidation(aepr *api.DXAPIEndPointRequest, id int64, data utils.JSON) error {
	_, row, err := t.DirectShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}
	if isDeleted, ok := row["is_deleted"].(bool); ok && isDeleted {
		return aepr.WriteResponseAndNewErrorf(http.StatusNotFound, "", "RECORD_NOT_FOUND:%d", id)
	}

	for k, v := range data {
		if v == nil {
			delete(data, k)
		}
	}

	t.SetUpdateAuditFields(aepr, data)

	err = t.EnsureDatabase()
	if err != nil {
		return err
	}

	txErr := t.Database.Tx(&aepr.Log, sql.LevelReadCommitted, func(dtx *databases.DXDatabaseTx) error {
		// Merge the current row with new data for validation
		mergedData := utils.JSON{}
		for k, v := range row {
			mergedData[k] = v
		}
		for k, v := range data {
			mergedData[k] = v
		}

		err := t.TxCheckValidationUniqueFieldNameGroupsForUpdate(dtx, id, mergedData)
		if err != nil {
			return err
		}

		_, err = t.DXRawTable.TxUpdateById(dtx, id, data)
		return err
	})
	if txErr != nil {
		return txErr
	}

	// Re-fetch and return the updated row
	_, updatedRow, err := t.DirectShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.ResultObjectName: updatedRow,
	})
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)
	return nil
}

// RequestEditWithValidation handles edit by ID API requests with unique field validation
func (t *DXTable) RequestEditWithValidation(aepr *api.DXAPIEndPointRequest) error {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	_, newKeyValues, err := aepr.GetParameterValueAsJSON("new")
	if err != nil {
		return err
	}

	for k, v := range newKeyValues {
		if v == nil {
			delete(newKeyValues, k)
		}
	}

	return t.DoUpdateWithValidation(aepr, id, newKeyValues)
}

// RequestEditByUidWithValidation handles edit by UID API requests with unique field validation
func (t *DXTable) RequestEditByUidWithValidation(aepr *api.DXAPIEndPointRequest) error {
	_, uid, err := aepr.GetParameterValueAsString(t.FieldNameForRowUid)
	if err != nil {
		return err
	}

	// Use Direct to just get the id
	_, row, err := t.DirectShouldGetByUid(&aepr.Log, uid)
	if err != nil {
		return err
	}

	id, ok := row[t.FieldNameForRowId].(int64)
	if !ok {
		return aepr.WriteResponseAndNewErrorf(http.StatusInternalServerError, "", "CANNOT_GET_ID_FROM_ROW")
	}

	_, data, err := aepr.GetParameterValueAsJSON("new")
	if err != nil {
		return err
	}

	return t.DoUpdateWithValidation(aepr, id, data)
}
