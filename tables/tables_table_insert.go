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

// Insert Operations (with audit fields)

// Insert inserts with audit fields
func (t *DXTable) Insert(l *log.DXLog, data utils.JSON, returningFieldNames []string) (sql.Result, utils.JSON, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.Insert(l, data, returningFieldNames)
}

// TxInsert inserts within a transaction with audit fields
func (t *DXTable) TxInsert(dtx *database.DXDatabaseTx, data utils.JSON, returningFieldNames []string) (sql.Result, utils.JSON, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.TxInsert(dtx, data, returningFieldNames)
}

// DoInsert is an API helper with audit fields
func (t *DXTable) DoInsert(aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
	t.SetInsertAuditFields(aepr, data)
	return t.DXRawTable.DoInsert(aepr, data)
}

// DoCreate inserts a row with audit fields and writes API response
func (t *DXTable) DoCreate(aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
	t.SetInsertAuditFields(aepr, data)
	newId, err := t.DXRawTable.InsertReturningId(&aepr.Log, data)
	if err != nil {
		return 0, err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.FieldNameForRowId: newId,
	}))
	return newId, nil
}

// RequestCreate handles create API requests (reads parameters and inserts)
func (t *DXTable) RequestCreate(aepr *api.DXAPIEndPointRequest) error {
	data := utils.JSON{}
	for k, v := range aepr.ParameterValues {
		data[k] = v.Value
	}
	_, err := t.DoCreate(aepr, data)
	return err
}

// DoCreateReturnUid inserts a row with audit fields and writes API response with uid (not id)
func (t *DXTable) DoCreateReturnUid(aepr *api.DXAPIEndPointRequest, data utils.JSON) (string, error) {
	t.SetInsertAuditFields(aepr, data)
	_, returningValues, err := t.DXRawTable.Insert(&aepr.Log, data, []string{t.FieldNameForRowUid})
	if err != nil {
		return "", err
	}
	newUid := ""
	if uid, ok := returningValues[t.FieldNameForRowUid].(string); ok {
		newUid = uid
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.FieldNameForRowUid: newUid,
	}))
	return newUid, nil
}

// RequestCreateReturnUid handles create API requests and returns uid in response
func (t *DXTable) RequestCreateReturnUid(aepr *api.DXAPIEndPointRequest) error {
	data := utils.JSON{}
	for k, v := range aepr.ParameterValues {
		data[k] = v.Value
	}
	_, err := t.DoCreateReturnUid(aepr, data)
	return err
}

// DoCreateReturnUidWithValidation inserts with unique field validation, audit fields, and returns uid
func (t *DXTable) DoCreateReturnUidWithValidation(aepr *api.DXAPIEndPointRequest, data utils.JSON) (string, error) {
	t.SetInsertAuditFields(aepr, data)

	err := t.EnsureDatabase()
	if err != nil {
		return "", err
	}

	newUid := ""
	txErr := t.Database.Tx(&aepr.Log, sql.LevelReadCommitted, func(dtx *database.DXDatabaseTx) error {
		err := t.TxCheckValidationUniqueFieldNameGroupsForInsert(dtx, data)
		if err != nil {
			return err
		}
		_, returningValues, err := t.DXRawTable.TxInsert(dtx, data, []string{t.FieldNameForRowUid})
		if err != nil {
			return err
		}
		if uid, ok := returningValues[t.FieldNameForRowUid].(string); ok {
			newUid = uid
		}
		return nil
	})
	if txErr != nil {
		return "", txErr
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.FieldNameForRowUid: newUid,
	}))
	return newUid, nil
}

// RequestCreateReturnUidWithValidation handles create API requests with unique field validation
func (t *DXTable) RequestCreateReturnUidWithValidation(aepr *api.DXAPIEndPointRequest) error {
	data := utils.JSON{}
	for k, v := range aepr.ParameterValues {
		data[k] = v.Value
	}
	_, err := t.DoCreateReturnUidWithValidation(aepr, data)
	return err
}

// DoCreateWithValidation inserts with unique field validation, audit fields, and returns id
func (t *DXTable) DoCreateWithValidation(aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
	t.SetInsertAuditFields(aepr, data)

	err := t.EnsureDatabase()
	if err != nil {
		return 0, err
	}

	var newId int64
	txErr := t.Database.Tx(&aepr.Log, sql.LevelReadCommitted, func(dtx *database.DXDatabaseTx) error {
		err := t.TxCheckValidationUniqueFieldNameGroupsForInsert(dtx, data)
		if err != nil {
			return err
		}
		_, returningValues, err := t.DXRawTable.TxInsert(dtx, data, []string{t.FieldNameForRowId})
		if err != nil {
			return err
		}
		newId, _ = utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
		return nil
	})
	if txErr != nil {
		return 0, txErr
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.FieldNameForRowId: newId,
	}))
	return newId, nil
}

// RequestCreateWithValidation handles create API requests with unique field validation (returns id)
func (t *DXTable) RequestCreateWithValidation(aepr *api.DXAPIEndPointRequest) error {
	data := utils.JSON{}
	for k, v := range aepr.ParameterValues {
		data[k] = v.Value
	}
	_, err := t.DoCreateWithValidation(aepr, data)
	return err
}
