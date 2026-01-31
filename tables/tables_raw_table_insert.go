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

// Insert inserts a new row and returns result
func (t *DXRawTable) Insert(l *log.DXLog, data utils.JSON, returningFieldNames []string) (sql.Result, utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.Insert(t.TableName(), data, returningFieldNames)
}

// TxInsert inserts within a transaction
func (t *DXRawTable) TxInsert(dtx *databases.DXDatabaseTx, data utils.JSON, returningFieldNames []string) (sql.Result, utils.JSON, error) {
	return dtx.Insert(t.TableName(), data, returningFieldNames)
}

// InsertReturningId is a simplified insert that returns just the new ID (backward compatible)
func (t *DXRawTable) InsertReturningId(l *log.DXLog, data utils.JSON) (int64, error) {
	_, returningValues, err := t.Insert(l, data, []string{t.FieldNameForRowId})
	if err != nil {
		return 0, err
	}
	newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
	return newId, nil
}

// TxInsertReturningId is a simplified TxInsert that returns just the new ID (backward compatible)
func (t *DXRawTable) TxInsertReturningId(dtx *databases.DXDatabaseTx, data utils.JSON) (int64, error) {
	_, returningValues, err := t.TxInsert(dtx, data, []string{t.FieldNameForRowId})
	if err != nil {
		return 0, err
	}
	newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
	return newId, nil
}

// DoInsert is an API helper that inserts and writes response
func (t *DXRawTable) DoInsert(aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
	if err := t.EnsureDatabase(); err != nil {
		return 0, err
	}

	returningFields := []string{t.FieldNameForRowId}
	if t.FieldNameForRowUid != "" {
		returningFields = append(returningFields, t.FieldNameForRowUid)
	}

	_, returningValues, err := t.Database.Insert(t.TableName(), data, returningFields)
	if err != nil {
		return 0, err
	}

	newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)

	response := utils.JSON{}

	if t.FieldNameForRowUid != "" {
		if uid, ok := returningValues[t.FieldNameForRowUid].(string); ok {
			response[t.FieldNameForRowUid] = uid
		}
	}

	responseData := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, response)
	aepr.WriteResponseAsJSON(http.StatusOK, nil, responseData)

	return newId, nil
}

// DoCreate inserts a row and writes API response (suppresses errors)
func (t *DXRawTable) DoCreate(aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
	returningFields := []string{t.FieldNameForRowId}
	if t.FieldNameForRowUid != "" {
		returningFields = append(returningFields, t.FieldNameForRowUid)
	}

	_, returningValues, err := t.Insert(&aepr.Log, data, returningFields)
	if err != nil {
		aepr.WriteResponseAsError(http.StatusConflict, err)
		return 0, nil
	}

	newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)

	response := utils.JSON{}

	if t.FieldNameForRowUid != "" {
		if uid, ok := returningValues[t.FieldNameForRowUid].(string); ok {
			response[t.FieldNameForRowUid] = uid
		}
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, response))
	return newId, nil
}

// RequestCreate handles create API requests (alias for backward compatibility)
func (t *DXRawTable) RequestCreate(aepr *api.DXAPIEndPointRequest) error {
	data := utils.JSON{}
	for k, v := range aepr.ParameterValues {
		data[k] = v.Value
	}
	_, err := t.DoCreate(aepr, data)
	return err
}

// DoCreateWithValidation inserts with unique field validation and writes API response
func (t *DXRawTable) DoCreateWithValidation(aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
	err := t.EnsureDatabase()
	if err != nil {
		return 0, err
	}

	returningFields := []string{t.FieldNameForRowId}
	if t.FieldNameForRowUid != "" {
		returningFields = append(returningFields, t.FieldNameForRowUid)
	}

	var newId int64
	var response utils.JSON
	txErr := t.Database.Tx(&aepr.Log, sql.LevelReadCommitted, func(dtx *databases.DXDatabaseTx) error {
		err := t.TxCheckValidationUniqueFieldNameGroupsForInsert(dtx, data)
		if err != nil {
			return err
		}
		_, returningValues, err := t.TxInsert(dtx, data, returningFields)
		if err != nil {
			return err
		}
		newId, _ = utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
		response = utils.JSON{}
		if t.FieldNameForRowUid != "" {
			if uid, ok := returningValues[t.FieldNameForRowUid].(string); ok {
				response[t.FieldNameForRowUid] = uid
			}
		}
		return nil
	})
	if txErr != nil {
		return 0, txErr
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, response))
	return newId, nil
}

// RequestCreateWithValidation handles create API requests with unique field validation
func (t *DXRawTable) RequestCreateWithValidation(aepr *api.DXAPIEndPointRequest) error {
	data := utils.JSON{}
	for k, v := range aepr.ParameterValues {
		data[k] = v.Value
	}
	_, err := t.DoCreateWithValidation(aepr, data)
	return err
}
