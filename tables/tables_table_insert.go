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
