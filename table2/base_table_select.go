package table

import (
	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/database/protected/db"
	"github.com/donnyhardyanto/dxlib/database2/database_type"
	utils2 "github.com/donnyhardyanto/dxlib/database2/db/utils"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
	"net/http"
)

func (bt *DXBaseTable) Select(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderByFieldNameDirections utils2.FieldsOrderBy, limit any, offset any, forUpdatePart any) (rowsInfo *database_type.RowsInfo, r []utils.JSON, err error) {

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return nil, nil, err
	}

	rowsInfo, r, err = bt.Database.Select(bt.ListViewNameId, bt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, orderByFieldNameDirections, limit, offset, forUpdatePart)
	if err != nil {
		return rowsInfo, nil, err
	}

	return rowsInfo, r, err
}

func (bt *DXBaseTable) ShouldSelectOne(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderByFieldNameDirections utils2.FieldsOrderBy, offset any, forUpdate any) (rowsInfo *database_type.RowsInfo, r utils.JSON, err error) {

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return nil, nil, err
	}

	return bt.Database.ShouldSelectOne(bt.ListViewNameId, bt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, orderByFieldNameDirections, offset, forUpdate)
}

func (bt *DXBaseTable) SelectOne(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any, orderByFieldNameDirections utils2.FieldsOrderBy, offset any, forUpdate any) (
	rowsInfo *database_type.RowsInfo, r utils.JSON, err error) {

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return nil, nil, err
	}

	return bt.Database.SelectOne(bt.ListViewNameId, bt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, orderByFieldNameDirections, offset, forUpdate)
}

func (bt *DXBaseTable) RequestRead(aepr *api.DXAPIEndPointRequest) (err error) {
	_, id, err := aepr.GetParameterValueAsInt64(bt.FieldNameForRowId)
	if err != nil {
		return err
	}
	rowsInfo, d, err := bt.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil,
		utilsJson.Encapsulate(bt.ResponseEnvelopeObjectName, utils.JSON{
			bt.ResultObjectName: d,
			"rows_info":         rowsInfo,
		}),
	)

	return nil
}

func (bt *DXBaseTable) RequestReadByUid(aepr *api.DXAPIEndPointRequest) (err error) {
	_, uid, err := aepr.GetParameterValueAsString(bt.FieldNameForRowUid)
	if err != nil {
		return err
	}

	rowsInfo, d, err := bt.ShouldGetByUid(&aepr.Log, uid)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil,
		utilsJson.Encapsulate(bt.ResponseEnvelopeObjectName, utils.JSON{
			bt.ResultObjectName: d,
			"rows_info":         rowsInfo,
		}),
	)

	return nil
}

func (bt *DXBaseTable) RequestReadByNameId(aepr *api.DXAPIEndPointRequest) (err error) {
	_, nameid, err := aepr.GetParameterValueAsString(bt.FieldNameForRowNameId)
	if err != nil {
		return err
	}

	rowsInfo, d, err := bt.ShouldGetByNameId(&aepr.Log, nameid)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(
		bt.ResponseEnvelopeObjectName, utils.JSON{
			bt.ResultObjectName: d,
			"rows_info":         rowsInfo,
		}),
	)

	return nil
}

func (bt *DXBaseTable) RequestReadByUtag(aepr *api.DXAPIEndPointRequest) (err error) {
	_, utag, err := aepr.GetParameterValueAsString("utag")
	if err != nil {
		return err
	}

	rowsInfo, d, err := bt.ShouldGetByUtag(&aepr.Log, utag)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(
		bt.ResponseEnvelopeObjectName, utils.JSON{
			bt.ResultObjectName: d,
			"rows_info":         rowsInfo,
		}),
	)

	return nil
}

func (t *DXRawTable) TxShouldSelectOne(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	return tx.ShouldSelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, nil)
}
func (t *DXRawTable) TxShouldSelectOneForUpdate(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	return tx.ShouldSelectOne(t.NameId, t.FieldTypeMapping, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, true)
}

func (t *DXRawTable) TxSelect(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy, limit any) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {

	return tx.Select(t.ListViewNameId, t.FieldTypeMapping, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, limit, false)
}

func (t *DXRawTable) TxSelectOne(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	return tx.SelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, false)
}

func (t *DXRawTable) TxSelectOneForUpdate(tx *database.DXDatabaseTx, whereAndFieldNameValues utils.JSON,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	return tx.SelectOne(t.NameId, t.FieldTypeMapping, nil, whereAndFieldNameValues, nil, orderbyFieldNameDirections, true)
}
