package table2

import (
	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/database2"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
	"github.com/pkg/errors"
	"net/http"
)

func (bt *DXBaseTable2) Insert(newKeyValues utils.JSON) (newId int64, newUid string, err error) {
	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return 0, "", err
	}

	var returningFieldNames []string
	if bt.FieldNameForRowId != "" {
		returningFieldNames = append(returningFieldNames, bt.FieldNameForRowId)
	}
	if bt.FieldNameForRowUid != "" {
		returningFieldNames = append(returningFieldNames, bt.FieldNameForRowUid)
	}

	_, returningFieldValues, err := bt.Database.Insert(bt.NameId, newKeyValues, returningFieldNames)
	if err != nil {
		return 0, "", err
	}
	var ok bool
	if bt.FieldNameForRowId != "" {
		newId, ok = returningFieldValues[bt.FieldNameForRowId].(int64)
		if !ok {
			return 0, "", errors.New("IMPOSSIBLE:INSERT_NOT_RETURNING_ID")
		}
	}
	if bt.FieldNameForRowUid != "" {
		newUid, ok = returningFieldValues[bt.FieldNameForRowUid].(string)
		if !ok {
			return 0, "", errors.New("IMPOSSIBLE:INSERT_NOT_RETURNING_UID")
		}
	}

	return newId, newUid, nil
}

func (bt *DXBaseTable2) DoRequestInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (err error) {

	// Execute OnBeforeInsert callback if provided
	if bt.OnBeforeInsert != nil {
		if err := bt.OnBeforeInsert(aepr, newKeyValues); err != nil {
			return err
		}
	}

	newId, newUid, err := bt.Insert(newKeyValues)
	if err != nil {
		return err
	}

	data := utilsJson.Encapsulate(bt.ResponseEnvelopeObjectName, utils.JSON{
		bt.FieldNameForRowId:  newId,
		bt.FieldNameForRowUid: newUid,
	})
	aepr.WriteResponseAsJSON(http.StatusOK, nil, data)

	return nil
}

// Note: New name is RequestInsert, the RequestCreate is depreciated

func (bt *DXBaseTable2) RequestInsert(aepr *api.DXAPIEndPointRequest) (err error) {
	p := map[string]interface{}{}
	for k, v := range aepr.ParameterValues {
		p[k] = v.Value
	}
	err = bt.DoRequestInsert(aepr, p)
	if err != nil {
		return err
	}
	return nil
}

// Note: New name is RequestInsert, the RequestCreate is depreciated

func (bt *DXBaseTable2) RequestCreate(aepr *api.DXAPIEndPointRequest) (err error) {
	return bt.RequestInsert(aepr)
}

func (bt *DXBaseTable2) TxInsert(tx *database2.DXDatabaseTx, newKeyValues utils.JSON) (newId int64, newUid string, err error) {
	var returningFieldNames []string
	if bt.FieldNameForRowId != "" {
		returningFieldNames = append(returningFieldNames, bt.FieldNameForRowId)
	}
	if bt.FieldNameForRowUid != "" {
		returningFieldNames = append(returningFieldNames, bt.FieldNameForRowUid)
	}

	_, returningFieldValues, err := tx.Insert(bt.NameId, newKeyValues, returningFieldNames)
	if err != nil {
		return 0, "", err
	}
	var ok bool
	if bt.FieldNameForRowId != "" {
		newId, ok = returningFieldValues[bt.FieldNameForRowId].(int64)
		if !ok {
			return 0, "", errors.New("IMPOSSIBLE:INSERT_NOT_RETURNING_ID")
		}
	}
	if bt.FieldNameForRowUid != "" {
		newUid, ok = returningFieldValues[bt.FieldNameForRowUid].(string)
		if !ok {
			return 0, "", errors.New("IMPOSSIBLE:INSERT_NOT_RETURNING_UID")
		}
	}

	return newId, newUid, nil
}
