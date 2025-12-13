package table2

import (
	"net/http"
	"time"
	_ "time/tzdata"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
)

/*type DXTable2 struct {
	DatabaseNameId             string
	Database                   *database2.DXDatabase
	NameId                     string
	ResultObjectName           string
	ListViewNameId             string
	FieldNameForRowId          string
	FieldNameForRowNameId      string
	FieldNameForRowUid         string
	DXDatabaseTableFieldTypeMapping           db.DXDatabaseTableFieldTypeMapping
	ResponseEnvelopeObjectName string
}*/

func DoBeforeInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) error {
	newKeyValues["is_deleted"] = false

	// Set timestamp and user tracking fields
	tt := time.Now().UTC()
	newKeyValues["created_at"] = tt
	_, ok := newKeyValues["created_by_user_id"]
	if !ok {
		if aepr.CurrentUser.Id != "" {
			newKeyValues["created_by_user_id"] = aepr.CurrentUser.Id
			newKeyValues["created_by_user_nameid"] = aepr.CurrentUser.LoginId
		} else {
			newKeyValues["created_by_user_id"] = "0"
			newKeyValues["created_by_user_nameid"] = "SYSTEM"
		}
		newKeyValues["last_modified_at"] = tt
		if aepr.CurrentUser.Id != "" {
			newKeyValues["last_modified_by_user_id"] = aepr.CurrentUser.Id
			newKeyValues["last_modified_by_user_nameid"] = aepr.CurrentUser.LoginId
		} else {
			newKeyValues["last_modified_by_user_id"] = "0"
			newKeyValues["last_modified_by_user_nameid"] = "SYSTEM"
		}
	}
	return nil
}

/*func (t *DXTable2) DoInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, err error) {
	newKeyValues["is_deleted"] = false

	tt := time.Now().UTC()
	newKeyValues["created_at"] = tt
	_, ok := newKeyValues["created_by_user_id"]
	if !ok {
		if aepr.CurrentUser.Id != "" {
			newKeyValues["created_by_user_id"] = aepr.CurrentUser.Id
			newKeyValues["created_by_user_nameid"] = aepr.CurrentUser.LoginId
		} else {
			newKeyValues["created_by_user_id"] = "0"
			newKeyValues["created_by_user_nameid"] = "SYSTEM"
		}
		newKeyValues["last_modified_at"] = tt
		if aepr.CurrentUser.Id != "" {
			newKeyValues["last_modified_by_user_id"] = aepr.CurrentUser.Id
			newKeyValues["last_modified_by_user_nameid"] = aepr.CurrentUser.LoginId
		} else {
			newKeyValues["last_modified_by_user_id"] = "0"
			newKeyValues["last_modified_by_user_nameid"] = "SYSTEM"
		}
	}

	if t.Database == nil {
		t.Database = database2.Manager.Databases[t.DatabaseNameId]
	}

	newId, err = t.Database.Insert(t.NameId, t.FieldNameForRowId, newKeyValues)
	if err != nil {
		return 0, err
	}

	p := utils.JSON{
		t.FieldNameForRowId: newId,
	}

	if t.FieldNameForRowUid != "" {
		_, n, err := t.Database.SelectOne(t.ListViewNameId, nil, nil, utils.JSON{
			"id": newId,
		}, nil, nil)
		if err != nil {
			return 0, err
		}
		uid, ok := n[t.FieldNameForRowUid].(string)
		if !ok {
			return 0, errors.New("IMPOSSIBLE:UID")
		}
		p[t.FieldNameForRowUid] = uid
	}

	data := utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, p)
	aepr.WriteResponseAsJSON(http.StatusOK, nil, data)

	return newId, nil
}*/

/*func (t *DXTable2) DoCreate(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, err error) {
	newId, err = t.DoInsert(aepr, newKeyValues)
	if err != nil {
		return 0, err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{
		t.FieldNameForRowId: newId,
	},
	))

	return newId, nil
}*/

/*func (t *DXTable2) InRequestTxInsert(aepr *api.DXAPIEndPointRequest, tx *database2.DXDatabaseTx, newKeyValues utils.JSON) (newId int64, err error) {
	n := utils.NowAsString()
	newKeyValues["is_deleted"] = false
	newKeyValues["created_at"] = n
	_, ok := newKeyValues["created_by_user_id"]
	if !ok {
		if aepr.CurrentUser.Id != "" {
			newKeyValues["created_by_user_id"] = aepr.CurrentUser.Id
			newKeyValues["created_by_user_nameid"] = aepr.CurrentUser.LoginId
		} else {
			newKeyValues["created_by_user_id"] = "0"
			newKeyValues["created_by_user_nameid"] = "SYSTEM"
		}
		newKeyValues["last_modified_at"] = n
		if aepr.CurrentUser.Id != "" {
			newKeyValues["last_modified_by_user_id"] = aepr.CurrentUser.Id
			newKeyValues["last_modified_by_user_nameid"] = aepr.CurrentUser.LoginId
		} else {
			newKeyValues["last_modified_by_user_id"] = "0"
			newKeyValues["last_modified_by_user_nameid"] = "SYSTEM"
		}
	}

	newId, err = tx.Insert(t.NameId, newKeyValues)
	return newId, err
}*/

/*func (t *DXTable2) UpdateOne(l *log.DXLog, FieldValueForId int64, setKeyValues utils.JSON) (result sql.Result, err error) {
	_, _, err = t.ShouldGetById(l, FieldValueForId)
	if err != nil {
		return nil, err
	}

	if t.Database == nil {
		t.Database = database2.Manager.Databases[t.DatabaseNameId]
	}

	return t.Database.Update(t.NameId, setKeyValues, utils.JSON{
		t.FieldNameForRowId: FieldValueForId,
	})
}*/

/*func (t *DXTable2) InRequestInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, err error) {
	n := utils.NowAsString()
	newKeyValues["is_deleted"] = false
	newKeyValues["created_at"] = n
	_, ok := newKeyValues["created_by_user_id"]
	if !ok {
		if aepr.CurrentUser.Id != "" {
			newKeyValues["created_by_user_id"] = aepr.CurrentUser.Id
			newKeyValues["created_by_user_nameid"] = aepr.CurrentUser.LoginId
		} else {
			newKeyValues["created_by_user_id"] = "0"
			newKeyValues["created_by_user_nameid"] = "SYSTEM"
		}
		newKeyValues["last_modified_at"] = n
		if aepr.CurrentUser.Id != "" {
			newKeyValues["last_modified_by_user_id"] = aepr.CurrentUser.Id
			newKeyValues["last_modified_by_user_nameid"] = aepr.CurrentUser.LoginId
		} else {
			newKeyValues["last_modified_by_user_id"] = "0"
			newKeyValues["last_modified_by_user_nameid"] = "SYSTEM"
		}
	}

	if t.Database == nil {
		t.Database = database2.Manager.Databases[t.DatabaseNameId]
	}

	newId, err = t.Database.Insert(t.NameId, t.FieldNameForRowId, newKeyValues)
	return newId, err
}*/

func (t *DXTable2) RequestRead(aepr *api.DXAPIEndPointRequest) (err error) {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	rowsInfo, d, err := t.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{t.ResultObjectName: d, "rows_info": rowsInfo}))

	return nil
}

func (t *DXTable2) RequestReadByUid(aepr *api.DXAPIEndPointRequest) (err error) {
	_, uid, err := aepr.GetParameterValueAsString(t.FieldNameForRowUid)
	if err != nil {
		return err
	}

	rowsInfo, d, err := t.ShouldGetByUid(&aepr.Log, uid)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{t.ResultObjectName: d, "rows_info": rowsInfo}))

	return nil
}

func (t *DXTable2) RequestReadByNameId(aepr *api.DXAPIEndPointRequest) (err error) {
	_, nameid, err := aepr.GetParameterValueAsString(t.FieldNameForRowNameId)
	if err != nil {
		return err
	}

	rowsInfo, d, err := t.ShouldGetByNameId(&aepr.Log, nameid)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{t.ResultObjectName: d, "rows_info": rowsInfo}))

	return nil
}

func (t *DXTable2) RequestReadByUtag(aepr *api.DXAPIEndPointRequest) (err error) {
	_, utag, err := aepr.GetParameterValueAsString("utag")
	if err != nil {
		return err
	}

	rowsInfo, d, err := t.ShouldGetByUtag(&aepr.Log, utag)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, utilsJson.Encapsulate(t.ResponseEnvelopeObjectName, utils.JSON{t.ResultObjectName: d, "rows_info": rowsInfo}))

	return nil
}

func (t *DXTable2) AddNotDeletedClause(whereAndFieldNameValues utils.JSON) (whereAndFieldNameValuesWithNotDeletedClause utils.JSON) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}
	whereAndFieldNameValues["is_deleted"] = false
	return whereAndFieldNameValues
}
