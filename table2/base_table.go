package table

import (
	"github.com/donnyhardyanto/dxlib/api"
	database "github.com/donnyhardyanto/dxlib/database2"
	"github.com/donnyhardyanto/dxlib/database2/db"
	utils2 "github.com/donnyhardyanto/dxlib/database2/db/utils"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
	"github.com/pkg/errors"
	"net/http"
	"time"
)

type TableInterface interface {
	Initialize() TableInterface
	DbEnsureInitialize() error
	DoInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, err error)
}

// DXBaseTable contains common fields for all table types
type DXBaseTable struct {
	DatabaseNameId             string
	Database                   *database.DXDatabase
	NameId                     string
	ResultObjectName           string
	ListViewNameId             string
	FieldNameForRowId          string
	FieldNameForRowNameId      string
	FieldNameForRowUid         string
	FieldNameForRowUtag        string
	ResponseEnvelopeObjectName string
	FieldTypeMapping           utils2.FieldTypeMapping
	OnBeforeInsert             func(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) error
}

func (bt *DXBaseTable) Initialize() TableInterface {
	return bt
}

func (bt *DXBaseTable) DbEnsureInitialize() error {
	if bt.Database == nil {
		bt.Database = database.Manager.Databases[bt.DatabaseNameId]
		if bt.Database == nil {
			return errors.Errorf("database not found: %s", bt.DatabaseNameId)
		}
	}
	if !bt.Database.Connected {
		err := bt.Database.Connect()
		if err != nil {
			return err
		}
	}
	return nil
}

func (bt *DXBaseTable) DoInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, err error) {
	// Execute OnBeforeInsert callback if provided
	if bt.OnBeforeInsert != nil {
		if err := bt.OnBeforeInsert(aepr, newKeyValues); err != nil {
			return 0, err
		}
	}

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return 0, err
	}

	// Perform the insertion
	newId, err = bt.Database.Insert(bt.NameId, bt.FieldNameForRowId, newKeyValues)
	if err != nil {
		return 0, err
	}

	// Prepare response
	p := utils.JSON{
		bt.FieldNameForRowId: newId,
	}

	// Handle UID if needed
	if bt.FieldNameForRowUid != "" {
		_, n, err := bt.Database.SelectOne(bt.ListViewNameId, nil, nil, utils.JSON{
			"id": newId,
		}, nil, nil)
		if err != nil {
			return 0, err
		}
		uid, ok := n[bt.FieldNameForRowUid].(string)
		if !ok {
			return 0, errors.New("IMPOSSIBLE:UID")
		}
		p[bt.FieldNameForRowUid] = uid
	}

	// Write response
	data := utilsJson.Encapsulate(bt.ResponseEnvelopeObjectName, p)
	aepr.WriteResponseAsJSON(http.StatusOK, nil, data)

	return newId, nil
}

func (bt *DXBaseTable) DoDelete(aepr *api.DXAPIEndPointRequest, id int64) (err error) {

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return err
	}

	_, _, err = bt.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}

	_, err = db.Delete(bt.Database.Connection, bt.NameId, utils.JSON{
		bt.FieldNameForRowId: id,
	})
	if err != nil {
		aepr.Log.Errorf("Error at %s.DoDelete (%s) ", bt.NameId, err.Error())
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

func (bt *DXBaseTable) DoDeleteByUid(aepr *api.DXAPIEndPointRequest, uid string) (err error) {

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return err
	}

	_, _, err = bt.ShouldGetByUid(&aepr.Log, uid)
	if err != nil {
		return err
	}

	_, err = db.Delete(bt.Database.Connection, bt.NameId, utils.JSON{
		bt.FieldNameForRowUid: uid,
	})
	if err != nil {
		aepr.Log.Errorf("Error at %s.DoDeleteByUid (%s) ", bt.NameId, err.Error())
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

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

type DXRawTable2 struct {
	DXBaseTable
}

type DXTable2 struct {
	DXRawTable2
}

func (bt *DXTable2) Initialize() TableInterface {
	bt.OnBeforeInsert = DoBeforeInsert
	return bt
}

type DXProperyTable2 struct {
	DXTable2
}
