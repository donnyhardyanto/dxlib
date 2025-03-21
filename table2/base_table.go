package table

import (
	"github.com/donnyhardyanto/dxlib/api"
	database "github.com/donnyhardyanto/dxlib/database2"
	utils2 "github.com/donnyhardyanto/dxlib/database2/db/utils"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/pkg/errors"
	"time"
)

type TableInterface interface {
	Initialize() TableInterface
	DbEnsureInitialize() error
	DoRequestInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, newUid string, err error)
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
	OnBeforeUpdate             func(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) error
}

func (bt *DXBaseTable) Initialize() TableInterface {
	return bt
}

func (bt *DXBaseTable) DbEnsureInitialize() (err error) {
	if bt.Database == nil {
		bt.Database = database.Manager.Databases[bt.DatabaseNameId]
		if bt.Database == nil {
			return errors.Errorf("database not found: %s", bt.DatabaseNameId)
		}
	}
	if !bt.Database.Connected {
		err := bt.Database.Connect()
		if err != nil {
			return errors.Wrap(err, "error occured")
		}
	}
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

type DXPropertyTable2 struct {
	DXTable2
}
