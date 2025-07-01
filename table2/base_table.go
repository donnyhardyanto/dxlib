package table2

import (
	"github.com/donnyhardyanto/dxlib/api"
	database "github.com/donnyhardyanto/dxlib/database2"
	"github.com/donnyhardyanto/dxlib/database2/database_type"
	utils2 "github.com/donnyhardyanto/dxlib/database2/db/utils"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/pkg/errors"
	"time"
)

type TableInterface interface {
	Initialize() TableInterface
	DbEnsureInitialize() error
	//	DoRequestInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, newUid string, err error)
}

// DXBaseTable contains common fields for all table types
type DXBaseTable2 struct {
	DatabaseType                database_type.DXDatabaseType
	DatabaseNameId              string
	Database                    *database.DXDatabase
	NameId                      string
	ResultObjectName            string
	ListViewNameId              string
	FieldNameForRowId           string
	FieldNameForRowNameId       string
	FieldNameForRowUid          string
	FieldNameForRowUtag         string
	ResponseEnvelopeObjectName  string
	FieldTypeMapping            utils2.FieldTypeMapping
	OnBeforeInsert              func(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) error
	OnBeforeUpdate              func(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) error
	OnResultProcessEachListRow  func(aepr *api.DXAPIEndPointRequest, bt *DXBaseTable2, rowData utils.JSON) (newRowData utils.JSON, err error)
	OnResponseObjectConstructor func(aepr *api.DXAPIEndPointRequest, bt *DXBaseTable2, rawResponseObject utils.JSON) (responseObject utils.JSON, err error)
}

func (bt *DXBaseTable2) Initialize() TableInterface {
	return bt
}

func (bt *DXBaseTable2) DbEnsureInitialize() (err error) {
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
	driverName := bt.Database.Connection.DriverName()
	bt.DatabaseType = database_type.StringToDXDatabaseType(driverName)

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
	DXBaseTable2
}

type DXTable2 struct {
	DXRawTable2
}

func (t *DXTable2) Initialize() TableInterface {
	t.OnBeforeInsert = DoBeforeInsert
	return t
}

type DXPropertyTable2 struct {
	DXTable2
}
