package table2

import (
	"time"

	"github.com/donnyhardyanto/dxlib/api"
	database "github.com/donnyhardyanto/dxlib/database2"
	"github.com/donnyhardyanto/dxlib/database2/db"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/pkg/errors"
)

type TableInterface interface {
	Initialize() TableInterface
	DbEnsureInitializeConnection() error
	//	DoRequestInsert(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) (newId int64, newUid string, err error)
}

type DXBaseTable2OnResultProcessEachListRow func(aepr *api.DXAPIEndPointRequest, rowData utils.JSON) (newRowData utils.JSON, err error)

// DXBaseTable contains common fields for all table types
type DXBaseTable2 struct {
	DatabaseType               db.DXDatabaseType
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
	FieldTypeMapping           db.DXDatabaseTableFieldTypeMapping
	DoOverrideSelectValues     func(whereKeyValues utils.JSON) (utils.JSON, error)
	DoOverrideInsertValues     func(newKeyValues utils.JSON) (utils.JSON, error)
	DoOverrideUpdateValues     func(newKeyValues utils.JSON) (utils.JSON, error)

	OnBeforeInsert              func(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) error
	OnBeforeUpdate              func(aepr *api.DXAPIEndPointRequest, newKeyValues utils.JSON) error
	OnResultProcessEachListRow  DXBaseTable2OnResultProcessEachListRow
	OnResponseObjectConstructor func(aepr *api.DXAPIEndPointRequest, rawResponseObject utils.JSON) (responseObject utils.JSON, err error)
}

func (bt *DXBaseTable2) Initialize() TableInterface {
	return bt
}

func (bt *DXBaseTable2) DbEnsureInitializeConnection() (err error) {
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
	driverName := bt.Database.Connection.DriverName()
	bt.DatabaseType = db.StringToDXDatabaseType(driverName)

	return nil
}

type DXRawTable2 struct {
	DXBaseTable2
}

// DXTable2 /* soft delete and is_deleted support */
type DXTable2 struct {
	DXRawTable2
}

func (t *DXTable2) DoOverrideSelectValues(whereKeyValues utils.JSON) (utils.JSON, error) {
	if whereKeyValues == nil {
		whereKeyValues = utils.JSON{}
	}
	whereKeyValues["is_deleted"] = false
	return whereKeyValues, nil
}

func (t *DXTable2) DoOverrideInsertValues(newKeyValues utils.JSON) (utils.JSON, error) {
	tt := time.Now().UTC()
	newKeyValues["is_deleted"] = false
	newKeyValues["created_at"] = tt
	newKeyValues["last_modified_at"] = tt

	_, ok := newKeyValues["created_by_user_id"]
	if !ok {
		newKeyValues["created_by_user_id"] = "0"
		newKeyValues["created_by_user_nameid"] = "SYSTEM"
		newKeyValues["last_modified_by_user_id"] = "0"
		newKeyValues["last_modified_by_user_nameid"] = "SYSTEM"
	}
	return newKeyValues, nil
}

func (t *DXTable2) DoOverrideUpdateValues(newKeyValues utils.JSON) (utils.JSON, error) {
	tt := time.Now().UTC()
	newKeyValues["last_modified_at"] = tt

	_, ok := newKeyValues["last_modified_by_user_id"]
	if !ok {
		newKeyValues["last_modified_by_user_id"] = "0"
		newKeyValues["last_modified_by_user_nameid"] = "SYSTEM"
	}
	return newKeyValues, nil
}

func (t *DXTable2) Initialize() TableInterface {
	t.OnBeforeInsert = DoBeforeInsert
	return t
}

type DXPropertyTable2 struct {
	DXTable2
}
