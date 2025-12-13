package table

import (
	"database/sql"
	"net/http"
	"time"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/database/protected/db"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
	"github.com/pkg/errors"
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
	FieldTypeMapping           db.FieldTypeMapping
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
	return nil
}

func (bt *DXBaseTable) Select(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections db.FieldsOrderBy, limit any, forUpdatePart any) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return nil, nil, err
	}

	rowsInfo, r, err = bt.Database.Select(bt.ListViewNameId, bt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, limit, forUpdatePart)
	if err != nil {
		return rowsInfo, nil, err
	}

	return rowsInfo, r, err
}

func (bt *DXBaseTable) ShouldSelectOne(log *log.DXLog, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections db.FieldsOrderBy) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return nil, nil, err
	}

	return bt.Database.ShouldSelectOne(bt.ListViewNameId, bt.FieldTypeMapping, nil, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections)
}

func (bt *DXBaseTable) SelectOne(log *log.DXLog, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any, orderbyFieldNameDirections db.FieldsOrderBy) (
	rowsInfo *db.RowsInfo, r utils.JSON, err error) {

	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	if bt.Database == nil {
		bt.Database = database.Manager.Databases[bt.DatabaseNameId]
	}

	if !bt.Database.Connected {
		err := bt.Database.Connect()
		if err != nil {
			return nil, nil, err
		}
	}

	return bt.Database.SelectOne(bt.ListViewNameId, bt.FieldTypeMapping, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections)
}

func (t *DXBaseTable) GetById(log *log.DXLog, id int64) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.SelectOne(log, nil, utils.JSON{
		t.FieldNameForRowId: id,
	}, nil, map[string]string{t.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (t *DXBaseTable) ShouldGetById(log *log.DXLog, id int64) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, utils.JSON{
		t.FieldNameForRowId: id,
	}, nil, map[string]string{t.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (t *DXBaseTable) ShouldGetByUid(log *log.DXLog, uid string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, utils.JSON{
		t.FieldNameForRowUid: uid,
	}, nil, map[string]string{t.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (t *DXBaseTable) ShouldGetByUtag(log *log.DXLog, utag string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, utils.JSON{
		"utag": utag,
	}, nil, map[string]string{t.FieldNameForRowId: "asc"})
	return rowsInfo, r, err
}

func (t *DXBaseTable) GetByNameId(log *log.DXLog, nameid string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.SelectOne(log, nil, utils.JSON{
		t.FieldNameForRowNameId: nameid,
	}, nil, map[string]string{t.FieldNameForRowNameId: "asc"})
	return rowsInfo, r, err
}

func (t *DXBaseTable) ShouldGetByNameId(log *log.DXLog, nameid string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, utils.JSON{
		t.FieldNameForRowNameId: nameid,
	}, nil, map[string]string{t.FieldNameForRowNameId: "asc"})
	return rowsInfo, r, err
}

func (t *DXBaseTable) TxShouldGetById(tx *database.DXDatabaseTx, id int64) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.ShouldSelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, utils.JSON{
		t.FieldNameForRowId: id,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXBaseTable) TxShouldGetByIdNotDeleted(tx *database.DXDatabaseTx, id int64) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.ShouldSelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, utils.JSON{
		t.FieldNameForRowId: id,
		"is_deleted":        false,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXBaseTable) TxGetByNameId(tx *database.DXDatabaseTx, nameId string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.SelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, utils.JSON{
		t.FieldNameForRowNameId: nameId,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXBaseTable) TxShouldGetByNameId(tx *database.DXDatabaseTx, nameId string) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.ShouldSelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, utils.JSON{
		t.FieldNameForRowNameId: nameId,
	}, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXBaseTable) Upsert(setKeyValues utils.JSON, whereAndFieldNameValues utils.JSON) (result sql.Result, newId int64, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	if t.Database == nil {
		t.Database = database.Manager.Databases[t.DatabaseNameId]
	}

	_, r, err := t.Database.SelectOne(t.NameId, nil, nil, whereAndFieldNameValues, nil, nil)
	if err != nil {
		return nil, 0, err
	}
	if r == nil {
		newSetKeyValues := utilsJson.DeepMerge2(setKeyValues, whereAndFieldNameValues)

		newId, err = t.Database.Insert(t.NameId, t.FieldNameForRowId, newSetKeyValues)
		return nil, newId, err
	} else {
		result, err = t.Database.Update(t.NameId, setKeyValues, whereAndFieldNameValues)
		return result, 0, err
	}
}

func (t *DXBaseTable) TxInsert(tx *database.DXDatabaseTx, newKeyValues utils.JSON) (newId int64, err error) {
	newId, err = tx.Insert(t.NameId, newKeyValues)
	return newId, err
}

func (t *DXBaseTable) TxUpsert(tx *database.DXDatabaseTx, setKeyValues utils.JSON, whereAndFieldNameValues utils.JSON) (result sql.Result, newId int64, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	_, r, err := tx.SelectOne(t.NameId, nil, nil, whereAndFieldNameValues, nil, nil, nil)
	if err != nil {
		return nil, 0, err
	}
	if r == nil {
		newSetKeyValues := utilsJson.DeepMerge2(setKeyValues, whereAndFieldNameValues)

		newId, err = tx.Insert(t.NameId, newSetKeyValues)
		return nil, newId, err
	} else {
		result, err = tx.Update(t.NameId, setKeyValues, whereAndFieldNameValues)
		return result, 0, err
	}
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
		aepr.Log.Errorf(err, "Error at %s.DoDelete (%s) ", bt.NameId, err.Error())
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
		aepr.Log.Errorf(err, "Error at %s.DoDeleteByUid (%s) ", bt.NameId, err.Error())
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

/*type DXRawTable2 struct {
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
*/
