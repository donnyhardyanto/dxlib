package table2

import (
	database "github.com/donnyhardyanto/dxlib/database2"
	"github.com/donnyhardyanto/dxlib/database2/db"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/donnyhardyanto/dxlib/errors"
)

func (bt *DXBaseTable2) GetById(log *log.DXLog, id int64) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	if bt.FieldNameForRowId == "" {
		return nil, nil, errors.New("Field name for row id is not set")
	}
	rowsInfo, r, err = bt.SelectOne(log, nil, utils.JSON{
		bt.FieldNameForRowId: id,
	}, nil, nil, nil, map[string]string{bt.FieldNameForRowId: "asc"}, nil, nil)
	return rowsInfo, r, err
}

func (bt *DXBaseTable2) ShouldGetById(log *log.DXLog, id int64) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	if bt.FieldNameForRowId == "" {
		return nil, nil, errors.New("Field name for row id is not set")
	}
	rowsInfo, r, err = bt.ShouldSelectOne(log, nil, utils.JSON{
		bt.FieldNameForRowId: id,
	}, nil, nil, nil, map[string]string{bt.FieldNameForRowId: "asc"}, nil, nil)
	return rowsInfo, r, err
}

func (bt *DXBaseTable2) ShouldGetByUid(log *log.DXLog, uid string) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	if bt.FieldNameForRowUid == "" {
		return nil, nil, errors.New("Field name for row uid is not set")
	}
	rowsInfo, r, err = bt.ShouldSelectOne(log, nil, utils.JSON{
		bt.FieldNameForRowUid: uid,
	}, nil, nil, nil, map[string]string{bt.FieldNameForRowId: "asc"}, nil, nil)
	return rowsInfo, r, err
}

func (bt *DXBaseTable2) ShouldGetByUtag(log *log.DXLog, utag string) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = bt.ShouldSelectOne(log, nil, utils.JSON{
		"utag": utag,
	}, nil, nil, nil, map[string]string{bt.FieldNameForRowId: "asc"}, nil, nil)
	return rowsInfo, r, err
}

func (bt *DXBaseTable2) GetByNameId(log *log.DXLog, nameid string) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	if bt.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("Field name for row name id is not set")
	}
	rowsInfo, r, err = bt.SelectOne(log, nil, utils.JSON{
		bt.FieldNameForRowNameId: nameid,
	}, nil, nil, nil, map[string]string{bt.FieldNameForRowNameId: "asc"}, nil, nil)
	return rowsInfo, r, err
}

func (bt *DXBaseTable2) ShouldGetByNameId(log *log.DXLog, nameid string) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	if bt.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("Field name for row name id is not set")
	}
	rowsInfo, r, err = bt.ShouldSelectOne(log, nil, utils.JSON{
		bt.FieldNameForRowNameId: nameid,
	}, nil, nil, nil, map[string]string{bt.FieldNameForRowNameId: "asc"}, nil, nil)
	return rowsInfo, r, err
}

func (bt *DXBaseTable2) TxShouldGetById(tx *database.DXDatabaseTx, id int64) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	if bt.FieldNameForRowId == "" {
		return nil, nil, errors.New("Field name for row id is not set")
	}
	rowsInfo, r, err = tx.ShouldSelectOne(bt.ListViewNameId, bt.FieldTypeMapping, nil, utils.JSON{
		bt.FieldNameForRowId: id,
	}, nil, nil, nil, nil, nil, nil)
	return rowsInfo, r, err
}

func (bt *DXBaseTable2) TxGetByNameId(tx *database.DXDatabaseTx, nameId string) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	if bt.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("Field name for row name id is not set")
	}
	rowsInfo, r, err = tx.SelectOne(bt.ListViewNameId, bt.FieldTypeMapping, nil, utils.JSON{
		bt.FieldNameForRowNameId: nameId,
	}, nil, nil, nil, nil, nil, nil)
	return rowsInfo, r, err
}

func (bt *DXBaseTable2) TxShouldGetByNameId(tx *database.DXDatabaseTx, nameId string) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	if bt.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("Field name for row name id is not set")
	}
	rowsInfo, r, err = tx.ShouldSelectOne(bt.ListViewNameId, bt.FieldTypeMapping, nil, utils.JSON{
		bt.FieldNameForRowNameId: nameId,
	}, nil, nil, nil, nil, nil, nil)
	return rowsInfo, r, err
}

func (bt *DXBaseTable2) SelectAll(log *log.DXLog) (rowsInfo *db.DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	if bt.FieldNameForRowId == "" {
		return nil, nil, errors.New("Field name for row id is not set")
	}
	return bt.Select(log, nil, nil, nil, nil, nil,
		map[string]string{bt.FieldNameForRowId: "asc"}, nil, nil, nil)
}
