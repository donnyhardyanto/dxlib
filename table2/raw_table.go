package table2

import (
	_ "time/tzdata"

	"github.com/donnyhardyanto/dxlib/database2"
	"github.com/donnyhardyanto/dxlib/database2/db"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

func (t *DXRawTable2) GetById(log *log.DXLog, id int64) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.SelectOne(log, nil, utils.JSON{
		t.FieldNameForRowId: id,
	}, nil, nil, nil, map[string]string{t.FieldNameForRowId: "asc"}, nil, nil)
	return rowsInfo, r, err
}

func (t *DXRawTable2) ShouldGetById(log *log.DXLog, id int64) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, nil, utils.JSON{
		t.FieldNameForRowId: id,
	}, nil, nil, nil, map[string]string{t.FieldNameForRowId: "asc"}, nil, nil)
	return rowsInfo, r, err
}

func (t *DXRawTable2) ShouldGetByUid(log *log.DXLog, uid string) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, nil, utils.JSON{
		t.FieldNameForRowUid: uid,
	}, nil, nil, nil, map[string]string{t.FieldNameForRowId: "asc"}, nil, nil)
	return rowsInfo, r, err
}
func (t *DXRawTable2) ShouldGetByUtag(log *log.DXLog, utag string) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, nil, utils.JSON{
		"utag": utag,
	}, nil, nil, nil, map[string]string{t.FieldNameForRowId: "asc"}, nil, nil)
	return rowsInfo, r, err
}

func (t *DXRawTable2) GetByNameId(log *log.DXLog, nameid string) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.SelectOne(log, nil, utils.JSON{
		t.FieldNameForRowNameId: nameid,
	}, nil, nil, nil, map[string]string{t.FieldNameForRowNameId: "asc"}, nil, nil)
	return rowsInfo, r, err
}

func (t *DXRawTable2) ShouldGetByNameId(log *log.DXLog, nameid string) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, nil, utils.JSON{
		t.FieldNameForRowNameId: nameid,
	}, nil, nil, nil, map[string]string{t.FieldNameForRowNameId: "asc"}, nil, nil)
	return rowsInfo, r, err
}

func (t *DXRawTable2) TxGetByNameId(tx *database2.DXDatabaseTx, nameId string) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.SelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, utils.JSON{
		t.FieldNameForRowNameId: nameId,
		"is_deleted":            false,
	}, nil, nil, nil, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXRawTable2) TxShouldGetByNameId(tx *database2.DXDatabaseTx, nameId string) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.ShouldSelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, utils.JSON{
		t.FieldNameForRowNameId: nameId,
		"is_deleted":            false,
	}, nil, nil, nil, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXRawTable2) TxShouldGetById(tx *database2.DXDatabaseTx, id int64) (rowsInfo *db.DXDatabaseTableRowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.ShouldSelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, utils.JSON{
		t.FieldNameForRowId: id,
	}, nil, nil, nil, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXRawTable2) SelectAll(log *log.DXLog) (rowsInfo *db.DXDatabaseTableRowsInfo, r []utils.JSON, err error) {
	return t.Select(log, nil, nil, nil, nil, nil,
		map[string]string{t.FieldNameForRowId: "asc"}, nil, nil, nil)
}
