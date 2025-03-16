package table

import (
	database "github.com/donnyhardyanto/dxlib/database2"
	"github.com/donnyhardyanto/dxlib/database2/database_type"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/pkg/errors"
)

func (t *DXBaseTable) GetById(log *log.DXLog, id int64) (rowsInfo *database_type.RowsInfo, r utils.JSON, err error) {
	if t.FieldNameForRowId == "" {
		return nil, nil, errors.New("Field name for row id is not set")
	}
	rowsInfo, r, err = t.SelectOne(log, nil, utils.JSON{
		t.FieldNameForRowId: id,
	}, nil, map[string]string{t.FieldNameForRowId: "asc"}, nil, nil)
	return rowsInfo, r, err
}

func (t *DXBaseTable) ShouldGetById(log *log.DXLog, id int64) (rowsInfo *database_type.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, utils.JSON{
		t.FieldNameForRowId: id,
	}, nil, map[string]string{t.FieldNameForRowId: "asc"}, nil, nil)
	return rowsInfo, r, err
}

func (t *DXBaseTable) ShouldGetByUid(log *log.DXLog, uid string) (rowsInfo *database_type.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, utils.JSON{
		t.FieldNameForRowUid: uid,
	}, nil, map[string]string{t.FieldNameForRowId: "asc"}, nil, nil)
	return rowsInfo, r, err
}

func (t *DXBaseTable) ShouldGetByUtag(log *log.DXLog, utag string) (rowsInfo *database_type.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, utils.JSON{
		"utag": utag,
	}, nil, map[string]string{t.FieldNameForRowId: "asc"}, nil, nil)
	return rowsInfo, r, err
}

func (t *DXBaseTable) GetByNameId(log *log.DXLog, nameid string) (rowsInfo *database_type.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.SelectOne(log, nil, utils.JSON{
		t.FieldNameForRowNameId: nameid,
	}, nil, map[string]string{t.FieldNameForRowNameId: "asc"}, nil, nil)
	return rowsInfo, r, err
}

func (t *DXBaseTable) ShouldGetByNameId(log *log.DXLog, nameid string) (rowsInfo *database_type.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = t.ShouldSelectOne(log, utils.JSON{
		t.FieldNameForRowNameId: nameid,
	}, nil, map[string]string{t.FieldNameForRowNameId: "asc"}, nil, nil)
	return rowsInfo, r, err
}

func (t *DXBaseTable) TxShouldGetById(tx *database.DXDatabaseTx, id int64) (rowsInfo *database_type.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.TxShouldSelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, utils.JSON{
		t.FieldNameForRowId: id,
	}, nil, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXBaseTable) TxGetByNameId(tx *database.DXDatabaseTx, nameId string) (rowsInfo *database_type.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.TxSelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, utils.JSON{
		t.FieldNameForRowNameId: nameId,
	}, nil, nil, nil, nil)
	return rowsInfo, r, err
}

func (t *DXBaseTable) TxShouldGetByNameId(tx *database.DXDatabaseTx, nameId string) (rowsInfo *database_type.RowsInfo, r utils.JSON, err error) {
	rowsInfo, r, err = tx.TxShouldSelectOne(t.ListViewNameId, t.FieldTypeMapping, nil, utils.JSON{
		t.FieldNameForRowNameId: nameId,
	}, nil, nil, nil, nil)
	return rowsInfo, r, err
}
