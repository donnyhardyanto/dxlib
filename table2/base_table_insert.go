package table

import (
	database "github.com/donnyhardyanto/dxlib/database2"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/pkg/errors"
)

func (bt *DXBaseTable) Insert(newKeyValues utils.JSON) (newId int64, newUid string, err error) {
	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return 0, "", err
	}

	var p []string
	if bt.FieldNameForRowId != "" {
		p = append(p, bt.FieldNameForRowId)
	}
	if bt.FieldNameForRowUid != "" {
		p = append(p, bt.FieldNameForRowUid)
	}
	r, err := bt.Database.Insert(bt.NameId, newKeyValues, p)
	if err != nil {
		return 0, "", err
	}
	ok := false
	if bt.FieldNameForRowId != "" {
		newId, ok = r[bt.FieldNameForRowId].(int64)
		if !ok {
			return 0, "", errors.New("IMPOSSIBLE:ID_IN_INT64_NOT_FOUND")
		}
	}
	if bt.FieldNameForRowUid != "" {
		newUid, ok = r[bt.FieldNameForRowUid].(string)
		if !ok {
			return 0, "", errors.New("IMPOSSIBLE:UID_IN_STRING_NOT_FOUND")
		}
	}
	return newId, newUid, err
}

func (bt *DXBaseTable) TxInsert(tx *database.DXDatabaseTx, newKeyValues utils.JSON) (newId int64, newUid string, err error) {
	// Ensure database is initialized
	if err := bt.DbEnsureInitialize(); err != nil {
		return 0, "", err
	}

	var p []string
	if bt.FieldNameForRowId != "" {
		p = append(p, bt.FieldNameForRowId)
	}
	if bt.FieldNameForRowUid != "" {
		p = append(p, bt.FieldNameForRowUid)
	}
	r, err := tx.Insert(bt.NameId, newKeyValues, p)
	if err != nil {
		return 0, "", err
	}
	ok := false
	if bt.FieldNameForRowId != "" {
		newId, ok = r[bt.FieldNameForRowId].(int64)
		if !ok {
			return 0, "", errors.New("IMPOSSIBLE:ID_IN_INT64_NOT_FOUND")
		}
	}
	if bt.FieldNameForRowUid != "" {
		newUid, ok = r[bt.FieldNameForRowUid].(string)
		if !ok {
			return 0, "", errors.New("IMPOSSIBLE:UID_IN_STRING_NOT_FOUND")
		}
	}
	return newId, newUid, err
}
