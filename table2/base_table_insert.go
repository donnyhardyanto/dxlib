package table2

import (
	"github.com/donnyhardyanto/dxlib/database2"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/pkg/errors"
)

func (bt *DXBaseTable2) Insert(newKeyValues utils.JSON) (newId int64, newUid string, err error) {
	// Ensure database is initialized
	if err := bt.DbEnsureInitializeConnection(); err != nil {
		return 0, "", err
	}

	var returningFieldNames []string
	if bt.FieldNameForRowId != "" {
		returningFieldNames = append(returningFieldNames, bt.FieldNameForRowId)
	}
	if bt.FieldNameForRowUid != "" {
		returningFieldNames = append(returningFieldNames, bt.FieldNameForRowUid)
	}

	if bt.DoOverrideInsertValues != nil {
		newKeyValues, err = bt.DoOverrideInsertValues(newKeyValues)
		if err != nil {
			return 0, "", err
		}
	}

	_, returningFieldValues, err := bt.Database.Insert(bt.NameId, newKeyValues, returningFieldNames)
	if err != nil {
		return 0, "", err
	}
	var ok bool
	if bt.FieldNameForRowId != "" {
		newId, ok = returningFieldValues[bt.FieldNameForRowId].(int64)
		if !ok {
			return 0, "", errors.New("IMPOSSIBLE:INSERT_NOT_RETURNING_ID")
		}
	}
	if bt.FieldNameForRowUid != "" {
		newUid, ok = returningFieldValues[bt.FieldNameForRowUid].(string)
		if !ok {
			return 0, "", errors.New("IMPOSSIBLE:INSERT_NOT_RETURNING_UID")
		}
	}

	return newId, newUid, nil
}

func (bt *DXBaseTable2) TxInsert(tx *database2.DXDatabaseTx, newKeyValues utils.JSON) (newId int64, newUid string, err error) {
	var returningFieldNames []string
	if bt.FieldNameForRowId != "" {
		returningFieldNames = append(returningFieldNames, bt.FieldNameForRowId)
	}
	if bt.FieldNameForRowUid != "" {
		returningFieldNames = append(returningFieldNames, bt.FieldNameForRowUid)
	}

	_, returningFieldValues, err := tx.Insert(bt.NameId, newKeyValues, returningFieldNames)
	if err != nil {
		return 0, "", err
	}
	var ok bool
	if bt.FieldNameForRowId != "" {
		newId, ok = returningFieldValues[bt.FieldNameForRowId].(int64)
		if !ok {
			return 0, "", errors.New("IMPOSSIBLE:INSERT_NOT_RETURNING_ID")
		}
	}
	if bt.FieldNameForRowUid != "" {
		newUid, ok = returningFieldValues[bt.FieldNameForRowUid].(string)
		if !ok {
			return 0, "", errors.New("IMPOSSIBLE:INSERT_NOT_RETURNING_UID")
		}
	}

	return newId, newUid, nil
}
