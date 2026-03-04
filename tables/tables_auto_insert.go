package tables

import (
	"context"
	"database/sql"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
)

func newUniqueFieldViolationError(tableName string, data utils.JSON) *api.ErrUniqueFieldViolation {
	fields := make([]string, 0, len(data))
	for fieldName := range data {
		fields = append(fields, fieldName)
	}
	return &api.ErrUniqueFieldViolation{
		TableName: tableName,
		Fields:    fields,
		Values:    data,
	}
}

// DXRawTable Auto Insert Methods

// TxInsertAuto inserts using table's EncryptionColumnDefs and EncryptionKeyDefs
// Data fields matching DataFieldName are auto-encrypted to FieldName
func (t *DXRawTable) TxInsertAuto(
	dtx *databases.DXDatabaseTx,
	data utils.JSON,
	returningFieldNames []string,
) (sql.Result, utils.JSON, error) {
	// Always set ALL encryption session keys upfront when any encryption config exists.
	// This ensures DB triggers/functions that check session keys (e.g. app.encryption_key)
	// work correctly, even if the current data doesn't contain all encrypted fields.
	if t.HasEncryptionConfig() {
		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, nil, err
		}
	}
	if len(t.EncryptionColumnDefs) > 0 {
		encryptionColumns := t.convertEncryptionColumnDefsForWrite(data)
		return t.TxInsertWithEncryption(dtx, data, encryptionColumns, returningFieldNames)
	}
	return dtx.Insert(dtx.Ctx, t.GetFullTableName(), data, returningFieldNames)
}

func (t *DXRawTable) TxCheckValidationUniqueFieldNameGroupsForInsert(dtx *databases.DXDatabaseTx, data utils.JSON) (err error) {
	for _, v := range t.ValidationUniqueFieldNameGroups {
		k := utils.JSON{}
		for _, f := range v {
			val, ok := data[f] // Use local variables for the 'comma-ok' check
			if !ok {
				return errors.Errorf("CHECK_VALIDATION_UNIQUE_FIELD_NAME_GROUPS_FOR_INSERT:MISSING_REQUIRED_FIELD_IN:TABLE=%s,FIELD=%s", t.GetFullTableName(), f)
			}
			k[f] = val
		}

		// Ensure c and err are properly captured
		c, err := db.TxCount(dtx.Ctx, dtx.Tx, t.GetFullTableName(), "", k, nil, nil, nil, "")
		if err != nil {
			return err
		}

		if c > 0 {
			return newUniqueFieldViolationError(t.GetFullTableName(), k)
		}
	}
	return nil
}

func (t *DXRawTable) TxCheckValidationUniqueFieldNameGroupsForUpdate(dtx *databases.DXDatabaseTx, id any, data utils.JSON) (err error) {
	for _, v := range t.ValidationUniqueFieldNameGroups {
		k := utils.JSON{}
		for _, f := range v {
			val, ok := data[f]
			if !ok {
				return errors.Errorf("CHECK_VALIDATION_UNIQUE_FIELD_NAME_GROUPS_FOR_UPDATE:MISSING_REQUIRED_FIELD_IN:TABLE=%s,FIELDNAME=%s", t.GetFullTableName(), f)
			}
			k[f] = val
		}

		_, d, err := db.TxSelect(dtx.Ctx, dtx.Tx, t.GetFullTableName(), nil, []string{t.FieldNameForRowId}, k, nil, nil, nil, nil, nil, nil, nil)
		if err != nil {
			return err
		}

		if len(d) > 1 {
			return newUniqueFieldViolationError(t.GetFullTableName(), k)
		}

		if len(d) == 1 {
			row := d[0]
			dbId, ok := row[t.FieldNameForRowId]
			if !ok {
				return errors.Errorf("SHOULD_NOT_HAPPEN:FIELDNAMEFORROWID_NOT_FOUND:TABLE=%s,FIELDNAMEGROUP=%v", t.GetFullTableName(), k)
			}

			// Use the smart ValueMatch helper
			if !utils.IsValuesMatch(dbId, id) {
				return newUniqueFieldViolationError(t.GetFullTableName(), k)
			}
		}
	}
	return nil
}

// InsertAuto inserts using table's EncryptionColumnDefs and EncryptionKeyDefs (creates transaction if needed)
func (t *DXRawTable) InsertAuto(
	ctx context.Context,
	l *log.DXLog,
	data utils.JSON,
	returningFieldNames []string,
) (result sql.Result, returning utils.JSON, err error) {
	if err = t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}

	if !t.HasEncryptionConfig() {
		// No encryption at all, no transaction needed
		return t.Database.Insert(ctx, t.GetFullTableName(), data, returningFieldNames)
	}

	// Encryption configured, need transaction
	txErr := t.Database.Tx(ctx, l, databases.LevelReadCommitted, func(dtx *databases.DXDatabaseTx) error {
		err = t.TxCheckValidationUniqueFieldNameGroupsForInsert(dtx, data)
		if err != nil {
			return err
		}

		result, returning, err = t.TxInsertAuto(dtx, data, returningFieldNames)
		return err
	})
	if txErr != nil {
		return nil, nil, txErr
	}
	return result, returning, nil
}

// TxInsertAutoReturningId inserts and returns the new ID
func (t *DXRawTable) TxInsertAutoReturningId(
	dtx *databases.DXDatabaseTx,
	data utils.JSON,
) (int64, error) {
	_, returningValues, err := t.TxInsertAuto(dtx, data, []string{t.FieldNameForRowId})
	if err != nil {
		return 0, err
	}
	newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
	return newId, nil
}

// InsertAutoReturningId inserts and returns the new ID
func (t *DXRawTable) InsertAutoReturningId(
	ctx context.Context,
	l *log.DXLog,
	data utils.JSON,
) (int64, error) {
	_, returningValues, err := t.InsertAuto(ctx, l, data, []string{t.FieldNameForRowId})
	if err != nil {
		return 0, err
	}
	newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
	return newId, nil
}

// DXTable Auto Insert Methods (with audit fields)

// TxInsertAuto inserts with audit fields using table's EncryptionColumnDefs
func (t *DXTable) TxInsertAuto(
	dtx *databases.DXDatabaseTx,
	data utils.JSON,
	returningFieldNames []string,
) (sql.Result, utils.JSON, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.TxInsertAuto(dtx, data, returningFieldNames)
}

// InsertAuto inserts with audit fields using table's EncryptionColumnDefs
func (t *DXTable) InsertAuto(
	ctx context.Context,
	l *log.DXLog,
	data utils.JSON,
	returningFieldNames []string,
) (sql.Result, utils.JSON, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.InsertAuto(ctx, l, data, returningFieldNames)
}

// TxInsertAutoReturningId inserts with audit fields and returns the new ID
func (t *DXTable) TxInsertAutoReturningId(
	dtx *databases.DXDatabaseTx,
	data utils.JSON,
) (int64, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.TxInsertAutoReturningId(dtx, data)
}

// InsertAutoReturningId inserts with audit fields and returns the new ID
func (t *DXTable) InsertAutoReturningId(
	ctx context.Context,
	l *log.DXLog,
	data utils.JSON,
) (int64, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.InsertAutoReturningId(ctx, l, data)
}
