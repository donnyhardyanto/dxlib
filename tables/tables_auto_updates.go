package tables

import (
	"database/sql"

	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DXRawTable Auto Update Methods

// TxUpdateAuto updates using table's EncryptionColumnDefs and EncryptionKeyDefs
func (t *DXRawTable) TxUpdateAuto(
	dtx *databases.DXDatabaseTx,
	data utils.JSON,
	where utils.JSON,
	returningFieldNames []string,
) (sql.Result, []utils.JSON, error) {
	if len(t.EncryptionColumnDefs) > 0 {
		// Has column-specific encryption, use encrypted update path
		encryptionColumns := t.convertEncryptionColumnDefsForWrite(data)
		return t.TxUpdateWithEncryption(dtx, data, encryptionColumns, where, returningFieldNames)
	}
	if len(t.EncryptionKeyDefs) > 0 {
		// Only session keys needed, set them then regular update
		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, nil, err
		}
	}
	return dtx.Update(t.TableName(), data, where, returningFieldNames)
}

// UpdateAuto updates using table's EncryptionColumnDefs and EncryptionKeyDefs (creates transaction if needed)
func (t *DXRawTable) UpdateAuto(
	l *log.DXLog,
	data utils.JSON,
	where utils.JSON,
	returningFieldNames []string,
) (sql.Result, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}

	if !t.HasEncryptionConfig() {
		// No encryption at all, no transaction needed
		return t.Database.Update(t.TableName(), data, where, returningFieldNames)
	}

	// Encryption configured, need transaction
	dtx, err := t.Database.TransactionBegin(databases.LevelReadCommitted)
	if err != nil {
		return nil, nil, err
	}
	defer dtx.Finish(l, err)

	return t.TxUpdateAuto(dtx, data, where, returningFieldNames)
}

// TxUpdateByIdAuto updates by ID using table's EncryptionColumnDefs
func (t *DXRawTable) TxUpdateByIdAuto(
	dtx *databases.DXDatabaseTx,
	id int64,
	data utils.JSON,
) (sql.Result, error) {
	result, _, err := t.TxUpdateAuto(dtx, data, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// UpdateByIdAuto updates by ID using table's EncryptionColumnDefs
func (t *DXRawTable) UpdateByIdAuto(
	l *log.DXLog,
	id int64,
	data utils.JSON,
) (sql.Result, error) {
	result, _, err := t.UpdateAuto(l, data, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// DXTable Auto Update Methods (with audit fields)

// TxUpdateAuto updates with audit fields using table's EncryptionColumnDefs
func (t *DXTable) TxUpdateAuto(
	dtx *databases.DXDatabaseTx,
	data utils.JSON,
	where utils.JSON,
	returningFieldNames []string,
) (sql.Result, []utils.JSON, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.TxUpdateAuto(dtx, data, where, returningFieldNames)
}

// UpdateAuto updates with audit fields using table's EncryptionColumnDefs
func (t *DXTable) UpdateAuto(
	l *log.DXLog,
	data utils.JSON,
	where utils.JSON,
	returningFieldNames []string,
) (sql.Result, []utils.JSON, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.UpdateAuto(l, data, where, returningFieldNames)
}

// TxUpdateByIdAuto updates by ID with audit fields
func (t *DXTable) TxUpdateByIdAuto(
	dtx *databases.DXDatabaseTx,
	id int64,
	data utils.JSON,
) (sql.Result, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.TxUpdateByIdAuto(dtx, id, data)
}

// UpdateByIdAuto updates by ID with audit fields
func (t *DXTable) UpdateByIdAuto(
	l *log.DXLog,
	id int64,
	data utils.JSON,
) (sql.Result, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.UpdateByIdAuto(l, id, data)
}
