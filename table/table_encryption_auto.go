package table

import (
	"database/sql"

	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
)

// ============================================================================
// DXRawTable Auto Encryption Methods
// Uses table's EncryptedColumnDefs and DecryptedColumnDefs
// ============================================================================

// ============================================================================
// Auto Insert Methods
// ============================================================================

// TxInsertAuto inserts using table's EncryptedColumnDefs
// Data fields matching DataFieldName are auto-encrypted to FieldName
func (t *DXRawTable) TxInsertAuto(
	dtx *database.DXDatabaseTx,
	data utils.JSON,
	returningFieldNames []string,
) (sql.Result, utils.JSON, error) {
	if len(t.EncryptedColumnDefs) == 0 {
		// No encryption defined, use regular insert
		return dtx.Insert(t.TableName(), data, returningFieldNames)
	}
	return dtx.InsertWithEncryption(t.TableName(), data, t.EncryptedColumnDefs, returningFieldNames)
}

// InsertAuto inserts using table's EncryptedColumnDefs (creates transaction)
func (t *DXRawTable) InsertAuto(
	l *log.DXLog,
	data utils.JSON,
	returningFieldNames []string,
) (sql.Result, utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}

	if len(t.EncryptedColumnDefs) == 0 {
		return t.Database.Insert(t.TableName(), data, returningFieldNames)
	}

	dtx, err := t.Database.TransactionBegin(database.LevelReadCommitted)
	if err != nil {
		return nil, nil, err
	}
	defer dtx.Finish(l, err)

	return t.TxInsertAuto(dtx, data, returningFieldNames)
}

// TxInsertAutoReturningId inserts and returns the new ID
func (t *DXRawTable) TxInsertAutoReturningId(
	dtx *database.DXDatabaseTx,
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
	l *log.DXLog,
	data utils.JSON,
) (int64, error) {
	_, returningValues, err := t.InsertAuto(l, data, []string{t.FieldNameForRowId})
	if err != nil {
		return 0, err
	}
	newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
	return newId, nil
}

// ============================================================================
// Auto Update Methods
// ============================================================================

// TxUpdateAuto updates using table's EncryptedColumnDefs
func (t *DXRawTable) TxUpdateAuto(
	dtx *database.DXDatabaseTx,
	data utils.JSON,
	where utils.JSON,
	returningFieldNames []string,
) (sql.Result, []utils.JSON, error) {
	if len(t.EncryptedColumnDefs) == 0 {
		return dtx.Update(t.TableName(), data, where, returningFieldNames)
	}
	return dtx.UpdateWithEncryption(t.TableName(), data, t.EncryptedColumnDefs, where, returningFieldNames)
}

// UpdateAuto updates using table's EncryptedColumnDefs (creates transaction)
func (t *DXRawTable) UpdateAuto(
	l *log.DXLog,
	data utils.JSON,
	where utils.JSON,
	returningFieldNames []string,
) (sql.Result, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}

	if len(t.EncryptedColumnDefs) == 0 {
		return t.Database.Update(t.TableName(), data, where, returningFieldNames)
	}

	dtx, err := t.Database.TransactionBegin(database.LevelReadCommitted)
	if err != nil {
		return nil, nil, err
	}
	defer dtx.Finish(l, err)

	return t.TxUpdateAuto(dtx, data, where, returningFieldNames)
}

// TxUpdateByIdAuto updates by ID using table's EncryptedColumnDefs
func (t *DXRawTable) TxUpdateByIdAuto(
	dtx *database.DXDatabaseTx,
	id int64,
	data utils.JSON,
) (sql.Result, error) {
	result, _, err := t.TxUpdateAuto(dtx, data, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// UpdateByIdAuto updates by ID using table's EncryptedColumnDefs
func (t *DXRawTable) UpdateByIdAuto(
	l *log.DXLog,
	id int64,
	data utils.JSON,
) (sql.Result, error) {
	result, _, err := t.UpdateAuto(l, data, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// ============================================================================
// Auto Select Methods
// ============================================================================

// TxSelectAuto selects using table's DecryptedColumnDefs
func (t *DXRawTable) TxSelectAuto(
	dtx *database.DXDatabaseTx,
	columns []string,
	where utils.JSON,
	orderBy *string,
	limit *int,
) ([]utils.JSON, error) {
	if len(t.DecryptedColumnDefs) == 0 {
		// No decryption needed, use regular select
		_, rows, err := dtx.Select(t.ListViewNameId, t.FieldTypeMapping, columns, where, nil, nil, nil, nil, limit, nil, nil)
		return rows, err
	}
	return dtx.SelectWithEncryption(t.ListViewNameId, columns, t.DecryptedColumnDefs, where, orderBy, limit)
}

// SelectAuto selects using table's DecryptedColumnDefs (creates transaction)
func (t *DXRawTable) SelectAuto(
	l *log.DXLog,
	columns []string,
	where utils.JSON,
	orderBy *string,
	limit *int,
) ([]utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, err
	}

	dtx, err := t.Database.TransactionBegin(database.LevelReadCommitted)
	if err != nil {
		return nil, err
	}
	defer dtx.Finish(l, err)

	return t.TxSelectAuto(dtx, columns, where, orderBy, limit)
}

// TxSelectOneAuto selects one row using table's DecryptedColumnDefs
func (t *DXRawTable) TxSelectOneAuto(
	dtx *database.DXDatabaseTx,
	columns []string,
	where utils.JSON,
) (utils.JSON, error) {
	limit := 1
	rows, err := t.TxSelectAuto(dtx, columns, where, nil, &limit)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows[0], nil
}

// SelectOneAuto selects one row using table's DecryptedColumnDefs
func (t *DXRawTable) SelectOneAuto(
	l *log.DXLog,
	columns []string,
	where utils.JSON,
) (utils.JSON, error) {
	limit := 1
	rows, err := t.SelectAuto(l, columns, where, nil, &limit)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows[0], nil
}

// TxSelectByIdAuto selects by ID using table's DecryptedColumnDefs
func (t *DXRawTable) TxSelectByIdAuto(
	dtx *database.DXDatabaseTx,
	id int64,
	columns []string,
) (utils.JSON, error) {
	return t.TxSelectOneAuto(dtx, columns, utils.JSON{t.FieldNameForRowId: id})
}

// SelectByIdAuto selects by ID using table's DecryptedColumnDefs
func (t *DXRawTable) SelectByIdAuto(
	l *log.DXLog,
	id int64,
	columns []string,
) (utils.JSON, error) {
	return t.SelectOneAuto(l, columns, utils.JSON{t.FieldNameForRowId: id})
}

// ============================================================================
// DXTable Auto Methods (with audit fields)
// ============================================================================

// TxInsertAuto inserts with audit fields using table's EncryptedColumnDefs
func (t *DXTable) TxInsertAuto(
	dtx *database.DXDatabaseTx,
	data utils.JSON,
	returningFieldNames []string,
) (sql.Result, utils.JSON, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.TxInsertAuto(dtx, data, returningFieldNames)
}

// InsertAuto inserts with audit fields using table's EncryptedColumnDefs
func (t *DXTable) InsertAuto(
	l *log.DXLog,
	data utils.JSON,
	returningFieldNames []string,
) (sql.Result, utils.JSON, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.InsertAuto(l, data, returningFieldNames)
}

// TxInsertAutoReturningId inserts with audit fields and returns the new ID
func (t *DXTable) TxInsertAutoReturningId(
	dtx *database.DXDatabaseTx,
	data utils.JSON,
) (int64, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.TxInsertAutoReturningId(dtx, data)
}

// InsertAutoReturningId inserts with audit fields and returns the new ID
func (t *DXTable) InsertAutoReturningId(
	l *log.DXLog,
	data utils.JSON,
) (int64, error) {
	t.SetInsertAuditFields(nil, data)
	return t.DXRawTable.InsertAutoReturningId(l, data)
}

// TxUpdateAuto updates with audit fields using table's EncryptedColumnDefs
func (t *DXTable) TxUpdateAuto(
	dtx *database.DXDatabaseTx,
	data utils.JSON,
	where utils.JSON,
	returningFieldNames []string,
) (sql.Result, []utils.JSON, error) {
	t.SetUpdateAuditFields(nil, data)
	return t.DXRawTable.TxUpdateAuto(dtx, data, where, returningFieldNames)
}

// UpdateAuto updates with audit fields using table's EncryptedColumnDefs
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
	dtx *database.DXDatabaseTx,
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

// TxSelectAuto selects using table's DecryptedColumnDefs
func (t *DXTable) TxSelectAuto(
	dtx *database.DXDatabaseTx,
	columns []string,
	where utils.JSON,
	orderBy *string,
	limit *int,
) ([]utils.JSON, error) {
	return t.DXRawTable.TxSelectAuto(dtx, columns, where, orderBy, limit)
}

// SelectAuto selects using table's DecryptedColumnDefs
func (t *DXTable) SelectAuto(
	l *log.DXLog,
	columns []string,
	where utils.JSON,
	orderBy *string,
	limit *int,
) ([]utils.JSON, error) {
	return t.DXRawTable.SelectAuto(l, columns, where, orderBy, limit)
}

// TxSelectOneAuto selects one row using table's DecryptedColumnDefs
func (t *DXTable) TxSelectOneAuto(
	dtx *database.DXDatabaseTx,
	columns []string,
	where utils.JSON,
) (utils.JSON, error) {
	return t.DXRawTable.TxSelectOneAuto(dtx, columns, where)
}

// SelectOneAuto selects one row using table's DecryptedColumnDefs
func (t *DXTable) SelectOneAuto(
	l *log.DXLog,
	columns []string,
	where utils.JSON,
) (utils.JSON, error) {
	return t.DXRawTable.SelectOneAuto(l, columns, where)
}

// TxSelectByIdAuto selects by ID using table's DecryptedColumnDefs
func (t *DXTable) TxSelectByIdAuto(
	dtx *database.DXDatabaseTx,
	id int64,
	columns []string,
) (utils.JSON, error) {
	return t.DXRawTable.TxSelectByIdAuto(dtx, id, columns)
}

// SelectByIdAuto selects by ID using table's DecryptedColumnDefs
func (t *DXTable) SelectByIdAuto(
	l *log.DXLog,
	id int64,
	columns []string,
) (utils.JSON, error) {
	return t.DXRawTable.SelectByIdAuto(l, id, columns)
}
