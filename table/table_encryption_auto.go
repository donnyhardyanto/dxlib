package table

import (
	"database/sql"

	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
)

// ============================================================================
// DXRawTable Auto Encryption Methods
// Uses table's EncryptedColumnDefs and DecryptedColumnDefs
// ============================================================================

// TxSetDecryptionSessionKeys sets the PostgreSQL session keys needed for decryption within a transaction
// Call this before executing raw queries on views that use pgp_sym_decrypt
func (t *DXRawTable) TxSetDecryptionSessionKeys(dtx *database.DXDatabaseTx) error {
	if len(t.DecryptedColumnDefs) == 0 {
		log.Log.Debugf("TxSetDecryptionSessionKeys: no DecryptedColumnDefs for table %s", t.TableName())
		return nil
	}

	log.Log.Debugf("TxSetDecryptionSessionKeys: setting keys for table %s with %d defs", t.TableName(), len(t.DecryptedColumnDefs))

	// Collect unique session keys
	sessionKeys := make(map[string]string)
	for _, def := range t.DecryptedColumnDefs {
		if def.SecureMemoryKey != "" && def.SessionKey != "" {
			sessionKeys[def.SessionKey] = def.SecureMemoryKey
			log.Log.Debugf("TxSetDecryptionSessionKeys: will set session key %s from memory key %s", def.SessionKey, def.SecureMemoryKey)
		}
	}

	// Set each session key
	for sessionKey, memoryKey := range sessionKeys {
		log.Log.Debugf("TxSetDecryptionSessionKeys: setting session key %s from secure memory %s", sessionKey, memoryKey)
		if err := dtx.TxSetSessionKeyFromSecureMemory(memoryKey, sessionKey); err != nil {
			log.Log.Errorf(err, "TxSetDecryptionSessionKeys: failed to set session key %s from memory key %s", sessionKey, memoryKey)
			return errors.Wrapf(err, "SET_DECRYPTION_SESSION_KEY_ERROR:%s", sessionKey)
		}
		log.Log.Debugf("TxSetDecryptionSessionKeys: successfully set session key %s", sessionKey)
	}

	return nil
}

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

// TxShouldSelectOneAuto selects one row or returns error if not found, using table's DecryptedColumnDefs
func (t *DXRawTable) TxShouldSelectOneAuto(
	dtx *database.DXDatabaseTx,
	columns []string,
	where utils.JSON,
) (utils.JSON, error) {
	row, err := t.TxSelectOneAuto(dtx, columns, where)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s", t.ListViewNameId)
	}
	return row, nil
}

// ShouldSelectOneAuto selects one row or returns error if not found, using table's DecryptedColumnDefs
func (t *DXRawTable) ShouldSelectOneAuto(
	l *log.DXLog,
	columns []string,
	where utils.JSON,
) (utils.JSON, error) {
	row, err := t.SelectOneAuto(l, columns, where)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s", t.ListViewNameId)
	}
	return row, nil
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

// TxGetByIdAuto returns a row by ID using table's DecryptedColumnDefs within a transaction
func (t *DXRawTable) TxGetByIdAuto(dtx *database.DXDatabaseTx, id int64) (utils.JSON, error) {
	return t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowId: id})
}

// TxShouldGetByIdAuto returns a row by ID or error if not found, using table's DecryptedColumnDefs within a transaction
func (t *DXRawTable) TxShouldGetByIdAuto(dtx *database.DXDatabaseTx, id int64) (utils.JSON, error) {
	row, err := t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowId: id})
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s", t.ListViewNameId)
	}
	return row, nil
}

// GetByIdAuto returns a row by ID using table's DecryptedColumnDefs
func (t *DXRawTable) GetByIdAuto(l *log.DXLog, id int64) (utils.JSON, error) {
	return t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowId: id})
}

// ShouldGetByIdAuto returns a row by ID or error if not found, using table's DecryptedColumnDefs
func (t *DXRawTable) ShouldGetByIdAuto(l *log.DXLog, id int64) (utils.JSON, error) {
	row, err := t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowId: id})
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s", t.ListViewNameId)
	}
	return row, nil
}

// GetByNameIdAuto returns a row by NameId using table's DecryptedColumnDefs
func (t *DXRawTable) GetByNameIdAuto(l *log.DXLog, nameId string) (utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil
	}
	return t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowNameId: nameId})
}

// ShouldGetByNameIdAuto returns a row by NameId or error if not found, using table's DecryptedColumnDefs
func (t *DXRawTable) ShouldGetByNameIdAuto(l *log.DXLog, nameId string) (utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil
	}
	row, err := t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowNameId: nameId})
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s", t.ListViewNameId)
	}
	return row, nil
}

// GetByUidAuto returns a row by UID using table's DecryptedColumnDefs
func (t *DXRawTable) GetByUidAuto(l *log.DXLog, uid string) (utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowUid: uid})
}

// ShouldGetByUidAuto returns a row by UID or error if not found, using table's DecryptedColumnDefs
func (t *DXRawTable) ShouldGetByUidAuto(l *log.DXLog, uid string) (utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, errors.New("FieldNameForRowUid not configured")
	}
	row, err := t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowUid: uid})
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s:uid=%s", t.ListViewNameId, uid)
	}
	return row, nil
}

// GetByUtagAuto returns a row by Utag using table's DecryptedColumnDefs
func (t *DXRawTable) GetByUtagAuto(l *log.DXLog, utag string) (utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowUtag: utag})
}

// ShouldGetByUtagAuto returns a row by Utag or error if not found, using table's DecryptedColumnDefs
func (t *DXRawTable) ShouldGetByUtagAuto(l *log.DXLog, utag string) (utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, errors.New("FieldNameForRowUtag not configured")
	}
	row, err := t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowUtag: utag})
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s:utag=%s", t.ListViewNameId, utag)
	}
	return row, nil
}

// TxGetByUidAuto returns a row by UID using table's DecryptedColumnDefs within a transaction
func (t *DXRawTable) TxGetByUidAuto(dtx *database.DXDatabaseTx, uid string) (utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowUid: uid})
}

// TxShouldGetByUidAuto returns a row by UID or error if not found, using table's DecryptedColumnDefs within a transaction
func (t *DXRawTable) TxShouldGetByUidAuto(dtx *database.DXDatabaseTx, uid string) (utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, errors.New("FieldNameForRowUid not configured")
	}
	row, err := t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowUid: uid})
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s:uid=%s", t.ListViewNameId, uid)
	}
	return row, nil
}

// TxGetByNameIdAuto returns a row by NameId using table's DecryptedColumnDefs within a transaction
func (t *DXRawTable) TxGetByNameIdAuto(dtx *database.DXDatabaseTx, nameId string) (utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil
	}
	return t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowNameId: nameId})
}

// TxShouldGetByNameIdAuto returns a row by NameId or error if not found, using table's DecryptedColumnDefs within a transaction
func (t *DXRawTable) TxShouldGetByNameIdAuto(dtx *database.DXDatabaseTx, nameId string) (utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil
	}
	row, err := t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowNameId: nameId})
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s", t.ListViewNameId)
	}
	return row, nil
}

// TxGetByUtagAuto returns a row by Utag using table's DecryptedColumnDefs within a transaction
func (t *DXRawTable) TxGetByUtagAuto(dtx *database.DXDatabaseTx, utag string) (utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowUtag: utag})
}

// TxShouldGetByUtagAuto returns a row by Utag or error if not found, using table's DecryptedColumnDefs within a transaction
func (t *DXRawTable) TxShouldGetByUtagAuto(dtx *database.DXDatabaseTx, utag string) (utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, errors.New("FieldNameForRowUtag not configured")
	}
	row, err := t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowUtag: utag})
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s:utag=%s", t.ListViewNameId, utag)
	}
	return row, nil
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

// TxShouldSelectOneAuto selects one row or returns error if not found, using table's DecryptedColumnDefs within a transaction
func (t *DXTable) TxShouldSelectOneAuto(
	dtx *database.DXDatabaseTx,
	columns []string,
	where utils.JSON,
) (utils.JSON, error) {
	return t.DXRawTable.TxShouldSelectOneAuto(dtx, columns, where)
}

// ShouldSelectOneAuto selects one row or returns error if not found, using table's DecryptedColumnDefs
func (t *DXTable) ShouldSelectOneAuto(
	l *log.DXLog,
	columns []string,
	where utils.JSON,
) (utils.JSON, error) {
	return t.DXRawTable.ShouldSelectOneAuto(l, columns, where)
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

// TxGetByIdAuto returns a row by ID using table's DecryptedColumnDefs within a transaction
func (t *DXTable) TxGetByIdAuto(dtx *database.DXDatabaseTx, id int64) (utils.JSON, error) {
	return t.DXRawTable.TxGetByIdAuto(dtx, id)
}

// TxShouldGetByIdAuto returns a row by ID or error if not found, using table's DecryptedColumnDefs within a transaction
func (t *DXTable) TxShouldGetByIdAuto(dtx *database.DXDatabaseTx, id int64) (utils.JSON, error) {
	return t.DXRawTable.TxShouldGetByIdAuto(dtx, id)
}

// GetByIdAuto returns a row by ID using table's DecryptedColumnDefs
func (t *DXTable) GetByIdAuto(l *log.DXLog, id int64) (utils.JSON, error) {
	return t.DXRawTable.GetByIdAuto(l, id)
}

// ShouldGetByIdAuto returns a row by ID or error if not found, using table's DecryptedColumnDefs
func (t *DXTable) ShouldGetByIdAuto(l *log.DXLog, id int64) (utils.JSON, error) {
	return t.DXRawTable.ShouldGetByIdAuto(l, id)
}

// GetByIdNotDeletedAuto returns a non-deleted row by ID using table's DecryptedColumnDefs
func (t *DXTable) GetByIdNotDeletedAuto(l *log.DXLog, id int64) (utils.JSON, error) {
	return t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowId: id})
}

// ShouldGetByIdNotDeletedAuto returns a non-deleted row by ID or error if not found
func (t *DXTable) ShouldGetByIdNotDeletedAuto(l *log.DXLog, id int64) (utils.JSON, error) {
	row, err := t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowId: id})
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s", t.ListViewNameId)
	}
	return row, nil
}

// TxGetByIdNotDeletedAuto returns a non-deleted row by ID using table's DecryptedColumnDefs within a transaction
func (t *DXTable) TxGetByIdNotDeletedAuto(dtx *database.DXDatabaseTx, id int64) (utils.JSON, error) {
	return t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowId: id})
}

// TxShouldGetByIdNotDeletedAuto returns a non-deleted row by ID or error if not found within a transaction
func (t *DXTable) TxShouldGetByIdNotDeletedAuto(dtx *database.DXDatabaseTx, id int64) (utils.JSON, error) {
	row, err := t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowId: id})
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s", t.ListViewNameId)
	}
	return row, nil
}

// GetByNameIdAuto returns a row by NameId using table's DecryptedColumnDefs
func (t *DXTable) GetByNameIdAuto(l *log.DXLog, nameId string) (utils.JSON, error) {
	return t.DXRawTable.GetByNameIdAuto(l, nameId)
}

// ShouldGetByNameIdAuto returns a row by NameId or error if not found
func (t *DXTable) ShouldGetByNameIdAuto(l *log.DXLog, nameId string) (utils.JSON, error) {
	return t.DXRawTable.ShouldGetByNameIdAuto(l, nameId)
}

// GetByUidAuto returns a row by UID using table's DecryptedColumnDefs
func (t *DXTable) GetByUidAuto(l *log.DXLog, uid string) (utils.JSON, error) {
	return t.DXRawTable.GetByUidAuto(l, uid)
}

// ShouldGetByUidAuto returns a row by UID or error if not found
func (t *DXTable) ShouldGetByUidAuto(l *log.DXLog, uid string) (utils.JSON, error) {
	return t.DXRawTable.ShouldGetByUidAuto(l, uid)
}

// GetByUidNotDeletedAuto returns a non-deleted row by UID using table's DecryptedColumnDefs
func (t *DXTable) GetByUidNotDeletedAuto(l *log.DXLog, uid string) (utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowUid: uid})
}

// ShouldGetByUidNotDeletedAuto returns a non-deleted row by UID or error if not found
func (t *DXTable) ShouldGetByUidNotDeletedAuto(l *log.DXLog, uid string) (utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, errors.New("FieldNameForRowUid not configured")
	}
	row, err := t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowUid: uid})
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s:uid=%s", t.ListViewNameId, uid)
	}
	return row, nil
}

// TxGetByUidAuto returns a row by UID using table's DecryptedColumnDefs within a transaction
func (t *DXTable) TxGetByUidAuto(dtx *database.DXDatabaseTx, uid string) (utils.JSON, error) {
	return t.DXRawTable.TxGetByUidAuto(dtx, uid)
}

// TxShouldGetByUidAuto returns a row by UID or error if not found, using table's DecryptedColumnDefs within a transaction
func (t *DXTable) TxShouldGetByUidAuto(dtx *database.DXDatabaseTx, uid string) (utils.JSON, error) {
	return t.DXRawTable.TxShouldGetByUidAuto(dtx, uid)
}

// TxGetByUidNotDeletedAuto returns a non-deleted row by UID using table's DecryptedColumnDefs within a transaction
func (t *DXTable) TxGetByUidNotDeletedAuto(dtx *database.DXDatabaseTx, uid string) (utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowUid: uid})
}

// TxShouldGetByUidNotDeletedAuto returns a non-deleted row by UID or error if not found within a transaction
func (t *DXTable) TxShouldGetByUidNotDeletedAuto(dtx *database.DXDatabaseTx, uid string) (utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, errors.New("FieldNameForRowUid not configured")
	}
	row, err := t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowUid: uid})
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s:uid=%s", t.ListViewNameId, uid)
	}
	return row, nil
}

// TxGetByNameIdAuto returns a row by NameId using table's DecryptedColumnDefs within a transaction
func (t *DXTable) TxGetByNameIdAuto(dtx *database.DXDatabaseTx, nameId string) (utils.JSON, error) {
	return t.DXRawTable.TxGetByNameIdAuto(dtx, nameId)
}

// TxShouldGetByNameIdAuto returns a row by NameId or error if not found, using table's DecryptedColumnDefs within a transaction
func (t *DXTable) TxShouldGetByNameIdAuto(dtx *database.DXDatabaseTx, nameId string) (utils.JSON, error) {
	return t.DXRawTable.TxShouldGetByNameIdAuto(dtx, nameId)
}

// GetByNameIdNotDeletedAuto returns a non-deleted row by NameId using table's DecryptedColumnDefs
func (t *DXTable) GetByNameIdNotDeletedAuto(l *log.DXLog, nameId string) (utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil
	}
	return t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowNameId: nameId})
}

// ShouldGetByNameIdNotDeletedAuto returns a non-deleted row by NameId or error if not found
func (t *DXTable) ShouldGetByNameIdNotDeletedAuto(l *log.DXLog, nameId string) (utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil
	}
	row, err := t.SelectOneAuto(l, nil, utils.JSON{t.FieldNameForRowNameId: nameId})
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s", t.ListViewNameId)
	}
	return row, nil
}

// TxGetByNameIdNotDeletedAuto returns a non-deleted row by NameId using table's DecryptedColumnDefs within a transaction
func (t *DXTable) TxGetByNameIdNotDeletedAuto(dtx *database.DXDatabaseTx, nameId string) (utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil
	}
	return t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowNameId: nameId})
}

// TxShouldGetByNameIdNotDeletedAuto returns a non-deleted row by NameId or error if not found within a transaction
func (t *DXTable) TxShouldGetByNameIdNotDeletedAuto(dtx *database.DXDatabaseTx, nameId string) (utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil
	}
	row, err := t.TxSelectOneAuto(dtx, nil, utils.JSON{t.FieldNameForRowNameId: nameId})
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s", t.ListViewNameId)
	}
	return row, nil
}

// GetByUtagAuto returns a row by Utag using table's DecryptedColumnDefs
func (t *DXTable) GetByUtagAuto(l *log.DXLog, utag string) (utils.JSON, error) {
	return t.DXRawTable.GetByUtagAuto(l, utag)
}

// ShouldGetByUtagAuto returns a row by Utag or error if not found, using table's DecryptedColumnDefs
func (t *DXTable) ShouldGetByUtagAuto(l *log.DXLog, utag string) (utils.JSON, error) {
	return t.DXRawTable.ShouldGetByUtagAuto(l, utag)
}

// TxGetByUtagAuto returns a row by Utag using table's DecryptedColumnDefs within a transaction
func (t *DXTable) TxGetByUtagAuto(dtx *database.DXDatabaseTx, utag string) (utils.JSON, error) {
	return t.DXRawTable.TxGetByUtagAuto(dtx, utag)
}

// TxShouldGetByUtagAuto returns a row by Utag or error if not found, using table's DecryptedColumnDefs within a transaction
func (t *DXTable) TxShouldGetByUtagAuto(dtx *database.DXDatabaseTx, utag string) (utils.JSON, error) {
	return t.DXRawTable.TxShouldGetByUtagAuto(dtx, utag)
}

// ============================================================================
// DXRawTable Paging Auto Methods
// ============================================================================

// PagingAuto executes paging query using table's DecryptedColumnDefs
// It sets up encryption session keys before executing the query
func (t *DXRawTable) PagingAuto(
	l *log.DXLog,
	rowPerPage, pageIndex int64,
	whereClause string,
	whereArgs utils.JSON,
	orderBy string,
) (*PagingResult, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, err
	}

	// If no decryption needed, use regular paging
	if len(t.DecryptedColumnDefs) == 0 {
		return t.Paging(l, rowPerPage, pageIndex, whereClause, orderBy, whereArgs)
	}

	// Convert DecryptedColumnDefs to DecryptedColumn
	decryptedColumns := make([]DecryptedColumn, len(t.DecryptedColumnDefs))
	for i, def := range t.DecryptedColumnDefs {
		decryptedColumns[i] = DecryptedColumn{
			FieldName:       def.FieldName,
			AliasName:       def.AliasName,
			SecureMemoryKey: def.SecureMemoryKey,
			SessionKey:      def.SessionKey,
			ViewHasDecrypt:  def.ViewHasDecrypt,
		}
	}

	return t.PagingWithEncryption(l, nil, decryptedColumns, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
}

// ============================================================================
// DXTable Paging Auto Methods
// ============================================================================

// PagingAuto executes paging query using table's DecryptedColumnDefs
func (t *DXTable) PagingAuto(
	l *log.DXLog,
	rowPerPage, pageIndex int64,
	whereClause string,
	whereArgs utils.JSON,
	orderBy string,
) (*PagingResult, error) {
	return t.DXRawTable.PagingAuto(l, rowPerPage, pageIndex, whereClause, whereArgs, orderBy)
}
