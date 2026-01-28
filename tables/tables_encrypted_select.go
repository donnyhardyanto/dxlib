package tables

import (
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/database/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
)

// DXRawTable Encrypted Select Methods

// SelectWithEncryption selects with decrypted columns (creates transaction internally)
func (t *DXRawTable) SelectWithEncryption(l *log.DXLog, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}

	dtx, err := t.Database.TransactionBegin(database.LevelReadCommitted)
	if err != nil {
		return nil, nil, err
	}
	defer dtx.Finish(l, err)

	return t.TxSelectWithEncryption(dtx, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, limit, forUpdatePart)
}

// SelectOneWithEncryption selects one row with decrypted columns
func (t *DXRawTable) SelectOneWithEncryption(l *log.DXLog, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	rowsInfo, rows, err := t.SelectWithEncryption(l, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, 1, nil)
	if err != nil {
		return rowsInfo, nil, err
	}
	if len(rows) == 0 {
		return rowsInfo, nil, nil
	}
	return rowsInfo, rows[0], nil
}

// ShouldSelectOneWithEncryption selects one row or returns error if not found
func (t *DXRawTable) ShouldSelectOneWithEncryption(l *log.DXLog, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	rowsInfo, row, err := t.SelectOneWithEncryption(l, fieldNames, encryptionColumns, where, joinSQLPart, orderBy)
	if err != nil {
		return rowsInfo, nil, err
	}
	if row == nil {
		return rowsInfo, nil, errors.Errorf("ROW_SHOULD_EXIST_BUT_NOT_FOUND:%s", t.ListViewNameId)
	}
	return rowsInfo, row, nil
}

// SelectByIdWithEncryption selects by ID with decrypted columns
func (t *DXRawTable) SelectByIdWithEncryption(l *log.DXLog, id int64, fieldNames []string,
	encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.SelectOneWithEncryption(l, fieldNames, encryptionColumns, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// GetByIdWithEncryption returns a row by ID with decrypted columns
func (t *DXRawTable) GetByIdWithEncryption(l *log.DXLog, id int64, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.SelectOneWithEncryption(l, nil, encryptionColumns, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// ShouldGetByIdWithEncryption returns a row by ID or error if not found
func (t *DXRawTable) ShouldGetByIdWithEncryption(l *log.DXLog, id int64, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.ShouldSelectOneWithEncryption(l, nil, encryptionColumns, utils.JSON{t.FieldNameForRowId: id}, nil, nil)
}

// GetByUidWithEncryption returns a row by UID with decrypted columns
func (t *DXRawTable) GetByUidWithEncryption(l *log.DXLog, uid string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.SelectOneWithEncryption(l, nil, encryptionColumns, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// ShouldGetByUidWithEncryption returns a row by UID or error if not found
func (t *DXRawTable) ShouldGetByUidWithEncryption(l *log.DXLog, uid string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUid == "" {
		return nil, nil, errors.New("FieldNameForRowUid not configured")
	}
	return t.ShouldSelectOneWithEncryption(l, nil, encryptionColumns, utils.JSON{t.FieldNameForRowUid: uid}, nil, nil)
}

// GetByNameIdWithEncryption returns a row by NameId with decrypted columns
func (t *DXRawTable) GetByNameIdWithEncryption(l *log.DXLog, nameId string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.SelectOneWithEncryption(l, nil, encryptionColumns, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// ShouldGetByNameIdWithEncryption returns a row by NameId or error if not found
func (t *DXRawTable) ShouldGetByNameIdWithEncryption(l *log.DXLog, nameId string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowNameId == "" {
		return nil, nil, errors.New("FieldNameForRowNameId not configured")
	}
	return t.ShouldSelectOneWithEncryption(l, nil, encryptionColumns, utils.JSON{t.FieldNameForRowNameId: nameId}, nil, nil)
}

// GetByUtagWithEncryption returns a row by Utag with decrypted columns
func (t *DXRawTable) GetByUtagWithEncryption(l *log.DXLog, utag string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.SelectOneWithEncryption(l, nil, encryptionColumns, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil)
}

// ShouldGetByUtagWithEncryption returns a row by Utag or error if not found
func (t *DXRawTable) ShouldGetByUtagWithEncryption(l *log.DXLog, utag string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if t.FieldNameForRowUtag == "" {
		return nil, nil, errors.New("FieldNameForRowUtag not configured")
	}
	return t.ShouldSelectOneWithEncryption(l, nil, encryptionColumns, utils.JSON{t.FieldNameForRowUtag: utag}, nil, nil)
}

// DXRawTable Encrypted Paging Methods

// PagingWithEncryption executes paging query with decrypted columns
func (t *DXRawTable) PagingWithEncryption(
	l *log.DXLog,
	columns []string,
	encryptionColumns []EncryptionColumn,
	whereClause string,
	whereArgs utils.JSON,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, err
	}

	dtx, err := t.Database.TransactionBegin(database.LevelReadCommitted)
	if err != nil {
		return nil, err
	}
	defer dtx.Finish(l, err)

	return t.TxPagingWithEncryption(dtx, columns, encryptionColumns, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
}

// PagingWithEncryptionAndBuilder executes paging with QueryBuilder and decrypted columns
func (t *DXRawTable) PagingWithEncryptionAndBuilder(
	l *log.DXLog,
	columns []string,
	encryptionColumns []EncryptionColumn,
	qb *QueryBuilder,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	whereClause, whereArgs := qb.Build()
	return t.PagingWithEncryption(l, columns, encryptionColumns, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
}

// DXTable Encrypted Select Methods (delegates to DXRawTable)

// SelectWithEncryption selects with decrypted columns
func (t *DXTable) SelectWithEncryption(l *log.DXLog, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return t.DXRawTable.SelectWithEncryption(l, fieldNames, encryptionColumns, where, joinSQLPart, orderBy, limit, forUpdatePart)
}

// SelectOneWithEncryption selects one row with decrypted columns
func (t *DXTable) SelectOneWithEncryption(l *log.DXLog, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.SelectOneWithEncryption(l, fieldNames, encryptionColumns, where, joinSQLPart, orderBy)
}

// ShouldSelectOneWithEncryption selects one row or returns error if not found
func (t *DXTable) ShouldSelectOneWithEncryption(l *log.DXLog, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.ShouldSelectOneWithEncryption(l, fieldNames, encryptionColumns, where, joinSQLPart, orderBy)
}

// SelectByIdWithEncryption selects by ID with decrypted columns
func (t *DXTable) SelectByIdWithEncryption(l *log.DXLog, id int64, fieldNames []string,
	encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.SelectByIdWithEncryption(l, id, fieldNames, encryptionColumns)
}

// GetByIdWithEncryption returns a row by ID with decrypted columns
func (t *DXTable) GetByIdWithEncryption(l *log.DXLog, id int64, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.GetByIdWithEncryption(l, id, encryptionColumns)
}

// ShouldGetByIdWithEncryption returns a row by ID or error if not found
func (t *DXTable) ShouldGetByIdWithEncryption(l *log.DXLog, id int64, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.ShouldGetByIdWithEncryption(l, id, encryptionColumns)
}

// GetByUidWithEncryption returns a row by UID with decrypted columns
func (t *DXTable) GetByUidWithEncryption(l *log.DXLog, uid string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.GetByUidWithEncryption(l, uid, encryptionColumns)
}

// ShouldGetByUidWithEncryption returns a row by UID or error if not found
func (t *DXTable) ShouldGetByUidWithEncryption(l *log.DXLog, uid string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.ShouldGetByUidWithEncryption(l, uid, encryptionColumns)
}

// GetByNameIdWithEncryption returns a row by NameId with decrypted columns
func (t *DXTable) GetByNameIdWithEncryption(l *log.DXLog, nameId string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.GetByNameIdWithEncryption(l, nameId, encryptionColumns)
}

// ShouldGetByNameIdWithEncryption returns a row by NameId or error if not found
func (t *DXTable) ShouldGetByNameIdWithEncryption(l *log.DXLog, nameId string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.ShouldGetByNameIdWithEncryption(l, nameId, encryptionColumns)
}

// GetByUtagWithEncryption returns a row by Utag with decrypted columns
func (t *DXTable) GetByUtagWithEncryption(l *log.DXLog, utag string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.GetByUtagWithEncryption(l, utag, encryptionColumns)
}

// ShouldGetByUtagWithEncryption returns a row by Utag or error if not found
func (t *DXTable) ShouldGetByUtagWithEncryption(l *log.DXLog, utag string, encryptionColumns []EncryptionColumn) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return t.DXRawTable.ShouldGetByUtagWithEncryption(l, utag, encryptionColumns)
}

// DXTable Encrypted Paging Methods (delegates to DXRawTable)

// PagingWithEncryption executes paging query with decrypted columns
func (t *DXTable) PagingWithEncryption(
	l *log.DXLog,
	columns []string,
	encryptionColumns []EncryptionColumn,
	whereClause string,
	whereArgs utils.JSON,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	return t.DXRawTable.PagingWithEncryption(l, columns, encryptionColumns, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
}

// PagingWithEncryptionAndBuilder executes paging with QueryBuilder and decrypted columns
func (t *DXTable) PagingWithEncryptionAndBuilder(
	l *log.DXLog,
	columns []string,
	encryptionColumns []EncryptionColumn,
	qb *QueryBuilder,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	return t.DXRawTable.PagingWithEncryptionAndBuilder(l, columns, encryptionColumns, qb, orderBy, rowPerPage, pageIndex)
}

// Internal Select Helper Functions

// setSessionKeysForDecryption sets all unique session keys from secure memory for SELECT
func setSessionKeysForDecryption(dtx *database.DXDatabaseTx, encryptionColumns []EncryptionColumn) error {
	sessionKeys := make(map[string]string) // sessionKey -> secureMemoryKey

	for _, col := range encryptionColumns {
		if col.EncryptionKeyDef != nil && col.EncryptionKeyDef.SecureMemoryKey != "" && col.EncryptionKeyDef.SessionKey != "" {
			sessionKeys[col.EncryptionKeyDef.SessionKey] = col.EncryptionKeyDef.SecureMemoryKey
		}
	}

	for sessionKey, memoryKey := range sessionKeys {
		if err := dtx.TxSetSessionKeyFromSecureMemory(memoryKey, sessionKey); err != nil {
			return errors.Wrapf(err, "ENCRYPTED_SELECT_SET_SESSION_KEY_ERROR:%s", sessionKey)
		}
	}

	return nil
}

// decryptExpression returns database-specific decryption SQL expression
func decryptExpression(dbType base.DXDatabaseType, fieldName string, sessionKey string) string {
	keyExpr := sessionKeyExpression(dbType, sessionKey)

	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return fmt.Sprintf("pgp_sym_decrypt(%s, %s)", fieldName, keyExpr)
	case base.DXDatabaseTypeSQLServer:
		return fmt.Sprintf("CONVERT(VARCHAR(MAX), DECRYPTBYPASSPHRASE(%s, %s))", keyExpr, fieldName)
	case base.DXDatabaseTypeOracle:
		return fmt.Sprintf("UTL_RAW.CAST_TO_VARCHAR2(%s)", fieldName)
	case base.DXDatabaseTypeMariaDB:
		return fmt.Sprintf("AES_DECRYPT(%s, %s)", fieldName, keyExpr)
	default:
		return fieldName
	}
}

// buildSelectColumns builds SELECT column list with decryption expressions
func buildSelectColumns(dbType base.DXDatabaseType, columns []string, encryptionColumns []EncryptionColumn) string {
	var selectCols []string

	// Add regular columns (or * if empty)
	if len(columns) == 0 {
		selectCols = append(selectCols, "*")
	} else {
		for _, col := range columns {
			selectCols = append(selectCols, col)
		}
	}

	// Add decrypted columns
	for _, col := range encryptionColumns {
		if col.ViewHasDecrypt {
			// View already has decryption, just select the alias
			selectCols = append(selectCols, col.AliasName)
		} else {
			// Build decrypt expression
			expr := decryptExpression(dbType, col.FieldName, col.EncryptionKeyDef.SessionKey)
			selectCols = append(selectCols, fmt.Sprintf("%s AS %s", expr, col.AliasName))
		}
	}

	return strings.Join(selectCols, ", ")
}

// orderByToString converts DXDatabaseTableFieldsOrderBy to string
func orderByToString(orderBy db.DXDatabaseTableFieldsOrderBy) string {
	if orderBy == nil || len(orderBy) == 0 {
		return ""
	}
	var parts []string
	for field, direction := range orderBy {
		parts = append(parts, fmt.Sprintf("%s %s", field, direction))
	}
	return strings.Join(parts, ", ")
}

// limitToInt converts limit any to int
func limitToInt(limit any) int {
	if limit == nil {
		return 0
	}
	switch v := limit.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case int32:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

// executeEncryptedSelect builds and executes SELECT with decrypted columns
func executeEncryptedSelect(
	dtx *database.DXDatabaseTx,
	tableName string,
	fieldTypeMapping db.DXDatabaseTableFieldTypeMapping,
	dbType base.DXDatabaseType,
	fieldNames []string,
	encryptionColumns []EncryptionColumn,
	where utils.JSON,
	joinSQLPart any,
	orderBy db.DXDatabaseTableFieldsOrderBy,
	limit any,
	forUpdatePart any,
) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {

	selectCols := buildSelectColumns(dbType, fieldNames, encryptionColumns)

	// Build WHERE clause
	var whereClauses []string
	var args []any
	argIndex := 1

	for fieldName, value := range where {
		if value == nil {
			whereClauses = append(whereClauses, fieldName+" IS NULL")
		} else if sqlExpr, ok := value.(db.SQLExpression); ok {
			whereClauses = append(whereClauses, sqlExpr.String())
		} else {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", fieldName, placeholder(dbType, argIndex)))
			args = append(args, value)
			argIndex++
		}
	}

	// Build SQL
	sqlStr := fmt.Sprintf("SELECT"+" %s FROM %s", selectCols, tableName)

	// Add JOIN if specified
	if joinSQLPart != nil {
		if joinStr, ok := joinSQLPart.(string); ok && joinStr != "" {
			sqlStr += " " + joinStr
		}
	}

	if len(whereClauses) > 0 {
		sqlStr += " WHERE " + strings.Join(whereClauses, " AND ")
	}

	orderByStr := orderByToString(orderBy)
	if orderByStr != "" {
		sqlStr += " ORDER BY " + orderByStr
	}

	limitInt := limitToInt(limit)
	if limitInt > 0 {
		sqlStr += fmt.Sprintf(" LIMIT %d", limitInt)
	}

	// Add FOR UPDATE if specified
	if forUpdatePart != nil {
		if forUpdateStr, ok := forUpdatePart.(string); ok && forUpdateStr != "" {
			sqlStr += " " + forUpdateStr
		} else if forUpdateBool, ok := forUpdatePart.(bool); ok && forUpdateBool {
			sqlStr += " FOR UPDATE"
		}
	}

	// Execute
	rows, err := dtx.Tx.Queryx(sqlStr, args...)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "ENCRYPTED_SELECT_ERROR")
	}
	defer func(rows *sqlx.Rows) {
		err := rows.Close()
		if err != nil {
			log.Log.Error("ERROR_IN_ROWS_CLOSE", err)
		}
	}(rows)

	// Get column info
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, errors.Wrapf(err, "ENCRYPTED_SELECT_COLUMNS_ERROR")
	}

	rowsInfo := &db.DXDatabaseTableRowsInfo{
		Columns: columns,
	}

	var results []utils.JSON
	for rows.Next() {
		row := make(map[string]any)
		if err := rows.MapScan(row); err != nil {
			return rowsInfo, nil, errors.Wrapf(err, "ENCRYPTED_SELECT_SCAN_ERROR")
		}
		// Convert []byte to string for decrypted text fields
		for k, v := range row {
			if b, ok := v.([]byte); ok {
				row[k] = string(b)
			}
		}
		results = append(results, row)
	}

	return rowsInfo, results, nil
}

// executeEncryptedPaging builds and executes paging query with decrypted columns
func executeEncryptedPaging(
	dtx *database.DXDatabaseTx,
	tableName string,
	dbType base.DXDatabaseType,
	columns []string,
	encryptionColumns []EncryptionColumn,
	whereClause string,
	whereArgs utils.JSON,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {

	selectCols := buildSelectColumns(dbType, columns, encryptionColumns)

	// Count total rows
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM"+" %s", tableName)
	if whereClause != "" {
		countSQL += " WHERE " + whereClause
	}

	var totalRows int64
	row := dtx.Tx.QueryRowx(countSQL)
	if err := row.Scan(&totalRows); err != nil {
		return nil, errors.Wrapf(err, "ENCRYPTED_PAGING_COUNT_ERROR")
	}

	// Calculate pagination
	offset := (pageIndex - 1) * rowPerPage
	if offset < 0 {
		offset = 0
	}

	totalPages := totalRows / rowPerPage
	if totalRows%rowPerPage > 0 {
		totalPages++
	}

	// Build SELECT with paging
	sqlStr := fmt.Sprintf("SELECT"+" %s FROM %s", selectCols, tableName)

	if whereClause != "" {
		sqlStr += " WHERE " + whereClause
	}

	if orderBy != "" {
		sqlStr += " ORDER BY " + orderBy
	}

	sqlStr += fmt.Sprintf(" LIMIT %d OFFSET %d", rowPerPage, offset)

	// Execute
	rows, err := dtx.Tx.NamedQuery(sqlStr, whereArgs)
	if err != nil {
		return nil, errors.Wrapf(err, "ENCRYPTED_PAGING_QUERY_ERROR")
	}
	defer func(rows *sqlx.Rows) {
		err := rows.Close()
		if err != nil {
			log.Log.Error("ERROR_IN_ROWS_CLOSE", err)
		}
	}(rows)

	// Get column info from result set
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrapf(err, "ENCRYPTED_PAGING_COLUMNS_ERROR")
	}
	rowsInfo := &db.DXDatabaseTableRowsInfo{
		Columns: columnNames,
	}

	var results []utils.JSON
	for rows.Next() {
		row := make(map[string]any)
		if err := rows.MapScan(row); err != nil {
			return nil, errors.Wrapf(err, "ENCRYPTED_PAGING_SCAN_ERROR")
		}
		// Convert []byte to string for decrypted text fields
		for k, v := range row {
			if b, ok := v.([]byte); ok {
				row[k] = string(b)
			}
		}
		results = append(results, row)
	}

	return &PagingResult{
		RowsInfo:   rowsInfo,
		Rows:       results,
		TotalRows:  totalRows,
		TotalPages: totalPages,
	}, nil
}
