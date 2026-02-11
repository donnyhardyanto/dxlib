package tables

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/databases/db/query"
	"github.com/donnyhardyanto/dxlib/databases/db/query/builder"
	utils2 "github.com/donnyhardyanto/dxlib/databases/db/query/utils"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	tableQueryBuilder "github.com/donnyhardyanto/dxlib/tables/query_builder"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DXRawTable Encrypted Select Methods

// SelectWithEncryption selects with decrypted columns (creates transaction internally)
func (t *DXRawTable) SelectWithEncryption(l *log.DXLog, fieldNames []string, encryptionColumns []EncryptionColumn,
	where utils.JSON, joinSQLPart any, orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}

	dtx, err := t.Database.TransactionBegin(databases.LevelReadCommitted)
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

	dtx, err := t.Database.TransactionBegin(databases.LevelReadCommitted)
	if err != nil {
		return nil, err
	}
	defer dtx.Finish(l, err)

	return t.TxPagingWithEncryption(dtx, columns, encryptionColumns, whereClause, whereArgs, orderBy, rowPerPage, pageIndex)
}

// PagingWithEncryptionAndBuilder executes paging with TableSelectQueryBuilder and decrypted columns
func (t *DXRawTable) PagingWithEncryptionAndBuilder(
	l *log.DXLog,
	columns []string,
	encryptionColumns []EncryptionColumn,
	tqb *tableQueryBuilder.TableSelectQueryBuilder,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	whereClause, whereArgs, err := tqb.Build()
	if err != nil {
		return nil, err
	}
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

// PagingWithEncryptionAndBuilder executes paging with TableSelectQueryBuilder and decrypted columns
func (t *DXTable) PagingWithEncryptionAndBuilder(
	l *log.DXLog,
	columns []string,
	encryptionColumns []EncryptionColumn,
	tqb *tableQueryBuilder.TableSelectQueryBuilder,
	orderBy string,
	rowPerPage int64,
	pageIndex int64,
) (*PagingResult, error) {
	return t.DXRawTable.PagingWithEncryptionAndBuilder(l, columns, encryptionColumns, tqb, orderBy, rowPerPage, pageIndex)
}

// Internal Select Helper Functions

// setSessionKeysForDecryption sets all unique session keys from secure memory for SELECT
func setSessionKeysForDecryption(dtx *databases.DXDatabaseTx, encryptionColumns []EncryptionColumn) error {
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

// validateFieldName checks if a field name is safe for SQL
func validateFieldName(fieldName string) error {
	if !utils2.IsValidIdentifier(fieldName) {
		return errors.Errorf("INVALID_FIELD_NAME:%s", fieldName)
	}
	return nil
}

// validateOrderByDirection validates ORDER BY direction
func validateOrderByDirection(direction string) error {
	dir := strings.ToLower(direction)
	if dir != "asc" && dir != "desc" {
		return errors.Errorf("INVALID_ORDER_BY_DIRECTION:%s", direction)
	}
	return nil
}

// convertJoinToQueryBuilder safely converts joinSQLPart to QueryBuilder joins
func convertJoinToQueryBuilder(qb *builder.SelectQueryBuilder, joinSQLPart any) error {
	if joinSQLPart == nil {
		return nil
	}

	// Support structured JoinDef (preferred, safe by design)
	if joins, ok := joinSQLPart.([]builder.JoinDef); ok {
		for _, j := range joins {
			if err := validateFieldName(j.OnLeft); err != nil {
				return errors.Wrapf(err, "INVALID_JOIN_ON_LEFT")
			}
			if err := validateFieldName(j.OnRight); err != nil {
				return errors.Wrapf(err, "INVALID_JOIN_ON_RIGHT")
			}
			qb.Joins = append(qb.Joins, j)
		}
		return nil
	}

	// Support string (parse and validate)
	if joinStr, ok := joinSQLPart.(string); ok && joinStr != "" {
		// Parse JOIN string: "INNER JOIN table ON t1.id = t2.id"
		return parseAndValidateJoinString(qb, joinStr)
	}

	return nil
}

// parseAndValidateJoinString parses JOIN string and validates all components
func parseAndValidateJoinString(qb *builder.SelectQueryBuilder, joinStr string) error {
	// Normalize whitespace
	joinStr = strings.TrimSpace(joinStr)
	if joinStr == "" {
		return nil
	}

	// Find all JOIN clauses using regex
	joinPattern := regexp.MustCompile(`(?i)(INNER|LEFT|RIGHT|FULL)\s+JOIN\s+([^\s]+)(?:\s+(?:AS\s+)?([^\s]+))?\s+ON\s+([^\s=]+)\s*=\s*([^\s]+)`)
	matches := joinPattern.FindAllStringSubmatch(joinStr, -1)

	if len(matches) == 0 {
		// No valid JOIN found - check if there's partial JOIN syntax
		if strings.Contains(strings.ToUpper(joinStr), "JOIN") {
			return errors.New("INVALID_JOIN_SYNTAX:Could not parse JOIN clause")
		}
		return nil
	}

	for _, match := range matches {
		joinTypeStr := strings.ToUpper(strings.TrimSpace(match[1]))
		tableName := strings.TrimSpace(match[2])
		alias := strings.TrimSpace(match[3])
		leftField := strings.TrimSpace(match[4])
		rightField := strings.TrimSpace(match[5])

		// Validate table name
		if err := validateFieldName(tableName); err != nil {
			return errors.Wrapf(err, "INVALID_JOIN_TABLE_NAME")
		}

		// Validate alias if provided
		if alias != "" {
			// Skip "ON" keyword if it was captured as alias
			if strings.ToUpper(alias) == "ON" {
				alias = ""
			} else if err := validateFieldName(alias); err != nil {
				return errors.Wrapf(err, "INVALID_JOIN_ALIAS")
			}
		}

		// Validate ON fields
		if err := validateFieldName(leftField); err != nil {
			return errors.Wrapf(err, "INVALID_JOIN_ON_LEFT_FIELD")
		}
		if err := validateFieldName(rightField); err != nil {
			return errors.Wrapf(err, "INVALID_JOIN_ON_RIGHT_FIELD")
		}

		// Map join type
		var jt builder.JoinType
		switch joinTypeStr {
		case "INNER":
			jt = builder.JoinTypeInner
		case "LEFT":
			jt = builder.JoinTypeLeft
		case "RIGHT":
			jt = builder.JoinTypeRight
		case "FULL":
			jt = builder.JoinTypeFull
		default:
			return errors.Errorf("INVALID_JOIN_TYPE:%s", joinTypeStr)
		}

		qb.Joins = append(qb.Joins, builder.JoinDef{
			Type:    jt,
			Table:   tableName,
			Alias:   alias,
			OnLeft:  leftField,
			OnRight: rightField,
		})
	}

	return nil
}

// executeEncryptedSelect executes SELECT with encrypted column decryption.
//
// SECURITY: This function uses SelectQueryBuilder to prevent SQL injection.
// - Field names validated with IsValidIdentifier()
// - ORDER BY directions validated (asc/desc only)
// - WHERE values always parameterized
// - JOIN clauses parsed and validated (table names, aliases, ON conditions)
//
// Parameters:
//   - joinSQLPart: Supports nil, []builder.JoinDef (preferred), or string (validated)
//     String format: "INNER|LEFT|RIGHT|FULL JOIN table [AS alias] ON field1 = field2"
//     All components are validated for SQL injection protection
//   - orderBy: Field names validated, only asc/desc allowed
//   - where: Field names validated, values parameterized
func executeEncryptedSelect(
	dtx *databases.DXDatabaseTx,
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

	// 1. Create query builder
	qb := builder.NewSelectQueryBuilderWithSource(dbType, tableName)

	// 2. Build SELECT fields with decryption
	if len(fieldNames) == 0 {
		qb.Select("*")
	} else {
		for _, field := range fieldNames {
			if err := validateFieldName(field); err != nil {
				return nil, nil, err
			}
			qb.Select(field)
		}
	}

	// Add encrypted columns with decryption expressions
	for _, col := range encryptionColumns {
		if col.ViewHasDecrypt {
			qb.Select(col.AliasName)
		} else {
			expr := db.DecryptExpression(dbType, col.FieldName, col.EncryptionKeyDef.SessionKey)
			qb.Select(fmt.Sprintf("%s AS %s", expr, col.AliasName))
		}
	}

	// 3. Convert and validate JOIN clause
	if err := convertJoinToQueryBuilder(qb, joinSQLPart); err != nil {
		return nil, nil, err
	}

	// 4. Build WHERE clause with validation
	for fieldName, value := range where {
		if err := validateFieldName(fieldName); err != nil {
			return nil, nil, err
		}

		if value == nil {
			qb.And(qb.QuoteIdentifier(fieldName) + " IS NULL")
		} else if sqlExpr, ok := value.(db.SQLExpression); ok {
			qb.And(sqlExpr.String())
		} else {
			paramName := qb.GenerateParamName(fieldName)
			qb.AndWithParam(
				qb.QuoteIdentifier(fieldName)+" = :"+paramName,
				paramName,
				value,
			)
		}
	}

	// 5. Build ORDER BY with validation
	if orderBy != nil {
		for fieldName, direction := range orderBy {
			if err := validateFieldName(fieldName); err != nil {
				return nil, nil, err
			}
			if err := validateOrderByDirection(direction); err != nil {
				return nil, nil, err
			}
			qb.AddOrderBy(fieldName, strings.ToLower(direction), "")
		}
	}

	// 6. Add LIMIT
	if limit != nil {
		switch v := limit.(type) {
		case int:
			qb.Limit(int64(v))
		case int64:
			qb.Limit(v)
		}
	}

	// 7. Add FOR UPDATE
	if forUpdatePart != nil {
		if forUpdateStr, ok := forUpdatePart.(string); ok && forUpdateStr != "" {
			qb.ForUpdatePart = forUpdateStr
		} else if forUpdateBool, ok := forUpdatePart.(bool); ok && forUpdateBool {
			qb.ForUpdate()
		}
	}

	// 8. Execute query
	rowsInfo, rows, err := query.TxSelectWithSelectQueryBuilder2(dtx, qb, fieldTypeMapping)
	if err != nil {
		return nil, nil, errors.Wrap(err, "ENCRYPTED_SELECT_ERROR")
	}

	// 9. Post-process bytes to strings (preserve existing behavior)
	for _, row := range rows {
		for k, v := range row {
			if b, ok := v.([]byte); ok {
				row[k] = string(b)
			}
		}
	}

	return rowsInfo, rows, nil
}

// executeEncryptedPaging executes paging query with encrypted column decryption.
//
// SECURITY: This function uses SelectQueryBuilder to prevent SQL injection.
// - ORDER BY string parsed and validated (field names and directions)
// - WHERE clause provided by caller (assumed to be already parameterized)
// - All field names validated with IsValidIdentifier()
func executeEncryptedPaging(
	dtx *databases.DXDatabaseTx,
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

	// 1. COUNT query
	qbCount := builder.NewSelectQueryBuilderWithSource(dbType, tableName)
	if whereClause != "" {
		qbCount.And(whereClause)
		qbCount.Args = whereArgs
	}

	totalRows, err := query.TxCountWithSelectQueryBuilder2(dtx, qbCount)
	if err != nil {
		return nil, errors.Wrap(err, "ENCRYPTED_PAGING_COUNT_ERROR")
	}

	// 2. Calculate pagination
	offset := (pageIndex - 1) * rowPerPage
	if offset < 0 {
		offset = 0
	}
	totalPages := totalRows / rowPerPage
	if totalRows%rowPerPage > 0 {
		totalPages++
	}

	// 3. SELECT query builder
	qbSelect := builder.NewSelectQueryBuilderWithSource(dbType, tableName)

	// 4. Build SELECT fields with decryption
	if len(columns) == 0 {
		qbSelect.Select("*")
	} else {
		for _, field := range columns {
			if err := validateFieldName(field); err != nil {
				return nil, err
			}
			qbSelect.Select(field)
		}
	}

	// Add encrypted columns
	for _, col := range encryptionColumns {
		if col.ViewHasDecrypt {
			qbSelect.Select(col.AliasName)
		} else {
			expr := db.DecryptExpression(dbType, col.FieldName, col.EncryptionKeyDef.SessionKey)
			qbSelect.Select(fmt.Sprintf("%s AS %s", expr, col.AliasName))
		}
	}

	// 5. Add WHERE
	if whereClause != "" {
		qbSelect.And(whereClause)
		qbSelect.Args = whereArgs
	}

	// 6. Parse and validate ORDER BY string
	if orderBy != "" {
		parts := strings.Split(orderBy, ",")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			tokens := strings.Fields(part)
			if len(tokens) == 0 {
				continue
			}

			fieldName := tokens[0]
			direction := "asc"
			nullPlacement := ""

			if len(tokens) >= 2 {
				direction = tokens[1]
			}
			if len(tokens) >= 4 && strings.ToLower(tokens[2]) == "nulls" {
				nullPlacement = tokens[3]
			}

			if err := validateFieldName(fieldName); err != nil {
				return nil, err
			}
			if err := validateOrderByDirection(direction); err != nil {
				return nil, err
			}

			qbSelect.AddOrderBy(fieldName, strings.ToLower(direction), strings.ToLower(nullPlacement))
		}
	}

	// 7. Add LIMIT/OFFSET
	qbSelect.Limit(rowPerPage)
	qbSelect.Offset(offset)

	// 8. Execute query (no fieldTypeMapping since we handle it manually)
	rowsInfo, rows, err := query.TxSelectWithSelectQueryBuilder2(dtx, qbSelect, nil)
	if err != nil {
		return nil, errors.Wrap(err, "ENCRYPTED_PAGING_QUERY_ERROR")
	}

	// 9. Post-process bytes to strings
	for _, row := range rows {
		for k, v := range row {
			if b, ok := v.([]byte); ok {
				row[k] = string(b)
			}
		}
	}

	return &PagingResult{
		RowsInfo:   rowsInfo,
		Rows:       rows,
		TotalRows:  totalRows,
		TotalPages: totalPages,
	}, nil
}
