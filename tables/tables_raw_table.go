package tables

import (
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/databases/db/query"
	"github.com/donnyhardyanto/dxlib/databases/export"
	"github.com/donnyhardyanto/dxlib/databases/models"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/language"
	"github.com/donnyhardyanto/dxlib/log"
	tableQueryBuilder "github.com/donnyhardyanto/dxlib/tables/query_builder"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
)

// DXRawTable - Basic table wrapper without soft-delete

/*type DXRawTableInterface interface {
	GetSearchTextFieldNames() []string
	GetOrderByFieldNames() []string
	GetFullTableName() string
	GetFilterableFieldNames() []string
}*/

// DXRawTable wraps database3 with connection management and basic CRUD
type DXRawTable struct {
	DatabaseNameId             string
	Database                   *databases.DXDatabase
	DBTable                    *models.ModelDBTable
	TableNameDirect            string // Used when DBTable is nil
	FieldNameForRowId          string
	FieldNameForRowUid         string
	FieldNameForRowUtag        string
	FieldNameForRowNameId      string
	ResultObjectName           string
	ListViewNameId             string // View name for list/search queries
	ResponseEnvelopeObjectName string
	FieldTypeMapping           db.DXDatabaseTableFieldTypeMapping
	FieldMaxLengths            map[string]int // Maximum lengths for fields (for truncation)

	// Encryption definitions for automatic encryption/decryption
	EncryptionKeyDefs    []*databases.EncryptionKeyDef   // session keys only (for views that already handle decryption)
	EncryptionColumnDefs []databases.EncryptionColumnDef // for INSERT/UPDATE/SELECT encryption/decryption

	ValidationUniqueFieldNameGroups [][]string
	SearchTextFieldNames            []string
	OrderByFieldNames               []string
	FilterableFieldNames            []string // Whitelist of fields that can be filtered via filter_key_values
}

// EnsureDatabase ensures databases connection is initialized
func (t *DXRawTable) EnsureDatabase() error {
	if t.Database == nil {
		t.Database = databases.Manager.GetOrCreate(t.DatabaseNameId)
		if t.Database == nil {
			return errors.Errorf("databases not found: %s", t.DatabaseNameId)
		}
	}
	return t.Database.EnsureConnection()
}

// GetDbType returns the databases type
func (t *DXRawTable) GetDbType() base.DXDatabaseType {
	if t.Database == nil {
		return base.DXDatabaseTypePostgreSQL
	}
	return t.Database.DatabaseType
}

// GetFullTableName returns the full table name from DBTable or TableNameDirect
func (t *DXRawTable) GetFullTableName() string {
	if t.DBTable != nil {
		return t.DBTable.FullTableName()
	}
	return t.TableNameDirect
}

func (t *DXRawTable) GetSearchTextFieldNames() []string {
	return t.SearchTextFieldNames
}

func (t *DXRawTable) GetOrderByFieldNames() []string {
	return t.OrderByFieldNames
}

func (t *DXRawTable) GetFilterableFieldNames() []string {
	return t.FilterableFieldNames
}

func (t *DXRawTable) GetEncryptedFieldAliasNames() []string {
	if len(t.EncryptionColumnDefs) == 0 {
		return nil
	}
	aliases := make([]string, 0, len(t.EncryptionColumnDefs))
	for _, colDef := range t.EncryptionColumnDefs {
		if colDef.AliasName != "" {
			aliases = append(aliases, colDef.AliasName)
		}
	}
	return aliases
}

// Delete Operations (Hard Delete)

// Delete performs hard delete of rows matching where condition
func (t *DXRawTable) Delete(l *log.DXLog, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}
	return t.Database.Delete(t.GetFullTableName(), where, returningFieldNames)
}

// TxDelete deletes within a transaction
func (t *DXRawTable) TxDelete(dtx *databases.DXDatabaseTx, where utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	return dtx.TxDelete(t.GetFullTableName(), where, returningFieldNames)
}

// DeleteById deletes a single row by ID
func (t *DXRawTable) DeleteById(l *log.DXLog, id int64) (sql.Result, error) {
	result, _, err := t.Delete(l, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// TxDeleteById deletes a single row by ID within a transaction
func (t *DXRawTable) TxDeleteById(dtx *databases.DXDatabaseTx, id int64) (sql.Result, error) {
	result, _, err := t.TxDelete(dtx, utils.JSON{t.FieldNameForRowId: id}, nil)
	return result, err
}

// DoDelete is an API helper that deletes and writes response
func (t *DXRawTable) DoDelete(aepr *api.DXAPIEndPointRequest, id int64) error {
	_, row, err := t.ShouldGetById(&aepr.Log, id)
	if err != nil {
		return err
	}
	if row == nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusNotFound, "", "RECORD_NOT_FOUND:%d", id)
	}

	_, err = t.DeleteById(&aepr.Log, id)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, nil)
	return nil
}

// TxHardDelete deletes rows within a transaction (hard delete)
func (t *DXRawTable) TxHardDelete(dtx *databases.DXDatabaseTx, where utils.JSON) (sql.Result, error) {
	result, _, err := t.TxDelete(dtx, where, nil)
	return result, err
}

// RequestHardDelete handles hard delete by ID API requests
func (t *DXRawTable) RequestHardDelete(aepr *api.DXAPIEndPointRequest) error {
	_, id, err := aepr.GetParameterValueAsInt64(t.FieldNameForRowId)
	if err != nil {
		return err
	}

	return t.DoDelete(aepr, id)
}

// Upsert Operations

// Upsert inserts or updates a row based on where condition
func (t *DXRawTable) Upsert(l *log.DXLog, data utils.JSON, where utils.JSON) (sql.Result, int64, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, 0, err
	}

	_, existing, err := t.SelectOne(l, nil, where, nil, nil)
	if err != nil {
		return nil, 0, err
	}

	if existing == nil {
		insertData := utilsJson.DeepMerge2(data, where)
		_, returningValues, err := t.Database.Insert(t.GetFullTableName(), insertData, []string{t.FieldNameForRowId})
		if err != nil {
			return nil, 0, err
		}
		newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
		return nil, newId, nil
	}

	result, _, err := t.Database.Update(t.GetFullTableName(), data, where, nil)
	return result, 0, err
}

// TxUpsert inserts or updates within a transaction
func (t *DXRawTable) TxUpsert(dtx *databases.DXDatabaseTx, data utils.JSON, where utils.JSON) (sql.Result, int64, error) {
	_, existing, err := t.TxSelectOne(dtx, nil, where, nil, nil, nil)
	if err != nil {
		return nil, 0, err
	}

	if existing == nil {
		insertData := utilsJson.DeepMerge2(data, where)
		_, returningValues, err := dtx.Insert(t.GetFullTableName(), insertData, []string{t.FieldNameForRowId})
		if err != nil {
			return nil, 0, err
		}
		newId, _ := utilsJson.GetInt64(returningValues, t.FieldNameForRowId)
		return nil, newId, nil
	}

	result, _, err := dtx.Update(t.GetFullTableName(), data, where, nil)
	return result, 0, err
}

// Paging Operations

// GetListViewName returns the view name for list queries (falls back to table name)
func (t *DXRawTable) GetListViewName() string {
	if t.ListViewNameId != "" {
		return t.ListViewNameId
	}
	return t.GetFullTableName()
}

// NewTableSelectQueryBuilder creates a TableSelectQueryBuilder with the table's database type
func (t *DXRawTable) NewTableSelectQueryBuilder() *tableQueryBuilder.TableSelectQueryBuilder {
	return tableQueryBuilder.NewTableSelectQueryBuilder(t.GetDbType(), t)
}

// NewTableInsertQueryBuilder creates a TableInsertQueryBuilder with the table's database type
func (t *DXRawTable) NewTableInsertQueryBuilder() *tableQueryBuilder.TableInsertQueryBuilder {
	return tableQueryBuilder.NewTableInsertQueryBuilder(t.GetDbType(), t)
}

// NewTableUpdateQueryBuilder creates a TableUpdateQueryBuilder with the table's database type
func (t *DXRawTable) NewTableUpdateQueryBuilder() *tableQueryBuilder.TableUpdateQueryBuilder {
	return tableQueryBuilder.NewTableUpdateQueryBuilder(t.GetDbType(), t)
}

// NewTableDeleteQueryBuilder creates a TableDeleteQueryBuilder with the table's database type
func (t *DXRawTable) NewTableDeleteQueryBuilder() *tableQueryBuilder.TableDeleteQueryBuilder {
	return tableQueryBuilder.NewTableDeleteQueryBuilder(t.GetDbType(), t)
}

// DoPagingWithSelectQueryBuilder executes a paging query using SelectQueryBuilder (core implementation).
// Supports EncryptionColumnDefs and EncryptionKeyDefs for encrypted tables.
// Uses CountWithSelectQueryBuilder2 for total count and SelectWithSelectQueryBuilder2 for rows.
func (t *DXRawTable) DoPagingWithSelectQueryBuilder(l *log.DXLog, qb *tableQueryBuilder.TableSelectQueryBuilder) (*PagingResult, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, err
	}
	// Set source to list view name
	qb.SourceName = t.GetListViewName()

	dtx, err := t.Database.TransactionBegin(databases.LevelReadCommitted)
	if err != nil {
		return nil, err
	}
	defer dtx.Finish(l, err)

	// Set encryption session keys if needed
	if len(t.EncryptionColumnDefs) > 0 || len(t.EncryptionKeyDefs) > 0 {
		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, err
		}
	}

	// Count
	totalRows, err := query.TxCountWithSelectQueryBuilder2(dtx, qb.SelectQueryBuilder)
	if err != nil {
		return nil, err
	}

	rowPerPage := qb.LimitValue
	totalPages := int64(0)
	if rowPerPage > 0 {
		totalPages = (totalRows + rowPerPage - 1) / rowPerPage
	}

	// Use OrderByFieldNames to control which columns are returned
	var outFields []string

	if len(t.EncryptionColumnDefs) > 0 {
		dbType := base.StringToDXDatabaseType(dtx.Tx.DriverName())
		encryptionColumns := t.convertEncryptionColumnDefsForSelect()

		// Process each field in OrderByFieldNames
		for _, fieldName := range t.OrderByFieldNames {
			added := false
			// Check if this field needs runtime decryption
			for _, col := range encryptionColumns {
				if col.AliasName == fieldName && !col.ViewHasDecrypt {
					// Field needs runtime decryption - add decryption expression
					expr := db.DecryptExpression(dbType, col.FieldName, col.EncryptionKeyDef.SessionKey)
					outFields = append(outFields, fmt.Sprintf("%s AS %s", expr, fieldName))
					added = true
					break
				}
			}
			if !added {
				// Field doesn't need runtime decryption - add as-is
				outFields = append(outFields, fieldName)
			}
		}
	} else {
		// No encryption - use OrderByFieldNames directly
		outFields = t.OrderByFieldNames
	}

	qb.OutFields = outFields

	// Select
	rowsInfo, rows, err := query.TxSelectWithSelectQueryBuilder2(dtx, qb.SelectQueryBuilder)
	if err != nil {
		return nil, err
	}

	return &PagingResult{
		RowsInfo:   rowsInfo,
		Rows:       rows,
		TotalRows:  totalRows,
		TotalPages: totalPages,
	}, nil
}

// PagingWithSelectQueryBuilder executes a paging query using TableSelectQueryBuilder.
// Delegates to DoPagingWithSelectQueryBuilder.
func (t *DXRawTable) PagingWithSelectQueryBuilder(l *log.DXLog, qb *tableQueryBuilder.TableSelectQueryBuilder) (*PagingResult, error) {
	return t.DoPagingWithSelectQueryBuilder(l, qb)
}

func (t *DXRawTable) RequestSearchPagingList(aepr *api.DXAPIEndPointRequest) error {

	qb := t.NewTableSelectQueryBuilder()

	return t.DoRequestSearchPagingList(aepr, qb, nil)
}

// DoRequestSearchPagingList executes paging with a pre-built TableSelectQueryBuilder and writes JSON response.
// Parses row_per_page and page_index from the request. The caller is responsible for building the query builder
// with all WHERE and ORDER BY conditions. Optional onResultList callback allows post-processing of rows.
func (t *DXRawTable) DoRequestSearchPagingList(aepr *api.DXAPIEndPointRequest, qb *tableQueryBuilder.TableSelectQueryBuilder, onResultList OnResultList) error {
	_, searchText, err := aepr.GetParameterValueAsString("search_text")
	if err != nil {
		return err
	}

	isFilterKeyValuesExist, filterKeyValues, err := aepr.GetParameterValueAsJSON("filter_key_values")
	if err != nil {
		return err
	}

	_, orderByArray, err := aepr.GetParameterValueAsArrayOfAny("order_by")
	if err != nil {
		return err
	}

	isIncludeDeletedExist, isIncludeDeleted, err := aepr.GetParameterValueAsBool("is_include_deleted")
	if err != nil {
		return err
	}

	_, rowPerPage, err := aepr.GetParameterValueAsInt64("row_per_page")
	if err != nil {
		return err
	}

	_, pageIndex, err := aepr.GetParameterValueAsInt64("page_index")
	if err != nil {
		return err
	}

	if searchText != "" {
		qb.SearchLike(searchText, t.SearchTextFieldNames...)
	}
	if isFilterKeyValuesExist && filterKeyValues != nil {
		err := t.processFilterKeyValues(qb, filterKeyValues)
		if err != nil {
			return err
		}
	}

	// Apply "not deleted" filter by default, unless explicitly told to include deleted
	if !isIncludeDeletedExist || !isIncludeDeleted {
		if qb.IsFieldExist("is_deleted") {
			qb.Eq("is_deleted", false)
		}
	}

	// Parse order_by into OrderBy calls with validation
	qb.ParseOrderByFromArray(orderByArray)

	qb.Limit(rowPerPage)
	if pageIndex > 0 {
		qb.Offset(pageIndex * rowPerPage)
	}

	result, err := t.DoPagingWithSelectQueryBuilder(&aepr.Log, qb)
	if err != nil {
		return err
	}

	if onResultList != nil {
		result.Rows, err = onResultList(aepr, result.Rows)
		if err != nil {
			return err
		}
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, result.ToResponseJSON())
	return nil
}

func (t *DXRawTable) RequestSearchPagingDownload(aepr *api.DXAPIEndPointRequest) error {
	_, searchText, err := aepr.GetParameterValueAsString("search_text")
	if err != nil {
		return err
	}

	isFilterKeyValuesExist, filterKeyValues, err := aepr.GetParameterValueAsJSON("filter_key_values")
	if err != nil {
		return err
	}

	_, orderByArray, err := aepr.GetParameterValueAsArrayOfAny("order_by")
	if err != nil {
		return err
	}

	if err := t.EnsureDatabase(); err != nil {
		return err
	}

	qb := t.NewTableSelectQueryBuilder()
	if searchText != "" {
		qb.SearchLike(searchText, t.SearchTextFieldNames...)
	}
	if isFilterKeyValuesExist && filterKeyValues != nil {
		err := t.processFilterKeyValues(qb, filterKeyValues)
		if err != nil {
			return err
		}
	}

	// Parse order_by into OrderBy calls with validation
	qb.ParseOrderByFromArray(orderByArray)

	return t.DoRequestSearchPagingDownload(aepr, qb)
}

// DoRequestSearchPagingDownload executes paging with a pre-built TableSelectQueryBuilder and writes file download response.
// Parses row_per_page, page_index, format, language, header_translate_fallback from the request.
// The caller is responsible for building the query builder with all WHERE and ORDER BY conditions.
func (t *DXRawTable) DoRequestSearchPagingDownload(aepr *api.DXAPIEndPointRequest, qb *tableQueryBuilder.TableSelectQueryBuilder) error {
	_, rowPerPage, err := aepr.GetParameterValueAsInt64("row_per_page")
	if err != nil {
		return err
	}

	_, pageIndex, err := aepr.GetParameterValueAsInt64("page_index")
	if err != nil {
		return err
	}

	_, format, err := aepr.GetParameterValueAsString("format")
	if err != nil {
		return err
	}

	// Get optional language parameter (default: DXLanguageDefault)
	_, langStr, _ := aepr.GetParameterValueAsString("language")
	lang := language.DXLanguage(langStr)
	if langStr == "" {
		lang = language.DXLanguageDefault
	}

	// Get optional header_fallback parameter (default: "original")
	_, headerTranslateFallbackStr, _ := aepr.GetParameterValueAsString("header_translate_fallback")
	fallback := language.DXTranslateFallbackMode(headerTranslateFallbackStr)
	if headerTranslateFallbackStr == "" {
		fallback = language.DXTranslateFallbackModeOriginal
	}

	qb.Limit(rowPerPage)
	if pageIndex > 0 {
		qb.Offset(pageIndex * rowPerPage)
	}

	pagingResult, err := t.DoPagingWithSelectQueryBuilder(&aepr.Log, qb)
	if err != nil {
		return err
	}

	// Set export options with language
	opts := export.ExportOptions{
		Format:            export.ExportFormat(format),
		SheetName:         "Sheet1",
		DateFormat:        "2006-01-02 15:04:05",
		Language:          lang,
		TranslateFallback: fallback,
	}

	// Get file as stream
	data, contentType, err := export.ExportToStream(pagingResult.RowsInfo, pagingResult.Rows, opts)
	if err != nil {
		return err
	}

	// Override contentType based on format
	switch format {
	case DXTableExportFormatXLS:
		contentType = "application/vnd.ms-excel"
	case DXTableExportFormatXLSX:
		contentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case DXTableExportFormatCSV:
		contentType = "application/octet-stream"
	}

	// Set response headers
	filename := fmt.Sprintf("export_%s_%s.%s", t.GetFullTableName(), time.Now().Format("20060102_150405"), format)

	rw := *aepr.GetResponseWriter()
	rw.Header().Set("Content-Type", contentType)
	rw.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	rw.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	rw.Header().Set("X-Content-Type-Options", "nosniff")

	rw.WriteHeader(http.StatusOK)
	aepr.ResponseStatusCode = http.StatusOK

	if _, err = rw.Write(data); err != nil {
		return err
	}

	aepr.ResponseHeaderSent = true
	aepr.ResponseBodySent = true

	return nil
}

// OnResultList is a callback type for paging result processing
type OnResultList func(aepr *api.DXAPIEndPointRequest, list []utils.JSON) ([]utils.JSON, error)

// === Filter Operator Support (SQL-injection-proof filtering) ===

// isFieldFilterable checks if a field is in the filterable fields whitelist
func (t *DXRawTable) isFieldFilterable(fieldName string) bool {
	if len(t.FilterableFieldNames) == 0 {
		return true // No restriction if not specified (backwards compat)
	}
	for _, allowed := range t.FilterableFieldNames {
		if allowed == fieldName {
			return true
		}
	}
	return false
}

// isOperatorFormat checks if a value is in operator format {"op": "...", "value": ...}
func isOperatorFormat(value any) bool {
	if valueMap, ok := value.(map[string]any); ok {
		_, hasOp := valueMap["op"]
		return hasOp
	}
	return false
}

// applyOperatorFilter applies operator-based filtering (SQL injection proof)
func (t *DXRawTable) applyOperatorFilter(
	qb *tableQueryBuilder.TableSelectQueryBuilder,
	fieldName string,
	operatorObj map[string]any,
) error {
	op, ok := operatorObj["op"].(string)
	if !ok {
		return errors.New("OPERATOR_MUST_BE_STRING")
	}

	value := operatorObj["value"]
	paramName := qb.GenerateParamName(fieldName)
	quotedField := qb.QuoteIdentifier(fieldName)

	switch op {
	case "not_null_and_not_empty", "not_empty":
		// No parameters needed
		qb.And(fmt.Sprintf("(%s IS NOT NULL AND %s <> '')", quotedField, quotedField))

	case "not_null":
		qb.And(fmt.Sprintf("%s IS NOT NULL", quotedField))

	case "is_null":
		qb.And(fmt.Sprintf("%s IS NULL", quotedField))

	case "gt":
		qb.AndWithParam(
			fmt.Sprintf("%s > :%s", quotedField, paramName),
			paramName, value)

	case "gte":
		qb.AndWithParam(
			fmt.Sprintf("%s >= :%s", quotedField, paramName),
			paramName, value)

	case "lt":
		qb.AndWithParam(
			fmt.Sprintf("%s < :%s", quotedField, paramName),
			paramName, value)

	case "lte":
		qb.AndWithParam(
			fmt.Sprintf("%s <= :%s", quotedField, paramName),
			paramName, value)

	case "eq":
		qb.Eq(fieldName, value)

	case "ne":
		qb.Ne(fieldName, value)

	case "in":
		qb.EqOrIn(fieldName, value) // Already handles IN with params

	case "not_in":
		qb.NotIn(fieldName, value)

	case "like":
		valueStr, ok := value.(string)
		if !ok {
			return errors.New("LIKE_VALUE_MUST_BE_STRING")
		}
		qb.Like(fieldName, valueStr)

	case "ilike":
		valueStr, ok := value.(string)
		if !ok {
			return errors.New("ILIKE_VALUE_MUST_BE_STRING")
		}
		qb.ILike(fieldName, valueStr)

	case "between":
		values, ok := value.([]any)
		if !ok || len(values) != 2 {
			return errors.New("BETWEEN_REQUIRES_ARRAY_OF_2_VALUES")
		}
		param1 := paramName + "_1"
		param2 := paramName + "_2"
		qb.AndWithParams(
			fmt.Sprintf("%s BETWEEN :%s AND :%s", quotedField, param1, param2),
			map[string]any{param1: values[0], param2: values[1]})

	default:
		return errors.Errorf("UNSUPPORTED_OPERATOR:%s", op)
	}

	return nil
}

// processFilterKeyValues processes filter_key_values with operator support
func (t *DXRawTable) processFilterKeyValues(
	qb *tableQueryBuilder.TableSelectQueryBuilder,
	filterKeyValues map[string]any,
) error {
	for fieldName, filterValue := range filterKeyValues {
		// SECURITY: Validate field name against whitelist
		if !t.isFieldFilterable(fieldName) {
			return errors.Errorf("FIELD_NOT_FILTERABLE:%s", fieldName)
		}

		// Detect operator format vs simple value
		if operatorMap, ok := filterValue.(map[string]any); ok && isOperatorFormat(filterValue) {
			err := t.applyOperatorFilter(qb, fieldName, operatorMap)
			if err != nil {
				return err
			}
		} else {
			// Backwards compatible: simple equality or IN
			qb.EqOrIn(fieldName, filterValue)
		}
	}
	return nil
}
