package tables

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/databases/export"
	"github.com/donnyhardyanto/dxlib/databases/models"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
)

// DXRawTable - Basic table wrapper without soft-delete

type DXRawTableInterface interface {
	GetSearchTextFieldNames() []string
	GetFullTableName() string
}

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

// NewQueryBuilder creates a QueryBuilder with the table's databases type
func (t *DXRawTable) NewQueryBuilder() *QueryBuilder {
	return NewQueryBuilder(t.GetDbType(), t)
}

// Paging executes a paging query with WHERE clause and ORDER BY
func (t *DXRawTable) Paging(l *log.DXLog, rowPerPage, pageIndex int64, whereClause, orderBy string, args utils.JSON) (*PagingResult, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, err
	}

	if len(t.EncryptionColumnDefs) > 0 {
		encryptionColumns := t.convertEncryptionColumnDefsForSelect()
		return t.PagingWithEncryption(l, nil, encryptionColumns, whereClause, args, orderBy, rowPerPage, pageIndex)
	}
	if len(t.EncryptionKeyDefs) > 0 {
		dtx, err := t.Database.TransactionBegin(databases.LevelReadCommitted)
		if err != nil {
			return nil, err
		}
		defer dtx.Finish(l, err)
		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, err
		}
		return executeEncryptedPaging(dtx, t.ListViewNameId, t.Database.DatabaseType, nil, nil, whereClause, args, orderBy, rowPerPage, pageIndex)
	}

	rowsInfo, list, totalRows, totalPages, _, err := db.NamedQueryPaging(
		t.Database.Connection,
		t.FieldTypeMapping,
		"",
		rowPerPage,
		pageIndex,
		"*",
		t.GetListViewName(),
		whereClause,
		"",
		orderBy,
		args,
	)
	if err != nil {
		return nil, err
	}

	return &PagingResult{
		RowsInfo:   rowsInfo,
		Rows:       list,
		TotalRows:  totalRows,
		TotalPages: totalPages,
	}, nil
}

// PagingWithBuilder executes a paging query using a QueryBuilder
func (t *DXRawTable) PagingWithBuilder(l *log.DXLog, rowPerPage, pageIndex int64, qb *QueryBuilder, orderBy string) (*PagingResult, error) {
	whereClause, args := qb.Build()
	return t.Paging(l, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// DoPaging is an API helper that handles paging
func (t *DXRawTable) DoPaging(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, whereClause, orderBy string, args utils.JSON) (*PagingResult, error) {
	result, err := t.Paging(&aepr.Log, rowPerPage, pageIndex, whereClause, orderBy, args)
	if err != nil {
		aepr.Log.Errorf(err, "Error at paging table %s (%s)", t.GetFullTableName(), err.Error())
		return nil, err
	}
	return result, nil
}

// DoPagingWithBuilder is an API helper using QueryBuilder
func (t *DXRawTable) DoPagingWithBuilder(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, qb *QueryBuilder, orderBy string) (*PagingResult, error) {
	whereClause, args := qb.Build()
	return t.DoPaging(aepr, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// DoPagingResponse executes paging and writes standard JSON response
func (t *DXRawTable) DoPagingResponse(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, whereClause, orderBy string, args utils.JSON) error {
	result, err := t.DoPaging(aepr, rowPerPage, pageIndex, whereClause, orderBy, args)
	if err != nil {
		return err
	}
	aepr.WriteResponseAsJSON(http.StatusOK, nil, result.ToResponseJSON())
	return nil
}

// DoPagingResponseWithBuilder executes paging with QueryBuilder and writes response
func (t *DXRawTable) DoPagingResponseWithBuilder(aepr *api.DXAPIEndPointRequest, rowPerPage, pageIndex int64, qb *QueryBuilder, orderBy string) error {
	whereClause, args := qb.Build()
	return t.DoPagingResponse(aepr, rowPerPage, pageIndex, whereClause, orderBy, args)
}

// RequestPagingList handles list/paging API requests
func (t *DXRawTable) RequestPagingList(aepr *api.DXAPIEndPointRequest) error {
	isExistFilterWhere, filterWhere, err := aepr.GetParameterValueAsString("filter_where")
	if err != nil {
		return err
	}
	if !isExistFilterWhere {
		filterWhere = ""
	}

	isExistFilterOrderBy, filterOrderBy, err := aepr.GetParameterValueAsString("filter_order_by")
	if err != nil {
		return err
	}
	if !isExistFilterOrderBy {
		filterOrderBy = ""
	}

	isExistFilterKeyValues, filterKeyValues, err := aepr.GetParameterValueAsJSON("filter_key_values")
	if err != nil {
		return err
	}
	if !isExistFilterKeyValues {
		filterKeyValues = nil
	}

	_, rowPerPage, err := aepr.GetParameterValueAsInt64("row_per_page")
	if err != nil {
		return err
	}

	_, pageIndex, err := aepr.GetParameterValueAsInt64("page_index")
	if err != nil {
		return err
	}

	result, err := t.Paging(&aepr.Log, rowPerPage, pageIndex, filterWhere, filterOrderBy, filterKeyValues)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, result.ToResponseJSON())
	return nil
}

func BuildOrderByString(orderByArray []any) string {
	if len(orderByArray) == 0 {
		return ""
	}
	var parts []string
	for _, item := range orderByArray {
		entry, ok := item.(utils.JSON)
		if !ok {
			continue
		}
		fieldName, _ := entry["field_name"].(string)
		direction, _ := entry["direction"].(string)
		nullOrder, _ := entry["null_order"].(string)
		if fieldName != "" && direction != "" {
			part := fieldName + " " + direction
			if nullOrder != "" {
				part += " nulls " + nullOrder
			}
			parts = append(parts, part)
		}
	}
	return strings.Join(parts, ", ")
}

func (t *DXRawTable) RequestSearchPagingList(aepr *api.DXAPIEndPointRequest) error {
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
	orderByStr := BuildOrderByString(orderByArray)

	if err := t.EnsureDatabase(); err != nil {
		return err
	}

	qb := NewQueryBuilder(t.Database.DatabaseType, t)
	if searchText != "" {
		qb.SearchLike(searchText, t.SearchTextFieldNames...)
	}
	if isFilterKeyValuesExist && filterKeyValues != nil {
		for k, v := range filterKeyValues {
			qb.Eq(k, v)
		}
	}

	_, rowPerPage, err := aepr.GetParameterValueAsInt64("row_per_page")
	if err != nil {
		return err
	}

	_, pageIndex, err := aepr.GetParameterValueAsInt64("page_index")
	if err != nil {
		return err
	}

	result, err := t.PagingWithBuilder(&aepr.Log, rowPerPage, pageIndex, qb, orderByStr)
	if err != nil {
		return err
	}

	aepr.WriteResponseAsJSON(http.StatusOK, nil, result.ToResponseJSON())
	return nil
}

// RequestListDownload handles list download API requests (export to xlsx/csv/xls)
func (t *DXRawTable) RequestListDownload(aepr *api.DXAPIEndPointRequest) error {
	isExistFilterWhere, filterWhere, err := aepr.GetParameterValueAsString("filter_where")
	if err != nil {
		return err
	}
	if !isExistFilterWhere {
		filterWhere = ""
	}

	isExistFilterOrderBy, filterOrderBy, err := aepr.GetParameterValueAsString("filter_order_by")
	if err != nil {
		return err
	}
	if !isExistFilterOrderBy {
		filterOrderBy = ""
	}

	isExistFilterKeyValues, filterKeyValues, err := aepr.GetParameterValueAsJSON("filter_key_values")
	if err != nil {
		return err
	}
	if !isExistFilterKeyValues {
		filterKeyValues = nil
	}

	_, rowPerPage, err := aepr.GetParameterValueAsInt64("row_per_page")
	if err != nil {
		rowPerPage = 0 // No limit if not specified
	}

	_, pageIndex, err := aepr.GetParameterValueAsInt64("page_index")
	if err != nil {
		pageIndex = 0
	}

	_, format, err := aepr.GetParameterValueAsString("format")
	if err != nil {
		return aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, "", "FORMAT_PARAMETER_ERROR:%s", err.Error())
	}
	format = strings.ToLower(format)

	// Validate format
	switch format {
	case "xls", "xlsx", "csv":
	default:
		return aepr.WriteResponseAndNewErrorf(http.StatusBadRequest, "", "UNSUPPORTED_EXPORT_FORMAT:%s", format)
	}

	if err := t.EnsureDatabase(); err != nil {
		return err
	}

	rowsInfo, list, _, _, _, err := db.NamedQueryPaging(
		t.Database.Connection,
		db.DXDatabaseTableFieldTypeMapping(t.FieldTypeMapping),
		"",
		rowPerPage,
		pageIndex,
		"*",
		t.GetListViewName(),
		filterWhere,
		"",
		filterOrderBy,
		filterKeyValues,
	)
	if err != nil {
		return err
	}

	// Set export options
	opts := export.ExportOptions{
		Format:     export.ExportFormat(format),
		SheetName:  "Sheet1",
		DateFormat: "2006-01-02 15:04:05",
	}

	// Get file as stream
	data, contentType, err := export.ExportToStream(rowsInfo, list, opts)
	if err != nil {
		return err
	}

	// Override contentType based on format
	switch format {
	case "xls":
		contentType = "application/vnd.ms-excel"
	case "xlsx":
		contentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case "csv":
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

// DoRequestPagingList handles paging with optional result processing
func (t *DXRawTable) DoRequestPagingList(aepr *api.DXAPIEndPointRequest, filterWhere string, filterOrderBy string, filterKeyValues utils.JSON, onResultList OnResultList) error {
	_, rowPerPage, err := aepr.GetParameterValueAsInt64("row_per_page")
	if err != nil {
		return err
	}

	_, pageIndex, err := aepr.GetParameterValueAsInt64("page_index")
	if err != nil {
		return err
	}

	result, err := t.Paging(&aepr.Log, rowPerPage, pageIndex, filterWhere, filterOrderBy, filterKeyValues)
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
