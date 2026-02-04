package tables

import (
	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/models"
	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
)

type DXTableExportFormat = string

const (
	DXTableExportFormatXLS  DXTableExportFormat = "xls"
	DXTableExportFormatCSV  DXTableExportFormat = "csv"
	DXTableExportFormatXLSX DXTableExportFormat = "xlsx"
)

var DXTableExportFormatEnumSetAll = []any{DXTableExportFormatXLS, DXTableExportFormatXLSX, DXTableExportFormatCSV}

// Table3 Manager - Registry for tables

// DXTableManager manages a collection of DXTable instances
type DXTableManager struct {
	Tables                               map[string]*DXTable
	RawTables                            map[string]*DXRawTable
	AuditOnlyTables                      map[string]*DXTableAuditOnly
	StandardOperationResponsePossibility map[string]*api.DXAPIEndPointResponsePossibilities
}

// ConnectAll connects all registered tables to their databases
func (tm *DXTableManager) ConnectAll() error {
	for _, t := range tm.Tables {
		err := t.EnsureDatabase()
		if err != nil {
			return err
		}
	}
	for _, t := range tm.RawTables {
		err := t.EnsureDatabase()
		if err != nil {
			return err
		}
	}
	for _, t := range tm.AuditOnlyTables {
		err := t.EnsureDatabase()
		if err != nil {
			return err
		}
	}
	return nil
}

var (
	DXAPIEndPointResponsePossibilityCreate = api.DXAPIEndPointResponsePossibilities{
		"success": api.DXAPIEndPointResponsePossibility{
			StatusCode:  200,
			Description: "Success - 200",
			DataTemplate: []*api.DXAPIEndPointParameter{
				{NameId: "id", Type: dxlibTypes.APIParameterTypeInt64ZP, Description: "", IsMustExist: true},
			},
		},
		"invalid_request": api.DXAPIEndPointResponsePossibility{
			StatusCode:   400,
			Description:  "Invalid request - 400",
			DataTemplate: nil,
		},
		"invalid_credential": api.DXAPIEndPointResponsePossibility{
			StatusCode:   409,
			Description:  "Invalid credential - 409",
			DataTemplate: nil,
		},
		"unprocessable_entity": api.DXAPIEndPointResponsePossibility{
			StatusCode:   422,
			Description:  "Unprocessable entity - 422",
			DataTemplate: nil,
		},
		"internal_error": api.DXAPIEndPointResponsePossibility{
			StatusCode:  500,
			Description: "Internal error - 500",
		},
	}
	DXAPIEndPointResponsePossibilityCreateByUid = api.DXAPIEndPointResponsePossibilities{
		"success": api.DXAPIEndPointResponsePossibility{
			StatusCode:  200,
			Description: "Success - 200",
			DataTemplate: []*api.DXAPIEndPointParameter{
				{NameId: "uid", Type: dxlibTypes.APIParameterTypeString, Description: "", IsMustExist: true},
			},
		},
		"invalid_request": api.DXAPIEndPointResponsePossibility{
			StatusCode:   400,
			Description:  "Invalid request - 400",
			DataTemplate: nil,
		},
		"invalid_credential": api.DXAPIEndPointResponsePossibility{
			StatusCode:   409,
			Description:  "Invalid credential - 409",
			DataTemplate: nil,
		},
		"unprocessable_entity": api.DXAPIEndPointResponsePossibility{
			StatusCode:   422,
			Description:  "Unprocessable entity - 422",
			DataTemplate: nil,
		},
		"internal_error": api.DXAPIEndPointResponsePossibility{
			StatusCode:  500,
			Description: "Internal error - 500",
		},
	}
	DXAPIEndPointResponsePossibilityRead = api.DXAPIEndPointResponsePossibilities{
		"success": api.DXAPIEndPointResponsePossibility{
			StatusCode:   200,
			Description:  "Success - 200",
			DataTemplate: nil,
		},
		"invalid_request": api.DXAPIEndPointResponsePossibility{
			StatusCode:   400,
			Description:  "Invalid request - 400",
			DataTemplate: nil,
		},
		"invalid_credential": api.DXAPIEndPointResponsePossibility{
			StatusCode:   409,
			Description:  "Invalid credential - 409",
			DataTemplate: nil,
		},
		"unprocessable_entity": api.DXAPIEndPointResponsePossibility{
			StatusCode:   422,
			Description:  "Unprocessable entity - 422",
			DataTemplate: nil,
		},
		"internal_error": api.DXAPIEndPointResponsePossibility{
			StatusCode:  500,
			Description: "Internal error - 500",
		},
	}
	DXAPIEndPointResponsePossibilityUpdate = api.DXAPIEndPointResponsePossibilities{
		"success": api.DXAPIEndPointResponsePossibility{
			StatusCode:   200,
			Description:  "Success - 200",
			DataTemplate: nil,
		},
		"invalid_request": api.DXAPIEndPointResponsePossibility{
			StatusCode:   400,
			Description:  "Invalid request - 400",
			DataTemplate: nil,
		},
		"invalid_credential": api.DXAPIEndPointResponsePossibility{
			StatusCode:   409,
			Description:  "Invalid credential - 409",
			DataTemplate: nil,
		},
		"unprocessable_entity": api.DXAPIEndPointResponsePossibility{
			StatusCode:   422,
			Description:  "Unprocessable entity - 422",
			DataTemplate: nil,
		},
		"internal_error": api.DXAPIEndPointResponsePossibility{
			StatusCode:  500,
			Description: "Internal error - 500",
		},
	}
	DXAPIEndPointResponsePossibilityDelete = api.DXAPIEndPointResponsePossibilities{
		"success": api.DXAPIEndPointResponsePossibility{
			StatusCode:   200,
			Description:  "Success - 200",
			DataTemplate: nil,
		},
		"invalid_request": api.DXAPIEndPointResponsePossibility{
			StatusCode:   400,
			Description:  "Invalid request - 400",
			DataTemplate: nil,
		},
		"invalid_credential": api.DXAPIEndPointResponsePossibility{
			StatusCode:   409,
			Description:  "Invalid credential - 409",
			DataTemplate: nil,
		},
		"unprocessable_entity": api.DXAPIEndPointResponsePossibility{
			StatusCode:   422,
			Description:  "Unprocessable entity - 422",
			DataTemplate: nil,
		},
		"internal_error": api.DXAPIEndPointResponsePossibility{
			StatusCode:  500,
			Description: "Internal error - 500",
		},
	}
	DXAPIEndPointResponsePossibilityList = api.DXAPIEndPointResponsePossibilities{
		"success": api.DXAPIEndPointResponsePossibility{
			StatusCode:  200,
			Description: "Success - 200",
			DataTemplate: []*api.DXAPIEndPointParameter{
				{NameId: "list", Type: dxlibTypes.APIParameterTypeJSON, Description: "", IsMustExist: true, Children: []api.DXAPIEndPointParameter{
					{NameId: "rows", Type: dxlibTypes.APIParameterTypeArray, Description: "", IsMustExist: true},
					{NameId: "total_rows", Type: dxlibTypes.APIParameterTypeInt64, Description: "", IsMustExist: true},
					{NameId: "total_page", Type: dxlibTypes.APIParameterTypeInt64, Description: "", IsMustExist: true},
				}},
			},
		},
		"invalid_request": api.DXAPIEndPointResponsePossibility{
			StatusCode:   400,
			Description:  "Invalid request - 400",
			DataTemplate: nil,
		},
		"invalid_credential": api.DXAPIEndPointResponsePossibility{
			StatusCode:   409,
			Description:  "Invalid credential - 409",
			DataTemplate: nil,
		},
		"unprocessable_entity": api.DXAPIEndPointResponsePossibility{
			StatusCode:   422,
			Description:  "Unprocessable entity - 422",
			DataTemplate: nil,
		},
		"internal_error": api.DXAPIEndPointResponsePossibility{
			StatusCode:  500,
			Description: "Internal error - 500",
		},
	}
)

// Manager is the global table3 manager instance
var Manager = DXTableManager{
	Tables:          make(map[string]*DXTable),
	RawTables:       make(map[string]*DXRawTable),
	AuditOnlyTables: make(map[string]*DXTableAuditOnly),
	StandardOperationResponsePossibility: map[string]*api.DXAPIEndPointResponsePossibilities{
		"create": &DXAPIEndPointResponsePossibilityCreate,
		"read":   &DXAPIEndPointResponsePossibilityRead,
		"edit":   &DXAPIEndPointResponsePossibilityUpdate,
		"delete": &DXAPIEndPointResponsePossibilityDelete,
		"list":   &DXAPIEndPointResponsePossibilityList,
	},
}

// RegisterTable registers a DXTable with the manager
func (m *DXTableManager) RegisterTable(name string, table *DXTable) {
	m.Tables[name] = table
}

// RegisterRawTable registers a DXRawTable with the manager
func (m *DXTableManager) RegisterRawTable(name string, table *DXRawTable) {
	m.RawTables[name] = table
}

// RegisterAuditOnlyTable registers a DXTableAuditOnly with the manager
func (m *DXTableManager) RegisterAuditOnlyTable(name string, table *DXTableAuditOnly) {
	m.AuditOnlyTables[name] = table
}

// GetTable returns a registered DXTable by name
func (m *DXTableManager) GetTable(name string) *DXTable {
	return m.Tables[name]
}

// GetRawTable returns a registered DXRawTable by name
func (m *DXTableManager) GetRawTable(name string) *DXRawTable {
	return m.RawTables[name]
}

// GetAuditOnlyTable returns a registered DXTableAuditOnly by name
func (m *DXTableManager) GetAuditOnlyTable(name string) *DXTableAuditOnly {
	return m.AuditOnlyTables[name]
}

// Factory Functions

// NewDXRawTable creates a new DXRawTable wrapping a models.ModelDBTable
func NewDXRawTable(
	databaseNameId string, dbTable *models.ModelDBTable, fieldNameForRowId string, validationUniqueFieldNameGroups [][]string, searchTextFieldNames []string, orderByFieldNames []string) *DXRawTable {
	return &DXRawTable{
		DatabaseNameId:                  databaseNameId,
		DBTable:                         dbTable,
		FieldNameForRowId:               fieldNameForRowId,
		ValidationUniqueFieldNameGroups: validationUniqueFieldNameGroups,
		SearchTextFieldNames:            searchTextFieldNames,
		OrderByFieldNames:               orderByFieldNames,
	}
}

// NewDXRawTableWithView creates a new DXRawTable with a custom list view
func NewDXRawTableWithView(
	databaseNameId string, dbTable *models.ModelDBTable, fieldNameForRowId, listViewNameId string, validationUniqueFieldNameGroups [][]string, searchTextFieldNames []string, orderByFieldNames []string) *DXRawTable {
	return &DXRawTable{
		DatabaseNameId:                  databaseNameId,
		DBTable:                         dbTable,
		FieldNameForRowId:               fieldNameForRowId,
		ListViewNameId:                  listViewNameId,
		ValidationUniqueFieldNameGroups: validationUniqueFieldNameGroups,
		SearchTextFieldNames:            searchTextFieldNames,
		OrderByFieldNames:               orderByFieldNames,
	}
}

// NewDXTable creates a new DXTable wrapping a models.ModelDBTable
func NewDXTable(
	databaseNameId string, dbTable *models.ModelDBTable, fieldNameForRowId string, validationUniqueFieldNameGroups [][]string, searchTextFieldNames []string, orderByFieldNames []string) *DXTable {
	return &DXTable{
		DXRawTable: DXRawTable{
			DatabaseNameId:                  databaseNameId,
			DBTable:                         dbTable,
			FieldNameForRowId:               fieldNameForRowId,
			ValidationUniqueFieldNameGroups: validationUniqueFieldNameGroups,
			SearchTextFieldNames:            searchTextFieldNames,
			OrderByFieldNames:               orderByFieldNames,
		},
	}
}

// NewDXTableWithView creates a new DXTable with a custom list view
func NewDXTableWithView(
	databaseNameId string, dbTable *models.ModelDBTable, fieldNameForRowId, listViewNameId string, validationUniqueFieldNameGroups [][]string, searchTextFieldNames []string, orderByFieldNames []string) *DXTable {
	return &DXTable{
		DXRawTable: DXRawTable{
			DatabaseNameId:                  databaseNameId,
			DBTable:                         dbTable,
			FieldNameForRowId:               fieldNameForRowId,
			ListViewNameId:                  listViewNameId,
			ValidationUniqueFieldNameGroups: validationUniqueFieldNameGroups,
			SearchTextFieldNames:            searchTextFieldNames,
			OrderByFieldNames:               orderByFieldNames,
		},
	}
}

// Simple Factory Functions - without models.ModelDBTable (for gradual migration)

// NewDXRawTableSimple creates a DXRawTable with direct table name (no models.ModelDBTable needed)
func NewDXRawTableSimple(
	databaseNameId, tableName, resultObjectName, listViewNameId, fieldNameForRowId, fieldNameForRowUid, fieldNameForRowNameId,
	responseEnvelopeObjectName string, encryptionKeyDefs []*databases.EncryptionKeyDef, validationUniqueFieldNameGroups [][]string, searchTextFieldNames []string, orderByFieldNames []string) *DXRawTable {
	return &DXRawTable{
		DatabaseNameId:                  databaseNameId,
		TableNameDirect:                 tableName,
		ResultObjectName:                resultObjectName,
		ListViewNameId:                  listViewNameId,
		FieldNameForRowId:               fieldNameForRowId,
		FieldNameForRowUid:              fieldNameForRowUid,
		FieldNameForRowNameId:           fieldNameForRowNameId,
		ResponseEnvelopeObjectName:      responseEnvelopeObjectName,
		EncryptionKeyDefs:               encryptionKeyDefs,
		ValidationUniqueFieldNameGroups: validationUniqueFieldNameGroups,
		SearchTextFieldNames:            searchTextFieldNames,
		OrderByFieldNames:               orderByFieldNames,
	}
}

// NewDXTableSimple creates a DXTable with direct table name (no models.ModelDBTable needed)
func NewDXTableSimple(
	databaseNameId, tableName, resultObjectName, listViewNameId, fieldNameForRowId, fieldNameForRowUid, fieldNameForRowNameId,
	responseEnvelopeObjectName string, encryptionKeyDefs []*databases.EncryptionKeyDef, validationUniqueFieldNameGroups [][]string, searchTextFieldNames []string, orderByFieldNames []string) *DXTable {
	return &DXTable{
		DXRawTable: DXRawTable{
			DatabaseNameId:                  databaseNameId,
			TableNameDirect:                 tableName,
			ResultObjectName:                resultObjectName,
			ListViewNameId:                  listViewNameId,
			FieldNameForRowId:               fieldNameForRowId,
			FieldNameForRowUid:              fieldNameForRowUid,
			FieldNameForRowNameId:           fieldNameForRowNameId,
			ResponseEnvelopeObjectName:      responseEnvelopeObjectName,
			EncryptionKeyDefs:               encryptionKeyDefs,
			ValidationUniqueFieldNameGroups: validationUniqueFieldNameGroups,
			SearchTextFieldNames:            searchTextFieldNames,
			OrderByFieldNames:               orderByFieldNames,
		},
	}
}

// NewDXTableWithEncryption creates a DXTable with encryption/decryption definitions
func NewDXTableWithEncryption(
	databaseNameId, tableName, resultObjectName, listViewNameId,
	fieldNameForRowId, fieldNameForRowUid, fieldNameForRowNameId, responseEnvelopeObjectName string,
	encryptionKeyDefs []*databases.EncryptionKeyDef,
	encryptionColumnDefs []databases.EncryptionColumnDef, validationUniqueFieldNameGroups [][]string, searchTextFieldNames []string, orderByFieldNames []string) *DXTable {
	return &DXTable{
		DXRawTable: DXRawTable{
			DatabaseNameId:                  databaseNameId,
			TableNameDirect:                 tableName,
			ResultObjectName:                resultObjectName,
			ListViewNameId:                  listViewNameId,
			FieldNameForRowId:               fieldNameForRowId,
			FieldNameForRowUid:              fieldNameForRowUid,
			FieldNameForRowNameId:           fieldNameForRowNameId,
			ResponseEnvelopeObjectName:      responseEnvelopeObjectName,
			EncryptionKeyDefs:               encryptionKeyDefs,
			EncryptionColumnDefs:            encryptionColumnDefs,
			ValidationUniqueFieldNameGroups: validationUniqueFieldNameGroups,
			SearchTextFieldNames:            searchTextFieldNames,
			OrderByFieldNames:               orderByFieldNames,
		},
	}
}

// NewDXTableAuditOnlySimple creates a DXTableAuditOnly with direct table name
// Use this for tables that have audit fields (created_at, created_by_*, last_modified_*) but NO is_deleted column
func NewDXTableAuditOnlySimple(
	databaseNameId, tableName, resultObjectName, listViewNameId, fieldNameForRowId, fieldNameForRowUid, fieldNameForRowNameId,
	responseEnvelopeObjectName string, encryptionKeyDefs []*databases.EncryptionKeyDef, validationUniqueFieldNameGroups [][]string, searchTextFieldNames []string, orderByFieldNames []string) *DXTableAuditOnly {
	return &DXTableAuditOnly{
		DXRawTable: DXRawTable{
			DatabaseNameId:                  databaseNameId,
			TableNameDirect:                 tableName,
			ResultObjectName:                resultObjectName,
			ListViewNameId:                  listViewNameId,
			FieldNameForRowId:               fieldNameForRowId,
			FieldNameForRowUid:              fieldNameForRowUid,
			FieldNameForRowNameId:           fieldNameForRowNameId,
			ResponseEnvelopeObjectName:      responseEnvelopeObjectName,
			EncryptionKeyDefs:               encryptionKeyDefs,
			ValidationUniqueFieldNameGroups: validationUniqueFieldNameGroups,
			SearchTextFieldNames:            searchTextFieldNames,
			OrderByFieldNames:               orderByFieldNames,
		},
	}
}
