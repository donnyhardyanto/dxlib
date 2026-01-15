package table2

import (
	"github.com/donnyhardyanto/dxlib/api"
	database2 "github.com/donnyhardyanto/dxlib/database2"
	"github.com/donnyhardyanto/dxlib/log"
	dxlibTypes "github.com/donnyhardyanto/dxlib/types"
)

type DXTableManager2 struct {
	Tables                               map[string]*DXTable2
	RawTables                            map[string]*DXRawTable2
	PropertyTables                       map[string]*DXPropertyTable2
	StandardOperationResponsePossibility map[string]map[string]*api.DXAPIEndPointResponsePossibility
}

func (tm *DXTableManager2) ConnectAll() (err error) {
	for _, t := range tm.Tables {
		d, ok := database2.Manager.Databases[t.DatabaseNameId]
		if !ok {
			err = log.Log.ErrorAndCreateErrorf("database nameid '%s' not found in database manager", t.DatabaseNameId)
			return err
		}
		t.Database = d
	}
	for _, t := range tm.RawTables {
		d, ok := database2.Manager.Databases[t.DatabaseNameId]
		if !ok {
			err = log.Log.ErrorAndCreateErrorf("database nameid '%s' not found in database manager", t.DatabaseNameId)
			return err
		}
		t.Database = d
	}
	return nil
}

func (tm *DXTableManager2) NewTable(databaseNameId, tableNameId, resultObjectName string, tableListViewNameId string, tableFieldNameForRowNameId string, tableFieldNameForRowId string,
	tableFieldNameForRowUid string, responseEnvelopeObjectName string) *DXTable2 {
	if tableListViewNameId == "" {
		tableListViewNameId = tableNameId
	}
	t := DXTable2{}
	t.DatabaseNameId = databaseNameId
	t.NameId = tableNameId
	t.ResultObjectName = resultObjectName
	t.ListViewNameId = tableListViewNameId
	t.FieldNameForRowId = tableFieldNameForRowId
	t.FieldNameForRowNameId = tableFieldNameForRowNameId
	t.FieldNameForRowUid = tableFieldNameForRowUid
	t.ResponseEnvelopeObjectName = responseEnvelopeObjectName
	t.Database = database2.Manager.Databases[databaseNameId]
	tm.Tables[tableNameId] = &t
	return &t
}

func (tm *DXTableManager2) NewPropertyTable(databaseNameId, tableNameId, resultObjectName string, tableListViewNameId string, tableFieldNameForRowNameId string, tableFieldNameForRowId string, tableFieldNameForRowUid string, responseEnvelopeObjectName string) *DXPropertyTable2 {
	if tableListViewNameId == "" {
		tableListViewNameId = tableNameId
	}
	t := DXPropertyTable2{}
	t.DatabaseNameId = databaseNameId
	t.NameId = tableNameId
	t.ResultObjectName = resultObjectName
	t.ListViewNameId = tableListViewNameId
	t.FieldNameForRowId = tableFieldNameForRowId
	t.FieldNameForRowNameId = tableFieldNameForRowNameId
	t.FieldNameForRowUid = tableFieldNameForRowUid
	t.ResponseEnvelopeObjectName = responseEnvelopeObjectName
	t.Database = database2.Manager.Databases[databaseNameId]
	tm.PropertyTables[tableNameId] = &t
	return &t
}

func (tm *DXTableManager2) NewRawTable(databaseNameId, tableNameId, resultObjectName string, tableListViewNameId string, tableFieldNameForRowNameId string, tableFieldNameForRowId string,
	tableFieldNameForRowUid string, responseEnvelopeObjectName string) *DXRawTable2 {
	if tableListViewNameId == "" {
		tableListViewNameId = tableNameId
	}
	t := DXRawTable2{}
	t.DatabaseNameId = databaseNameId
	t.NameId = tableNameId
	t.ResultObjectName = resultObjectName
	t.ListViewNameId = tableListViewNameId
	t.FieldNameForRowId = tableFieldNameForRowId
	t.FieldNameForRowNameId = tableFieldNameForRowNameId
	t.FieldNameForRowUid = tableFieldNameForRowUid
	t.ResponseEnvelopeObjectName = responseEnvelopeObjectName
	t.Database = database2.Manager.Databases[databaseNameId]
	tm.RawTables[tableNameId] = &t
	return &t
}

var Manager DXTableManager2

func init() {
	Manager = DXTableManager2{
		Tables:         map[string]*DXTable2{},
		RawTables:      map[string]*DXRawTable2{},
		PropertyTables: map[string]*DXPropertyTable2{},
		StandardOperationResponsePossibility: map[string]map[string]*api.DXAPIEndPointResponsePossibility{
			"create": {
				"success": &api.DXAPIEndPointResponsePossibility{
					StatusCode:  200,
					Description: "Success - 200",
					DataTemplate: []*api.DXAPIEndPointParameter{
						{NameId: "id", Type: dxlibTypes.APIParameterTypeInt64, Description: "", IsMustExist: true},
					},
				},
				"invalid_request": &api.DXAPIEndPointResponsePossibility{
					StatusCode:   400,
					Description:  "Invalid request - 400",
					DataTemplate: nil,
				},
				"invalid_credential": &api.DXAPIEndPointResponsePossibility{
					StatusCode:   409,
					Description:  "Invalid credential - 409",
					DataTemplate: nil,
				},
				"unprocessable_entity": &api.DXAPIEndPointResponsePossibility{
					StatusCode:   422,
					Description:  "Unprocessable entity - 422",
					DataTemplate: nil,
				},
				"internal_error": &api.DXAPIEndPointResponsePossibility{
					StatusCode:  500,
					Description: "Internal error - 500",
				}},
			"read": {
				"success": &api.DXAPIEndPointResponsePossibility{
					StatusCode:   200,
					Description:  "Success - 200",
					DataTemplate: []*api.DXAPIEndPointParameter{},
				},
				"invalid_request": &api.DXAPIEndPointResponsePossibility{
					StatusCode:   400,
					Description:  "Invalid request - 400",
					DataTemplate: nil,
				},
				"invalid_credential": &api.DXAPIEndPointResponsePossibility{
					StatusCode:   409,
					Description:  "Invalid credential - 409",
					DataTemplate: nil,
				},
				"unprocessable_entity": &api.DXAPIEndPointResponsePossibility{
					StatusCode:   422,
					Description:  "Unprocessable entity - 422",
					DataTemplate: nil,
				},
				"internal_error": &api.DXAPIEndPointResponsePossibility{
					StatusCode:  500,
					Description: "Internal error - 500",
				}},
			"edit": {
				"success": &api.DXAPIEndPointResponsePossibility{
					StatusCode:   200,
					Description:  "Success - 200",
					DataTemplate: []*api.DXAPIEndPointParameter{},
				},
				"invalid_request": &api.DXAPIEndPointResponsePossibility{
					StatusCode:   400,
					Description:  "Invalid request - 400",
					DataTemplate: nil,
				},
				"invalid_credential": &api.DXAPIEndPointResponsePossibility{
					StatusCode:   409,
					Description:  "Invalid credential - 409",
					DataTemplate: nil,
				},
				"unprocessable_entity": &api.DXAPIEndPointResponsePossibility{
					StatusCode:   422,
					Description:  "Unprocessable entity - 422",
					DataTemplate: nil,
				},
				"internal_error": &api.DXAPIEndPointResponsePossibility{
					StatusCode:  500,
					Description: "Internal error - 500",
				}},
			"delete": {
				"success": &api.DXAPIEndPointResponsePossibility{
					StatusCode:   200,
					Description:  "Success - 200",
					DataTemplate: []*api.DXAPIEndPointParameter{},
				},
				"invalid_request": &api.DXAPIEndPointResponsePossibility{
					StatusCode:   400,
					Description:  "Invalid request - 400",
					DataTemplate: nil,
				},
				"invalid_credential": &api.DXAPIEndPointResponsePossibility{
					StatusCode:   409,
					Description:  "Invalid credential - 409",
					DataTemplate: nil,
				},
				"unprocessable_entity": &api.DXAPIEndPointResponsePossibility{
					StatusCode:   422,
					Description:  "Unprocessable entity - 422",
					DataTemplate: nil,
				},
				"internal_error": &api.DXAPIEndPointResponsePossibility{
					StatusCode:  500,
					Description: "Internal error - 500",
				}},
			"list": {
				"success": &api.DXAPIEndPointResponsePossibility{
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
				"invalid_request": &api.DXAPIEndPointResponsePossibility{
					StatusCode:   400,
					Description:  "Invalid request - 400",
					DataTemplate: nil,
				},
				"invalid_credential": &api.DXAPIEndPointResponsePossibility{
					StatusCode:   409,
					Description:  "Invalid credential - 409",
					DataTemplate: nil,
				},
				"unprocessable_entity": &api.DXAPIEndPointResponsePossibility{
					StatusCode:   422,
					Description:  "Unprocessable entity - 422",
					DataTemplate: nil,
				},
				"internal_error": &api.DXAPIEndPointResponsePossibility{
					StatusCode:  500,
					Description: "Internal error - 500",
				}},
		}}
}
