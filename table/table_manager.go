package table

import (
	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/database2"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/table2"
	"github.com/pkg/errors"
)

type DXTableManager struct {
	Table2s                              map[string]*table2.DXTable2
	RawTable2s                           map[string]*table2.DXRawTable2
	PropertyTable2s                      map[string]*table2.DXPropertyTable2
	Tables                               map[string]*DXTable
	RawTables                            map[string]*DXRawTable
	PropertyTables                       map[string]*DXPropertyTable
	StandardOperationResponsePossibility map[string]map[string]*api.DXAPIEndPointResponsePossibility
}

func (tm *DXTableManager) ConnectAll() (err error) {
	for _, t := range tm.Table2s {
		d, ok := database2.Manager.Databases[t.DatabaseNameId]
		if !ok {
			err = log.Log.ErrorAndCreateErrorf("database nameid '%s' not found in database manager", t.DatabaseNameId)
			return errors.Wrap(err, "error occured")
		}
		t.Database = d
	}
	for _, t := range tm.RawTable2s {
		d, ok := database2.Manager.Databases[t.DatabaseNameId]
		if !ok {
			err = log.Log.ErrorAndCreateErrorf("database nameid '%s' not found in database manager", t.DatabaseNameId)
			return errors.Wrap(err, "error occured")
		}
		t.Database = d
	}
	for _, t := range tm.PropertyTable2s {
		d, ok := database2.Manager.Databases[t.DatabaseNameId]
		if !ok {
			err = log.Log.ErrorAndCreateErrorf("database nameid '%s' not found in database manager", t.DatabaseNameId)
			return errors.Wrap(err, "error occured")
		}
		t.Database = d
	}
	for _, t := range tm.Tables {
		d, ok := database.Manager.Databases[t.DatabaseNameId]
		if !ok {
			err = log.Log.ErrorAndCreateErrorf("database nameid '%s' not found in database manager", t.DatabaseNameId)
			return errors.Wrap(err, "error occured")
		}
		t.Database = d
	}
	for _, t := range tm.RawTables {
		d, ok := database.Manager.Databases[t.DatabaseNameId]
		if !ok {
			err = log.Log.ErrorAndCreateErrorf("database nameid '%s' not found in database manager", t.DatabaseNameId)
			return errors.Wrap(err, "error occured")
		}
		t.Database = d
	}
	return nil
}

func (tm *DXTableManager) NewTable2(databaseNameId, tableNameId, resultObjectName string, tableListViewNameId string, tableFieldNameForRowNameId string, tableFieldNameForRowId string,
	tableFieldNameForRowUid string, responseEnvelopeObjectName string) *table2.DXTable2 {
	if tableListViewNameId == "" {
		tableListViewNameId = tableNameId
	}
	t := table2.DXTable2{}
	t.DatabaseNameId = databaseNameId
	t.NameId = tableNameId
	t.ResultObjectName = resultObjectName
	t.ListViewNameId = tableListViewNameId
	t.FieldNameForRowId = tableFieldNameForRowId
	t.FieldNameForRowNameId = tableFieldNameForRowNameId
	t.FieldNameForRowUid = tableFieldNameForRowUid
	t.ResponseEnvelopeObjectName = responseEnvelopeObjectName
	t.Database = database2.Manager.Databases[databaseNameId]
	tm.Table2s[tableNameId] = &t
	return &t
}

func (tm *DXTableManager) NewPropertyTable2(databaseNameId, tableNameId, resultObjectName string, tableListViewNameId string, tableFieldNameForRowNameId string, tableFieldNameForRowId string, tableFieldNameForRowUid string, responseEnvelopeObjectName string) *table2.DXPropertyTable2 {
	if tableListViewNameId == "" {
		tableListViewNameId = tableNameId
	}
	t := table2.DXPropertyTable2{}
	t.DatabaseNameId = databaseNameId
	t.NameId = tableNameId
	t.ResultObjectName = resultObjectName
	t.ListViewNameId = tableListViewNameId
	t.FieldNameForRowId = tableFieldNameForRowId
	t.FieldNameForRowNameId = tableFieldNameForRowNameId
	t.FieldNameForRowUid = tableFieldNameForRowUid
	t.ResponseEnvelopeObjectName = responseEnvelopeObjectName
	t.Database = database2.Manager.Databases[databaseNameId]
	tm.PropertyTable2s[tableNameId] = &t
	return &t
}

func (tm *DXTableManager) NewRawTable2(databaseNameId, tableNameId, resultObjectName string, tableListViewNameId string, tableFieldNameForRowNameId string, tableFieldNameForRowId string,
	tableFieldNameForRowUid string, responseEnvelopeObjectName string) *table2.DXRawTable2 {
	if tableListViewNameId == "" {
		tableListViewNameId = tableNameId
	}
	t := table2.DXRawTable2{}
	t.DatabaseNameId = databaseNameId
	t.NameId = tableNameId
	t.ResultObjectName = resultObjectName
	t.ListViewNameId = tableListViewNameId
	t.FieldNameForRowId = tableFieldNameForRowId
	t.FieldNameForRowNameId = tableFieldNameForRowNameId
	t.FieldNameForRowUid = tableFieldNameForRowUid
	t.ResponseEnvelopeObjectName = responseEnvelopeObjectName
	t.Database = database2.Manager.Databases[databaseNameId]
	tm.RawTable2s[tableNameId] = &t
	return &t
}
func (tm *DXTableManager) NewTable(databaseNameId, tableNameId, resultObjectName string, tableListViewNameId string, tableFieldNameForRowNameId string, tableFieldNameForRowId string, tableFieldNameForRowUid string, responseEnvelopeObjectName string) *DXTable {
	if tableListViewNameId == "" {
		tableListViewNameId = tableNameId
	}
	t := DXTable{
		DatabaseNameId:             databaseNameId,
		NameId:                     tableNameId,
		ResultObjectName:           resultObjectName,
		ListViewNameId:             tableListViewNameId,
		FieldNameForRowId:          tableFieldNameForRowId,
		FieldNameForRowNameId:      tableFieldNameForRowNameId,
		FieldNameForRowUid:         tableFieldNameForRowUid,
		ResponseEnvelopeObjectName: responseEnvelopeObjectName,
	}
	t.Database = database.Manager.Databases[databaseNameId]
	tm.Tables[tableNameId] = &t
	return &t
}

func (tm *DXTableManager) NewPropertyTable(databaseNameId, tableNameId, resultObjectName string, tableListViewNameId string, tableFieldNameForRowNameId string, tableFieldNameForRowId string, tableFieldNameForRowUid string, responseEnvelopeObjectName string) *DXPropertyTable {
	if tableListViewNameId == "" {
		tableListViewNameId = tableNameId
	}
	t := DXPropertyTable{
		DatabaseNameId:             databaseNameId,
		NameId:                     tableNameId,
		ResultObjectName:           resultObjectName,
		ListViewNameId:             tableListViewNameId,
		FieldNameForRowId:          tableFieldNameForRowId,
		FieldNameForRowNameId:      tableFieldNameForRowNameId,
		FieldNameForRowUid:         tableFieldNameForRowUid,
		ResponseEnvelopeObjectName: responseEnvelopeObjectName,
	}
	t.Database = database.Manager.Databases[databaseNameId]
	tm.PropertyTables[tableNameId] = &t
	return &t
}

func (tm *DXTableManager) NewRawTable(databaseNameId, tableNameId, resultObjectName string, tableListViewNameId string, tableFieldNameForRowNameId string, tableFieldNameForRowId string, tableFieldNameForRowUid string, responseEnvelopeObjectName string) *DXRawTable {
	if tableListViewNameId == "" {
		tableListViewNameId = tableNameId
	}
	t := DXRawTable{
		DatabaseNameId:             databaseNameId,
		NameId:                     tableNameId,
		ResultObjectName:           resultObjectName,
		ListViewNameId:             tableListViewNameId,
		FieldNameForRowId:          tableFieldNameForRowId,
		FieldNameForRowNameId:      tableFieldNameForRowNameId,
		FieldNameForRowUid:         tableFieldNameForRowUid,
		ResponseEnvelopeObjectName: responseEnvelopeObjectName,
	}
	t.Database = database.Manager.Databases[databaseNameId]
	tm.RawTables[tableNameId] = &t
	return &t
}

var Manager DXTableManager

func init() {
	Manager = DXTableManager{
		Table2s:         map[string]*table2.DXTable2{},
		RawTable2s:      map[string]*table2.DXRawTable2{},
		PropertyTable2s: map[string]*table2.DXPropertyTable2{},
		Tables:          map[string]*DXTable{},
		RawTables:       map[string]*DXRawTable{},
		PropertyTables:  map[string]*DXPropertyTable{},
		StandardOperationResponsePossibility: map[string]map[string]*api.DXAPIEndPointResponsePossibility{
			"create": {
				"success": &api.DXAPIEndPointResponsePossibility{
					StatusCode:  200,
					Description: "Success - 200",
					DataTemplate: []*api.DXAPIEndPointParameter{
						{NameId: "id", Type: "int64", Description: "", IsMustExist: true},
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
						{NameId: "list", Type: "json", Description: "", IsMustExist: true, Children: []api.DXAPIEndPointParameter{
							{NameId: "rows", Type: "array", Description: "", IsMustExist: true},
							{NameId: "total_rows", Type: "int64", Description: "", IsMustExist: true},
							{NameId: "total_page", Type: "int64", Description: "", IsMustExist: true},
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
