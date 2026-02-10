package tables

import (
	"encoding/json"

	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

// DXPropertyTable - Property Table for key-value storage with typed values

// DXPropertyTable is a table specialized for storing typed property values
type DXPropertyTable struct {
	DXTable
}

// NewDXPropertyTableSimple creates a DXPropertyTable with direct table name
func NewDXPropertyTableSimple(databaseNameId, tableName, resultObjectName, listViewNameId, fieldNameForRowId, fieldNameForRowUid, fieldNameForRowNameId, responseEnvelopeObjectName string, encryptionKeyDefs []*databases.EncryptionKeyDef, validationUniqueFieldNameGroups [][]string, searchTextFieldNames []string, orderByFieldNames []string, filterableFields []string) *DXPropertyTable {
	return &DXPropertyTable{
		DXTable: DXTable{
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
				FilterableFields:                filterableFields,
			},
		},
	}
}

// propertyGetAs is a helper to extract typed values from property rows
func propertyGetAs[T any](l *log.DXLog, expectedType string, property map[string]any) (T, error) {
	var zero T

	actualType, ok := property["type"].(string)
	if !ok {
		return zero, l.ErrorAndCreateErrorf("INVALID_TYPE_FIELD_FORMAT: %T", property["type"])
	}
	if actualType != expectedType {
		return zero, l.ErrorAndCreateErrorf("TYPE_MISMATCH_ERROR: EXPECTED_%s_GOT_%s", expectedType, actualType)
	}

	rawValue, err := utils.GetJSONFromKV(property, "value")
	if err != nil {
		return zero, l.ErrorAndCreateErrorf("MISSING_VALUE_FIELD")
	}

	value, ok := rawValue["value"].(T)
	if !ok {
		return zero, l.ErrorAndCreateErrorf("PropertyGetAs:CAN_NOT_GET_VALUE:%v", err)
	}

	return value, nil
}

// GetAsString gets a string property value
func (pt *DXPropertyTable) GetAsString(l *log.DXLog, propertyId string) (string, error) {
	_, v, err := pt.ShouldSelectOne(l, nil, utils.JSON{"nameid": propertyId}, nil, nil)
	if err != nil {
		return "", err
	}
	return propertyGetAs[string](l, "STRING", v)
}

// SetAsString sets a string property value
func (pt *DXPropertyTable) SetAsString(l *log.DXLog, propertyId string, value string) error {
	v, err := json.Marshal(utils.JSON{"value": value})
	if err != nil {
		return err
	}
	_, _, err = pt.Upsert(l, utils.JSON{
		"type":  "STRING",
		"value": string(v),
	}, utils.JSON{
		"nameid": propertyId,
	})
	return err
}

// GetAsInt gets an int property value
func (pt *DXPropertyTable) GetAsInt(l *log.DXLog, propertyId string) (int, error) {
	_, v, err := pt.ShouldSelectOne(l, nil, utils.JSON{"nameid": propertyId}, nil, nil)
	if err != nil {
		return 0, err
	}
	vv, err := propertyGetAs[float64](l, "INT", v)
	if err != nil {
		return 0, err
	}
	return int(vv), nil
}

// GetAsIntOrDefault gets an int property value, returns default if not found
func (pt *DXPropertyTable) GetAsIntOrDefault(l *log.DXLog, propertyId string, defaultValue int) (int, error) {
	_, v, err := pt.SelectOne(l, nil, utils.JSON{"nameid": propertyId}, nil, nil)
	if err != nil {
		return 0, err
	}
	if v == nil {
		err = pt.SetAsInt(l, propertyId, defaultValue)
		if err != nil {
			return 0, err
		}
		return defaultValue, nil
	}
	vv, err := propertyGetAs[float64](l, "INT", v)
	if err != nil {
		return 0, err
	}
	return int(vv), nil
}

// SetAsInt sets an int property value
func (pt *DXPropertyTable) SetAsInt(l *log.DXLog, propertyId string, value int) error {
	v, err := json.Marshal(utils.JSON{"value": value})
	if err != nil {
		return err
	}
	_, _, err = pt.Upsert(l, utils.JSON{
		"type":  "INT",
		"value": string(v),
	}, utils.JSON{
		"nameid": propertyId,
	})
	return err
}

// TxSetAsInt sets an int property value within a transaction
func (pt *DXPropertyTable) TxSetAsInt(dtx *databases.DXDatabaseTx, propertyId string, value int) error {
	v, err := json.Marshal(utils.JSON{"value": value})
	if err != nil {
		return err
	}
	_, _, err = pt.TxUpsert(dtx, utils.JSON{
		"type":  "INT",
		"value": string(v),
	}, utils.JSON{
		"nameid": propertyId,
	})
	return err
}

// GetAsInt64 gets an int64 property value
func (pt *DXPropertyTable) GetAsInt64(l *log.DXLog, propertyId string) (int64, error) {
	_, v, err := pt.ShouldSelectOne(l, nil, utils.JSON{"nameid": propertyId}, nil, nil)
	if err != nil {
		return 0, err
	}
	vv, err := propertyGetAs[float64](l, "INT64", v)
	if err != nil {
		return 0, err
	}
	return int64(vv), nil
}

// SetAsInt64 sets an int64 property value
func (pt *DXPropertyTable) SetAsInt64(l *log.DXLog, propertyId string, value int64) error {
	v, err := json.Marshal(utils.JSON{"value": value})
	if err != nil {
		return err
	}
	_, _, err = pt.Upsert(l, utils.JSON{
		"type":  "INT64",
		"value": string(v),
	}, utils.JSON{
		"nameid": propertyId,
	})
	return err
}

// GetAsJSON gets a JSON property value
func (pt *DXPropertyTable) GetAsJSON(l *log.DXLog, propertyId string) (map[string]any, error) {
	_, v, err := pt.ShouldSelectOne(l, nil, utils.JSON{"nameid": propertyId}, nil, nil)
	if err != nil {
		return nil, err
	}
	return propertyGetAs[map[string]any](l, "JSON", v)
}

// SetAsJSON sets a JSON property value
func (pt *DXPropertyTable) SetAsJSON(l *log.DXLog, propertyId string, value map[string]any) error {
	v, err := json.Marshal(utils.JSON{"value": value})
	if err != nil {
		return errors.Wrap(err, "SetAsJSON.Marshal")
	}
	_, _, err = pt.Upsert(l, utils.JSON{
		"type":  "JSON",
		"value": string(v),
	}, utils.JSON{
		"nameid": propertyId,
	})
	return err
}

// TxSetAsJSON sets a JSON property value within a transaction
func (pt *DXPropertyTable) TxSetAsJSON(dtx *databases.DXDatabaseTx, propertyId string, value map[string]any) error {
	v, err := json.Marshal(utils.JSON{"value": value})
	if err != nil {
		return errors.Wrap(err, "TxSetAsJSON.Marshal")
	}
	_, _, err = pt.TxUpsert(dtx, utils.JSON{
		"type":  "JSON",
		"value": string(v),
	}, utils.JSON{
		"nameid": propertyId,
	})
	return err
}
