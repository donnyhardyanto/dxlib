package types

import (
	"testing"

	"github.com/donnyhardyanto/dxlib/base"
)

// DataTypeID is an integer identifier: same SQL/Go as Int64, but a DISTINCT
// APIParameterType so consumers can tell it apart from a generic Int64.
func TestDataTypeID(t *testing.T) {
	if DataTypeID.GoType != GoTypeInt64 {
		t.Errorf("GoType = %q, want %q", DataTypeID.GoType, GoTypeInt64)
	}
	if DataTypeID.APIParameterType != APIParameterTypeID {
		t.Errorf("APIParameterType = %q, want %q", DataTypeID.APIParameterType, APIParameterTypeID)
	}
	if DataTypeID.APIParameterType == APIParameterTypeInt64 {
		t.Errorf("ID must NOT share APIParameterType with Int64 (would be indistinguishable)")
	}
	if got := DataTypeID.TypeByDatabaseType[base.DXDatabaseTypePostgreSQL]; got != "BIGINT" {
		t.Errorf("PostgreSQL type = %q, want BIGINT", got)
	}
	if got := DataTypeID.TypeByDatabaseType[base.DXDatabaseTypeOracle]; got != "NUMBER(19)" {
		t.Errorf("Oracle type = %q, want NUMBER(19)", got)
	}
	// DataType embeds map fields (TypeByDatabaseType, DefaultValueByDatabaseType),
	// making it structurally non-comparable with != — compare the identifying
	// field instead of the whole struct.
	if got := Types[APIParameterTypeID].APIParameterType; got != DataTypeID.APIParameterType {
		t.Errorf("Types map missing APIParameterTypeID -> DataTypeID, got APIParameterType %q", got)
	}
}
