package types

import (
	"testing"

	"github.com/donnyhardyanto/dxlib/base"
)

// Money is a registered type like the others: listed in DataTypes and Types,
// found by name, and mapped to NUMERIC(23,4) on every engine.
func TestDataTypeMoneyRegistered(t *testing.T) {
	if got := Types[APIParameterTypeMoney].APIParameterType; got != APIParameterTypeMoney {
		t.Errorf("Types[money].APIParameterType = %q, want %q", got, APIParameterTypeMoney)
	}

	found := false
	for _, dt := range DataTypes {
		if dt.APIParameterType == APIParameterTypeMoney {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("DataTypes does not list money")
	}

	for _, name := range []string{"money", " Money "} {
		dt, err := GetDataTypeFromString(name)
		if err != nil {
			t.Fatalf("GetDataTypeFromString(%q): %v", name, err)
		}
		if dt.APIParameterType != APIParameterTypeMoney {
			t.Errorf("GetDataTypeFromString(%q) = %q, want %q", name, dt.APIParameterType, APIParameterTypeMoney)
		}
		if dt.JSONType != JSONTypeString {
			t.Errorf("JSONType = %q, want %q", dt.JSONType, JSONTypeString)
		}
		if dt.GoType != GoTypeMoney {
			t.Errorf("GoType = %q, want %q", dt.GoType, GoTypeMoney)
		}
		for db, want := range map[base.DXDatabaseType]string{
			base.DXDatabaseTypePostgreSQL: "NUMERIC(23,4)",
			base.DXDatabaseTypeSQLServer:  "DECIMAL(23,4)",
			base.DXDatabaseTypeMariaDB:    "DECIMAL(23,4)",
			base.DXDatabaseTypeOracle:     "NUMBER(23,4)",
		} {
			if got := dt.TypeByDatabaseType[db]; got != want {
				t.Errorf("%v type = %q, want %q", db, got, want)
			}
		}
	}
}

// The deprecated decimal type stays out of the registry.
func TestDataTypeDecimalNotRegistered(t *testing.T) {
	if _, err := GetDataTypeFromString("decimal"); err == nil {
		t.Errorf("GetDataTypeFromString(decimal) resolved; the deprecated type must stay unregistered")
	}
}
