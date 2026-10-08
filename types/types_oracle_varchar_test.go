package types

import (
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/base"
)

// Oracle keeps VARCHAR reserved for a future redefinition and documents
// VARCHAR2 as the type to write; every string type must emit it on Oracle.
func TestNoTypeEmitsPlainVarcharOnOracle(t *testing.T) {
	for apiType, dt := range Types {
		got, ok := dt.TypeByDatabaseType[base.DXDatabaseTypeOracle]
		if !ok {
			continue
		}
		if strings.HasPrefix(strings.ToUpper(got), "VARCHAR(") {
			t.Errorf("%s: Oracle type %q, want VARCHAR2(n)", apiType, got)
		}
	}
}

// The nine parameter string types took the engine-agnostic VARCHAR constants
// for Oracle in a literals-to-constants refactor; they must say VARCHAR2 like
// UID and the width types do.
func TestParameterStringTypesUseVarchar2OnOracle(t *testing.T) {
	for name, c := range map[string]struct {
		dt   DataType
		want string
	}{
		"String":                  {DataTypeString, "VARCHAR2(1024)"},
		"ProtectedString":         {DataTypeProtectedString, "VARCHAR2(1024)"},
		"ProtectedSQLString":      {DataTypeProtectedSQLString, "VARCHAR2(1024)"},
		"ProtectedNonEmptyString": {DataTypeProtectedNonEmptyString, "VARCHAR2(1024)"},
		"NullableString":          {DataTypeNullableString, "VARCHAR2(1024)"},
		"NonEmptyString":          {DataTypeNonEmptyString, "VARCHAR2(1024)"},
		"Email":                   {DataTypeEmail, "VARCHAR2(255)"},
		"PhoneNumber":             {DataTypePhoneNumber, "VARCHAR2(255)"},
		"NPWP":                    {DataTypeNPWP, "VARCHAR2(255)"},
	} {
		if got := c.dt.TypeByDatabaseType[base.DXDatabaseTypeOracle]; got != c.want {
			t.Errorf("%s: Oracle = %q, want %q", name, got, c.want)
		}
		// The other engines keep plain VARCHAR of the same width.
		width := strings.TrimSuffix(strings.TrimPrefix(c.want, "VARCHAR2("), ")")
		for _, db := range []base.DXDatabaseType{base.DXDatabaseTypePostgreSQL, base.DXDatabaseTypeSQLServer, base.DXDatabaseTypeMariaDB} {
			if got := c.dt.TypeByDatabaseType[db]; got != "VARCHAR("+width+")" {
				t.Errorf("%s: %v = %q, want VARCHAR(%s)", name, db, got, width)
			}
		}
	}
}
