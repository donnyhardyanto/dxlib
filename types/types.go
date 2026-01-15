package types

import (
	"fmt"
	"strings"
)

type TypeDef struct {
	APIParameterType string
	JSONType         string
	GoType           string
	DbTypePostgreSQL string
	DbTypeSqlserver  string
	DbTypeMysql      string
	DbTypeOracle     string
}

type TModel struct {
}

type APIParameterType string

type JSONType string

const (
	JSONTypeString  JSONType = "string"
	JSONTypeNumber  JSONType = "number"
	JSONTypeBoolean JSONType = "boolean"
	JSONTypeObject  JSONType = "object"
	JSONTypeArray   JSONType = "array"
)

type GoType string

const (
	GoTypeString                  GoType = "string"
	GoTypeStringPointer           GoType = "*string"
	GoTypeInt64                   GoType = "int64"
	GoTypeInt64Pointer            GoType = "*int64"
	GoTypeFloat32                 GoType = "float32"
	GoTypeFloat64                 GoType = "float64"
	GoTypeBool                    GoType = "bool"
	GoTypeTime                    GoType = "time.Time"
	GoTypeMapStringInterface      GoType = "map[string]interface{}"
	GoTypeSliceInterface          GoType = "[]interface{}"
	GoTypeSliceString             GoType = "[]string"
	GoTypeSliceInt64              GoType = "[]int64"
	GoTypeSliceMapStringInterface GoType = "[]map[string]interface{}"
)

const (
	// String types
	APIParameterTypeString             APIParameterType = "string"
	APIParameterTypeProtectedString    APIParameterType = "protected-string"
	APIParameterTypeProtectedSQLString APIParameterType = "protected-sql-string"
	APIParameterTypeNullableString     APIParameterType = "nullable-string"
	APIParameterTypeNonEmptyString     APIParameterType = "non-empty-string"
	APIParameterTypeEmail              APIParameterType = "email"
	APIParameterTypePhoneNumber        APIParameterType = "phonenumber"
	APIParameterTypeNPWP               APIParameterType = "npwp"

	// Integer types
	APIParameterTypeInt64         APIParameterType = "int64"
	APIParameterTypeInt64P        APIParameterType = "int64p"
	APIParameterTypeInt64ZP       APIParameterType = "int64zp"
	APIParameterTypeNullableInt64 APIParameterType = "nullable-int64"

	// Float32 types
	APIParameterTypeFloat32   APIParameterType = "float32"
	APIParameterTypeFloat32P  APIParameterType = "float32p"
	APIParameterTypeFloat32ZP APIParameterType = "float32zp"

	// Float64 types
	APIParameterTypeFloat64   APIParameterType = "float64"
	APIParameterTypeFloat64P  APIParameterType = "float64p"
	APIParameterTypeFloat64ZP APIParameterType = "float64zp"

	// Boolean type
	APIParameterTypeBoolean APIParameterType = "bool"

	// Date/Time types
	APIParameterTypeISO8601 APIParameterType = "iso8601"
	APIParameterTypeDate    APIParameterType = "date"
	APIParameterTypeTime    APIParameterType = "time"

	// JSON types
	APIParameterTypeJSON            APIParameterType = "json"
	APIParameterTypeJSONPassthrough APIParameterType = "json-passthrough"

	// Array types
	APIParameterTypeArray             APIParameterType = "array"
	APIParameterTypeArrayString       APIParameterType = "array-string"
	APIParameterTypeArrayInt64        APIParameterType = "array-int64"
	APIParameterTypeArrayJSONTemplate APIParameterType = "array-json-template"
)

var (
	TypeDefString = TypeDef{
		APIParameterType: string(APIParameterTypeString),
		JSONType:         string(JSONTypeString),
		GoType:           string(GoTypeString),
		DbTypePostgreSQL: "VARCHAR(1024)",
		DbTypeSqlserver:  "VARCHAR(1024)",
		DbTypeMysql:      "VARCHAR(1024)",
		DbTypeOracle:     "VARCHAR(1024)",
	}

	TypeDefProtectedString = TypeDef{
		APIParameterType: string(APIParameterTypeProtectedString),
		JSONType:         string(JSONTypeString),
		GoType:           string(GoTypeString),
		DbTypePostgreSQL: "VARCHAR(1024)",
		DbTypeSqlserver:  "VARCHAR(1024)",
		DbTypeMysql:      "VARCHAR(1024)",
		DbTypeOracle:     "VARCHAR(1024)",
	}

	TypeDefProtectedSQLString = TypeDef{
		APIParameterType: string(APIParameterTypeProtectedSQLString),
		JSONType:         string(JSONTypeString),
		GoType:           string(GoTypeString),
		DbTypePostgreSQL: "VARCHAR(1024)",
		DbTypeSqlserver:  "VARCHAR(1024)",
		DbTypeMysql:      "VARCHAR(1024)",
		DbTypeOracle:     "VARCHAR(1024)",
	}

	TypeDefNullableString = TypeDef{
		APIParameterType: string(APIParameterTypeNullableString),
		JSONType:         string(JSONTypeString),
		GoType:           string(GoTypeStringPointer),
		DbTypePostgreSQL: "VARCHAR(1024)",
		DbTypeSqlserver:  "VARCHAR(1024)",
		DbTypeMysql:      "VARCHAR(1024)",
		DbTypeOracle:     "VARCHAR(1024)",
	}

	TypeDefNonEmptyString = TypeDef{
		APIParameterType: string(APIParameterTypeNonEmptyString),
		JSONType:         string(JSONTypeString),
		GoType:           string(GoTypeString),
		DbTypePostgreSQL: "VARCHAR(1024)",
		DbTypeSqlserver:  "VARCHAR(1024)",
		DbTypeMysql:      "VARCHAR(1024)",
		DbTypeOracle:     "VARCHAR(1024)",
	}

	TypeDefEmail = TypeDef{
		APIParameterType: string(APIParameterTypeEmail),
		JSONType:         string(JSONTypeString),
		GoType:           string(GoTypeString),
		DbTypePostgreSQL: "VARCHAR(255)",
		DbTypeSqlserver:  "VARCHAR(255)",
		DbTypeMysql:      "VARCHAR(255)",
		DbTypeOracle:     "VARCHAR(255)",
	}

	TypeDefPhoneNumber = TypeDef{
		APIParameterType: string(APIParameterTypePhoneNumber),
		JSONType:         string(JSONTypeString),
		GoType:           string(GoTypeString),
		DbTypePostgreSQL: "VARCHAR(50)",
		DbTypeSqlserver:  "VARCHAR(50)",
		DbTypeMysql:      "VARCHAR(50)",
		DbTypeOracle:     "VARCHAR(50)",
	}

	TypeDefNPWP = TypeDef{
		APIParameterType: string(APIParameterTypeNPWP),
		JSONType:         string(JSONTypeString),
		GoType:           string(GoTypeString),
		DbTypePostgreSQL: "VARCHAR(50)",
		DbTypeSqlserver:  "VARCHAR(50)",
		DbTypeMysql:      "VARCHAR(50)",
		DbTypeOracle:     "VARCHAR(50)",
	}

	// Integer types
	TypeDefInt64 = TypeDef{
		APIParameterType: string(APIParameterTypeInt64),
		JSONType:         string(JSONTypeNumber),
		GoType:           string(GoTypeInt64),
		DbTypePostgreSQL: "BIGINT",
		DbTypeSqlserver:  "BIGINT",
		DbTypeMysql:      "BIGINT",
		DbTypeOracle:     "NUMBER(19)",
	}

	TypeDefInt64P = TypeDef{
		APIParameterType: string(APIParameterTypeInt64P),
		JSONType:         string(JSONTypeNumber),
		GoType:           string(GoTypeInt64),
		DbTypePostgreSQL: "BIGINT",
		DbTypeSqlserver:  "BIGINT",
		DbTypeMysql:      "BIGINT",
		DbTypeOracle:     "NUMBER(19)",
	}

	TypeDefInt64ZP = TypeDef{
		APIParameterType: string(APIParameterTypeInt64ZP),
		JSONType:         string(JSONTypeNumber),
		GoType:           string(GoTypeInt64),
		DbTypePostgreSQL: "BIGINT",
		DbTypeSqlserver:  "BIGINT",
		DbTypeMysql:      "BIGINT",
		DbTypeOracle:     "NUMBER(19)",
	}

	TypeDefNullableInt64 = TypeDef{
		APIParameterType: string(APIParameterTypeNullableInt64),
		JSONType:         string(JSONTypeNumber),
		GoType:           string(GoTypeInt64Pointer),
		DbTypePostgreSQL: "BIGINT",
		DbTypeSqlserver:  "BIGINT",
		DbTypeMysql:      "BIGINT",
		DbTypeOracle:     "NUMBER(19)",
	}

	// Float32 types
	TypeDefFloat32 = TypeDef{
		APIParameterType: string(APIParameterTypeFloat32),
		JSONType:         string(JSONTypeNumber),
		GoType:           string(GoTypeFloat32),
		DbTypePostgreSQL: "REAL",
		DbTypeSqlserver:  "REAL",
		DbTypeMysql:      "FLOAT",
		DbTypeOracle:     "BINARY_FLOAT",
	}

	TypeDefFloat32P = TypeDef{
		APIParameterType: string(APIParameterTypeFloat32P),
		JSONType:         string(JSONTypeNumber),
		GoType:           string(GoTypeFloat32),
		DbTypePostgreSQL: "REAL",
		DbTypeSqlserver:  "REAL",
		DbTypeMysql:      "FLOAT",
		DbTypeOracle:     "BINARY_FLOAT",
	}

	TypeDefFloat32ZP = TypeDef{
		APIParameterType: string(APIParameterTypeFloat32ZP),
		JSONType:         string(JSONTypeNumber),
		GoType:           string(GoTypeFloat32),
		DbTypePostgreSQL: "REAL",
		DbTypeSqlserver:  "REAL",
		DbTypeMysql:      "FLOAT",
		DbTypeOracle:     "BINARY_FLOAT",
	}

	// Float64 types
	TypeDefFloat64 = TypeDef{
		APIParameterType: string(APIParameterTypeFloat64),
		JSONType:         string(JSONTypeNumber),
		GoType:           string(GoTypeFloat64),
		DbTypePostgreSQL: "DOUBLE PRECISION",
		DbTypeSqlserver:  "FLOAT",
		DbTypeMysql:      "DOUBLE",
		DbTypeOracle:     "BINARY_DOUBLE",
	}

	TypeDefFloat64P = TypeDef{
		APIParameterType: string(APIParameterTypeFloat64P),
		JSONType:         string(JSONTypeNumber),
		GoType:           string(GoTypeFloat64),
		DbTypePostgreSQL: "DOUBLE PRECISION",
		DbTypeSqlserver:  "FLOAT",
		DbTypeMysql:      "DOUBLE",
		DbTypeOracle:     "BINARY_DOUBLE",
	}

	TypeDefFloat64ZP = TypeDef{
		APIParameterType: string(APIParameterTypeFloat64ZP),
		JSONType:         string(JSONTypeNumber),
		GoType:           string(GoTypeFloat64),
		DbTypePostgreSQL: "DOUBLE PRECISION",
		DbTypeSqlserver:  "FLOAT",
		DbTypeMysql:      "DOUBLE",
		DbTypeOracle:     "BINARY_DOUBLE",
	}

	// Boolean type
	TypeDefBool = TypeDef{
		APIParameterType: string(APIParameterTypeBoolean),
		JSONType:         string(JSONTypeBoolean),
		GoType:           string(GoTypeBool),
		DbTypePostgreSQL: "BOOLEAN",
		DbTypeSqlserver:  "BIT",
		DbTypeMysql:      "BOOLEAN",
		DbTypeOracle:     "NUMBER(1)",
	}

	// Date/Time types
	TypeDefISO8601 = TypeDef{
		APIParameterType: string(APIParameterTypeISO8601),
		JSONType:         string(JSONTypeString),
		GoType:           string(GoTypeTime),
		DbTypePostgreSQL: "TIMESTAMP WITH TIME ZONE",
		DbTypeSqlserver:  "DATETIMEOFFSET",
		DbTypeMysql:      "DATETIME",
		DbTypeOracle:     "TIMESTAMP WITH TIME ZONE",
	}

	TypeDefDate = TypeDef{
		APIParameterType: string(APIParameterTypeDate),
		JSONType:         string(JSONTypeString),
		GoType:           string(GoTypeTime),
		DbTypePostgreSQL: "DATE",
		DbTypeSqlserver:  "DATE",
		DbTypeMysql:      "DATE",
		DbTypeOracle:     "DATE",
	}

	TypeDefTime = TypeDef{
		APIParameterType: string(APIParameterTypeTime),
		JSONType:         string(JSONTypeString),
		GoType:           string(GoTypeTime),
		DbTypePostgreSQL: "TIME",
		DbTypeSqlserver:  "TIME",
		DbTypeMysql:      "TIME",
		DbTypeOracle:     "DATE",
	}

	// JSON types
	TypeDefJSON = TypeDef{
		APIParameterType: string(APIParameterTypeJSON),
		JSONType:         string(JSONTypeObject),
		GoType:           string(GoTypeMapStringInterface),
		DbTypePostgreSQL: "JSONB",
		DbTypeSqlserver:  "NVARCHAR(MAX)",
		DbTypeMysql:      "JSON",
		DbTypeOracle:     "CLOB",
	}

	TypeDefJSONPassthrough = TypeDef{
		APIParameterType: string(APIParameterTypeJSONPassthrough),
		JSONType:         string(JSONTypeObject),
		GoType:           string(GoTypeMapStringInterface),
		DbTypePostgreSQL: "JSONB",
		DbTypeSqlserver:  "NVARCHAR(MAX)",
		DbTypeMysql:      "JSON",
		DbTypeOracle:     "CLOB",
	}

	// Array types
	TypeDefArray = TypeDef{
		APIParameterType: string(APIParameterTypeArray),
		JSONType:         string(JSONTypeArray),
		GoType:           string(GoTypeSliceInterface),
		DbTypePostgreSQL: "JSONB",
		DbTypeSqlserver:  "NVARCHAR(MAX)",
		DbTypeMysql:      "JSON",
		DbTypeOracle:     "CLOB",
	}

	TypeDefArrayString = TypeDef{
		APIParameterType: string(APIParameterTypeArrayString),
		JSONType:         string(JSONTypeArray),
		GoType:           string(GoTypeSliceString),
		DbTypePostgreSQL: "TEXT[]",
		DbTypeSqlserver:  "NVARCHAR(MAX)",
		DbTypeMysql:      "JSON",
		DbTypeOracle:     "CLOB",
	}

	TypeDefArrayInt64 = TypeDef{
		APIParameterType: string(APIParameterTypeArrayInt64),
		JSONType:         string(JSONTypeArray),
		GoType:           string(GoTypeSliceInt64),
		DbTypePostgreSQL: "BIGINT[]",
		DbTypeSqlserver:  "NVARCHAR(MAX)",
		DbTypeMysql:      "JSON",
		DbTypeOracle:     "CLOB",
	}

	TypeDefArrayJSONTemplate = TypeDef{
		APIParameterType: string(APIParameterTypeArrayJSONTemplate),
		JSONType:         string(JSONTypeArray),
		GoType:           string(GoTypeSliceMapStringInterface),
		DbTypePostgreSQL: "JSONB",
		DbTypeSqlserver:  "NVARCHAR(MAX)",
		DbTypeMysql:      "JSON",
		DbTypeOracle:     "CLOB",
	}
)

var (
	TypeDefs = []TypeDef{
		// String types
		TypeDefString,
		TypeDefProtectedString,
		TypeDefProtectedSQLString,
		TypeDefNullableString,
		TypeDefNonEmptyString,
		TypeDefEmail,
		TypeDefPhoneNumber,
		TypeDefNPWP,

		// Integer types
		TypeDefInt64,
		TypeDefInt64P,
		TypeDefInt64ZP,
		TypeDefNullableInt64,

		// Float32 types
		TypeDefFloat32,
		TypeDefFloat32P,
		TypeDefFloat32ZP,

		// Float64 types
		TypeDefFloat64,
		TypeDefFloat64P,
		TypeDefFloat64ZP,

		// Boolean type
		TypeDefBool,

		// Date/Time types
		TypeDefISO8601,
		TypeDefDate,
		TypeDefTime,

		// JSON types
		TypeDefJSON,
		TypeDefJSONPassthrough,

		// Array types
		TypeDefArray,
		TypeDefArrayString,
		TypeDefArrayInt64,
		TypeDefArrayJSONTemplate,
	}

	Types = map[APIParameterType]TypeDef{
		// String types
		APIParameterTypeString:             TypeDefString,
		APIParameterTypeProtectedString:    TypeDefProtectedString,
		APIParameterTypeProtectedSQLString: TypeDefProtectedSQLString,
		APIParameterTypeNullableString:     TypeDefNullableString,
		APIParameterTypeNonEmptyString:     TypeDefNonEmptyString,
		APIParameterTypeEmail:              TypeDefEmail,
		APIParameterTypePhoneNumber:        TypeDefPhoneNumber,
		APIParameterTypeNPWP:               TypeDefNPWP,

		// Integer types
		APIParameterTypeInt64:         TypeDefInt64,
		APIParameterTypeInt64P:        TypeDefInt64P,
		APIParameterTypeInt64ZP:       TypeDefInt64ZP,
		APIParameterTypeNullableInt64: TypeDefNullableInt64,

		// Float32 types
		APIParameterTypeFloat32:   TypeDefFloat32,
		APIParameterTypeFloat32P:  TypeDefFloat32P,
		APIParameterTypeFloat32ZP: TypeDefFloat32ZP,

		// Float64 types
		APIParameterTypeFloat64:   TypeDefFloat64,
		APIParameterTypeFloat64P:  TypeDefFloat64P,
		APIParameterTypeFloat64ZP: TypeDefFloat64ZP,

		// Boolean type
		APIParameterTypeBoolean: TypeDefBool,

		// Date/Time types
		APIParameterTypeISO8601: TypeDefISO8601,
		APIParameterTypeDate:    TypeDefDate,
		APIParameterTypeTime:    TypeDefTime,

		// JSON types
		APIParameterTypeJSON:            TypeDefJSON,
		APIParameterTypeJSONPassthrough: TypeDefJSONPassthrough,

		// Array types
		APIParameterTypeArray:             TypeDefArray,
		APIParameterTypeArrayString:       TypeDefArrayString,
		APIParameterTypeArrayInt64:        TypeDefArrayInt64,
		APIParameterTypeArrayJSONTemplate: TypeDefArrayJSONTemplate,
	}
)

func GetTypeDefFromString(s string) (*TypeDef, error) {
	s = strings.Trim(s, " ")
	s = strings.ToLower(s)
	for _, t := range Types {
		if t.APIParameterType == s {
			return &t, nil
		}
	}
	return nil, fmt.Errorf("unknown type: %s", s)
}
