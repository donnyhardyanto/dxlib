package types

import (
	"fmt"
	"strings"
)

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
	GoTypeSliceByte               GoType = "[]byte"
	GoTypeSliceMapStringInterface GoType = "[]map[string]interface{}"
)

const (
	APIParameterTypeEncryptedBlob APIParameterType = "encrypted-blob"
	APIParameterTypeBlob          APIParameterType = "blob"

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

type DataType struct {
	APIParameterType APIParameterType
	JSONType         JSONType
	GoType           GoType
	DbTypePostgreSQL string
	DbTypeSqlserver  string
	DbTypeMysql      string
	DbTypeOracle     string
}

type Field struct {
	Name               string
	Type               DataType
	IsPrimaryKey       bool
	IsAutoIncrement    bool
	IsNotNull          bool
	IsUnique           bool
	DefaultValue       string            // SQL expression for DEFAULT clause (used when DefaultValueByDBType not specified)
	DefaultValueByDBType map[string]string // Database-specific default values. Keys: "postgresql", "sqlserver", "oracle", "mariadb"
	IsEncrypted        bool
	IsHashed           bool
	EncryptedDataName  string
	EncryptedDataType  DataType
	EncryptedDataKeyID string
	HashDataName       string
	HashDataType       DataType
	HashDataSaltID     string
}

// DBTypeKeyPostgreSQL is the key for PostgreSQL in DefaultValueByDBType map
const DBTypeKeyPostgreSQL = "postgresql"

// DBTypeKeySQLServer is the key for SQL Server in DefaultValueByDBType map
const DBTypeKeySQLServer = "sqlserver"

// DBTypeKeyOracle is the key for Oracle in DefaultValueByDBType map
const DBTypeKeyOracle = "oracle"

// DBTypeKeyMariaDB is the key for MariaDB/MySQL in DefaultValueByDBType map
const DBTypeKeyMariaDB = "mariadb"
type Entity struct {
	Name   string
	Type   DataType
	Fields []Field
}

var (
	DataTypeEncryptedBlob = DataType{
		APIParameterType: APIParameterTypeEncryptedBlob,
		JSONType:         JSONTypeString,
		GoType:           GoTypeSliceByte,
		DbTypePostgreSQL: "BYTEA",
		DbTypeSqlserver:  "VARBINARY(MAX)",
		DbTypeMysql:      "LONGBLOB",
		DbTypeOracle:     "BLOB",
	}

	DataTypeBlob = DataType{
		APIParameterType: APIParameterTypeBlob,
		JSONType:         JSONTypeString,
		GoType:           GoTypeSliceByte,
		DbTypePostgreSQL: "BYTEA",
		DbTypeSqlserver:  "VARBINARY(MAX)",
		DbTypeMysql:      "LONGBLOB",
		DbTypeOracle:     "BLOB",
	}

	DataTypeString = DataType{
		APIParameterType: APIParameterTypeString,
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbTypePostgreSQL: "VARCHAR(1024)",
		DbTypeSqlserver:  "VARCHAR(1024)",
		DbTypeMysql:      "VARCHAR(1024)",
		DbTypeOracle:     "VARCHAR(1024)",
	}

	DataTypeProtectedString = DataType{
		APIParameterType: APIParameterTypeProtectedString,
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbTypePostgreSQL: "VARCHAR(1024)",
		DbTypeSqlserver:  "VARCHAR(1024)",
		DbTypeMysql:      "VARCHAR(1024)",
		DbTypeOracle:     "VARCHAR(1024)",
	}

	DataTypeProtectedSQLString = DataType{
		APIParameterType: APIParameterTypeProtectedSQLString,
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbTypePostgreSQL: "VARCHAR(1024)",
		DbTypeSqlserver:  "VARCHAR(1024)",
		DbTypeMysql:      "VARCHAR(1024)",
		DbTypeOracle:     "VARCHAR(1024)",
	}

	DataTypeNullableString = DataType{
		APIParameterType: APIParameterTypeNullableString,
		JSONType:         JSONTypeString,
		GoType:           GoTypeStringPointer,
		DbTypePostgreSQL: "VARCHAR(1024)",
		DbTypeSqlserver:  "VARCHAR(1024)",
		DbTypeMysql:      "VARCHAR(1024)",
		DbTypeOracle:     "VARCHAR(1024)",
	}

	DataTypeNonEmptyString = DataType{
		APIParameterType: APIParameterTypeNonEmptyString,
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbTypePostgreSQL: "VARCHAR(1024)",
		DbTypeSqlserver:  "VARCHAR(1024)",
		DbTypeMysql:      "VARCHAR(1024)",
		DbTypeOracle:     "VARCHAR(1024)",
	}

	DataTypeEmail = DataType{
		APIParameterType: APIParameterTypeEmail,
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbTypePostgreSQL: "VARCHAR(255)",
		DbTypeSqlserver:  "VARCHAR(255)",
		DbTypeMysql:      "VARCHAR(255)",
		DbTypeOracle:     "VARCHAR(255)",
	}

	DataTypePhoneNumber = DataType{
		APIParameterType: APIParameterTypePhoneNumber,
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbTypePostgreSQL: "VARCHAR(50)",
		DbTypeSqlserver:  "VARCHAR(50)",
		DbTypeMysql:      "VARCHAR(50)",
		DbTypeOracle:     "VARCHAR(50)",
	}

	DataTypeNPWP = DataType{
		APIParameterType: APIParameterTypeNPWP,
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbTypePostgreSQL: "VARCHAR(50)",
		DbTypeSqlserver:  "VARCHAR(50)",
		DbTypeMysql:      "VARCHAR(50)",
		DbTypeOracle:     "VARCHAR(50)",
	}

	// Integer types
	DataTypeInt64 = DataType{
		APIParameterType: APIParameterTypeInt64,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeInt64,
		DbTypePostgreSQL: "BIGINT",
		DbTypeSqlserver:  "BIGINT",
		DbTypeMysql:      "BIGINT",
		DbTypeOracle:     "NUMBER(19)",
	}

	DataTypeInt64P = DataType{
		APIParameterType: APIParameterTypeInt64P,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeInt64,
		DbTypePostgreSQL: "BIGINT",
		DbTypeSqlserver:  "BIGINT",
		DbTypeMysql:      "BIGINT",
		DbTypeOracle:     "NUMBER(19)",
	}

	DataTypeInt64ZP = DataType{
		APIParameterType: APIParameterTypeInt64ZP,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeInt64,
		DbTypePostgreSQL: "BIGINT",
		DbTypeSqlserver:  "BIGINT",
		DbTypeMysql:      "BIGINT",
		DbTypeOracle:     "NUMBER(19)",
	}

	DataTypeNullableInt64 = DataType{
		APIParameterType: APIParameterTypeNullableInt64,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeInt64Pointer,
		DbTypePostgreSQL: "BIGINT",
		DbTypeSqlserver:  "BIGINT",
		DbTypeMysql:      "BIGINT",
		DbTypeOracle:     "NUMBER(19)",
	}

	// Float32 types
	DataTypeFloat32 = DataType{
		APIParameterType: APIParameterTypeFloat32,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeFloat32,
		DbTypePostgreSQL: "REAL",
		DbTypeSqlserver:  "REAL",
		DbTypeMysql:      "FLOAT",
		DbTypeOracle:     "BINARY_FLOAT",
	}

	DataTypeFloat32P = DataType{
		APIParameterType: APIParameterTypeFloat32P,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeFloat32,
		DbTypePostgreSQL: "REAL",
		DbTypeSqlserver:  "REAL",
		DbTypeMysql:      "FLOAT",
		DbTypeOracle:     "BINARY_FLOAT",
	}

	DataTypeFloat32ZP = DataType{
		APIParameterType: APIParameterTypeFloat32ZP,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeFloat32,
		DbTypePostgreSQL: "REAL",
		DbTypeSqlserver:  "REAL",
		DbTypeMysql:      "FLOAT",
		DbTypeOracle:     "BINARY_FLOAT",
	}

	// Float64 types
	DataTypeFloat64 = DataType{
		APIParameterType: APIParameterTypeFloat64,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeFloat64,
		DbTypePostgreSQL: "DOUBLE PRECISION",
		DbTypeSqlserver:  "FLOAT",
		DbTypeMysql:      "DOUBLE",
		DbTypeOracle:     "BINARY_DOUBLE",
	}

	DataTypeFloat64P = DataType{
		APIParameterType: APIParameterTypeFloat64P,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeFloat64,
		DbTypePostgreSQL: "DOUBLE PRECISION",
		DbTypeSqlserver:  "FLOAT",
		DbTypeMysql:      "DOUBLE",
		DbTypeOracle:     "BINARY_DOUBLE",
	}

	DataTypeFloat64ZP = DataType{
		APIParameterType: APIParameterTypeFloat64ZP,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeFloat64,
		DbTypePostgreSQL: "DOUBLE PRECISION",
		DbTypeSqlserver:  "FLOAT",
		DbTypeMysql:      "DOUBLE",
		DbTypeOracle:     "BINARY_DOUBLE",
	}

	// Boolean type
	DataTypeBool = DataType{
		APIParameterType: APIParameterTypeBoolean,
		JSONType:         JSONTypeBoolean,
		GoType:           GoTypeBool,
		DbTypePostgreSQL: "BOOLEAN",
		DbTypeSqlserver:  "BIT",
		DbTypeMysql:      "BOOLEAN",
		DbTypeOracle:     "NUMBER(1)",
	}

	// Date/Time types
	DataTypeISO8601 = DataType{
		APIParameterType: APIParameterTypeISO8601,
		JSONType:         JSONTypeString,
		GoType:           GoTypeTime,
		DbTypePostgreSQL: "TIMESTAMP WITH TIME ZONE",
		DbTypeSqlserver:  "DATETIMEOFFSET",
		DbTypeMysql:      "DATETIME",
		DbTypeOracle:     "TIMESTAMP WITH TIME ZONE",
	}

	DataTypeDate = DataType{
		APIParameterType: APIParameterTypeDate,
		JSONType:         JSONTypeString,
		GoType:           GoTypeTime,
		DbTypePostgreSQL: "DATE",
		DbTypeSqlserver:  "DATE",
		DbTypeMysql:      "DATE",
		DbTypeOracle:     "DATE",
	}

	DataTypeTime = DataType{
		APIParameterType: APIParameterTypeTime,
		JSONType:         JSONTypeString,
		GoType:           GoTypeTime,
		DbTypePostgreSQL: "TIME",
		DbTypeSqlserver:  "TIME",
		DbTypeMysql:      "TIME",
		DbTypeOracle:     "DATE",
	}

	// JSON types
	DataTypeJSON = DataType{
		APIParameterType: APIParameterTypeJSON,
		JSONType:         JSONTypeObject,
		GoType:           GoTypeMapStringInterface,
		DbTypePostgreSQL: "JSONB",
		DbTypeSqlserver:  "NVARCHAR(MAX)",
		DbTypeMysql:      "JSON",
		DbTypeOracle:     "CLOB",
	}

	DataTypeJSONPassthrough = DataType{
		APIParameterType: APIParameterTypeJSONPassthrough,
		JSONType:         JSONTypeObject,
		GoType:           GoTypeMapStringInterface,
		DbTypePostgreSQL: "JSONB",
		DbTypeSqlserver:  "NVARCHAR(MAX)",
		DbTypeMysql:      "JSON",
		DbTypeOracle:     "CLOB",
	}

	// Array types
	DataTypeArray = DataType{
		APIParameterType: APIParameterTypeArray,
		JSONType:         JSONTypeArray,
		GoType:           GoTypeSliceInterface,
		DbTypePostgreSQL: "JSONB",
		DbTypeSqlserver:  "NVARCHAR(MAX)",
		DbTypeMysql:      "JSON",
		DbTypeOracle:     "CLOB",
	}

	DataTypeArrayString = DataType{
		APIParameterType: APIParameterTypeArrayString,
		JSONType:         JSONTypeArray,
		GoType:           GoTypeSliceString,
		DbTypePostgreSQL: "TEXT[]",
		DbTypeSqlserver:  "NVARCHAR(MAX)",
		DbTypeMysql:      "JSON",
		DbTypeOracle:     "CLOB",
	}

	DataTypeArrayInt64 = DataType{
		APIParameterType: APIParameterTypeArrayInt64,
		JSONType:         JSONTypeArray,
		GoType:           GoTypeSliceInt64,
		DbTypePostgreSQL: "BIGINT[]",
		DbTypeSqlserver:  "NVARCHAR(MAX)",
		DbTypeMysql:      "JSON",
		DbTypeOracle:     "CLOB",
	}

	DataTypeArrayJSONTemplate = DataType{
		APIParameterType: APIParameterTypeArrayJSONTemplate,
		JSONType:         JSONTypeArray,
		GoType:           GoTypeSliceMapStringInterface,
		DbTypePostgreSQL: "JSONB",
		DbTypeSqlserver:  "NVARCHAR(MAX)",
		DbTypeMysql:      "JSON",
		DbTypeOracle:     "CLOB",
	}

	// Serial/Auto-increment type
	DataTypeSerial = DataType{
		APIParameterType: "serial",
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeInt64,
		DbTypePostgreSQL: "SERIAL",
		DbTypeSqlserver:  "INT IDENTITY(1,1)",
		DbTypeMysql:      "INT AUTO_INCREMENT",
		DbTypeOracle:     "NUMBER GENERATED BY DEFAULT AS IDENTITY",
	}

	// Regular integer (not bigint)
	DataTypeInt = DataType{
		APIParameterType: "int",
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeInt64,
		DbTypePostgreSQL: "INT",
		DbTypeSqlserver:  "INT",
		DbTypeMysql:      "INT",
		DbTypeOracle:     "NUMBER(10)",
	}

	// String with 255 length
	DataTypeString255 = DataType{
		APIParameterType: "string255",
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbTypePostgreSQL: "VARCHAR(255)",
		DbTypeSqlserver:  "VARCHAR(255)",
		DbTypeMysql:      "VARCHAR(255)",
		DbTypeOracle:     "VARCHAR2(255)",
	}

	// String with 32768 length (for large text)
	DataTypeString32768 = DataType{
		APIParameterType: "string32768",
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbTypePostgreSQL: "VARCHAR(32768)",
		DbTypeSqlserver:  "VARCHAR(MAX)",
		DbTypeMysql:      "TEXT",
		DbTypeOracle:     "CLOB",
	}
)

var (
	DataTypes = []DataType{
		// String types
		DataTypeString,
		DataTypeProtectedString,
		DataTypeProtectedSQLString,
		DataTypeNullableString,
		DataTypeNonEmptyString,
		DataTypeEmail,
		DataTypePhoneNumber,
		DataTypeNPWP,

		// Integer types
		DataTypeInt64,
		DataTypeInt64P,
		DataTypeInt64ZP,
		DataTypeNullableInt64,

		// Float32 types
		DataTypeFloat32,
		DataTypeFloat32P,
		DataTypeFloat32ZP,

		// Float64 types
		DataTypeFloat64,
		DataTypeFloat64P,
		DataTypeFloat64ZP,

		// Boolean type
		DataTypeBool,

		// Date/Time types
		DataTypeISO8601,
		DataTypeDate,
		DataTypeTime,

		// JSON types
		DataTypeJSON,
		DataTypeJSONPassthrough,

		// Array types
		DataTypeArray,
		DataTypeArrayString,
		DataTypeArrayInt64,
		DataTypeArrayJSONTemplate,
	}

	Types = map[APIParameterType]DataType{
		// String types
		APIParameterTypeString:             DataTypeString,
		APIParameterTypeProtectedString:    DataTypeProtectedString,
		APIParameterTypeProtectedSQLString: DataTypeProtectedSQLString,
		APIParameterTypeNullableString:     DataTypeNullableString,
		APIParameterTypeNonEmptyString:     DataTypeNonEmptyString,
		APIParameterTypeEmail:              DataTypeEmail,
		APIParameterTypePhoneNumber:        DataTypePhoneNumber,
		APIParameterTypeNPWP:               DataTypeNPWP,

		// Integer types
		APIParameterTypeInt64:         DataTypeInt64,
		APIParameterTypeInt64P:        DataTypeInt64P,
		APIParameterTypeInt64ZP:       DataTypeInt64ZP,
		APIParameterTypeNullableInt64: DataTypeNullableInt64,

		// Float32 types
		APIParameterTypeFloat32:   DataTypeFloat32,
		APIParameterTypeFloat32P:  DataTypeFloat32P,
		APIParameterTypeFloat32ZP: DataTypeFloat32ZP,

		// Float64 types
		APIParameterTypeFloat64:   DataTypeFloat64,
		APIParameterTypeFloat64P:  DataTypeFloat64P,
		APIParameterTypeFloat64ZP: DataTypeFloat64ZP,

		// Boolean type
		APIParameterTypeBoolean: DataTypeBool,

		// Date/Time types
		APIParameterTypeISO8601: DataTypeISO8601,
		APIParameterTypeDate:    DataTypeDate,
		APIParameterTypeTime:    DataTypeTime,

		// JSON types
		APIParameterTypeJSON:            DataTypeJSON,
		APIParameterTypeJSONPassthrough: DataTypeJSONPassthrough,

		// Array types
		APIParameterTypeArray:             DataTypeArray,
		APIParameterTypeArrayString:       DataTypeArrayString,
		APIParameterTypeArrayInt64:        DataTypeArrayInt64,
		APIParameterTypeArrayJSONTemplate: DataTypeArrayJSONTemplate,
	}
)

func GetDataTypeFromString(s string) (*DataType, error) {
	s = strings.Trim(s, " ")
	s = strings.ToLower(s)
	for _, t := range Types {
		if string(t.APIParameterType) == s {
			return &t, nil
		}
	}
	return nil, fmt.Errorf("unknown type: %s", s)
}
