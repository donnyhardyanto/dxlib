package types

import (
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
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
	APIParameterType           APIParameterType
	JSONType                   JSONType
	GoType                     GoType
	TypeByDatabaseType         map[base.DXDatabaseType]string // Database-specific SQL types
	DefaultValueByDatabaseType map[base.DXDatabaseType]string // Database-specific default values for this type
}

// UID Default Expressions for each databases type
// Format: hex(timestamp_microseconds) + uuid

// UIDDefaultExprPostgreSQL is the PostgreSQL UID default expression
const UIDDefaultExprPostgreSQL = "CONCAT(to_hex((extract(epoch from now()) * 1000000)::bigint), gen_random_uuid()::text)"

// UIDDefaultExprSQLServer is the SQL Server UID default expression
const UIDDefaultExprSQLServer = "CONCAT(CONVERT(VARCHAR(50), CAST(DATEDIFF_BIG(MICROSECOND, '1970-01-01', SYSUTCDATETIME()) AS VARBINARY(8)), 2), LOWER(REPLACE(CONVERT(VARCHAR(36), NEWID()), '-', '')))"

// UIDDefaultExprOracle is the Oracle UID default expression
const UIDDefaultExprOracle = "LOWER(TO_CHAR(ROUND((CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE) - TO_DATE('1970-01-01','YYYY-MM-DD')) * 86400000000), 'XXXXXXXXXXXXXXXX')) || LOWER(RAWTOHEX(SYS_GUID()))"

// UIDDefaultExprMariaDB is the MariaDB/MySQL UID default expression
const UIDDefaultExprMariaDB = "CONCAT(HEX(FLOOR(UNIX_TIMESTAMP(NOW(6)) * 1000000)), REPLACE(UUID(), '-', ''))"

var (
	DataTypeEncryptedBlob = DataType{
		APIParameterType:   APIParameterTypeEncryptedBlob,
		JSONType:           JSONTypeString,
		GoType:             GoTypeSliceByte,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "BYTEA", base.DXDatabaseTypeSQLServer: "VARBINARY(MAX)", base.DXDatabaseTypeMariaDB: "LONGBLOB", base.DXDatabaseTypeOracle: "BLOB"},
	}
	DataTypeBlob = DataType{
		APIParameterType:   APIParameterTypeBlob,
		JSONType:           JSONTypeString,
		GoType:             GoTypeSliceByte,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "BYTEA", base.DXDatabaseTypeSQLServer: "VARBINARY(MAX)", base.DXDatabaseTypeMariaDB: "LONGBLOB", base.DXDatabaseTypeOracle: "BLOB"},
	}

	DataTypeUID = DataType{
		APIParameterType:           APIParameterTypeString,
		JSONType:                   JSONTypeString,
		GoType:                     GoTypeString,
		TypeByDatabaseType:         map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(1024)", base.DXDatabaseTypeSQLServer: "VARCHAR(1024)", base.DXDatabaseTypeMariaDB: "VARCHAR(1024)", base.DXDatabaseTypeOracle: "VARCHAR2(1024)"},
		DefaultValueByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: UIDDefaultExprPostgreSQL, base.DXDatabaseTypeSQLServer: UIDDefaultExprSQLServer, base.DXDatabaseTypeOracle: UIDDefaultExprOracle, base.DXDatabaseTypeMariaDB: UIDDefaultExprMariaDB},
	}

	DataTypeString = DataType{
		APIParameterType:   APIParameterTypeString,
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(1024)", base.DXDatabaseTypeSQLServer: "VARCHAR(1024)", base.DXDatabaseTypeMariaDB: "VARCHAR(1024)", base.DXDatabaseTypeOracle: "VARCHAR(1024)"},
	}

	DataTypeProtectedString = DataType{
		APIParameterType:   APIParameterTypeProtectedString,
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(1024)", base.DXDatabaseTypeSQLServer: "VARCHAR(1024)", base.DXDatabaseTypeMariaDB: "VARCHAR(1024)", base.DXDatabaseTypeOracle: "VARCHAR(1024)"},
	}

	DataTypeProtectedSQLString = DataType{
		APIParameterType:   APIParameterTypeProtectedSQLString,
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(1024)", base.DXDatabaseTypeSQLServer: "VARCHAR(1024)", base.DXDatabaseTypeMariaDB: "VARCHAR(1024)", base.DXDatabaseTypeOracle: "VARCHAR(1024)"},
	}

	DataTypeNullableString = DataType{
		APIParameterType:   APIParameterTypeNullableString,
		JSONType:           JSONTypeString,
		GoType:             GoTypeStringPointer,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(1024)", base.DXDatabaseTypeSQLServer: "VARCHAR(1024)", base.DXDatabaseTypeMariaDB: "VARCHAR(1024)", base.DXDatabaseTypeOracle: "VARCHAR(1024)"},
	}

	DataTypeNonEmptyString = DataType{
		APIParameterType:   APIParameterTypeNonEmptyString,
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(1024)", base.DXDatabaseTypeSQLServer: "VARCHAR(1024)", base.DXDatabaseTypeMariaDB: "VARCHAR(1024)", base.DXDatabaseTypeOracle: "VARCHAR(1024)"},
	}

	DataTypeEmail = DataType{
		APIParameterType:   APIParameterTypeEmail,
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(255)", base.DXDatabaseTypeSQLServer: "VARCHAR(255)", base.DXDatabaseTypeMariaDB: "VARCHAR(255)", base.DXDatabaseTypeOracle: "VARCHAR(255)"},
	}

	DataTypePhoneNumber = DataType{
		APIParameterType:   APIParameterTypePhoneNumber,
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(50)", base.DXDatabaseTypeSQLServer: "VARCHAR(50)", base.DXDatabaseTypeMariaDB: "VARCHAR(50)", base.DXDatabaseTypeOracle: "VARCHAR(50)"},
	}

	DataTypeNPWP = DataType{
		APIParameterType:   APIParameterTypeNPWP,
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(50)", base.DXDatabaseTypeSQLServer: "VARCHAR(50)", base.DXDatabaseTypeMariaDB: "VARCHAR(50)", base.DXDatabaseTypeOracle: "VARCHAR(50)"},
	}

	DataTypeInt64 = DataType{
		APIParameterType:   APIParameterTypeInt64,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeInt64,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "BIGINT", base.DXDatabaseTypeSQLServer: "BIGINT", base.DXDatabaseTypeMariaDB: "BIGINT", base.DXDatabaseTypeOracle: "NUMBER(19)"},
	}

	DataTypeInt64P = DataType{
		APIParameterType:   APIParameterTypeInt64P,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeInt64,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "BIGINT", base.DXDatabaseTypeSQLServer: "BIGINT", base.DXDatabaseTypeMariaDB: "BIGINT", base.DXDatabaseTypeOracle: "NUMBER(19)"},
	}

	DataTypeInt64ZP = DataType{
		APIParameterType:   APIParameterTypeInt64ZP,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeInt64,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "BIGINT", base.DXDatabaseTypeSQLServer: "BIGINT", base.DXDatabaseTypeMariaDB: "BIGINT", base.DXDatabaseTypeOracle: "NUMBER(19)"},
	}

	DataTypeNullableInt64 = DataType{
		APIParameterType:   APIParameterTypeNullableInt64,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeInt64Pointer,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "BIGINT", base.DXDatabaseTypeSQLServer: "BIGINT", base.DXDatabaseTypeMariaDB: "BIGINT", base.DXDatabaseTypeOracle: "NUMBER(19)"},
	}

	DataTypeFloat32 = DataType{
		APIParameterType:   APIParameterTypeFloat32,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeFloat32,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "REAL", base.DXDatabaseTypeSQLServer: "REAL", base.DXDatabaseTypeMariaDB: "FLOAT", base.DXDatabaseTypeOracle: "BINARY_FLOAT"},
	}

	DataTypeFloat32P = DataType{
		APIParameterType:   APIParameterTypeFloat32P,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeFloat32,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "REAL", base.DXDatabaseTypeSQLServer: "REAL", base.DXDatabaseTypeMariaDB: "FLOAT", base.DXDatabaseTypeOracle: "BINARY_FLOAT"},
	}

	DataTypeFloat32ZP = DataType{
		APIParameterType:   APIParameterTypeFloat32ZP,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeFloat32,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "REAL", base.DXDatabaseTypeSQLServer: "REAL", base.DXDatabaseTypeMariaDB: "FLOAT", base.DXDatabaseTypeOracle: "BINARY_FLOAT"},
	}

	DataTypeFloat64 = DataType{
		APIParameterType:   APIParameterTypeFloat64,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeFloat64,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "DOUBLE PRECISION", base.DXDatabaseTypeSQLServer: "FLOAT", base.DXDatabaseTypeMariaDB: "DOUBLE", base.DXDatabaseTypeOracle: "BINARY_DOUBLE"},
	}

	DataTypeFloat64P = DataType{
		APIParameterType:   APIParameterTypeFloat64P,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeFloat64,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "DOUBLE PRECISION", base.DXDatabaseTypeSQLServer: "FLOAT", base.DXDatabaseTypeMariaDB: "DOUBLE", base.DXDatabaseTypeOracle: "BINARY_DOUBLE"},
	}

	DataTypeFloat64ZP = DataType{
		APIParameterType:   APIParameterTypeFloat64ZP,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeFloat64,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "DOUBLE PRECISION", base.DXDatabaseTypeSQLServer: "FLOAT", base.DXDatabaseTypeMariaDB: "DOUBLE", base.DXDatabaseTypeOracle: "BINARY_DOUBLE"},
	}

	DataTypeBool = DataType{
		APIParameterType:   APIParameterTypeBoolean,
		JSONType:           JSONTypeBoolean,
		GoType:             GoTypeBool,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "BOOLEAN", base.DXDatabaseTypeSQLServer: "BIT", base.DXDatabaseTypeMariaDB: "BOOLEAN", base.DXDatabaseTypeOracle: "NUMBER(1)"},
	}

	DataTypeISO8601 = DataType{
		APIParameterType:   APIParameterTypeISO8601,
		JSONType:           JSONTypeString,
		GoType:             GoTypeTime,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "TIMESTAMP WITH TIME ZONE", base.DXDatabaseTypeSQLServer: "DATETIMEOFFSET", base.DXDatabaseTypeMariaDB: "DATETIME", base.DXDatabaseTypeOracle: "TIMESTAMP WITH TIME ZONE"},
	}

	DataTypeDate = DataType{
		APIParameterType:   APIParameterTypeDate,
		JSONType:           JSONTypeString,
		GoType:             GoTypeTime,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "DATE", base.DXDatabaseTypeSQLServer: "DATE", base.DXDatabaseTypeMariaDB: "DATE", base.DXDatabaseTypeOracle: "DATE"},
	}

	DataTypeTime = DataType{
		APIParameterType:   APIParameterTypeTime,
		JSONType:           JSONTypeString,
		GoType:             GoTypeTime,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "TIME", base.DXDatabaseTypeSQLServer: "TIME", base.DXDatabaseTypeMariaDB: "TIME", base.DXDatabaseTypeOracle: "DATE"},
	}

	DataTypeJSON = DataType{
		APIParameterType:   APIParameterTypeJSON,
		JSONType:           JSONTypeObject,
		GoType:             GoTypeMapStringInterface,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "JSONB", base.DXDatabaseTypeSQLServer: "NVARCHAR(MAX)", base.DXDatabaseTypeMariaDB: "JSON", base.DXDatabaseTypeOracle: "CLOB"},
	}

	DataTypeJSONPassthrough = DataType{
		APIParameterType:   APIParameterTypeJSONPassthrough,
		JSONType:           JSONTypeObject,
		GoType:             GoTypeMapStringInterface,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "JSONB", base.DXDatabaseTypeSQLServer: "NVARCHAR(MAX)", base.DXDatabaseTypeMariaDB: "JSON", base.DXDatabaseTypeOracle: "CLOB"},
	}

	DataTypeArray = DataType{
		APIParameterType:   APIParameterTypeArray,
		JSONType:           JSONTypeArray,
		GoType:             GoTypeSliceInterface,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "JSONB", base.DXDatabaseTypeSQLServer: "NVARCHAR(MAX)", base.DXDatabaseTypeMariaDB: "JSON", base.DXDatabaseTypeOracle: "CLOB"},
	}

	DataTypeArrayString = DataType{
		APIParameterType:   APIParameterTypeArrayString,
		JSONType:           JSONTypeArray,
		GoType:             GoTypeSliceString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "TEXT[]", base.DXDatabaseTypeSQLServer: "NVARCHAR(MAX)", base.DXDatabaseTypeMariaDB: "JSON", base.DXDatabaseTypeOracle: "CLOB"},
	}

	DataTypeArrayInt64 = DataType{
		APIParameterType:   APIParameterTypeArrayInt64,
		JSONType:           JSONTypeArray,
		GoType:             GoTypeSliceInt64,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "BIGINT[]", base.DXDatabaseTypeSQLServer: "NVARCHAR(MAX)", base.DXDatabaseTypeMariaDB: "JSON", base.DXDatabaseTypeOracle: "CLOB"},
	}

	DataTypeArrayJSONTemplate = DataType{
		APIParameterType:   APIParameterTypeArrayJSONTemplate,
		JSONType:           JSONTypeArray,
		GoType:             GoTypeSliceMapStringInterface,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "JSONB", base.DXDatabaseTypeSQLServer: "NVARCHAR(MAX)", base.DXDatabaseTypeMariaDB: "JSON", base.DXDatabaseTypeOracle: "CLOB"},
	}

	DataTypeSerial = DataType{
		APIParameterType:   "serial",
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeInt64,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "SERIAL", base.DXDatabaseTypeSQLServer: "INT IDENTITY(1,1)", base.DXDatabaseTypeMariaDB: "INT AUTO_INCREMENT", base.DXDatabaseTypeOracle: "NUMBER GENERATED BY DEFAULT AS IDENTITY"},
	}

	DataTypeInt = DataType{
		APIParameterType:   "int",
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeInt64,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "INT", base.DXDatabaseTypeSQLServer: "INT", base.DXDatabaseTypeMariaDB: "INT", base.DXDatabaseTypeOracle: "NUMBER(10)"},
	}

	DataTypeString1 = DataType{
		APIParameterType:   "string1",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(1)", base.DXDatabaseTypeSQLServer: "VARCHAR(1)", base.DXDatabaseTypeMariaDB: "VARCHAR(1)", base.DXDatabaseTypeOracle: "VARCHAR2(1)"},
	}

	DataTypeString5 = DataType{
		APIParameterType:   "string5",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(5)", base.DXDatabaseTypeSQLServer: "VARCHAR(5)", base.DXDatabaseTypeMariaDB: "VARCHAR(5)", base.DXDatabaseTypeOracle: "VARCHAR2(5)"},
	}

	DataTypeString10 = DataType{
		APIParameterType:   "string10",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(10)", base.DXDatabaseTypeSQLServer: "VARCHAR(10)", base.DXDatabaseTypeMariaDB: "VARCHAR(10)", base.DXDatabaseTypeOracle: "VARCHAR2(10)"},
	}

	DataTypeString20 = DataType{
		APIParameterType:   "string20",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(20)", base.DXDatabaseTypeSQLServer: "VARCHAR(20)", base.DXDatabaseTypeMariaDB: "VARCHAR(20)", base.DXDatabaseTypeOracle: "VARCHAR2(20)"},
	}

	DataTypeString30 = DataType{
		APIParameterType:   "string30",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(30)", base.DXDatabaseTypeSQLServer: "VARCHAR(30)", base.DXDatabaseTypeMariaDB: "VARCHAR(30)", base.DXDatabaseTypeOracle: "VARCHAR2(30)"},
	}

	DataTypeString50 = DataType{
		APIParameterType:   "string50",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(50)", base.DXDatabaseTypeSQLServer: "VARCHAR(50)", base.DXDatabaseTypeMariaDB: "VARCHAR(50)", base.DXDatabaseTypeOracle: "VARCHAR2(50)"},
	}

	DataTypeString100 = DataType{
		APIParameterType:   "string100",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(100)", base.DXDatabaseTypeSQLServer: "VARCHAR(100)", base.DXDatabaseTypeMariaDB: "VARCHAR(100)", base.DXDatabaseTypeOracle: "VARCHAR2(100)"},
	}

	DataTypeString255 = DataType{
		APIParameterType:   "string255",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(255)", base.DXDatabaseTypeSQLServer: "VARCHAR(255)", base.DXDatabaseTypeMariaDB: "VARCHAR(255)", base.DXDatabaseTypeOracle: "VARCHAR2(255)"},
	}

	DataTypeString256 = DataType{
		APIParameterType:   "string256",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(256)", base.DXDatabaseTypeSQLServer: "VARCHAR(256)", base.DXDatabaseTypeMariaDB: "VARCHAR(256)", base.DXDatabaseTypeOracle: "VARCHAR2(256)"},
	}

	DataTypeString500 = DataType{
		APIParameterType:   "string500",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(500)", base.DXDatabaseTypeSQLServer: "VARCHAR(500)", base.DXDatabaseTypeMariaDB: "VARCHAR(500)", base.DXDatabaseTypeOracle: "VARCHAR2(500)"},
	}

	DataTypeString1024 = DataType{
		APIParameterType:   "string1024",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(1024)", base.DXDatabaseTypeSQLServer: "VARCHAR(1024)", base.DXDatabaseTypeMariaDB: "VARCHAR(1024)", base.DXDatabaseTypeOracle: "VARCHAR2(1024)"},
	}

	DataTypeString2048 = DataType{
		APIParameterType:   "string2048",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(2048)", base.DXDatabaseTypeSQLServer: "VARCHAR(2048)", base.DXDatabaseTypeMariaDB: "VARCHAR(2048)", base.DXDatabaseTypeOracle: "VARCHAR2(2048)"},
	}

	DataTypeString8096 = DataType{
		APIParameterType:   "string8096",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(8096)", base.DXDatabaseTypeSQLServer: "VARCHAR(8096)", base.DXDatabaseTypeMariaDB: "TEXT", base.DXDatabaseTypeOracle: "CLOB"},
	}

	DataTypeString32768 = DataType{
		APIParameterType:   "string32768",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(32768)", base.DXDatabaseTypeSQLServer: "VARCHAR(MAX)", base.DXDatabaseTypeMariaDB: "TEXT", base.DXDatabaseTypeOracle: "CLOB"},
	}

	DataTypeDecimal = DataType{
		APIParameterType:   "decimal",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "NUMERIC(30,4)", base.DXDatabaseTypeSQLServer: "DECIMAL(30,4)", base.DXDatabaseTypeMariaDB: "DECIMAL(30,4)", base.DXDatabaseTypeOracle: "NUMBER(30,4)"},
	}

	DataTypeGeometryPoint = DataType{
		APIParameterType:   "geometry_point",
		JSONType:           JSONTypeObject,
		GoType:             GoTypeMapStringInterface,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "geometry(Point, 4326)", base.DXDatabaseTypeSQLServer: "GEOGRAPHY", base.DXDatabaseTypeMariaDB: "POINT SRID 4326", base.DXDatabaseTypeOracle: "SDO_GEOMETRY"},
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
