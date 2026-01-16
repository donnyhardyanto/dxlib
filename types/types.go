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
	APIParameterType     APIParameterType
	JSONType             JSONType
	GoType               GoType
	DbType               map[string]string // Database-specific SQL types
	DefaultValueByDBType map[string]string // Database-specific default values for this type
}

func (dt DataType) GetDbType(key string) string {
	if dt.DbType == nil {
		return ""
	}
	return dt.DbType[key]
}

type Field struct {
	Name                 string
	Type                 DataType
	IsPrimaryKey         bool
	IsAutoIncrement      bool
	IsNotNull            bool
	IsUnique             bool
	DefaultValue         string            // SQL expression for DEFAULT clause (used when DefaultValueByDBType not specified)
	DefaultValueByDBType map[string]string // Database-specific default values. Keys: "postgresql", "sqlserver", "oracle", "mariadb"
	References           *Field            // Foreign key reference to another field
	IsEncrypted          bool
	IsHashed             bool
	EncryptedDataName    string
	EncryptedDataType    DataType
	EncryptedDataKeyID   string
	HashDataName         string
	HashDataType         DataType
	HashDataSaltID       string
}

// DBTypeKeyPostgreSQL is the key for PostgreSQL in DefaultValueByDBType map
const DBTypeKeyPostgreSQL = "postgresql"

// DBTypeKeySQLServer is the key for SQL Server in DefaultValueByDBType map
const DBTypeKeySQLServer = "sqlserver"

// DBTypeKeyOracle is the key for Oracle in DefaultValueByDBType map
const DBTypeKeyOracle = "oracle"

// DBTypeKeyMariaDB is the key for MariaDB/MySQL in DefaultValueByDBType map
const DBTypeKeyMariaDB = "mariadb"

// UID Default Expressions for each database type
// Format: hex(timestamp_microseconds) + uuid

// UIDDefaultExprPostgreSQL is the PostgreSQL UID default expression
const UIDDefaultExprPostgreSQL = "CONCAT(to_hex((extract(epoch from now()) * 1000000)::bigint), gen_random_uuid()::text)"

// UIDDefaultExprSQLServer is the SQL Server UID default expression
const UIDDefaultExprSQLServer = "CONCAT(CONVERT(VARCHAR(50), CAST(DATEDIFF_BIG(MICROSECOND, '1970-01-01', SYSUTCDATETIME()) AS VARBINARY(8)), 2), LOWER(REPLACE(CONVERT(VARCHAR(36), NEWID()), '-', '')))"

// UIDDefaultExprOracle is the Oracle UID default expression
const UIDDefaultExprOracle = "LOWER(TO_CHAR(ROUND((CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE) - TO_DATE('1970-01-01','YYYY-MM-DD')) * 86400000000), 'XXXXXXXXXXXXXXXX')) || LOWER(RAWTOHEX(SYS_GUID()))"

// UIDDefaultExprMariaDB is the MariaDB/MySQL UID default expression
const UIDDefaultExprMariaDB = "CONCAT(HEX(FLOOR(UNIX_TIMESTAMP(NOW(6)) * 1000000)), REPLACE(UUID(), '-', ''))"

// TDEConfig holds Transparent Data Encryption configuration for different database types
// Each database has different TDE approaches:
// - PostgreSQL: Uses table access method (e.g., "tde_heap" with pg_tde extension)
// - Oracle: Uses encrypted tablespace (tables created in pre-configured encrypted tablespace)
// - SQL Server: Database-level TDE (no per-table syntax, just a marker that TDE is expected)
// - MariaDB/MySQL: Uses InnoDB table encryption option (ENCRYPTION='Y')
type TDEConfig struct {
	// PostgreSQL: Table access method name for TDE (e.g., "tde_heap")
	PostgreSQLAccessMethod string

	// Oracle: Encrypted tablespace name where the table will be created
	OracleTablespace string

	// SQLServer: Flag indicating that database-level TDE should be enabled
	// No per-table syntax is needed; this is for documentation/validation purposes
	SQLServerTDEEnabled bool

	// MariaDB: Encryption option for InnoDB tables ('Y' for enabled, 'N' or empty for disabled)
	MariaDBEncryption string
}

// IsEnabled checks if TDE is configured for the specified database type key
func (t TDEConfig) IsEnabled(dbTypeKey string) bool {
	switch dbTypeKey {
	case DBTypeKeyPostgreSQL:
		return t.PostgreSQLAccessMethod != ""
	case DBTypeKeyOracle:
		return t.OracleTablespace != ""
	case DBTypeKeySQLServer:
		return t.SQLServerTDEEnabled
	case DBTypeKeyMariaDB:
		return t.MariaDBEncryption == "Y"
	default:
		return false
	}
}

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
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "BYTEA", DBTypeKeySQLServer: "VARBINARY(MAX)", DBTypeKeyMariaDB: "LONGBLOB", DBTypeKeyOracle: "BLOB"},
	}

	DataTypeBlob = DataType{
		APIParameterType: APIParameterTypeBlob,
		JSONType:         JSONTypeString,
		GoType:           GoTypeSliceByte,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "BYTEA", DBTypeKeySQLServer: "VARBINARY(MAX)", DBTypeKeyMariaDB: "LONGBLOB", DBTypeKeyOracle: "BLOB"},
	}

	DataTypeUID = DataType{
		APIParameterType:     APIParameterTypeString,
		JSONType:             JSONTypeString,
		GoType:               GoTypeString,
		DbType:               map[string]string{DBTypeKeyPostgreSQL: "VARCHAR(1024)", DBTypeKeySQLServer: "VARCHAR(1024)", DBTypeKeyMariaDB: "VARCHAR(1024)", DBTypeKeyOracle: "VARCHAR2(1024)"},
		DefaultValueByDBType: map[string]string{DBTypeKeyPostgreSQL: UIDDefaultExprPostgreSQL, DBTypeKeySQLServer: UIDDefaultExprSQLServer, DBTypeKeyOracle: UIDDefaultExprOracle, DBTypeKeyMariaDB: UIDDefaultExprMariaDB},
	}

	DataTypeString = DataType{
		APIParameterType: APIParameterTypeString,
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "VARCHAR(1024)", DBTypeKeySQLServer: "VARCHAR(1024)", DBTypeKeyMariaDB: "VARCHAR(1024)", DBTypeKeyOracle: "VARCHAR(1024)"},
	}

	DataTypeProtectedString = DataType{
		APIParameterType: APIParameterTypeProtectedString,
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "VARCHAR(1024)", DBTypeKeySQLServer: "VARCHAR(1024)", DBTypeKeyMariaDB: "VARCHAR(1024)", DBTypeKeyOracle: "VARCHAR(1024)"},
	}

	DataTypeProtectedSQLString = DataType{
		APIParameterType: APIParameterTypeProtectedSQLString,
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "VARCHAR(1024)", DBTypeKeySQLServer: "VARCHAR(1024)", DBTypeKeyMariaDB: "VARCHAR(1024)", DBTypeKeyOracle: "VARCHAR(1024)"},
	}

	DataTypeNullableString = DataType{
		APIParameterType: APIParameterTypeNullableString,
		JSONType:         JSONTypeString,
		GoType:           GoTypeStringPointer,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "VARCHAR(1024)", DBTypeKeySQLServer: "VARCHAR(1024)", DBTypeKeyMariaDB: "VARCHAR(1024)", DBTypeKeyOracle: "VARCHAR(1024)"},
	}

	DataTypeNonEmptyString = DataType{
		APIParameterType: APIParameterTypeNonEmptyString,
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "VARCHAR(1024)", DBTypeKeySQLServer: "VARCHAR(1024)", DBTypeKeyMariaDB: "VARCHAR(1024)", DBTypeKeyOracle: "VARCHAR(1024)"},
	}

	DataTypeEmail = DataType{
		APIParameterType: APIParameterTypeEmail,
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "VARCHAR(255)", DBTypeKeySQLServer: "VARCHAR(255)", DBTypeKeyMariaDB: "VARCHAR(255)", DBTypeKeyOracle: "VARCHAR(255)"},
	}

	DataTypePhoneNumber = DataType{
		APIParameterType: APIParameterTypePhoneNumber,
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "VARCHAR(50)", DBTypeKeySQLServer: "VARCHAR(50)", DBTypeKeyMariaDB: "VARCHAR(50)", DBTypeKeyOracle: "VARCHAR(50)"},
	}

	DataTypeNPWP = DataType{
		APIParameterType: APIParameterTypeNPWP,
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "VARCHAR(50)", DBTypeKeySQLServer: "VARCHAR(50)", DBTypeKeyMariaDB: "VARCHAR(50)", DBTypeKeyOracle: "VARCHAR(50)"},
	}

	DataTypeInt64 = DataType{
		APIParameterType: APIParameterTypeInt64,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeInt64,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "BIGINT", DBTypeKeySQLServer: "BIGINT", DBTypeKeyMariaDB: "BIGINT", DBTypeKeyOracle: "NUMBER(19)"},
	}

	DataTypeInt64P = DataType{
		APIParameterType: APIParameterTypeInt64P,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeInt64,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "BIGINT", DBTypeKeySQLServer: "BIGINT", DBTypeKeyMariaDB: "BIGINT", DBTypeKeyOracle: "NUMBER(19)"},
	}

	DataTypeInt64ZP = DataType{
		APIParameterType: APIParameterTypeInt64ZP,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeInt64,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "BIGINT", DBTypeKeySQLServer: "BIGINT", DBTypeKeyMariaDB: "BIGINT", DBTypeKeyOracle: "NUMBER(19)"},
	}

	DataTypeNullableInt64 = DataType{
		APIParameterType: APIParameterTypeNullableInt64,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeInt64Pointer,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "BIGINT", DBTypeKeySQLServer: "BIGINT", DBTypeKeyMariaDB: "BIGINT", DBTypeKeyOracle: "NUMBER(19)"},
	}

	DataTypeFloat32 = DataType{
		APIParameterType: APIParameterTypeFloat32,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeFloat32,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "REAL", DBTypeKeySQLServer: "REAL", DBTypeKeyMariaDB: "FLOAT", DBTypeKeyOracle: "BINARY_FLOAT"},
	}

	DataTypeFloat32P = DataType{
		APIParameterType: APIParameterTypeFloat32P,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeFloat32,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "REAL", DBTypeKeySQLServer: "REAL", DBTypeKeyMariaDB: "FLOAT", DBTypeKeyOracle: "BINARY_FLOAT"},
	}

	DataTypeFloat32ZP = DataType{
		APIParameterType: APIParameterTypeFloat32ZP,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeFloat32,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "REAL", DBTypeKeySQLServer: "REAL", DBTypeKeyMariaDB: "FLOAT", DBTypeKeyOracle: "BINARY_FLOAT"},
	}

	DataTypeFloat64 = DataType{
		APIParameterType: APIParameterTypeFloat64,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeFloat64,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "DOUBLE PRECISION", DBTypeKeySQLServer: "FLOAT", DBTypeKeyMariaDB: "DOUBLE", DBTypeKeyOracle: "BINARY_DOUBLE"},
	}

	DataTypeFloat64P = DataType{
		APIParameterType: APIParameterTypeFloat64P,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeFloat64,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "DOUBLE PRECISION", DBTypeKeySQLServer: "FLOAT", DBTypeKeyMariaDB: "DOUBLE", DBTypeKeyOracle: "BINARY_DOUBLE"},
	}

	DataTypeFloat64ZP = DataType{
		APIParameterType: APIParameterTypeFloat64ZP,
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeFloat64,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "DOUBLE PRECISION", DBTypeKeySQLServer: "FLOAT", DBTypeKeyMariaDB: "DOUBLE", DBTypeKeyOracle: "BINARY_DOUBLE"},
	}

	DataTypeBool = DataType{
		APIParameterType: APIParameterTypeBoolean,
		JSONType:         JSONTypeBoolean,
		GoType:           GoTypeBool,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "BOOLEAN", DBTypeKeySQLServer: "BIT", DBTypeKeyMariaDB: "BOOLEAN", DBTypeKeyOracle: "NUMBER(1)"},
	}

	DataTypeISO8601 = DataType{
		APIParameterType: APIParameterTypeISO8601,
		JSONType:         JSONTypeString,
		GoType:           GoTypeTime,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "TIMESTAMP WITH TIME ZONE", DBTypeKeySQLServer: "DATETIMEOFFSET", DBTypeKeyMariaDB: "DATETIME", DBTypeKeyOracle: "TIMESTAMP WITH TIME ZONE"},
	}

	DataTypeDate = DataType{
		APIParameterType: APIParameterTypeDate,
		JSONType:         JSONTypeString,
		GoType:           GoTypeTime,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "DATE", DBTypeKeySQLServer: "DATE", DBTypeKeyMariaDB: "DATE", DBTypeKeyOracle: "DATE"},
	}

	DataTypeTime = DataType{
		APIParameterType: APIParameterTypeTime,
		JSONType:         JSONTypeString,
		GoType:           GoTypeTime,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "TIME", DBTypeKeySQLServer: "TIME", DBTypeKeyMariaDB: "TIME", DBTypeKeyOracle: "DATE"},
	}

	DataTypeJSON = DataType{
		APIParameterType: APIParameterTypeJSON,
		JSONType:         JSONTypeObject,
		GoType:           GoTypeMapStringInterface,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "JSONB", DBTypeKeySQLServer: "NVARCHAR(MAX)", DBTypeKeyMariaDB: "JSON", DBTypeKeyOracle: "CLOB"},
	}

	DataTypeJSONPassthrough = DataType{
		APIParameterType: APIParameterTypeJSONPassthrough,
		JSONType:         JSONTypeObject,
		GoType:           GoTypeMapStringInterface,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "JSONB", DBTypeKeySQLServer: "NVARCHAR(MAX)", DBTypeKeyMariaDB: "JSON", DBTypeKeyOracle: "CLOB"},
	}

	DataTypeArray = DataType{
		APIParameterType: APIParameterTypeArray,
		JSONType:         JSONTypeArray,
		GoType:           GoTypeSliceInterface,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "JSONB", DBTypeKeySQLServer: "NVARCHAR(MAX)", DBTypeKeyMariaDB: "JSON", DBTypeKeyOracle: "CLOB"},
	}

	DataTypeArrayString = DataType{
		APIParameterType: APIParameterTypeArrayString,
		JSONType:         JSONTypeArray,
		GoType:           GoTypeSliceString,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "TEXT[]", DBTypeKeySQLServer: "NVARCHAR(MAX)", DBTypeKeyMariaDB: "JSON", DBTypeKeyOracle: "CLOB"},
	}

	DataTypeArrayInt64 = DataType{
		APIParameterType: APIParameterTypeArrayInt64,
		JSONType:         JSONTypeArray,
		GoType:           GoTypeSliceInt64,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "BIGINT[]", DBTypeKeySQLServer: "NVARCHAR(MAX)", DBTypeKeyMariaDB: "JSON", DBTypeKeyOracle: "CLOB"},
	}

	DataTypeArrayJSONTemplate = DataType{
		APIParameterType: APIParameterTypeArrayJSONTemplate,
		JSONType:         JSONTypeArray,
		GoType:           GoTypeSliceMapStringInterface,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "JSONB", DBTypeKeySQLServer: "NVARCHAR(MAX)", DBTypeKeyMariaDB: "JSON", DBTypeKeyOracle: "CLOB"},
	}

	DataTypeSerial = DataType{
		APIParameterType: "serial",
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeInt64,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "SERIAL", DBTypeKeySQLServer: "INT IDENTITY(1,1)", DBTypeKeyMariaDB: "INT AUTO_INCREMENT", DBTypeKeyOracle: "NUMBER GENERATED BY DEFAULT AS IDENTITY"},
	}

	DataTypeInt = DataType{
		APIParameterType: "int",
		JSONType:         JSONTypeNumber,
		GoType:           GoTypeInt64,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "INT", DBTypeKeySQLServer: "INT", DBTypeKeyMariaDB: "INT", DBTypeKeyOracle: "NUMBER(10)"},
	}

	DataTypeString255 = DataType{
		APIParameterType: "string255",
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "VARCHAR(255)", DBTypeKeySQLServer: "VARCHAR(255)", DBTypeKeyMariaDB: "VARCHAR(255)", DBTypeKeyOracle: "VARCHAR2(255)"},
	}

	DataTypeString32768 = DataType{
		APIParameterType: "string32768",
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "VARCHAR(32768)", DBTypeKeySQLServer: "VARCHAR(MAX)", DBTypeKeyMariaDB: "TEXT", DBTypeKeyOracle: "CLOB"},
	}

	DataTypeDecimal = DataType{
		APIParameterType: "decimal",
		JSONType:         JSONTypeString,
		GoType:           GoTypeString,
		DbType:           map[string]string{DBTypeKeyPostgreSQL: "NUMERIC(30,4)", DBTypeKeySQLServer: "DECIMAL(30,4)", DBTypeKeyMariaDB: "DECIMAL(30,4)", DBTypeKeyOracle: "NUMBER(30,4)"},
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
