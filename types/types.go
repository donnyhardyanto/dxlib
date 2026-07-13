package types

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/google/uuid"
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
	GoTypeInt32                   GoType = "int32"
	GoTypeInt64                   GoType = "int64"
	GoTypeInt64Pointer            GoType = "*int64"
	GoTypeFloat32                 GoType = "float32"
	GoTypeFloat64                 GoType = "float64"
	GoTypeMoney                   GoType = "decimal.Decimal"
	GoTypeBool                    GoType = "bool"
	GoTypeTime                    GoType = "time.Time"
	GoTypeMapStringInterface      GoType = "map[string]interface{}"
	GoTypeSliceInterface          GoType = "[]interface{}"
	GoTypeSliceString             GoType = "[]string"
	GoTypeSliceInt64              GoType = "[]int64"
	GoTypeSliceByte               GoType = "[]byte"
	GoTypeSliceMapStringInterface GoType = "[]map[string]interface{}"
	GoTypeMapStringString         GoType = "map[string]string"
)

const (
	APIParameterTypeEncryptedBlob APIParameterType = "encrypted-blob"
	APIParameterTypeBlob          APIParameterType = "blob"

	// String types
	APIParameterTypeString                  APIParameterType = "string"
	APIParameterTypeProtectedString         APIParameterType = "protected-string"
	APIParameterTypeProtectedSQLString      APIParameterType = "protected-sql-string"
	APIParameterTypeProtectedNonEmptyString APIParameterType = "protected-non-empty-string"
	APIParameterTypeNullableString          APIParameterType = "nullable-string"
	APIParameterTypeNonEmptyString          APIParameterType = "non-empty-string"
	APIParameterTypeEmail                   APIParameterType = "email"
	APIParameterTypePhoneNumber             APIParameterType = "phonenumber"
	APIParameterTypeNPWP                    APIParameterType = "npwp"

	// Integer types
	APIParameterTypeInt32         APIParameterType = "int32"
	APIParameterTypeInt32P        APIParameterType = "int32p"
	APIParameterTypeInt32ZP       APIParameterType = "int32zp"
	APIParameterTypeNullableInt32 APIParameterType = "nullable-int32"
	APIParameterTypeInt64         APIParameterType = "int64"
	APIParameterTypeInt64P        APIParameterType = "int64p"
	APIParameterTypeInt64ZP       APIParameterType = "int64zp"
	APIParameterTypeNullableInt64 APIParameterType = "nullable-int64"
	APIParameterTypeID            APIParameterType = "id" // integer identifier / primary key (BIGINT / int64)

	// Float32 types
	APIParameterTypeFloat32   APIParameterType = "float32"
	APIParameterTypeFloat32P  APIParameterType = "float32p"
	APIParameterTypeFloat32ZP APIParameterType = "float32zp"

	// Float64 types
	APIParameterTypeFloat64   APIParameterType = "float64"
	APIParameterTypeFloat64P  APIParameterType = "float64p"
	APIParameterTypeFloat64ZP APIParameterType = "float64zp"

	// Money type — a fixed-point monetary amount stored as NUMERIC(23,4) (19 integer
	// digits + 4 decimals), carried as a JSON string for JS precision safety and as
	// shopspring decimal.Decimal in Go. Currency is metadata on the data-model field,
	// not part of the type. Distinct from the (unused, string-backed) "decimal" type.
	APIParameterTypeMoney APIParameterType = "money"

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

	// Map types
	APIParameterTypeMapStringString APIParameterType = "map-string-string"
)

type DataType struct {
	Description                string // human-readable intended usage + peculiarity of this type
	APIParameterType           APIParameterType
	JSONType                   JSONType
	GoType                     GoType
	TypeByDatabaseType         map[base.DXDatabaseType]string // Database-specific SQL types
	DefaultValueByDatabaseType map[base.DXDatabaseType]string // Database-specific default values for this type
}

// SQL type constants to avoid duplication
const VARCHAR255 = "VARCHAR(255)"
const VARCHAR1024 = "VARCHAR(1024)"

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

// GenerateUID returns a collision-resistant, roughly time-sortable opaque id,
// generated in the application (no database round-trip). It is the Go equivalent
// of UIDDefaultExprPostgreSQL: lowercase hex of the current Unix time in
// microseconds, concatenated with a random UUIDv4 (dashed). ~52 chars — fits
// VARCHAR(255). Prefer this over a bare uuid for stored uids: the microsecond
// prefix makes a collision effectively impossible and keeps uids k-sortable
// (better index locality). The internal integer id, not this, is the FK key;
// this uid is the stable public handle (D-27) threaded across the async spine.
func GenerateUID() string {
	return strconv.FormatInt(time.Now().UTC().UnixMicro(), 16) + uuid.NewString()
}

var (
	DataTypeEncryptedBlob = DataType{
		Description:        "Binary data encrypted at rest (secrets/ciphertext); native BLOB/BYTEA, base64 JSON string, []byte in Go.",
		APIParameterType:   APIParameterTypeEncryptedBlob,
		JSONType:           JSONTypeString,
		GoType:             GoTypeSliceByte,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "BYTEA", base.DXDatabaseTypeSQLServer: "VARBINARY(MAX)", base.DXDatabaseTypeMariaDB: "LONGBLOB", base.DXDatabaseTypeOracle: "BLOB"},
	}
	DataTypeBlob = DataType{
		Description:        "Opaque binary payload (small inline files/images); native BLOB/BYTEA, base64 JSON string, []byte in Go. Large media belongs in object storage.",
		APIParameterType:   APIParameterTypeBlob,
		JSONType:           JSONTypeString,
		GoType:             GoTypeSliceByte,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "BYTEA", base.DXDatabaseTypeSQLServer: "VARBINARY(MAX)", base.DXDatabaseTypeMariaDB: "LONGBLOB", base.DXDatabaseTypeOracle: "BLOB"},
	}

	DataTypeUID = DataType{
		Description:                "Legacy wide opaque public identifier, VARCHAR(1024) with a DB-generated timestamp-hex + UUID default (anti-IDOR/ENUM). Not index-safe on SQL Server (>900B); the platform now uses String255 for uid.",
		APIParameterType:           APIParameterTypeString,
		JSONType:                   JSONTypeString,
		GoType:                     GoTypeString,
		TypeByDatabaseType:         map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: VARCHAR1024, base.DXDatabaseTypeSQLServer: VARCHAR1024, base.DXDatabaseTypeMariaDB: VARCHAR1024, base.DXDatabaseTypeOracle: "VARCHAR2(1024)"},
		DefaultValueByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: UIDDefaultExprPostgreSQL, base.DXDatabaseTypeSQLServer: UIDDefaultExprSQLServer, base.DXDatabaseTypeOracle: UIDDefaultExprOracle, base.DXDatabaseTypeMariaDB: UIDDefaultExprMariaDB},
	}

	DataTypeString = DataType{
		Description:        "General-purpose text up to 1024 chars (VARCHAR). Default when no length constraint matters.",
		APIParameterType:   APIParameterTypeString,
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: VARCHAR1024, base.DXDatabaseTypeSQLServer: VARCHAR1024, base.DXDatabaseTypeMariaDB: VARCHAR1024, base.DXDatabaseTypeOracle: VARCHAR1024},
	}

	DataTypeProtectedString = DataType{
		Description:        "PII/sensitive text, masked at display by role; VARCHAR(1024). The protected marker drives masking, not storage.",
		APIParameterType:   APIParameterTypeProtectedString,
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: VARCHAR1024, base.DXDatabaseTypeSQLServer: VARCHAR1024, base.DXDatabaseTypeMariaDB: VARCHAR1024, base.DXDatabaseTypeOracle: VARCHAR1024},
	}

	DataTypeProtectedSQLString = DataType{
		Description:        "Protected text also guarded against SQL injection (validated/escaped); VARCHAR(1024).",
		APIParameterType:   APIParameterTypeProtectedSQLString,
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: VARCHAR1024, base.DXDatabaseTypeSQLServer: VARCHAR1024, base.DXDatabaseTypeMariaDB: VARCHAR1024, base.DXDatabaseTypeOracle: VARCHAR1024},
	}

	DataTypeProtectedNonEmptyString = DataType{
		Description:        "Protected text that must be non-empty; VARCHAR(1024).",
		APIParameterType:   APIParameterTypeProtectedNonEmptyString,
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: VARCHAR1024, base.DXDatabaseTypeSQLServer: VARCHAR1024, base.DXDatabaseTypeMariaDB: VARCHAR1024, base.DXDatabaseTypeOracle: VARCHAR1024},
	}

	DataTypeNullableString = DataType{
		Description:        "Optional text (may be NULL); VARCHAR(1024), Go *string so NULL differs from empty.",
		APIParameterType:   APIParameterTypeNullableString,
		JSONType:           JSONTypeString,
		GoType:             GoTypeStringPointer,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: VARCHAR1024, base.DXDatabaseTypeSQLServer: VARCHAR1024, base.DXDatabaseTypeMariaDB: VARCHAR1024, base.DXDatabaseTypeOracle: VARCHAR1024},
	}

	DataTypeNonEmptyString = DataType{
		Description:        "Text that must be non-empty (rejects empty/whitespace); VARCHAR(1024).",
		APIParameterType:   APIParameterTypeNonEmptyString,
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: VARCHAR1024, base.DXDatabaseTypeSQLServer: VARCHAR1024, base.DXDatabaseTypeMariaDB: VARCHAR1024, base.DXDatabaseTypeOracle: VARCHAR1024},
	}

	DataTypeEmail = DataType{
		Description:        "Email address, format-validated; VARCHAR(255).",
		APIParameterType:   APIParameterTypeEmail,
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: VARCHAR255, base.DXDatabaseTypeSQLServer: VARCHAR255, base.DXDatabaseTypeMariaDB: VARCHAR255, base.DXDatabaseTypeOracle: VARCHAR255},
	}

	DataTypePhoneNumber = DataType{
		Description:        "Phone number, format-validated; VARCHAR(255).",
		APIParameterType:   APIParameterTypePhoneNumber,
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: VARCHAR255, base.DXDatabaseTypeSQLServer: VARCHAR255, base.DXDatabaseTypeMariaDB: VARCHAR255, base.DXDatabaseTypeOracle: VARCHAR255},
	}

	DataTypeNPWP = DataType{
		Description:        "Indonesian taxpayer id (NPWP), format-validated; VARCHAR(255).",
		APIParameterType:   APIParameterTypeNPWP,
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: VARCHAR255, base.DXDatabaseTypeSQLServer: VARCHAR255, base.DXDatabaseTypeMariaDB: VARCHAR255, base.DXDatabaseTypeOracle: VARCHAR255},
	}

	// DataTypeID is an integer identifier / primary key — same underlying type as
	// Int64 (BIGINT / int64) but a distinct named type + APIParameterType so it reads
	// as an identifier and is distinguishable by consumers (cf. UID vs String1024).
	DataTypeID = DataType{
		Description:        "Integer identifier / primary key (BIGINT, int64), JSON number. Internal/debug tier; public API exposes UID instead.",
		APIParameterType:   APIParameterTypeID,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeInt64,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "BIGINT", base.DXDatabaseTypeSQLServer: "BIGINT", base.DXDatabaseTypeMariaDB: "BIGINT", base.DXDatabaseTypeOracle: "NUMBER(19)"},
	}

	DataTypeInt32 = DataType{
		Description:        "32-bit signed integer (INT), JSON number, Go int32.",
		APIParameterType:   APIParameterTypeInt32,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeInt32,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "INT", base.DXDatabaseTypeSQLServer: "INT", base.DXDatabaseTypeMariaDB: "INT", base.DXDatabaseTypeOracle: "NUMBER(10)"},
	}

	DataTypeInt32P = DataType{
		Description:        "32-bit positive integer (> 0); INT.",
		APIParameterType:   APIParameterTypeInt32P,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeInt32,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "INT", base.DXDatabaseTypeSQLServer: "INT", base.DXDatabaseTypeMariaDB: "INT", base.DXDatabaseTypeOracle: "NUMBER(10)"},
	}

	DataTypeInt32ZP = DataType{
		Description:        "32-bit zero-or-positive integer (>= 0); INT.",
		APIParameterType:   APIParameterTypeInt32ZP,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeInt32,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "INT", base.DXDatabaseTypeSQLServer: "INT", base.DXDatabaseTypeMariaDB: "INT", base.DXDatabaseTypeOracle: "NUMBER(10)"},
	}

	DataTypeNullableInt32 = DataType{
		Description:        "Optional 32-bit integer; INT.",
		APIParameterType:   APIParameterTypeNullableInt32,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeInt32,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "INT", base.DXDatabaseTypeSQLServer: "INT", base.DXDatabaseTypeMariaDB: "INT", base.DXDatabaseTypeOracle: "NUMBER(10)"},
	}

	DataTypeInt64 = DataType{
		Description:        "64-bit signed integer (BIGINT), JSON number, Go int64.",
		APIParameterType:   APIParameterTypeInt64,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeInt64,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "BIGINT", base.DXDatabaseTypeSQLServer: "BIGINT", base.DXDatabaseTypeMariaDB: "BIGINT", base.DXDatabaseTypeOracle: "NUMBER(19)"},
	}

	DataTypeInt64P = DataType{
		Description:        "64-bit positive integer (> 0); BIGINT.",
		APIParameterType:   APIParameterTypeInt64P,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeInt64,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "BIGINT", base.DXDatabaseTypeSQLServer: "BIGINT", base.DXDatabaseTypeMariaDB: "BIGINT", base.DXDatabaseTypeOracle: "NUMBER(19)"},
	}

	DataTypeInt64ZP = DataType{
		Description:        "64-bit zero-or-positive integer (>= 0); BIGINT.",
		APIParameterType:   APIParameterTypeInt64ZP,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeInt64,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "BIGINT", base.DXDatabaseTypeSQLServer: "BIGINT", base.DXDatabaseTypeMariaDB: "BIGINT", base.DXDatabaseTypeOracle: "NUMBER(19)"},
	}

	DataTypeNullableInt64 = DataType{
		Description:        "Optional 64-bit integer (may be NULL); BIGINT, Go *int64.",
		APIParameterType:   APIParameterTypeNullableInt64,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeInt64Pointer,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "BIGINT", base.DXDatabaseTypeSQLServer: "BIGINT", base.DXDatabaseTypeMariaDB: "BIGINT", base.DXDatabaseTypeOracle: "NUMBER(19)"},
	}

	DataTypeFloat32 = DataType{
		Description:        "32-bit binary float (REAL), JSON number. Approximate and lossy; never use for money, use Money.",
		APIParameterType:   APIParameterTypeFloat32,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeFloat32,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "REAL", base.DXDatabaseTypeSQLServer: "REAL", base.DXDatabaseTypeMariaDB: "FLOAT", base.DXDatabaseTypeOracle: "BINARY_FLOAT"},
	}

	DataTypeFloat32P = DataType{
		Description:        "Positive 32-bit float (> 0); REAL. Not for money.",
		APIParameterType:   APIParameterTypeFloat32P,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeFloat32,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "REAL", base.DXDatabaseTypeSQLServer: "REAL", base.DXDatabaseTypeMariaDB: "FLOAT", base.DXDatabaseTypeOracle: "BINARY_FLOAT"},
	}

	DataTypeFloat32ZP = DataType{
		Description:        "Zero-or-positive 32-bit float (>= 0); REAL. Not for money.",
		APIParameterType:   APIParameterTypeFloat32ZP,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeFloat32,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "REAL", base.DXDatabaseTypeSQLServer: "REAL", base.DXDatabaseTypeMariaDB: "FLOAT", base.DXDatabaseTypeOracle: "BINARY_FLOAT"},
	}

	DataTypeFloat64 = DataType{
		Description:        "64-bit binary float (DOUBLE), JSON number. Approximate and lossy; never use for money, use Money.",
		APIParameterType:   APIParameterTypeFloat64,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeFloat64,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "DOUBLE PRECISION", base.DXDatabaseTypeSQLServer: "FLOAT", base.DXDatabaseTypeMariaDB: "DOUBLE", base.DXDatabaseTypeOracle: "BINARY_DOUBLE"},
	}

	DataTypeFloat64P = DataType{
		Description:        "Positive 64-bit float (> 0); DOUBLE. Not for money.",
		APIParameterType:   APIParameterTypeFloat64P,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeFloat64,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "DOUBLE PRECISION", base.DXDatabaseTypeSQLServer: "FLOAT", base.DXDatabaseTypeMariaDB: "DOUBLE", base.DXDatabaseTypeOracle: "BINARY_DOUBLE"},
	}

	DataTypeFloat64ZP = DataType{
		Description:        "Zero-or-positive 64-bit float (>= 0); DOUBLE. Not for money.",
		APIParameterType:   APIParameterTypeFloat64ZP,
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeFloat64,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "DOUBLE PRECISION", base.DXDatabaseTypeSQLServer: "FLOAT", base.DXDatabaseTypeMariaDB: "DOUBLE", base.DXDatabaseTypeOracle: "BINARY_DOUBLE"},
	}

	DataTypeBool = DataType{
		Description:        "Boolean true/false; BOOLEAN (BIT on SQL Server, NUMBER(1) on Oracle).",
		APIParameterType:   APIParameterTypeBoolean,
		JSONType:           JSONTypeBoolean,
		GoType:             GoTypeBool,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "BOOLEAN", base.DXDatabaseTypeSQLServer: "BIT", base.DXDatabaseTypeMariaDB: "BOOLEAN", base.DXDatabaseTypeOracle: "NUMBER(1)"},
	}

	DataTypeISO8601 = DataType{
		Description:        "Instant with timezone (RFC3339/ISO-8601); JSON string, Go time.Time; TIMESTAMP WITH TIME ZONE.",
		APIParameterType:   APIParameterTypeISO8601,
		JSONType:           JSONTypeString,
		GoType:             GoTypeTime,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "TIMESTAMP WITH TIME ZONE", base.DXDatabaseTypeSQLServer: "DATETIMEOFFSET", base.DXDatabaseTypeMariaDB: "DATETIME", base.DXDatabaseTypeOracle: "TIMESTAMP WITH TIME ZONE"},
	}

	DataTypeDate = DataType{
		Description:        "Calendar date, no time; JSON string YYYY-MM-DD, Go time.Time; DATE.",
		APIParameterType:   APIParameterTypeDate,
		JSONType:           JSONTypeString,
		GoType:             GoTypeTime,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "DATE", base.DXDatabaseTypeSQLServer: "DATE", base.DXDatabaseTypeMariaDB: "DATE", base.DXDatabaseTypeOracle: "DATE"},
	}

	DataTypeTime = DataType{
		Description:        "Time of day, no date; JSON string HH:MM:SS, Go time.Time; TIME (DATE on Oracle).",
		APIParameterType:   APIParameterTypeTime,
		JSONType:           JSONTypeString,
		GoType:             GoTypeTime,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "TIME", base.DXDatabaseTypeSQLServer: "TIME", base.DXDatabaseTypeMariaDB: "TIME", base.DXDatabaseTypeOracle: "DATE"},
	}

	DataTypeJSON = DataType{
		Description:        "Structured JSON object; JSONB native, Go map. For schemaless/nested data.",
		APIParameterType:   APIParameterTypeJSON,
		JSONType:           JSONTypeObject,
		GoType:             GoTypeMapStringInterface,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "JSONB", base.DXDatabaseTypeSQLServer: "NVARCHAR(MAX)", base.DXDatabaseTypeMariaDB: "JSON", base.DXDatabaseTypeOracle: "CLOB"},
	}

	DataTypeJSONPassthrough = DataType{
		Description:        "JSON object stored verbatim (no re-serialization); JSONB, preserves exact client bytes.",
		APIParameterType:   APIParameterTypeJSONPassthrough,
		JSONType:           JSONTypeObject,
		GoType:             GoTypeMapStringInterface,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "JSONB", base.DXDatabaseTypeSQLServer: "NVARCHAR(MAX)", base.DXDatabaseTypeMariaDB: "JSON", base.DXDatabaseTypeOracle: "CLOB"},
	}

	DataTypeArray = DataType{
		Description:        "JSON array of mixed values; JSONB, Go []interface{}.",
		APIParameterType:   APIParameterTypeArray,
		JSONType:           JSONTypeArray,
		GoType:             GoTypeSliceInterface,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "JSONB", base.DXDatabaseTypeSQLServer: "NVARCHAR(MAX)", base.DXDatabaseTypeMariaDB: "JSON", base.DXDatabaseTypeOracle: "CLOB"},
	}

	DataTypeArrayString = DataType{
		Description:        "Array of strings; native TEXT[] on PostgreSQL, JSON elsewhere; Go []string.",
		APIParameterType:   APIParameterTypeArrayString,
		JSONType:           JSONTypeArray,
		GoType:             GoTypeSliceString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "TEXT[]", base.DXDatabaseTypeSQLServer: "NVARCHAR(MAX)", base.DXDatabaseTypeMariaDB: "JSON", base.DXDatabaseTypeOracle: "CLOB"},
	}

	DataTypeArrayInt64 = DataType{
		Description:        "Array of 64-bit integers; native BIGINT[] on PostgreSQL, JSON elsewhere; Go []int64.",
		APIParameterType:   APIParameterTypeArrayInt64,
		JSONType:           JSONTypeArray,
		GoType:             GoTypeSliceInt64,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "BIGINT[]", base.DXDatabaseTypeSQLServer: "NVARCHAR(MAX)", base.DXDatabaseTypeMariaDB: "JSON", base.DXDatabaseTypeOracle: "CLOB"},
	}

	DataTypeArrayJSONTemplate = DataType{
		Description:        "Array of JSON objects (templated rows); JSONB, Go []map.",
		APIParameterType:   APIParameterTypeArrayJSONTemplate,
		JSONType:           JSONTypeArray,
		GoType:             GoTypeSliceMapStringInterface,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "JSONB", base.DXDatabaseTypeSQLServer: "NVARCHAR(MAX)", base.DXDatabaseTypeMariaDB: "JSON", base.DXDatabaseTypeOracle: "CLOB"},
	}

	DataTypeMapStringString = DataType{
		Description:        "String-to-string map; JSONB object, Go map[string]string.",
		APIParameterType:   APIParameterTypeMapStringString,
		JSONType:           JSONTypeObject,
		GoType:             GoTypeMapStringString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "JSONB", base.DXDatabaseTypeSQLServer: "NVARCHAR(MAX)", base.DXDatabaseTypeMariaDB: "JSON", base.DXDatabaseTypeOracle: "CLOB"},
	}

	DataTypeSerial = DataType{
		Description:        "32-bit DB auto-increment primary key (SERIAL/IDENTITY); column-only, not an API param. Prefer BigSerial (64-bit) for new PKs.",
		APIParameterType:   "serial",
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeInt32,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "SERIAL", base.DXDatabaseTypeSQLServer: "INT IDENTITY(1,1)", base.DXDatabaseTypeMariaDB: "INT AUTO_INCREMENT", base.DXDatabaseTypeOracle: "NUMBER GENERATED BY DEFAULT AS IDENTITY"},
	}

	// DataTypeBigSerial is a 64-bit auto-increment primary key (the BIGINT counterpart
	// of DataTypeSerial). Column-only (like Serial): no Types-map / validation entry.
	DataTypeBigSerial = DataType{
		Description:        "64-bit DB auto-increment primary key (BIGSERIAL/IDENTITY); column-only. The default surrogate id.",
		APIParameterType:   "bigserial",
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeInt64,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "BIGSERIAL", base.DXDatabaseTypeSQLServer: "BIGINT IDENTITY(1,1)", base.DXDatabaseTypeMariaDB: "BIGINT AUTO_INCREMENT", base.DXDatabaseTypeOracle: "NUMBER(19) GENERATED BY DEFAULT AS IDENTITY"},
	}

	DataTypeInt = DataType{
		Description:        "32-bit integer (INT); the bare-named alias of Int32.",
		APIParameterType:   "int",
		JSONType:           JSONTypeNumber,
		GoType:             GoTypeInt32,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "INT", base.DXDatabaseTypeSQLServer: "INT", base.DXDatabaseTypeMariaDB: "INT", base.DXDatabaseTypeOracle: "NUMBER(10)"},
	}

	DataTypeString1 = DataType{
		Description:        "Text up to 1 char (VARCHAR(1)); one-char codes/flags.",
		APIParameterType:   "string1",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(1)", base.DXDatabaseTypeSQLServer: "VARCHAR(1)", base.DXDatabaseTypeMariaDB: "VARCHAR(1)", base.DXDatabaseTypeOracle: "VARCHAR2(1)"},
	}

	DataTypeString5 = DataType{
		Description:        "Text up to 5 chars (VARCHAR(5)).",
		APIParameterType:   "string5",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(5)", base.DXDatabaseTypeSQLServer: "VARCHAR(5)", base.DXDatabaseTypeMariaDB: "VARCHAR(5)", base.DXDatabaseTypeOracle: "VARCHAR2(5)"},
	}

	DataTypeString10 = DataType{
		Description:        "Text up to 10 chars (VARCHAR(10)).",
		APIParameterType:   "string10",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(10)", base.DXDatabaseTypeSQLServer: "VARCHAR(10)", base.DXDatabaseTypeMariaDB: "VARCHAR(10)", base.DXDatabaseTypeOracle: "VARCHAR2(10)"},
	}

	DataTypeString20 = DataType{
		Description:        "Text up to 20 chars (VARCHAR(20)).",
		APIParameterType:   "string20",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(20)", base.DXDatabaseTypeSQLServer: "VARCHAR(20)", base.DXDatabaseTypeMariaDB: "VARCHAR(20)", base.DXDatabaseTypeOracle: "VARCHAR2(20)"},
	}

	DataTypeString30 = DataType{
		Description:        "Text up to 30 chars (VARCHAR(30)).",
		APIParameterType:   "string30",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(30)", base.DXDatabaseTypeSQLServer: "VARCHAR(30)", base.DXDatabaseTypeMariaDB: "VARCHAR(30)", base.DXDatabaseTypeOracle: "VARCHAR2(30)"},
	}

	DataTypeString50 = DataType{
		Description:        "Text up to 50 chars (VARCHAR(50)).",
		APIParameterType:   "string50",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(50)", base.DXDatabaseTypeSQLServer: "VARCHAR(50)", base.DXDatabaseTypeMariaDB: "VARCHAR(50)", base.DXDatabaseTypeOracle: "VARCHAR2(50)"},
	}

	DataTypeString100 = DataType{
		Description:        "Text up to 100 chars (VARCHAR(100)).",
		APIParameterType:   "string100",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(100)", base.DXDatabaseTypeSQLServer: "VARCHAR(100)", base.DXDatabaseTypeMariaDB: "VARCHAR(100)", base.DXDatabaseTypeOracle: "VARCHAR2(100)"},
	}

	DataTypeString255 = DataType{
		Description:        "Text up to 255 chars; index-safe key on all engines (255B < 900 SQL Server; 1020B < 3072 MariaDB). The platform uid width.",
		APIParameterType:   "string255",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: VARCHAR255, base.DXDatabaseTypeSQLServer: VARCHAR255, base.DXDatabaseTypeMariaDB: VARCHAR255, base.DXDatabaseTypeOracle: "VARCHAR2(255)"},
	}

	DataTypeString256 = DataType{
		Description:        "Text up to 256 chars (VARCHAR(256)).",
		APIParameterType:   "string256",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(256)", base.DXDatabaseTypeSQLServer: "VARCHAR(256)", base.DXDatabaseTypeMariaDB: "VARCHAR(256)", base.DXDatabaseTypeOracle: "VARCHAR2(256)"},
	}

	DataTypeString500 = DataType{
		Description:        "Text up to 500 chars (VARCHAR(500)).",
		APIParameterType:   "string500",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(500)", base.DXDatabaseTypeSQLServer: "VARCHAR(500)", base.DXDatabaseTypeMariaDB: "VARCHAR(500)", base.DXDatabaseTypeOracle: "VARCHAR2(500)"},
	}

	// DataTypeString512 is a 512-char string — index-safe as a key on all four engines
	// (MariaDB utf8mb4 512*4=2048B<3072B; SQL Server 512B<900B), unlike UID's 1024.
	DataTypeString512 = DataType{
		Description:        "Text up to 512 chars; index-safe key on all four engines, unlike the wider UID(1024).",
		APIParameterType:   "string512",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(512)", base.DXDatabaseTypeSQLServer: "VARCHAR(512)", base.DXDatabaseTypeMariaDB: "VARCHAR(512)", base.DXDatabaseTypeOracle: "VARCHAR2(512)"},
	}

	DataTypeString1024 = DataType{
		Description:        "Text up to 1024 chars (VARCHAR(1024)). NOT index-safe as a key on SQL Server (> 900B); use String255/512 for keys.",
		APIParameterType:   "string1024",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: VARCHAR1024, base.DXDatabaseTypeSQLServer: VARCHAR1024, base.DXDatabaseTypeMariaDB: VARCHAR1024, base.DXDatabaseTypeOracle: "VARCHAR2(1024)"},
	}

	DataTypeString2048 = DataType{
		Description:        "Text up to 2048 chars (VARCHAR(2048)).",
		APIParameterType:   "string2048",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(2048)", base.DXDatabaseTypeSQLServer: "VARCHAR(2048)", base.DXDatabaseTypeMariaDB: "VARCHAR(2048)", base.DXDatabaseTypeOracle: "VARCHAR2(2048)"},
	}

	DataTypeString8096 = DataType{
		Description:        "Long text up to 8096 chars; falls back to TEXT (MariaDB) / CLOB (Oracle).",
		APIParameterType:   "string8096",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(8096)", base.DXDatabaseTypeSQLServer: "VARCHAR(8096)", base.DXDatabaseTypeMariaDB: "TEXT", base.DXDatabaseTypeOracle: "CLOB"},
	}

	DataTypeString32768 = DataType{
		Description:        "Very long text; VARCHAR(MAX) (SQL Server) / TEXT (MariaDB) / CLOB (Oracle).",
		APIParameterType:   "string32768",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "VARCHAR(32768)", base.DXDatabaseTypeSQLServer: "VARCHAR(MAX)", base.DXDatabaseTypeMariaDB: "TEXT", base.DXDatabaseTypeOracle: "CLOB"},
	}

	// Deprecated: use DataTypeMoney (NUMERIC(23,4)) for monetary values; this legacy
	// NUMERIC(30,4) type is string-backed with no decimal semantics and is kept only
	// for backward compatibility.
	DataTypeDecimal = DataType{
		Description:        "Deprecated: use Money (NUMERIC(23,4)) for monetary values. Legacy fixed-point NUMERIC(30,4) backed by a plain Go string with no decimal semantics; kept only for compatibility.",
		APIParameterType:   "decimal",
		JSONType:           JSONTypeString,
		GoType:             GoTypeString,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "NUMERIC(30,4)", base.DXDatabaseTypeSQLServer: "DECIMAL(30,4)", base.DXDatabaseTypeMariaDB: "DECIMAL(30,4)", base.DXDatabaseTypeOracle: "NUMBER(30,4)"},
	}

	// DataTypeMoney — fixed-point monetary amount, NUMERIC(23,4) on every engine (19
	// integer digits cover 10,000-trillion needs + 4 decimals; fits SQL Server max 38,
	// MariaDB 65, Oracle 38, PostgreSQL unbounded). JSON string ("1250000.375") for JS
	// precision; Go shopspring decimal.Decimal. Currency (fixed or companion-field) is
	// metadata on the data-model field, not the type.
	DataTypeMoney = DataType{
		Description:        "Fixed-point monetary amount - essentially a number, but stored as NUMERIC(23,4) and carried as a JSON string (JS loses precision on large/decimal numbers) and as shopspring decimal.Decimal in Go for exact arithmetic. Currency is field metadata, not part of the type.",
		APIParameterType:   APIParameterTypeMoney,
		JSONType:           JSONTypeString,
		GoType:             GoTypeMoney,
		TypeByDatabaseType: map[base.DXDatabaseType]string{base.DXDatabaseTypePostgreSQL: "NUMERIC(23,4)", base.DXDatabaseTypeSQLServer: "DECIMAL(23,4)", base.DXDatabaseTypeMariaDB: "DECIMAL(23,4)", base.DXDatabaseTypeOracle: "NUMBER(23,4)"},
	}

	DataTypeGeometryPoint = DataType{
		Description:        "Geographic point (lon/lat, SRID 4326); native spatial type per engine (geometry/GEOGRAPHY/POINT/SDO_GEOMETRY), Go map.",
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
		DataTypeProtectedNonEmptyString,
		DataTypeNullableString,
		DataTypeNonEmptyString,
		DataTypeEmail,
		DataTypePhoneNumber,
		DataTypeNPWP,

		// Integer types
		DataTypeInt32,
		DataTypeInt32P,
		DataTypeInt32ZP,
		DataTypeNullableInt32,

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
		APIParameterTypeString:                  DataTypeString,
		APIParameterTypeProtectedString:         DataTypeProtectedString,
		APIParameterTypeProtectedSQLString:      DataTypeProtectedSQLString,
		APIParameterTypeProtectedNonEmptyString: DataTypeProtectedNonEmptyString,
		APIParameterTypeNullableString:          DataTypeNullableString,
		APIParameterTypeNonEmptyString:          DataTypeNonEmptyString,
		APIParameterTypeEmail:                   DataTypeEmail,
		APIParameterTypePhoneNumber:             DataTypePhoneNumber,
		APIParameterTypeNPWP:                    DataTypeNPWP,

		// Integer types
		APIParameterTypeInt32:         DataTypeInt32,
		APIParameterTypeInt32P:        DataTypeInt32P,
		APIParameterTypeInt32ZP:       DataTypeInt32ZP,
		APIParameterTypeNullableInt32: DataTypeNullableInt32,
		APIParameterTypeInt64:         DataTypeInt64,
		APIParameterTypeInt64P:        DataTypeInt64P,
		APIParameterTypeInt64ZP:       DataTypeInt64ZP,
		APIParameterTypeNullableInt64: DataTypeNullableInt64,
		APIParameterTypeID:            DataTypeID,

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

		// Map types
		APIParameterTypeMapStringString: DataTypeMapStringString,
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
