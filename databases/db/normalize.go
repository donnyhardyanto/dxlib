package db

// Row value normalisation.
//
// sqlx.MapScan hands back whatever the driver produced, so the same logical
// column arrives as a different Go type on every engine: a VARCHAR is a string
// on PostgreSQL/Oracle/SQL Server but []byte on MariaDB; an INT is int64 on
// PostgreSQL/SQL Server, []byte or int64 on MariaDB and a string on Oracle; a
// NUMBER(1) boolean is bool on PostgreSQL/SQL Server, int64 on MariaDB and the
// string "0"/"1" on Oracle. Consumers that type-assert then fail silently —
// `row["is_enabled"] != true` skips every row on MariaDB, where the value is
// int64(1).
//
// The canonical target type is whatever PostgreSQL already returns, which makes
// PostgreSQL a provable no-op and gives one unambiguous answer per column class:
//
//	text / CLOB / CHAR / NCHAR / ENUM   -> string
//	every integer width                 -> int64
//	FLOAT / DOUBLE / REAL               -> float64
//	DECIMAL / NUMERIC / MONEY           -> string   (never a float: keeps precision)
//	DATE / DATETIME / TIMESTAMP         -> time.Time (already correct everywhere)
//	BLOB / BYTEA / RAW / VARBINARY      -> []byte   (never touched)
//	JSON / JSONB / XML                  -> exempt
//	NULL                                -> untyped nil
//
// Three pieces of driver metadata cannot be taken at face value:
//
//  1. ScanType() lies on PostgreSQL — pgx reports string for a jsonb whose value
//     is []byte, and float64 for a numeric whose value is a string. A
//     ScanType-driven pass would corrupt the one engine that is already correct,
//     so PostgreSQL (and any unrecognised driver) is rejected before any
//     metadata is read.
//  2. ScanType() lies on Oracle NUMBER — go-ora keys off precision, so
//     NUMBER(10,0) (an integer) reports float64 while a bare NUMBER reports
//     int64, exactly backwards. NUMBER is decided from DecimalSize() alone.
//  3. A []byte scan type is ambiguous on SQL Server — VARBINARY, IMAGE,
//     UNIQUEIDENTIFIER and DECIMAL/NUMERIC/MONEY all report it. Only the
//     decimal family is converted; everything else stays binary.
//
// Safety rules, in order of importance: NULL stays untyped nil; missing or
// unrecognised metadata means no conversion; a failed parse returns the input
// unchanged (never a zero); and a metadata error never fails a query.

import (
	"database/sql"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Recognised driver keys. Everything else — "postgres", "postgresql",
// "postgres_v2" and any driver this package has not been taught — is refused.
const (
	normalizerDriverMariaDB   = "mariadb"
	normalizerDriverOracle    = "oracle"
	normalizerDriverSQLServer = "sqlserver"
)

// oracleBinaryFloatScale is go-ora's marker for Oracle's -127 binary-float
// scale, which a bare NUMBER (no declared precision) carries.
const oracleBinaryFloatScale int64 = 0xFF

// int64Bound is 2^63 as a float64: the first magnitude an int64 cannot hold.
const int64Bound float64 = 1 << 63

var (
	normalizeTypeAny     = reflect.TypeFor[any]()
	normalizeTypeAnyPtr  = reflect.TypeFor[*any]()
	normalizeTypeString  = reflect.TypeFor[string]()
	normalizeTypeBytes   = reflect.TypeFor[[]byte]()
	normalizeTypeTime    = reflect.TypeFor[time.Time]()
	normalizeTypeBool    = reflect.TypeFor[bool]()
	normalizeTypeInt64   = reflect.TypeFor[int64]()
	normalizeTypeFloat64 = reflect.TypeFor[float64]()

	normalizeTypeNullString  = reflect.TypeFor[sql.NullString]()
	normalizeTypeNullInt64   = reflect.TypeFor[sql.NullInt64]()
	normalizeTypeNullInt32   = reflect.TypeFor[sql.NullInt32]()
	normalizeTypeNullInt16   = reflect.TypeFor[sql.NullInt16]()
	normalizeTypeNullByte    = reflect.TypeFor[sql.NullByte]()
	normalizeTypeNullUint64  = reflect.TypeFor[sql.Null[uint64]]()
	normalizeTypeNullFloat64 = reflect.TypeFor[sql.NullFloat64]()
	normalizeTypeNullTime    = reflect.TypeFor[sql.NullTime]()
	normalizeTypeNullBool    = reflect.TypeFor[sql.NullBool]()
)

// ColumnTypesProvider is satisfied by *sqlx.Rows, *sql.Rows and *sqlx.Row.
type ColumnTypesProvider interface {
	ColumnTypes() ([]*sql.ColumnType, error)
}

// NewRowNormalizer builds the row normaliser for a live result set, or nil when
// nothing needs converting.
//
// Column metadata is strictly best-effort: an error is ignored, and so is a
// panic — go-mssqldb panics outright for a type id it has no scan mapping for,
// and reading metadata must never turn a working query into a failing one.
//
// For a *sqlx.Row this MUST be called before MapScan, which closes the
// underlying rows.
func NewRowNormalizer(driverName string, src ColumnTypesProvider) (normalize func(row map[string]any)) {
	defer func() {
		if r := recover(); r != nil {
			normalize = nil
		}
	}()
	if src == nil {
		return nil
	}
	colTypes, err := src.ColumnTypes()
	if err != nil {
		return nil
	}
	return BuildRowNormalizer(driverName, colTypes)
}

// rowColumnConversion is one entry of a query's normalisation plan.
type rowColumnConversion struct {
	name string
	conv func(v any) any
}

// BuildRowNormalizer plans the per-column conversions for one query and returns
// a function that applies them in place to each scanned row.
//
// It returns nil when nothing needs doing — PostgreSQL, an unrecognised driver,
// or a result set with no convertible column — so the per-row cost at the call
// site is a single nil check. All reflection happens here, once per query; the
// returned closures are plain type switches.
//
// Plan keys are the driver's own column names, so this must run on the row as
// MapScan produced it, before any key deformatting.
func BuildRowNormalizer(driverName string, colTypes []*sql.ColumnType) func(row map[string]any) {
	if normalizerDriverKey(driverName) == "" {
		return nil
	}
	var plan []rowColumnConversion
	for _, ct := range colTypes {
		if ct == nil {
			continue
		}
		precision, scale, hasDecimalSize := ct.DecimalSize()
		conv := buildColumnConversion(driverName, ct.DatabaseTypeName(), ct.ScanType(), precision, scale, hasDecimalSize)
		if conv == nil {
			continue
		}
		plan = append(plan, rowColumnConversion{name: ct.Name(), conv: conv})
	}
	if len(plan) == 0 {
		return nil
	}
	return func(row map[string]any) {
		for _, c := range plan {
			v, exists := row[c.name]
			if !exists {
				// Never materialise a key the scan did not produce: consumers
				// distinguish absent from NULL.
				continue
			}
			row[c.name] = c.conv(v)
		}
	}
}

// buildColumnConversion is the pure core of the normaliser: given one column's
// metadata it returns the value conversion for that column, or nil for "leave
// this column alone".
//
// It is split out from BuildRowNormalizer because sql.ColumnType has no exported
// constructor and therefore cannot be built in a test.
func buildColumnConversion(driverName, dbTypeName string, scanType reflect.Type,
	precision, scale int64, hasDecimalSize bool) func(v any) any {

	driverKey := normalizerDriverKey(driverName)
	if driverKey == "" {
		// PostgreSQL already returns the canonical types and its ScanType is
		// unreliable; an unknown driver's metadata cannot be trusted at all.
		// Bail out before reading any metadata.
		return nil
	}

	typeName := strings.ToUpper(strings.TrimSpace(dbTypeName))
	// MariaDB prefixes unsigned integer names, e.g. "UNSIGNED BIGINT".
	typeName = strings.TrimPrefix(typeName, "UNSIGNED ")

	// Documents are exempt: their carrier type differs per engine by design and
	// consumers unmarshal from either encoding.
	switch typeName {
	case "JSON", "JSONB", "XML", "XMLTYPE", "OCIXMLTYPE":
		return nil
	}

	// Oracle NUMBER: ScanType is backwards, the scale is authoritative. This one
	// rule fixes both Oracle integer columns and NUMBER(1) booleans, which
	// arrive as the strings "0"/"1".
	if driverKey == normalizerDriverOracle && typeName == "NUMBER" {
		if !hasDecimalSize {
			return nil
		}
		switch {
		case scale == oracleBinaryFloatScale:
			return convertToFloat64
		case scale == 0:
			return convertToInt64
		default:
			// A genuinely fractional NUMBER keeps the driver's string, exactly
			// as PostgreSQL returns NUMERIC, so no digits are lost.
			return nil
		}
	}

	scanType = unwrapNullScanType(scanType)
	if scanType == nil || scanType == normalizeTypeAnyPtr || scanType == normalizeTypeAny {
		// go-ora reports nil for LONG, go-sql-driver reports *any for a type it
		// does not model, go-mssqldb reports any for SQL_VARIANT.
		return nil
	}

	// A []byte scan type means binary — except on SQL Server, where the decimal
	// family shares it with VARBINARY, IMAGE and UNIQUEIDENTIFIER.
	if scanType == normalizeTypeBytes {
		if driverKey == normalizerDriverSQLServer {
			switch typeName {
			case "DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY":
				return convertToString
			}
		}
		return nil
	}

	// Already canonical everywhere, and a time must never be reconstructed from
	// a string.
	if scanType == normalizeTypeTime || scanType == normalizeTypeBool {
		return nil
	}

	switch classifyColumnTypeName(typeName) {
	case columnClassText, columnClassDecimal:
		return convertToString
	case columnClassInteger:
		return convertToInt64
	case columnClassFloat:
		return convertToFloat64
	}
	return nil
}

// normalizerDriverKey maps a sql driver name to the engine whose metadata this
// package knows how to read, or "" for one it must not touch.
func normalizerDriverKey(driverName string) string {
	switch strings.ToLower(strings.TrimSpace(driverName)) {
	case "mysql", "mariadb":
		// sqlx reports the registered Go driver name, which is "mysql".
		return normalizerDriverMariaDB
	case "oracle":
		return normalizerDriverOracle
	case "sqlserver", "mssql":
		return normalizerDriverSQLServer
	default:
		return ""
	}
}

type columnTypeClass int

const (
	columnClassUnknown columnTypeClass = iota
	columnClassText
	columnClassInteger
	columnClassFloat
	columnClassDecimal
)

// classifyColumnTypeName maps a DatabaseTypeName to a target class. An
// unrecognised name is deliberately left alone rather than guessed at.
func classifyColumnTypeName(typeName string) columnTypeClass {
	switch typeName {
	case "VARCHAR", "VARCHAR2", "NVARCHAR", "NVARCHAR2", "CHAR", "NCHAR", "CHARZ",
		"TEXT", "NTEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT",
		"CLOB", "NCLOB", "OCICLOBLOCATOR", "ENUM":
		return columnClassText
	case "INT", "INTEGER", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT":
		return columnClassInteger
	case "FLOAT", "DOUBLE", "DOUBLE PRECISION", "REAL",
		"BINARY_FLOAT", "BINARY_DOUBLE", "IBFLOAT", "IBDOUBLE":
		return columnClassFloat
	case "DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY":
		return columnClassDecimal
	}
	return columnClassUnknown
}

// unwrapNullScanType reduces a sql.NullXxx scan type to the underlying Go type.
// MariaDB reports sql.NullString for a nullable VARCHAR and string for a NOT
// NULL one, and sql.NullTime for every date column, so this has to happen once
// at plan-build time rather than per value.
func unwrapNullScanType(scanType reflect.Type) reflect.Type {
	switch scanType {
	case normalizeTypeNullString:
		return normalizeTypeString
	case normalizeTypeNullInt64, normalizeTypeNullInt32, normalizeTypeNullInt16,
		normalizeTypeNullByte, normalizeTypeNullUint64:
		return normalizeTypeInt64
	case normalizeTypeNullFloat64:
		return normalizeTypeFloat64
	case normalizeTypeNullTime:
		return normalizeTypeTime
	case normalizeTypeNullBool:
		return normalizeTypeBool
	default:
		return scanType
	}
}

// TextColumnToString converts one named column's []byte to a string, in place.
//
// The normaliser cannot do this on its own for a decrypted column: MariaDB's
// AES_DECRYPT() yields a binary string, which the driver reports with a []byte
// scan type indistinguishable from a real BLOB, so it is deliberately left
// alone. A caller that knows a specific column holds decrypted text converts
// exactly that column — never every column in the row, which is what silently
// corrupted genuine binary data.
//
// The name is matched exactly first, then case-insensitively: a row taken
// straight from MapScan carries the driver's own column names, and the
// case-folding engines upper-case them.
func TextColumnToString(row map[string]any, columnName string) {
	if row == nil || columnName == "" {
		return
	}
	if v, exists := row[columnName]; exists {
		if b, ok := v.([]byte); ok {
			row[columnName] = string(b)
		}
		return
	}
	for k, v := range row {
		if !strings.EqualFold(k, columnName) {
			continue
		}
		if b, ok := v.([]byte); ok {
			row[k] = string(b)
		}
		return
	}
}

func convertToString(v any) any {
	if v == nil {
		return v
	}
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

func convertToInt64(v any) any {
	if v == nil {
		return v
	}
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case int8:
		return int64(t)
	case int16:
		return int64(t)
	case int32:
		return int64(t)
	case uint8:
		return int64(t)
	case uint16:
		return int64(t)
	case uint32:
		return int64(t)
	case uint:
		if uint64(t) > math.MaxInt64 {
			return v
		}
		return int64(t)
	case uint64:
		if t > math.MaxInt64 {
			return v
		}
		return int64(t)
	case float32:
		return floatToInt64OrKeep(float64(t), v)
	case float64:
		return floatToInt64OrKeep(t, v)
	case []byte:
		return parseInt64OrKeep(string(t), v)
	case string:
		return parseInt64OrKeep(t, v)
	default:
		return v
	}
}

func convertToFloat64(v any) any {
	if v == nil {
		return v
	}
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int8:
		return float64(t)
	case int16:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case uint8:
		return float64(t)
	case uint16:
		return float64(t)
	case uint32:
		return float64(t)
	case uint:
		return float64(t)
	case uint64:
		return float64(t)
	case []byte:
		return parseFloat64OrKeep(string(t), v)
	case string:
		return parseFloat64OrKeep(t, v)
	default:
		return v
	}
}

// parseInt64OrKeep returns the parsed value, or the original untouched when the
// text is not a number. A malformed value must never silently become zero.
func parseInt64OrKeep(s string, original any) any {
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return original
	}
	return n
}

func parseFloat64OrKeep(s string, original any) any {
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return original
	}
	return f
}

func floatToInt64OrKeep(f float64, original any) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return original
	}
	if f != math.Trunc(f) {
		return original
	}
	if f < -int64Bound || f >= int64Bound {
		return original
	}
	return int64(f)
}
