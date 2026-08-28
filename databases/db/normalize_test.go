package db

// Tests for the row-value normaliser (normalize.go).
//
// Every case here guards one of the regressions that made the normaliser
// necessary, or one of the ways a normaliser can make things worse:
//
//   - PostgreSQL must stay a provable no-op. pgx's ScanType lies (string for a
//     jsonb that scans as []byte, float64 for a numeric that scans as a string),
//     so a metadata-driven pass would corrupt the only engine that is already
//     correct. Nothing may be planned for it, ever.
//   - MariaDB returns []byte for text and integers, which is why
//     `row["is_enabled"] != true` silently skipped every row.
//   - MariaDB reports scan types of string for TEXT with a text charset and
//     []byte for BLOB/VARBINARY with the binary charset. Converting the second
//     group would corrupt genuine binary data.
//   - go-ora keys NUMBER's ScanType off precision, so NUMBER(10,0) claims
//     float64 and a bare NUMBER claims int64 — backwards. Only DecimalSize may
//     decide.
//   - go-mssqldb reports a []byte scan type for both VARBINARY and
//     DECIMAL/NUMERIC/MONEY, so the name has to break the tie there and only
//     there.
//   - A failed parse must return the input untouched. A malformed value silently
//     becoming int64(0) is worse than the type inconsistency being fixed.
//   - NULL must stay untyped nil: consumers distinguish absent from NULL from a
//     wrong type.
//
// Everything targets buildColumnConversion, the pure core, because
// sql.ColumnType has no exported constructor and cannot be built in a test.

import (
	"database/sql"
	"reflect"
	"testing"
	"time"
)

// scan types as the four drivers report them.
var (
	tString      = reflect.TypeFor[string]()
	tBytes       = reflect.TypeFor[[]byte]()
	tInt8        = reflect.TypeFor[int8]()
	tInt32       = reflect.TypeFor[int32]()
	tInt64       = reflect.TypeFor[int64]()
	tUint64      = reflect.TypeFor[uint64]()
	tFloat32     = reflect.TypeFor[float32]()
	tFloat64     = reflect.TypeFor[float64]()
	tBool        = reflect.TypeFor[bool]()
	tTime        = reflect.TypeFor[time.Time]()
	tNullString  = reflect.TypeFor[sql.NullString]()
	tNullInt64   = reflect.TypeFor[sql.NullInt64]()
	tNullFloat64 = reflect.TypeFor[sql.NullFloat64]()
	tNullTime    = reflect.TypeFor[sql.NullTime]()
	tAnyPtr      = reflect.TypeFor[*any]()
	tAny         = reflect.TypeFor[any]()
)

// bytesEqual compares two values for identical dynamic type and content,
// handling []byte, which is not comparable with ==.
func valuesEqual(a, b any) bool {
	ab, aIsBytes := a.([]byte)
	bb, bIsBytes := b.([]byte)
	if aIsBytes != bIsBytes {
		return false
	}
	if aIsBytes {
		return string(ab) == string(bb)
	}
	return a == b
}

// PostgreSQL is the canonical engine: it must never get a conversion plan, for
// any type name, any scan type, any decimal size. Same for a driver this package
// does not recognise, whose metadata cannot be trusted at all.
func TestBuildColumnConversionUnrecognisedDriverIsAlwaysIdentity(t *testing.T) {
	drivers := []string{"postgres", "postgresql", "postgres_v2", "", "cockroach", "sqlite3"}
	typeNames := []string{
		"VARCHAR", "TEXT", "CHAR", "NUMBER", "INT", "BIGINT", "DECIMAL", "NUMERIC",
		"FLOAT", "DOUBLE", "JSONB", "BYTEA", "TIMESTAMP", "BOOL", "UUID", "",
	}
	scanTypes := []reflect.Type{
		tString, tBytes, tInt32, tInt64, tFloat64, tBool, tTime, tNullString, nil,
	}
	scales := []int64{0, 2, 0xFF}

	for _, driver := range drivers {
		for _, typeName := range typeNames {
			for _, scanType := range scanTypes {
				for _, scale := range scales {
					if conv := buildColumnConversion(driver, typeName, scanType, 10, scale, true); conv != nil {
						t.Fatalf("driver=%q type=%q scan=%v scale=%d: got a conversion, want nil",
							driver, typeName, scanType, scale)
					}
				}
			}
		}
	}
}

// sqlx reports the registered Go driver name, which for MariaDB is "mysql", not
// "mariadb". A normaliser that only matched "mariadb" would return nil for every
// MariaDB query — the exact bug it exists to fix, and invisible to an end-to-end
// suite whose consumers compensate downstream.
func TestBuildColumnConversionAcceptsMysqlDriverName(t *testing.T) {
	for _, driver := range []string{"mysql", "mariadb", "MySQL"} {
		conv := buildColumnConversion(driver, "VARCHAR", tString, 0, 0, false)
		if conv == nil {
			t.Fatalf("driver=%q: got nil conversion for VARCHAR, want one", driver)
		}
		if got := conv([]byte("abc")); got != "abc" {
			t.Fatalf("driver=%q: got %#v, want %q", driver, got, "abc")
		}
	}
}

func TestBuildColumnConversionMariaDBTextBecomesString(t *testing.T) {
	tests := []struct {
		name     string
		typeName string
		scanType reflect.Type
	}{
		{"varchar not null", "VARCHAR", tString},
		{"varchar nullable", "VARCHAR", tNullString},
		{"text", "TEXT", tString},
		{"text nullable", "TEXT", tNullString},
		{"longtext", "LONGTEXT", tNullString},
		{"mediumtext", "MEDIUMTEXT", tNullString},
		{"tinytext", "TINYTEXT", tNullString},
		{"char", "CHAR", tString},
		{"enum", "ENUM", tString},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conv := buildColumnConversion("mariadb", tt.typeName, tt.scanType, 0, 0, false)
			if conv == nil {
				t.Fatalf("got nil conversion, want one")
			}
			if got := conv([]byte("abc")); got != "abc" {
				t.Fatalf("[]byte: got %#v (%T), want %q", got, got, "abc")
			}
			// Already a string on a NOT NULL column read over the binary
			// protocol: must stay untouched, not be re-wrapped.
			if got := conv("abc"); got != "abc" {
				t.Fatalf("string: got %#v (%T), want %q", got, got, "abc")
			}
		})
	}
}

// A []byte scan type means the column carries binary data. Blanket
// []byte-to-string conversion is exactly the bug the deleted compensators had.
func TestBuildColumnConversionMariaDBBinaryIsUntouched(t *testing.T) {
	for _, typeName := range []string{"BLOB", "LONGBLOB", "MEDIUMBLOB", "TINYBLOB", "VARBINARY", "BINARY", "GEOMETRY"} {
		if conv := buildColumnConversion("mariadb", typeName, tBytes, 0, 0, false); conv != nil {
			t.Fatalf("type=%q: got a conversion, want nil (binary must be preserved)", typeName)
		}
	}
}

func TestBuildColumnConversionMariaDBIntegersBecomeInt64(t *testing.T) {
	tests := []struct {
		name     string
		typeName string
		scanType reflect.Type
		in       any
		want     any
	}{
		{"int narrow widens", "INT", tInt32, int32(7), int64(7)},
		{"int from text protocol", "INT", tInt32, []byte("7"), int64(7)},
		{"bigint nullable from text", "BIGINT", tNullInt64, []byte("42"), int64(42)},
		{"bigint already canonical", "BIGINT", tInt64, int64(42), int64(42)},
		{"tinyint widens", "TINYINT", tInt8, int8(1), int64(1)},
		{"tinyint bool-ish stays 1", "TINYINT", tNullInt64, int64(1), int64(1)},
		{"smallint from text", "SMALLINT", tInt32, []byte("-3"), int64(-3)},
		{"mediumint from text", "MEDIUMINT", tInt32, []byte("300"), int64(300)},
		{"unsigned prefix is stripped", "UNSIGNED BIGINT", tUint64, uint64(9), int64(9)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conv := buildColumnConversion("mariadb", tt.typeName, tt.scanType, 0, 0, false)
			if conv == nil {
				t.Fatalf("got nil conversion, want one")
			}
			got := conv(tt.in)
			if !valuesEqual(got, tt.want) {
				t.Fatalf("got %#v (%T), want %#v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

// DECIMAL must land on string, matching what PostgreSQL returns for NUMERIC.
// Parsing to a float would lose digits no consumer can recover.
func TestBuildColumnConversionMariaDBDecimalStaysString(t *testing.T) {
	for _, scanType := range []reflect.Type{tString, tNullString} {
		conv := buildColumnConversion("mariadb", "DECIMAL", scanType, 20, 4, true)
		if conv == nil {
			t.Fatalf("scan=%v: got nil conversion, want one", scanType)
		}
		got := conv([]byte("12345678901234567890.1234"))
		if got != "12345678901234567890.1234" {
			t.Fatalf("scan=%v: got %#v (%T), want the string unchanged", scanType, got, got)
		}
		if _, isFloat := got.(float64); isFloat {
			t.Fatalf("scan=%v: decimal was parsed to a float", scanType)
		}
	}
}

func TestBuildColumnConversionMariaDBFloatsBecomeFloat64(t *testing.T) {
	tests := []struct {
		name     string
		typeName string
		scanType reflect.Type
		in       any
		want     any
	}{
		{"float32 widens", "FLOAT", tFloat32, float32(1.5), float64(1.5)},
		{"double already canonical", "DOUBLE", tFloat64, float64(1.5), float64(1.5)},
		{"double from text protocol", "DOUBLE", tNullFloat64, []byte("1.5"), float64(1.5)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conv := buildColumnConversion("mariadb", tt.typeName, tt.scanType, 0, 0, false)
			if conv == nil {
				t.Fatalf("got nil conversion, want one")
			}
			if got := conv(tt.in); !valuesEqual(got, tt.want) {
				t.Fatalf("got %#v (%T), want %#v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

// Date and timestamp columns already arrive as time.Time on all four engines.
// Reconstructing one from a string is both unnecessary and lossy, so a
// time.Time (or bool) scan type must never be planned.
func TestBuildColumnConversionTimeAndBoolAreIdentity(t *testing.T) {
	cases := []struct {
		driver   string
		typeName string
		scanType reflect.Type
	}{
		{"mariadb", "DATETIME", tNullTime},
		{"mariadb", "TIMESTAMP", tNullTime},
		{"mariadb", "DATE", tNullTime},
		{"oracle", "DATE", tTime},
		{"oracle", "TIMESTAMP", tTime},
		{"oracle", "TimeStampDTY", tTime},
		{"sqlserver", "DATETIME2", tTime},
		{"sqlserver", "DATETIMEOFFSET", tTime},
		{"sqlserver", "SMALLDATETIME", tTime},
		{"sqlserver", "BIT", tBool},
	}
	for _, c := range cases {
		if conv := buildColumnConversion(c.driver, c.typeName, c.scanType, 0, 0, false); conv != nil {
			t.Fatalf("driver=%q type=%q: got a conversion, want nil", c.driver, c.typeName)
		}
	}
}

// go-ora reports float64 for NUMBER(10,0) — an integer — and int64 for a bare
// NUMBER, exactly backwards. The scale, and only the scale, decides.
func TestBuildColumnConversionOracleNumberScaleRule(t *testing.T) {
	tests := []struct {
		name           string
		scanType       reflect.Type
		precision      int64
		scale          int64
		hasDecimalSize bool
		in             any
		want           any // nil want means "expect no conversion at all"
	}{
		{
			name: "scale 0 is an integer despite a float64 scan type",
			// go-ora reports float64 here because precision > 0.
			scanType: tFloat64, precision: 10, scale: 0, hasDecimalSize: true,
			in: "42", want: int64(42),
		},
		{
			name: "scale 255 is a binary float despite an int64 scan type",
			// go-ora reports int64 for a bare NUMBER (precision 0).
			scanType: tInt64, precision: 0, scale: 0xFF, hasDecimalSize: true,
			in: "1.5", want: float64(1.5),
		},
		{
			name:     "positive scale keeps the string, like postgres numeric",
			scanType: tFloat64, precision: 20, scale: 4, hasDecimalSize: true,
			in: "12345678901234567890.1234", want: nil,
		},
		{
			name:     "no decimal size means no usable metadata",
			scanType: tFloat64, precision: 0, scale: 0, hasDecimalSize: false,
			in: "42", want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conv := buildColumnConversion("oracle", "NUMBER", tt.scanType, tt.precision, tt.scale, tt.hasDecimalSize)
			if tt.want == nil {
				if conv != nil {
					t.Fatalf("got a conversion, want nil")
				}
				return
			}
			if conv == nil {
				t.Fatalf("got nil conversion, want one")
			}
			if got := conv(tt.in); !valuesEqual(got, tt.want) {
				t.Fatalf("got %#v (%T), want %#v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

// A NUMBER(1) boolean arrives from go-ora as the string "0" or "1", so
// `row["is_enabled"].(bool)` and `== true` both fail. The scale-0 rule turns it
// into int64 0/1, the same shape MariaDB's TINYINT(1) produces.
func TestBuildColumnConversionOracleNumberBooleanDigits(t *testing.T) {
	conv := buildColumnConversion("oracle", "NUMBER", tFloat64, 1, 0, true)
	if conv == nil {
		t.Fatalf("got nil conversion, want one")
	}
	for in, want := range map[string]int64{"0": 0, "1": 1} {
		if got := conv(in); got != any(want) {
			t.Fatalf("in=%q: got %#v (%T), want int64(%d)", in, got, got, want)
		}
	}
}

func TestBuildColumnConversionOracleBinaryIsUntouched(t *testing.T) {
	for _, typeName := range []string{"RAW", "LongRaw", "OCIBlobLocator", "OCIFileLocator"} {
		if conv := buildColumnConversion("oracle", typeName, tBytes, 0, 0, false); conv != nil {
			t.Fatalf("type=%q: got a conversion, want nil (binary must be preserved)", typeName)
		}
	}
}

func TestBuildColumnConversionOracleTextBecomesString(t *testing.T) {
	// go-ora names VARCHAR2 "VARCHAR", NVARCHAR2/NCHAR "NCHAR" and CLOB
	// "OCIClobLocator"; all three scan as string.
	for _, typeName := range []string{"VARCHAR", "NCHAR", "CHAR", "OCIClobLocator"} {
		conv := buildColumnConversion("oracle", typeName, tString, 0, 0, false)
		if conv == nil {
			t.Fatalf("type=%q: got nil conversion, want one", typeName)
		}
		if got := conv("abc"); got != "abc" {
			t.Fatalf("type=%q: got %#v, want %q", typeName, got, "abc")
		}
	}
}

// go-mssqldb reports a []byte scan type for VARBINARY, IMAGE,
// UNIQUEIDENTIFIER *and* DECIMAL/NUMERIC/MONEY/SMALLMONEY. Only the decimal
// family may be turned into a string; converting the rest would corrupt binary
// data (a UNIQUEIDENTIFIER is 16 raw bytes, not text).
func TestBuildColumnConversionSQLServerDecimalVersusBinary(t *testing.T) {
	decimals := []string{"DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY"}
	binaries := []string{"VARBINARY", "BINARY", "IMAGE", "UNIQUEIDENTIFIER", "SQL_VARIANT"}

	for _, typeName := range decimals {
		conv := buildColumnConversion("sqlserver", typeName, tBytes, 19, 4, true)
		if conv == nil {
			t.Fatalf("type=%q: got nil conversion, want one", typeName)
		}
		got := conv([]byte("12.3400"))
		if got != "12.3400" {
			t.Fatalf("type=%q: got %#v (%T), want %q", typeName, got, got, "12.3400")
		}
	}
	for _, typeName := range binaries {
		if conv := buildColumnConversion("sqlserver", typeName, tBytes, 0, 0, false); conv != nil {
			t.Fatalf("type=%q: got a conversion, want nil (binary must be preserved)", typeName)
		}
	}
	// The decimal exception is SQL Server only: elsewhere a []byte scan type
	// genuinely means binary.
	for _, driver := range []string{"mariadb", "oracle"} {
		for _, typeName := range decimals {
			if conv := buildColumnConversion(driver, typeName, tBytes, 19, 4, true); conv != nil {
				t.Fatalf("driver=%q type=%q: got a conversion for a []byte scan type, want nil", driver, typeName)
			}
		}
	}
}

// JSON and XML documents are carried differently by every engine on purpose
// ([]byte on postgres/mariadb, string on oracle/sqlserver) and consumers
// unmarshal from either. Normalising them would only move the inconsistency.
func TestBuildColumnConversionDocumentTypesAreExempt(t *testing.T) {
	for _, driver := range []string{"mariadb", "mysql", "oracle", "sqlserver"} {
		for _, typeName := range []string{"JSON", "JSONB", "XML", "XMLType", "OCIXMLType"} {
			for _, scanType := range []reflect.Type{tString, tBytes, tNullString} {
				if conv := buildColumnConversion(driver, typeName, scanType, 0, 0, false); conv != nil {
					t.Fatalf("driver=%q type=%q scan=%v: got a conversion, want nil",
						driver, typeName, scanType)
				}
			}
		}
	}
}

// No usable metadata means no conversion: go-ora reports a nil scan type for
// LONG, go-sql-driver reports *any for a type it does not model, go-mssqldb
// reports any for SQL_VARIANT, and an unrecognised type name must be left alone
// rather than guessed at.
func TestBuildColumnConversionUnusableMetadataIsIdentity(t *testing.T) {
	cases := []struct {
		driver   string
		typeName string
		scanType reflect.Type
	}{
		{"oracle", "LONG", nil},
		{"oracle", "LongVarChar", nil},
		{"mariadb", "NULL", tAnyPtr},
		{"mariadb", "VECTOR", tAnyPtr},
		{"sqlserver", "SQL_VARIANT", tAny},
		{"mariadb", "TIME", tString},
		{"mariadb", "YEAR", tInt32},
		{"mariadb", "SET", tString},
		{"mariadb", "BIT", tBytes},
		{"oracle", "IntervalDS", tString},
		{"oracle", "ROWID", tString},
		{"sqlserver", "TIME", tTime},
		{"mariadb", "SOME_FUTURE_TYPE", tString},
		{"oracle", "", tString},
	}
	for _, c := range cases {
		if conv := buildColumnConversion(c.driver, c.typeName, c.scanType, 0, 0, false); conv != nil {
			t.Fatalf("driver=%q type=%q scan=%v: got a conversion, want nil", c.driver, c.typeName, c.scanType)
		}
	}
}

// A value that will not parse must come back exactly as it went in. Silently
// yielding int64(0) or float64(0) would turn a visible type mismatch into
// invisible wrong data.
func TestBuildColumnConversionMalformedValuesAreKeptUnchanged(t *testing.T) {
	tests := []struct {
		name     string
		driver   string
		typeName string
		scanType reflect.Type
		scale    int64
		hasSize  bool
		in       any
	}{
		{"mariadb int from garbage string", "mariadb", "INT", tInt32, 0, false, "abc"},
		{"mariadb int from garbage bytes", "mariadb", "INT", tInt32, 0, false, []byte("abc")},
		{"mariadb int from empty bytes", "mariadb", "INT", tInt32, 0, false, []byte("")},
		{"mariadb int overflow", "mariadb", "BIGINT", tInt64, 0, false, "99999999999999999999"},
		{"mariadb int from fractional text", "mariadb", "INT", tInt32, 0, false, "1.5"},
		{"mariadb float from garbage", "mariadb", "DOUBLE", tFloat64, 0, false, "xyz"},
		{"mariadb float from garbage bytes", "mariadb", "DOUBLE", tFloat64, 0, false, []byte("xyz")},
		{"oracle number scale 0 from garbage", "oracle", "NUMBER", tFloat64, 0, true, "not-a-number"},
		{"oracle number scale 255 from garbage", "oracle", "NUMBER", tInt64, 0xFF, true, "not-a-number"},
		{"mariadb int from an unexpected struct", "mariadb", "INT", tInt32, 0, false, time.Time{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conv := buildColumnConversion(tt.driver, tt.typeName, tt.scanType, 10, tt.scale, tt.hasSize)
			if conv == nil {
				t.Fatalf("got nil conversion, want one")
			}
			got := conv(tt.in)
			if !valuesEqual(got, tt.in) {
				t.Fatalf("got %#v (%T), want the input back unchanged: %#v (%T)", got, got, tt.in, tt.in)
			}
			switch got.(type) {
			case int64, float64:
				t.Fatalf("malformed input was coerced to a number: %#v", got)
			}
		})
	}
}

// NULL must survive as untyped nil. The consumer tells absent from NULL from a
// wrong type, and a nil that acquires a type (or becomes "" / 0) breaks that.
func TestBuildColumnConversionNullPassesThroughUntyped(t *testing.T) {
	cases := []struct {
		driver   string
		typeName string
		scanType reflect.Type
		scale    int64
		hasSize  bool
	}{
		{"mariadb", "VARCHAR", tNullString, 0, false},
		{"mariadb", "TEXT", tNullString, 0, false},
		{"mariadb", "BIGINT", tNullInt64, 0, false},
		{"mariadb", "DOUBLE", tNullFloat64, 0, false},
		{"mariadb", "DECIMAL", tNullString, 4, true},
		{"oracle", "NUMBER", tFloat64, 0, true},
		{"oracle", "NUMBER", tInt64, 0xFF, true},
		{"sqlserver", "DECIMAL", tBytes, 4, true},
	}
	for _, c := range cases {
		conv := buildColumnConversion(c.driver, c.typeName, c.scanType, 10, c.scale, c.hasSize)
		if conv == nil {
			t.Fatalf("driver=%q type=%q: got nil conversion, want one", c.driver, c.typeName)
		}
		got := conv(nil)
		if got != nil {
			t.Fatalf("driver=%q type=%q: NULL became %#v (%T), want untyped nil",
				c.driver, c.typeName, got, got)
		}
	}
}

// The plan builder itself must refuse PostgreSQL before it reads any metadata,
// and must return nil rather than an empty plan when no column needs work.
func TestBuildRowNormalizerReturnsNilWhenNothingToDo(t *testing.T) {
	for _, driver := range []string{"postgres", "postgresql", "postgres_v2", "unknown", "mysql", "oracle", "sqlserver"} {
		if normalize := BuildRowNormalizer(driver, nil); normalize != nil {
			t.Fatalf("driver=%q: got a normaliser for an empty column list, want nil", driver)
		}
	}
}

// A driver that errors or panics while reporting column metadata must not turn a
// working query into a failing one.
type failingColumnTypes struct{ panics bool }

func (f failingColumnTypes) ColumnTypes() ([]*sql.ColumnType, error) {
	if f.panics {
		panic("not implemented makeGoLangScanType for type 240")
	}
	return nil, sql.ErrNoRows
}

func TestNewRowNormalizerSurvivesBadMetadata(t *testing.T) {
	if normalize := NewRowNormalizer("mysql", failingColumnTypes{}); normalize != nil {
		t.Fatalf("got a normaliser after a metadata error, want nil")
	}
	if normalize := NewRowNormalizer("sqlserver", failingColumnTypes{panics: true}); normalize != nil {
		t.Fatalf("got a normaliser after a metadata panic, want nil")
	}
	if normalize := NewRowNormalizer("mysql", nil); normalize != nil {
		t.Fatalf("got a normaliser for a nil source, want nil")
	}
}
