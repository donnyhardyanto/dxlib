package database_type

import (
	"encoding/json"
	"fmt"
	"github.com/shopspring/decimal"
	"regexp"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"
)

type DXDatabaseType int64

const (
	UnknownDatabaseType DXDatabaseType = iota
	PostgreSQL
	MySQL
	Oracle
	SQLServer
)

func (t DXDatabaseType) String() string {
	switch t {
	case PostgreSQL:
		return "postgres"
	case MySQL:
		return "mysql"
	case Oracle:
		return "oracle"
	case SQLServer:
		return "sqlserver"
	default:

		return "unknown"
	}
}

func (t DXDatabaseType) Driver() string {
	switch t {
	case PostgreSQL:
		return "postgres"
	case MySQL:
		return "mysql"
	case Oracle:
		return "oracle"
	case SQLServer:
		return "sqlserver"
	default:

		return "unknown"
	}
}
func StringToDXDatabaseType(v string) DXDatabaseType {
	switch v {
	case "postgres", "postgresql":
		return PostgreSQL
	case "mariadb", "mysql":
		return MySQL
	case "oracle":
		return Oracle
	case "sqlserver":
		return SQLServer
	default:

		return UnknownDatabaseType
	}
}

func prepareArray(arr interface{}, driverName string) interface{} {
	switch driverName {
	case "postgres", "postgresql":
		switch v := arr.(type) {
		case []uint8:
			return fmt.Sprintf("ARRAY[%s]::smallint[]", joinUInt8s(v))
		case []string:
			return fmt.Sprintf("ARRAY[%s]::text[]", strings.Join(quoteStrings(v), ","))
		case []int:
			return fmt.Sprintf("ARRAY[%s]::integer[]", joinInts(v))
		case []int64:
			return fmt.Sprintf("ARRAY[%s]::bigint[]", joinInt64s(v))
		case []float64:
			return fmt.Sprintf("ARRAY[%s]::float8[]", joinFloats(v))
		}
	case "sqlserver":
		switch v := arr.(type) {
		case []uint8:
			return fmt.Sprintf("STRING_SPLIT('%s', ',')", joinUInt8s(v))
		case []string:
			return fmt.Sprintf("STRING_SPLIT('%s', ',')", strings.Join(quoteStrings(v), ","))
		case []int:
			return fmt.Sprintf("STRING_SPLIT('%s', ',')", joinInts(v))
		case []int64:
			return fmt.Sprintf("STRING_SPLIT('%s', ',')", joinInt64s(v))
		case []float64:
			return fmt.Sprintf("STRING_SPLIT('%s', ',')", joinFloats(v))
		}
	case "mysql", "mariadb":
		switch v := arr.(type) {
		case []string:
			return fmt.Sprintf("FIND_IN_SET(column, '%s')", strings.Join(v, ","))
		case []int:
			return fmt.Sprintf("FIND_IN_SET(column, '%s')", joinInts(v))
		case []int64:
			return fmt.Sprintf("FIND_IN_SET(column, '%s')", joinInt64s(v))
		case []float64:
			return fmt.Sprintf("FIND_IN_SET(column, '%s')", joinFloats(v))
		}
	case "oracle":
		switch v := arr.(type) {
		case []string:
			return fmt.Sprintf("APEX_UTIL.STRING_TO_TABLE('%s')", strings.Join(quoteStrings(v), ","))
		case []int:
			return fmt.Sprintf("APEX_UTIL.STRING_TO_TABLE('%s')", joinInts(v))
		case []int64:
			return fmt.Sprintf("APEX_UTIL.STRING_TO_TABLE('%s')", joinInt64s(v))
		case []float64:
			return fmt.Sprintf("APEX_UTIL.STRING_TO_TABLE('%s')", joinFloats(v))
		}
	}
	return arr
}

func prepareTextType(text string, driverName string) interface{} {
	if len(text) > 1024 {
		switch driverName {
		case "postgres", "postgresql":
			return fmt.Sprintf("CAST('%s' AS TEXT)", strings.ReplaceAll(text, "'", "''"))
		case "sqlserver":
			return fmt.Sprintf("CAST('%s' AS VARCHAR(MAX))", strings.ReplaceAll(text, "'", "''"))
		case "oracle":
			return fmt.Sprintf("TO_CLOB('%s')", strings.ReplaceAll(text, "'", "''"))
		case "mysql", "mariadb":
			return fmt.Sprintf("CAST('%s' AS LONGTEXT)", strings.ReplaceAll(text, "'", "''"))
		}
	}
	return fmt.Sprintf("'%s'", strings.ReplaceAll(text, "'", "''"))
}

func quoteStrings(arr []string) []string {
	quoted := make([]string, len(arr))
	for i, s := range arr {
		quoted[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''"))
	}
	return quoted
}

func joinInts(arr []int) string {
	strs := make([]string, len(arr))
	for i, n := range arr {
		strs[i] = strconv.Itoa(n)
	}
	return strings.Join(strs, ",")
}

func joinInt64s(arr []int64) string {
	strs := make([]string, len(arr))
	for i, n := range arr {
		strs[i] = strconv.FormatInt(n, 10)
	}
	return strings.Join(strs, ",")
}

func joinUInt8s(arr []uint8) string {
	strs := make([]string, len(arr))
	for i, n := range arr {
		strs[i] = strconv.Itoa(int(n))
	}
	return strings.Join(strs, ",")
}

func joinFloats(arr []float64) string {
	strs := make([]string, len(arr))
	for i, n := range arr {
		strs[i] = strconv.FormatFloat(n, 'f', -1, 64)
	}
	return strings.Join(strs, ",")
}

func isJSONArray(str string) bool {
	str = strings.TrimSpace(str)
	// Basic check if string starts with [ and ends with ]
	if !strings.HasPrefix(str, "[") || !strings.HasSuffix(str, "]") {
		return false
	}

	// Try to unmarshal to verify it's a valid JSON array
	var arr []interface{}
	err := json.Unmarshal([]byte(str), &arr)
	return err == nil
}

func parseJSONArrayToStringArray(jsonStr string) ([]string, error) {
	var arr []string
	err := json.Unmarshal([]byte(jsonStr), &arr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON array: %w", err)
	}
	return arr, nil
}

func prepareJsonArg(arg interface{}, driverName string) interface{} {
	if jsonStr, ok := arg.(string); ok {
		jsonStr = strings.TrimSpace(jsonStr)
		if isJSONArray(jsonStr) {
			if arr, err := parseJSONArrayToStringArray(jsonStr); err == nil {
				switch driverName {
				case "postgres", "postgresql":
					// For PostgreSQL, no need to wrap the JSON string in CAST
					// Just pass it directly and let the driver handle it
					return jsonStr
				case "oracle":
					// Oracle: Convert to nested table if it's string array
					return fmt.Sprintf("CAST(MULTISET(SELECT COLUMN_VALUE FROM TABLE(APEX_UTIL.STRING_TO_TABLE('%s'))) AS SYS.ODCIVARCHAR2LIST)",
						strings.Join(arr, ","))
				case "sqlserver":
					return fmt.Sprintf("STRING_SPLIT('%s', ',')", strings.Join(arr, ","))
				case "mysql", "mariadb":
					return fmt.Sprintf("FIND_IN_SET(column, '%s')", strings.Join(arr, ","))
				}
			}
		}

		// Regular JSON handling
		switch driverName {
		case "postgres", "postgresql":
			// For PostgreSQL, pass JSON string directly
			return jsonStr
		case "sqlserver":
			return fmt.Sprintf("JSON_QUERY('%s')", strings.ReplaceAll(jsonStr, "'", "''"))
		case "oracle":
			return fmt.Sprintf("JSON_OBJECT_T('%s')", strings.ReplaceAll(jsonStr, "'", "''"))
		case "mysql", "mariadb":
			return fmt.Sprintf("CAST('%s' AS JSON)", strings.ReplaceAll(jsonStr, "'", "''"))
		}
	}
	return arg
}

func prepareArg(arg interface{}, driverName string) interface{} {
	switch v := arg.(type) {
	case []string, []int, []int64, []float64:
		return prepareArray(v, driverName)
	case string:
		if isJSON(v) {
			return prepareJsonArg(v, driverName)
		}
		return prepareTextType(v, driverName)
	case time.Time:
		switch driverName {
		case "postgres", "postgresql":
			return v.Format(time.RFC3339) // Remove CAST wrapper
		case "sqlserver":
			return fmt.Sprintf("CAST('%s' AS DATETIMEOFFSET)", v.Format(time.RFC3339))
		case "oracle":
			return fmt.Sprintf("TO_TIMESTAMP_TZ('%s', 'YYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM')", v.Format(time.RFC3339))
		case "mysql", "mariadb":
			return fmt.Sprintf("CONVERT_TZ('%s', 'UTC', @@session.time_zone)", v.Format("2006-01-02 15:04:05"))
		}
	case []time.Time:
		times := make([]string, len(v))
		for i, t := range v {
			times[i] = fmt.Sprintf("'%s'", t.Format(time.RFC3339))
		}
		switch driverName {
		case "postgres", "postgresql":
			return fmt.Sprintf("ARRAY[%s]::timestamptz[]", strings.Join(times, ","))
		default:
			return strings.Join(times, ",")
		}
	case decimal.Decimal:
		switch driverName {
		case "postgres", "postgresql":
			return fmt.Sprintf("CAST('%s' AS DECIMAL)", v.String())
		case "sqlserver":
			return fmt.Sprintf("CAST('%s' AS DECIMAL(38,20))", v.String())
		case "oracle":
			return fmt.Sprintf("TO_NUMBER('%s')", v.String())
		case "mysql", "mariadb":
			return fmt.Sprintf("CAST('%s' AS DECIMAL(65,30))", v.String())
		}
	case []decimal.Decimal:
		vals := make([]string, len(v))
		for i, d := range v {
			vals[i] = d.String()
		}
		switch driverName {
		case "postgres", "postgresql":
			return fmt.Sprintf("ARRAY[%s]::decimal[]", strings.Join(vals, ","))
		default:
			return strings.Join(vals, ",")
		}
	}
	return arg
}

func isJSON(str string) bool {
	str = strings.TrimSpace(str)
	return (strings.HasPrefix(str, "{") && strings.HasSuffix(str, "}")) ||
		(strings.HasPrefix(str, "[") && strings.HasSuffix(str, "]"))
}

func ConvertParamsWithMap(query string, params map[string]any, dbType DXDatabaseType) (string, []any, error) {
	re := regexp.MustCompile(`[:@$][a-zA-Z][a-zA-Z0-9_]*`)
	matches := re.FindAllString(query, -1)

	orderedArgs := make([]any, 0)
	paramMap := make(map[string]int)

	result := re.ReplaceAllStringFunc(query, func(match string) string {
		name := match[1:]
		val, exists := params[name]
		if !exists {
			return match
		}

		if _, tracked := paramMap[name]; !tracked {
			paramMap[name] = len(orderedArgs)
			processedVal := prepareArg(val, dbType.Driver())
			orderedArgs = append(orderedArgs, processedVal)
		}
		return getParamPlaceholder(dbType, paramMap[name])
	})

	for _, match := range matches {
		name := match[1:]
		if _, exists := params[name]; !exists {
			return "", nil, fmt.Errorf("parameter %s not found in params map", name)
		}
	}

	return result, orderedArgs, nil
}

func getParamPlaceholder(dbType DXDatabaseType, index int) string {
	switch dbType {
	case PostgreSQL:
		return "$" + strconv.Itoa(index+1)
	case SQLServer:
		return "@p" + strconv.Itoa(index+1)
	case Oracle:
		return ":" + strconv.Itoa(index+1)
	case MySQL:
		return "?"
	default:
		return "$" + strconv.Itoa(index+1)
	}
}
