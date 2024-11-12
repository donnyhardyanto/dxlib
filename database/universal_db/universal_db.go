package universal_db

import (
	_ "encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

type DBType string

const (
	Postgres  DBType = "postgres"
	Oracle    DBType = "oracle"
	SQLServer DBType = "sqlserver"
	MySQL     DBType = "mysql"
	SQLite    DBType = "sqlite"
)

type Operator string

const (
	Eq     Operator = "eq"
	Neq    Operator = "neq"
	Like   Operator = "like"
	ILike  Operator = "ilike"
	Gt     Operator = "gt"
	Lt     Operator = "lt"
	Gte    Operator = "gte"
	Lte    Operator = "lte"
	In     Operator = "in"
	NotIn  Operator = "notin"
	IsNull Operator = "isnull"
)

type Condition struct {
	Operator Operator    `json:"operator"`
	Value    interface{} `json:"value"`
}

type WhereConditions map[string]Condition

// Adding new type for shorthand conditions
type Conditions map[string]interface{}

// Convert simple map to WhereConditions
func ConvertToWhereConditions(conditions Conditions) WhereConditions {
	whereConditions := make(WhereConditions)

	for field, value := range conditions {
		switch v := value.(type) {
		case Condition:
			// If it's already a Condition, use it directly
			whereConditions[field] = v
		case map[string]interface{}:
			// Check if it's a condition definition
			if op, ok := v["operator"]; ok {
				if val, ok := v["value"]; ok {
					whereConditions[field] = Condition{
						Operator: Operator(fmt.Sprintf("%v", op)),
						Value:    val,
					}
					continue
				}
			}
			// If not a condition definition, treat as Eq
			whereConditions[field] = CreateEqualsCondition(v)
		default:
			// For all other types, create equals condition
			whereConditions[field] = CreateEqualsCondition(v)
		}
	}

	return whereConditions
}

// BuildSelectFromMap builds a SELECT query from a simple map
func BuildSelectFromMap(tableName string, conditions Conditions, dbType DBType) (string, error) {
	whereConditions := ConvertToWhereConditions(conditions)
	return BuildSelect(tableName, whereConditions, dbType)
}

// ValidateFieldName checks if field name contains only allowed characters
func ValidateFieldName(field string) bool {
	// Only allow alphanumeric, underscore, and period for schema.table
	validField := regexp.MustCompile(`^[a-zA-Z0-9_\.]+$`)
	return validField.MatchString(field)
}

// SanitizeTableName sanitizes table name
func SanitizeTableName(table string) (string, error) {
	// Only allow alphanumeric, underscore, and period for schema.table
	validTable := regexp.MustCompile(`^[a-zA-Z0-9_\.]+$`)
	if !validTable.MatchString(table) {
		return "", errors.New("invalid table name")
	}
	return table, nil
}

// ValidateOperator checks if the operator is valid
func ValidateOperator(op Operator) bool {
	validOperators := map[Operator]bool{
		Eq: true, Neq: true, Like: true, ILike: true,
		Gt: true, Lt: true, Gte: true, Lte: true,
		In: true, NotIn: true, IsNull: true,
	}
	return validOperators[op]
}

func escapeString(value string, dbType DBType) string {
	switch dbType {
	case Postgres, SQLite, Oracle:
		return strings.ReplaceAll(value, "'", "''")
	case MySQL:
		str := strings.ReplaceAll(value, "\\", "\\\\")
		str = strings.ReplaceAll(str, "'", "\\'")
		return str
	case SQLServer:
		str := strings.ReplaceAll(value, "[", "[[]")
		str = strings.ReplaceAll(str, "'", "''")
		return str
	default:
		return value
	}
}

func formatValue(value interface{}, dbType DBType) (string, error) {
	switch v := value.(type) {
	case nil:
		return "NULL", nil
	case string:
		return fmt.Sprintf("'%s'", escapeString(v, dbType)), nil
	case int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", v), nil
	case time.Time:
		switch dbType {
		case Oracle:
			return fmt.Sprintf("TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS')",
				v.Format("2006-01-02 15:04:05")), nil
		case SQLServer:
			return fmt.Sprintf("CONVERT(DATETIME, '%s')",
				v.Format("2006-01-02 15:04:05")), nil
		default:
			return fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05")), nil
		}
	case bool:
		switch dbType {
		case Oracle, SQLServer, MySQL:
			if v {
				return "1", nil
			}
			return "0", nil
		default:
			if v {
				return "true", nil
			}
			return "false", nil
		}
	default:
		return "", fmt.Errorf("unsupported type for value: %T", value)
	}
}

func buildCondition(field string, condition Condition, dbType DBType) (string, error) {
	// Validate field name
	if !ValidateFieldName(field) {
		return "", fmt.Errorf("invalid field name: %s", field)
	}

	// Validate operator
	if !ValidateOperator(condition.Operator) {
		return "", fmt.Errorf("invalid operator: %s", condition.Operator)
	}

	switch condition.Operator {
	case IsNull:
		return fmt.Sprintf("%s IS NULL", field), nil

	case In, NotIn:
		values, ok := condition.Value.([]interface{})
		if !ok {
			return "", errors.New("invalid value for IN operator")
		}

		var formattedValues []string
		for _, v := range values {
			formatted, err := formatValue(v, dbType)
			if err != nil {
				return "", err
			}
			formattedValues = append(formattedValues, formatted)
		}

		operator := "IN"
		if condition.Operator == NotIn {
			operator = "NOT IN"
		}
		return fmt.Sprintf("%s %s (%s)", field, operator,
			strings.Join(formattedValues, ",")), nil

	case Like, ILike:
		formatted, err := formatValue(condition.Value, dbType)
		if err != nil {
			return "", err
		}

		// Remove the surrounding quotes added by formatValue
		formatted = strings.Trim(formatted, "'")

		switch dbType {
		case Postgres:
			if condition.Operator == ILike {
				return fmt.Sprintf("%s ILIKE '%%%s%%'", field, formatted), nil
			}
			return fmt.Sprintf("%s LIKE '%%%s%%'", field, formatted), nil
		case Oracle:
			if condition.Operator == ILike {
				return fmt.Sprintf("UPPER(%s) LIKE UPPER('%%%s%%')", field, formatted), nil
			}
			return fmt.Sprintf("%s LIKE '%%%s%%'", field, formatted), nil
		case SQLServer:
			if condition.Operator == ILike {
				return fmt.Sprintf("%s LIKE '%%%s%%' COLLATE SQL_Latin1_General_CP1_CI_AS",
					field, formatted), nil
			}
			return fmt.Sprintf("%s LIKE '%%%s%%'", field, formatted), nil
		default:
			return fmt.Sprintf("%s LIKE '%%%s%%'", field, formatted), nil
		}

	default:
		formatted, err := formatValue(condition.Value, dbType)
		if err != nil {
			return "", err
		}

		var operator string
		switch condition.Operator {
		case Eq:
			operator = "="
		case Neq:
			operator = "!="
		case Gt:
			operator = ">"
		case Lt:
			operator = "<"
		case Gte:
			operator = ">="
		case Lte:
			operator = "<="
		default:
			return "", fmt.Errorf("unsupported operator: %s", condition.Operator)
		}

		return fmt.Sprintf("%s %s %s", field, operator, formatted), nil
	}
}

// CreateCondition creates a validated condition
func CreateCondition(op Operator, value interface{}) Condition {
	return Condition{
		Operator: op,
		Value:    value,
	}
}

// CreateConditionWithValidation creates a condition with validation
func CreateConditionWithValidation(op Operator, value interface{}) (Condition, error) {
	// Validate operator
	if !ValidateOperator(op) {
		return Condition{}, fmt.Errorf("invalid operator: %s", op)
	}

	// Validate value based on operator
	if err := validateValue(op, value); err != nil {
		return Condition{}, err
	}

	return Condition{
		Operator: op,
		Value:    value,
	}, nil
}

// validateValue checks if the value is valid for the given operator
func validateValue(op Operator, value interface{}) error {
	// Handle nil value
	if value == nil {
		if op != IsNull {
			return fmt.Errorf("nil value is only allowed with IsNull operator")
		}
		return nil
	}

	switch op {
	case In, NotIn:
		// Check if value is a slice
		switch v := value.(type) {
		case []interface{}:
			if len(v) == 0 {
				return fmt.Errorf("empty slice not allowed for IN/NOT IN operator")
			}
			// Validate each element in the slice
			for _, elem := range v {
				if !isValidValueType(elem) {
					return fmt.Errorf("invalid type in slice: %T", elem)
				}
			}
		case []string, []int, []int64, []float64:
			// These types are OK
			return nil
		default:
			return fmt.Errorf("IN/NOT IN operator requires a slice, got %T", value)
		}

	case Like, ILike:
		// Like operators only work with strings
		if _, ok := value.(string); !ok {
			return fmt.Errorf("LIKE/ILIKE operator requires string value, got %T", value)
		}

	default:
		// Check if value is of valid type for other operators
		if !isValidValueType(value) {
			return fmt.Errorf("invalid value type: %T", value)
		}
	}

	return nil
}

// isValidValueType checks if the value is of a supported type
func isValidValueType(value interface{}) bool {
	switch value.(type) {
	case string, int, int32, int64, float32, float64, bool, time.Time:
		return true
	default:
		return false
	}
}

// Helper functions for common conditions
func CreateEqualsCondition(value interface{}) Condition {
	return CreateCondition(Eq, value)
}

func CreateNotEqualsCondition(value interface{}) Condition {
	return CreateCondition(Neq, value)
}

func CreateLikeCondition(value string) Condition {
	return CreateCondition(Like, value)
}

func CreateILikeCondition(value string) Condition {
	return CreateCondition(ILike, value)
}

func CreateInCondition(values interface{}) Condition {
	return CreateCondition(In, values)
}

func CreateGreaterThanCondition(value interface{}) Condition {
	return CreateCondition(Gt, value)
}

func CreateLessThanCondition(value interface{}) Condition {
	return CreateCondition(Lt, value)
}

func CreateGreaterOrEqualCondition(value interface{}) Condition {
	return CreateCondition(Gte, value)
}

func CreateLessOrEqualCondition(value interface{}) Condition {
	return CreateCondition(Lte, value)
}

func CreateIsNullCondition() Condition {
	return CreateCondition(IsNull, nil)
}
func BuildSelect(tableName string, conditions WhereConditions, dbType DBType) (string, error) {
	// Sanitize table name
	sanitizedTable, err := SanitizeTableName(tableName)
	if err != nil {
		return "", err
	}

	var whereClauses []string
	var errors []string

	for field, condition := range conditions {
		clause, err := buildCondition(field, condition, dbType)
		if err != nil {
			errors = append(errors, err.Error())
			continue
		}
		whereClauses = append(whereClauses, clause)
	}

	if len(errors) > 0 {
		return "", fmt.Errorf("validation errors: %s", strings.Join(errors, "; "))
	}

	query := fmt.Sprintf("SELECT * FROM %s", sanitizedTable)
	if len(whereClauses) > 0 {
		query += " WHERE " + strings.Join(whereClauses, " AND ")
	}

	return query, nil
}

func ProcessArrayValue(value interface{}) (Condition, bool) {
	switch v := value.(type) {
	case []string:
		return CreateInCondition(v), true
	case []int:
		interfaceSlice := make([]interface{}, len(v))
		for i, val := range v {
			interfaceSlice[i] = val
		}
		return CreateInCondition(interfaceSlice), true
	case []interface{}:
		return CreateInCondition(v), true
	}
	return Condition{}, false
}

// Smart condition creation based on value type and content
func CreateSmartCondition(value interface{}) Condition {
	switch v := value.(type) {
	case nil:
		return CreateIsNullCondition()
	case string:
		if strings.Contains(v, "%") {
			return CreateLikeCondition(v)
		}
		return CreateEqualsCondition(v)
	case []string, []int, []interface{}:
		if condition, ok := ProcessArrayValue(v); ok {
			return condition
		}
	}
	return CreateEqualsCondition(value)
}

// Helper function to merge multiple conditions
func MergeConditions(conditionSets ...Conditions) Conditions {
	merged := make(Conditions)
	for _, conditions := range conditionSets {
		for k, v := range conditions {
			merged[k] = v
		}
	}
	return merged
}

// ToInterfaceSlice converts a slice of any type to []interface{}
func ToInterfaceSlice[T any](slice []T) []interface{} {
	result := make([]interface{}, len(slice))
	for i, v := range slice {
		result[i] = v
	}
	return result
}

// For specific types you can also create convenience functions:
func StringsToInterface(slice []string) []interface{} {
	return ToInterfaceSlice(slice)
}

func IntsToInterface(slice []int) []interface{} {
	return ToInterfaceSlice(slice)
}

func Float64sToInterface(slice []float64) []interface{} {
	return ToInterfaceSlice(slice)
}
