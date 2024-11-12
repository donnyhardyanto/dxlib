package usage

import (
	"encoding/json"
	"fmt"
	"github.com/donnyhardyanto/dxlib/database/universal_db"
	"time"
)

func Example1() {
	// Safe usage
	safeConditions := universal_db.WhereConditions{
		"first_name": universal_db.CreateCondition(universal_db.ILike, "John's"),
		"age":        universal_db.CreateCondition(universal_db.Gte, 18),
		"status":     universal_db.CreateCondition(universal_db.In, []interface{}{"active", "pending"}),
	}

	query, err := universal_db.BuildSelect("users", safeConditions, universal_db.Postgres)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Println("Safe query:", query)

	// Attempting SQL injection
	unsafeConditions := universal_db.WhereConditions{
		"name": universal_db.CreateCondition(universal_db.Eq, "'; DROP TABLE users; --"),
	}

	query, err = universal_db.BuildSelect("users", unsafeConditions, universal_db.Postgres)
	if err != nil {
		fmt.Printf("Error with unsafe input: %v\n", err)
		return
	}
}

// Usage example
func Example2() {
	conditions := universal_db.WhereConditions{
		"name":       universal_db.CreateCondition(universal_db.ILike, "John's"), // Will be properly escaped
		"age":        universal_db.CreateCondition(universal_db.Gte, 18),
		"status":     universal_db.CreateCondition(universal_db.In, []interface{}{"active", "pending"}),
		"email":      universal_db.CreateCondition(universal_db.Like, "test@example.com"),
		"deleted_at": universal_db.CreateCondition(universal_db.IsNull, nil),
	}

	query, err := universal_db.BuildSelect("users", conditions, universal_db.Postgres)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Println(query)
}

func Example3() {
	// Simple usage
	conditions := universal_db.WhereConditions{
		"first_name":  universal_db.CreateEqualsCondition("John"),
		"age":         universal_db.CreateGreaterOrEqualCondition(18),
		"status":      universal_db.CreateInCondition(universal_db.ToInterfaceSlice([]string{"active", "pending"})),
		"created_at":  universal_db.CreateGreaterThanCondition(time.Now().AddDate(0, -1, 0)),
		"deleted_at":  universal_db.CreateIsNullCondition(),
		"is_verified": universal_db.CreateEqualsCondition(true),
	}

	// With validation
	validatedCondition, err := universal_db.CreateConditionWithValidation(universal_db.ILike, "%@gmail.com")
	if err == nil {
		conditions["email"] = validatedCondition
	}

	query, err := universal_db.BuildSelect("users", conditions, universal_db.Postgres)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Println(query)
}

func TestSQLInjection() {
	// Test cases with potential SQL injection attempts
	tests := []struct {
		tableName  string
		conditions universal_db.WhereConditions
	}{
		{
			"users'; DROP TABLE users; --",
			universal_db.WhereConditions{
				"name": universal_db.CreateCondition(universal_db.Eq, "test"),
			},
		},
		{
			"users",
			universal_db.WhereConditions{
				"name': CreateCondition(Eq, '1=1; --": universal_db.CreateCondition(universal_db.Eq, "test"),
			},
		},
		{
			"users",
			universal_db.WhereConditions{
				"name": universal_db.CreateCondition(universal_db.Eq, "'; DROP TABLE users; --"),
			},
		},
	}

	for _, test := range tests {
		query, err := universal_db.BuildSelect(test.tableName, test.conditions, universal_db.Postgres)
		if err != nil {
			fmt.Printf("Expected error caught: %v\n", err)
		} else {
			fmt.Printf("Generated safe query: %s\n", query)
		}
	}
}

func Example4() {
	// Simple equals conditions
	conditions1 := universal_db.Conditions{
		"name":       "John",
		"age":        25,
		"department": "IT",
	}

	// Mixed with explicit conditions
	conditions2 := universal_db.Conditions{
		"name":       "John",                                      // equals
		"age":        universal_db.CreateGreaterThanCondition(25), // explicit
		"status":     []string{"active", "pending"},               // automatic IN clause
		"email":      "%@gmail.com",                               // automatic LIKE
		"deleted_at": nil,                                         // automatic IS NULL
	}

	// Generate queries
	query1, err := universal_db.BuildSelectFromMap("users", conditions1, universal_db.Postgres)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Println("Simple query:", query1)
	}

	query2, err := universal_db.BuildSelectFromMap("users", conditions2, universal_db.Postgres)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Println("Mixed query:", query2)
	}
}

// Example usage
func Example5() {
	// 1. Simple equals conditions
	simpleConditions := universal_db.Conditions{
		"name":      "John",
		"age":       25,
		"is_active": true,
	}

	// 2. Mixed conditions
	mixedConditions := universal_db.Conditions{
		"name":       "John",                                      // equals
		"age":        universal_db.CreateGreaterThanCondition(25), // explicit condition
		"status":     universal_db.CreateInCondition([]string{"active", "pending"}),
		"department": "IT", // equals
		"salary":     universal_db.CreateGreaterThanCondition(50000),
	}

	// 3. From JSON string
	jsonStr := `{
        "name": "John",
        "age": {"operator": "gte", "value": 25},
        "status": ["active", "pending"],
        "email": "%@gmail.com"
    }`

	var jsonConditions universal_db.Conditions
	if err := json.Unmarshal([]byte(jsonStr), &jsonConditions); err != nil {
		fmt.Printf("JSON parse error: %v\n", err)
		return
	}

	// 4. Different databases
	databases := []universal_db.DBType{universal_db.Postgres, universal_db.Oracle, universal_db.SQLServer, universal_db.MySQL, universal_db.SQLite}

	for _, db := range databases {
		// Simple equals query
		query1, err := universal_db.BuildSelectFromMap("users", simpleConditions, db)
		if err != nil {
			fmt.Printf("Error building simple query: %v\n", err)
		} else {
			fmt.Printf("Simple query (%s): %s\n", db, query1)
		}

		// Mixed conditions query
		query2, err := universal_db.BuildSelectFromMap("users", mixedConditions, db)
		if err != nil {
			fmt.Printf("Error building mixed query: %v\n", err)
		} else {
			fmt.Printf("Mixed query (%s): %s\n", db, query2)
		}

		// JSON conditions query
		query3, err := universal_db.BuildSelectFromMap("users", jsonConditions, db)
		if err != nil {
			fmt.Printf("Error building JSON query: %v\n", err)
		} else {
			fmt.Printf("JSON query (%s): %s\n", db, query3)
		}
	}
}

// Example with error handling and validation
func Example6() {
	conditions := universal_db.Conditions{
		"name":       "O'Connor", // Will be properly escaped
		"age":        25,
		"email":      "%@gmail.com",                 // Will be treated as LIKE if contains %
		"status":     []string{"active", "pending"}, // Will be converted to IN clause
		"created_at": time.Now(),                    // Will be properly formatted for each DB
		"is_deleted": nil,                           // Will be converted to IS NULL
	}

	// Test with different databases
	for _, db := range []universal_db.DBType{universal_db.Postgres, universal_db.Oracle, universal_db.SQLServer, universal_db.MySQL, universal_db.SQLite} {
		query, err := universal_db.BuildSelectFromMap("users", conditions, db)
		if err != nil {
			fmt.Printf("Error (%s): %v\n", db, err)
			continue
		}
		fmt.Printf("Query (%s): %s\n", db, query)
	}
}

// Example usage:
func ExampleToInterfaceSlice() {
	// String slice
	strSlice := []string{"active", "pending", "inactive"}
	strInterface := universal_db.ToInterfaceSlice(strSlice)

	// Int slice
	intSlice := []int{1, 2, 3, 4, 5}
	intInterface := universal_db.ToInterfaceSlice(intSlice)

	// Using with conditions
	conditions := universal_db.Conditions{
		"status": universal_db.CreateInCondition(strInterface),
		"id":     universal_db.CreateInCondition(intInterface),
	}

	query, err := universal_db.BuildSelectFromMap("users", conditions, universal_db.Postgres)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Println(query)
}

// Usage example
func Example() {
	conditions := universal_db.Conditions{
		"status": universal_db.CreateInCondition(universal_db.StringsToInterface([]string{"active", "pending"})),
		"id":     universal_db.CreateInCondition(universal_db.IntsToInterface([]int{1, 2, 3, 4, 5})),
		"score":  universal_db.CreateInCondition(universal_db.Float64sToInterface([]float64{3.14, 2.71})),
	}

	query, err := universal_db.BuildSelectFromMap("users", conditions, universal_db.Postgres)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Println(query)
}
