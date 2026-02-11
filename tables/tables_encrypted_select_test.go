package tables

import (
	"testing"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/databases/db/query/builder"
)

// TestValidateFieldName tests field name validation
func TestValidateFieldName(t *testing.T) {
	tests := []struct {
		name      string
		fieldName string
		wantErr   bool
	}{
		{"valid simple", "user_id", false},
		{"valid with dot", "users.id", false},
		{"valid uppercase", "USER_ID", false},
		{"invalid with semicolon", "id; DROP TABLE users--", true},
		{"invalid with quote", "id'", true},
		{"invalid with dash", "user-id", true},
		{"invalid empty", "", true},
		{"invalid starting with number", "1id", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateFieldName(tt.fieldName)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateFieldName(%q) error = %v, wantErr %v", tt.fieldName, err, tt.wantErr)
			}
		})
	}
}

// TestValidateOrderByDirection tests ORDER BY direction validation
func TestValidateOrderByDirection(t *testing.T) {
	tests := []struct {
		name      string
		direction string
		wantErr   bool
	}{
		{"valid asc", "asc", false},
		{"valid desc", "desc", false},
		{"valid ASC", "ASC", false},
		{"valid DESC", "DESC", false},
		{"invalid random", "random", true},
		{"invalid injection", "ASC; DROP TABLE users--", true},
		{"invalid empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOrderByDirection(tt.direction)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateOrderByDirection(%q) error = %v, wantErr %v", tt.direction, err, tt.wantErr)
			}
		})
	}
}

// TestConvertJoinToQueryBuilder_Structured tests structured JoinDef conversion
func TestConvertJoinToQueryBuilder_Structured(t *testing.T) {
	qb := builder.NewSelectQueryBuilder(base.DXDatabaseTypePostgreSQL)

	joins := []builder.JoinDef{
		{
			Type:    builder.JoinTypeInner,
			Table:   "other_table",
			Alias:   "ot",
			OnLeft:  "t1.id",
			OnRight: "ot.t_id",
		},
	}

	err := convertJoinToQueryBuilder(qb, joins)
	if err != nil {
		t.Errorf("convertJoinToQueryBuilder() with valid JoinDef failed: %v", err)
	}

	if len(qb.Joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(qb.Joins))
	}
}

// TestConvertJoinToQueryBuilder_String tests string JOIN parsing
func TestConvertJoinToQueryBuilder_String(t *testing.T) {
	tests := []struct {
		name    string
		joinStr string
		wantErr bool
	}{
		{
			name:    "valid INNER JOIN",
			joinStr: "INNER JOIN other_table ON t1.id = other_table.t_id",
			wantErr: false,
		},
		{
			name:    "valid LEFT JOIN with alias",
			joinStr: "LEFT JOIN other_table AS ot ON t1.id = ot.t_id",
			wantErr: false,
		},
		{
			name:    "valid RIGHT JOIN",
			joinStr: "RIGHT JOIN other_table ot ON t1.id = ot.t_id",
			wantErr: false,
		},
		{
			name:    "SQL injection via table name",
			joinStr: "INNER JOIN malicious; DROP TABLE users-- ON 1=1",
			wantErr: true,
		},
		{
			name:    "SQL injection via ON clause",
			joinStr: "INNER JOIN other_table ON t1.id = t2.id; DROP TABLE users--",
			wantErr: true,
		},
		{
			name:    "invalid missing ON",
			joinStr: "INNER JOIN other_table",
			wantErr: true,
		},
		{
			name:    "invalid ON syntax",
			joinStr: "INNER JOIN other_table ON invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := builder.NewSelectQueryBuilder(base.DXDatabaseTypePostgreSQL)
			err := convertJoinToQueryBuilder(qb, tt.joinStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertJoinToQueryBuilder(%q) error = %v, wantErr %v", tt.joinStr, err, tt.wantErr)
			}
		})
	}
}

// TestExecuteEncryptedSelect_OrderByInjection tests SQL injection via ORDER BY
func TestExecuteEncryptedSelect_OrderByInjection(t *testing.T) {
	// This is a conceptual test - in real usage, we'd need a live database
	// Here we test the validation layer only

	orderBy := db.DXDatabaseTableFieldsOrderBy{
		"created_at; DROP TABLE users--": "DESC",
	}

	// Validate the field name would fail
	for fieldName := range orderBy {
		err := validateFieldName(fieldName)
		if err == nil {
			t.Error("Expected field name validation to fail for SQL injection attempt")
		}
	}
}

// TestExecuteEncryptedSelect_WhereInjection tests SQL injection via WHERE
func TestExecuteEncryptedSelect_WhereInjection(t *testing.T) {
	// Test that invalid field names in WHERE are rejected
	invalidFieldName := "org_id; DROP TABLE users--"

	err := validateFieldName(invalidFieldName)
	if err == nil {
		t.Error("Expected field name validation to fail for SQL injection attempt")
	}
}

// TestExecuteEncryptedPaging_OrderByParsing tests ORDER BY string parsing
func TestExecuteEncryptedPaging_OrderByParsing(t *testing.T) {
	// Test that malicious ORDER BY strings are rejected
	// Parse the order by string (simulating what executeEncryptedPaging does)
	tokens := []string{"created_at;", "DROP", "TABLE", "users--"}
	fieldName := tokens[0]

	err := validateFieldName(fieldName)
	if err == nil {
		t.Error("Expected field name validation to fail for SQL injection attempt")
	}
}
