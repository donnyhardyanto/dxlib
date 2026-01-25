package models

import (
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
)

// ============================================================================
// ModelDBFunction - Database function/stored procedure entity
// ============================================================================

// ModelDBFunctionParameter represents a function parameter
type ModelDBFunctionParameter struct {
	Name         string
	DataType     string                         // Generic type string
	DataTypeByDB map[base.DXDatabaseType]string // Database-specific type override
	Mode         string                         // IN, OUT, INOUT (default: IN)
	DefaultValue string                         // Optional default value
}

// ModelDBFunction represents a database function or stored procedure
type ModelDBFunction struct {
	ModelDBEntity
	Parameters      []ModelDBFunctionParameter
	ReturnType      string                         // Return type (e.g., "TRIGGER", "INTEGER", "TABLE", "VOID")
	ReturnTypeByDB  map[base.DXDatabaseType]string // Database-specific return type override
	Language        string                         // plpgsql, sql, etc.
	Body            string                         // Function body (between $$ and $$)
	BodyByDB        map[base.DXDatabaseType]string // Database-specific body override
	IsReplace       bool                           // CREATE OR REPLACE
	Volatility      string                         // VOLATILE, STABLE, IMMUTABLE (PostgreSQL)
	SecurityDefiner bool                           // SECURITY DEFINER vs SECURITY INVOKER
}

// NewModelDBFunction creates a new database function and registers it with the schema
func NewModelDBFunction(schema *ModelDBSchema, name string, order int, returnType string, language string, body string) *ModelDBFunction {
	fn := &ModelDBFunction{
		ModelDBEntity: ModelDBEntity{
			Name:   name,
			Type:   ModelDBEntityTypeFunction,
			Order:  order,
			Schema: schema,
		},
		Parameters:      []ModelDBFunctionParameter{},
		ReturnType:      returnType,
		Language:        language,
		Body:            body,
		IsReplace:       true, // Default to CREATE OR REPLACE
		Volatility:      "",
		SecurityDefiner: false,
	}
	if schema != nil {
		schema.Functions = append(schema.Functions, fn)
	}
	return fn
}

// AddParameter adds a parameter to the function
func (f *ModelDBFunction) AddParameter(name string, dataType string, mode string, defaultValue string) *ModelDBFunction {
	if mode == "" {
		mode = "IN"
	}
	f.Parameters = append(f.Parameters, ModelDBFunctionParameter{
		Name:         name,
		DataType:     dataType,
		Mode:         mode,
		DefaultValue: defaultValue,
	})
	return f
}

// SetBodyByDB sets database-specific function body
func (f *ModelDBFunction) SetBodyByDB(dbType base.DXDatabaseType, body string) *ModelDBFunction {
	if f.BodyByDB == nil {
		f.BodyByDB = make(map[base.DXDatabaseType]string)
	}
	f.BodyByDB[dbType] = body
	return f
}

// CreateDDL generates DDL script for the function based on database type
func (f *ModelDBFunction) CreateDDL(dbType base.DXDatabaseType) (string, error) {
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return f.createPostgreSQLDDL(), nil
	case base.DXDatabaseTypeSQLServer:
		return f.createSQLServerDDL(), nil
	case base.DXDatabaseTypeMariaDB:
		return f.createMariaDBDDL(), nil
	case base.DXDatabaseTypeOracle:
		return f.createOracleDDL(), nil
	default:
		return "", fmt.Errorf("unsupported database type: %v", dbType)
	}
}

func (f *ModelDBFunction) createPostgreSQLDDL() string {
	var sb strings.Builder

	// CREATE OR REPLACE FUNCTION
	if f.IsReplace {
		sb.WriteString("CREATE OR REPLACE FUNCTION ")
	} else {
		sb.WriteString("CREATE FUNCTION ")
	}

	sb.WriteString(f.FullName())

	// Parameters
	sb.WriteString("(")
	var params []string
	for _, p := range f.Parameters {
		param := ""
		if p.Mode != "" && p.Mode != "IN" {
			param = p.Mode + " "
		}
		if p.Name != "" {
			param += p.Name + " "
		}
		dataType := p.DataType
		if p.DataTypeByDB != nil {
			if dt, ok := p.DataTypeByDB[base.DXDatabaseTypePostgreSQL]; ok {
				dataType = dt
			}
		}
		param += dataType
		if p.DefaultValue != "" {
			param += " DEFAULT " + p.DefaultValue
		}
		params = append(params, param)
	}
	sb.WriteString(strings.Join(params, ", "))
	sb.WriteString(")\n")

	// Return type
	returnType := f.ReturnType
	if f.ReturnTypeByDB != nil {
		if rt, ok := f.ReturnTypeByDB[base.DXDatabaseTypePostgreSQL]; ok {
			returnType = rt
		}
	}
	sb.WriteString("    RETURNS " + returnType + " AS\n")

	// Body
	body := f.Body
	if f.BodyByDB != nil {
		if b, ok := f.BodyByDB[base.DXDatabaseTypePostgreSQL]; ok {
			body = b
		}
	}
	sb.WriteString("$$\n")
	sb.WriteString(body)
	sb.WriteString("\n$$")

	// Language
	sb.WriteString(" LANGUAGE " + f.Language)

	// Volatility
	if f.Volatility != "" {
		sb.WriteString(" " + f.Volatility)
	}

	// Security
	if f.SecurityDefiner {
		sb.WriteString(" SECURITY DEFINER")
	}

	sb.WriteString(";\n")

	return sb.String()
}

func (f *ModelDBFunction) createSQLServerDDL() string {
	var sb strings.Builder

	// SQL Server uses CREATE PROCEDURE or CREATE FUNCTION
	if f.ReturnType == "TRIGGER" {
		// SQL Server doesn't have TRIGGER return type, triggers are separate
		return "-- SQL Server: Trigger functions are handled differently\n"
	}

	if f.IsReplace {
		sb.WriteString("CREATE OR ALTER FUNCTION ")
	} else {
		sb.WriteString("CREATE FUNCTION ")
	}

	sb.WriteString(f.FullName())

	// Parameters
	sb.WriteString("(")
	var params []string
	for _, p := range f.Parameters {
		dataType := p.DataType
		if p.DataTypeByDB != nil {
			if dt, ok := p.DataTypeByDB[base.DXDatabaseTypeSQLServer]; ok {
				dataType = dt
			}
		}
		param := "@" + p.Name + " " + dataType
		if p.DefaultValue != "" {
			param += " = " + p.DefaultValue
		}
		params = append(params, param)
	}
	sb.WriteString(strings.Join(params, ", "))
	sb.WriteString(")\n")

	// Return type
	returnType := f.ReturnType
	if f.ReturnTypeByDB != nil {
		if rt, ok := f.ReturnTypeByDB[base.DXDatabaseTypeSQLServer]; ok {
			returnType = rt
		}
	}
	sb.WriteString("RETURNS " + returnType + "\n")
	sb.WriteString("AS\n")
	sb.WriteString("BEGIN\n")

	// Body
	body := f.Body
	if f.BodyByDB != nil {
		if b, ok := f.BodyByDB[base.DXDatabaseTypeSQLServer]; ok {
			body = b
		}
	}
	sb.WriteString(body)
	sb.WriteString("\nEND;\n")

	return sb.String()
}

func (f *ModelDBFunction) createMariaDBDDL() string {
	var sb strings.Builder

	// MariaDB/MySQL
	if f.IsReplace {
		sb.WriteString("DROP FUNCTION IF EXISTS " + f.FullName() + ";\n")
	}
	sb.WriteString("CREATE FUNCTION " + f.FullName())

	// Parameters
	sb.WriteString("(")
	var params []string
	for _, p := range f.Parameters {
		dataType := p.DataType
		if p.DataTypeByDB != nil {
			if dt, ok := p.DataTypeByDB[base.DXDatabaseTypeMariaDB]; ok {
				dataType = dt
			}
		}
		param := p.Name + " " + dataType
		params = append(params, param)
	}
	sb.WriteString(strings.Join(params, ", "))
	sb.WriteString(")\n")

	// Return type
	returnType := f.ReturnType
	if f.ReturnTypeByDB != nil {
		if rt, ok := f.ReturnTypeByDB[base.DXDatabaseTypeMariaDB]; ok {
			returnType = rt
		}
	}
	sb.WriteString("RETURNS " + returnType + "\n")
	sb.WriteString("BEGIN\n")

	// Body
	body := f.Body
	if f.BodyByDB != nil {
		if b, ok := f.BodyByDB[base.DXDatabaseTypeMariaDB]; ok {
			body = b
		}
	}
	sb.WriteString(body)
	sb.WriteString("\nEND;\n")

	return sb.String()
}

func (f *ModelDBFunction) createOracleDDL() string {
	var sb strings.Builder

	// Oracle
	if f.IsReplace {
		sb.WriteString("CREATE OR REPLACE FUNCTION ")
	} else {
		sb.WriteString("CREATE FUNCTION ")
	}

	sb.WriteString(f.FullName())

	// Parameters
	if len(f.Parameters) > 0 {
		sb.WriteString("(")
		var params []string
		for _, p := range f.Parameters {
			dataType := p.DataType
			if p.DataTypeByDB != nil {
				if dt, ok := p.DataTypeByDB[base.DXDatabaseTypeOracle]; ok {
					dataType = dt
				}
			}
			param := p.Name + " " + p.Mode + " " + dataType
			if p.DefaultValue != "" {
				param += " DEFAULT " + p.DefaultValue
			}
			params = append(params, param)
		}
		sb.WriteString(strings.Join(params, ", "))
		sb.WriteString(")")
	}

	// Return type
	returnType := f.ReturnType
	if f.ReturnTypeByDB != nil {
		if rt, ok := f.ReturnTypeByDB[base.DXDatabaseTypeOracle]; ok {
			returnType = rt
		}
	}
	sb.WriteString("\nRETURN " + returnType + " IS\n")
	sb.WriteString("BEGIN\n")

	// Body
	body := f.Body
	if f.BodyByDB != nil {
		if b, ok := f.BodyByDB[base.DXDatabaseTypeOracle]; ok {
			body = b
		}
	}
	sb.WriteString(body)
	sb.WriteString("\nEND;\n/\n")

	return sb.String()
}
