package models

import (
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
)

// ============================================================================
// ModelDBTrigger - Database trigger entity
// ============================================================================

// ModelDBTriggerTiming represents when the trigger fires
type ModelDBTriggerTiming string

const (
	ModelDBTriggerTimingBefore    ModelDBTriggerTiming = "BEFORE"
	ModelDBTriggerTimingAfter     ModelDBTriggerTiming = "AFTER"
	ModelDBTriggerTimingInsteadOf ModelDBTriggerTiming = "INSTEAD OF"
)

// ModelDBTriggerEvent represents the event that fires the trigger
type ModelDBTriggerEvent string

const (
	ModelDBTriggerEventInsert   ModelDBTriggerEvent = "INSERT"
	ModelDBTriggerEventUpdate   ModelDBTriggerEvent = "UPDATE"
	ModelDBTriggerEventDelete   ModelDBTriggerEvent = "DELETE"
	ModelDBTriggerEventTruncate ModelDBTriggerEvent = "TRUNCATE"
)

// DBTriggerScope represents the trigger scope
type ModelDBTriggerScope string

const (
	ModelDBTriggerScopeRow       ModelDBTriggerScope = "FOR EACH ROW"
	ModelDBTriggerScopeStatement ModelDBTriggerScope = "FOR EACH STATEMENT"
)

// ModelDBTrigger represents a database trigger
type ModelDBTrigger struct {
	ModelDBEntity
	Timing              ModelDBTriggerTiming
	Events              []ModelDBTriggerEvent // INSERT, UPDATE, DELETE, TRUNCATE
	UpdateColumns       []string              // Columns for UPDATE OF (optional)
	Scope               ModelDBTriggerScope   // FOR EACH ROW or FOR EACH STATEMENT
	When                string                // WHEN condition (optional)
	ExecuteFunction     *ModelDBFunction      // Reference to the function to execute
	ExecuteFunctionName string                // Or just the function name if not using ModelDBFunction reference
	IsConstraint        bool                  // Constraint trigger (PostgreSQL)
	Deferrable          bool                  // DEFERRABLE (constraint triggers)
	InitiallyDeferred   bool                  // INITIALLY DEFERRED (constraint triggers)

	// Owner reference - the table this trigger is on
	OwnerTable *ModelDBTable
}

// NewModelDBTrigger creates a new trigger for a table
func NewModelDBTrigger(table *ModelDBTable, name string, order int, timing ModelDBTriggerTiming, events []ModelDBTriggerEvent, scope ModelDBTriggerScope, executeFunctionName string) *ModelDBTrigger {
	trigger := &ModelDBTrigger{
		ModelDBEntity: ModelDBEntity{
			Name:   name,
			Type:   ModelDBEntityTypeTrigger,
			Order:  order,
			Schema: table.Schema,
		},
		Timing:              timing,
		Events:              events,
		Scope:               scope,
		ExecuteFunctionName: executeFunctionName,
		OwnerTable:          table,
	}
	table.Triggers = append(table.Triggers, trigger)

	// Also add to schema's trigger pointer list
	if table.Schema != nil {
		table.Schema.Triggers = append(table.Schema.Triggers, trigger)
	}

	return trigger
}

// SetExecuteFunction sets the function reference
func (t *ModelDBTrigger) SetExecuteFunction(fn *ModelDBFunction) *ModelDBTrigger {
	t.ExecuteFunction = fn
	t.ExecuteFunctionName = fn.FullName()
	return t
}

// SetWhen sets the WHEN condition
func (t *ModelDBTrigger) SetWhen(condition string) *ModelDBTrigger {
	t.When = condition
	return t
}

// SetUpdateColumns sets the columns for UPDATE OF
func (t *ModelDBTrigger) SetUpdateColumns(columns []string) *ModelDBTrigger {
	t.UpdateColumns = columns
	return t
}

// GetOwnerName returns the name of the table that owns this trigger
func (t *ModelDBTrigger) GetOwnerName() string {
	if t.OwnerTable != nil {
		return t.OwnerTable.FullTableName()
	}
	return ""
}

// CreateDDL generates DDL script for the trigger based on database type
func (t *ModelDBTrigger) CreateDDL(dbType base.DXDatabaseType) (string, error) {
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return t.createPostgreSQLDDL(), nil
	case base.DXDatabaseTypeSQLServer:
		return t.createSQLServerDDL(), nil
	case base.DXDatabaseTypeMariaDB:
		return t.createMariaDBDDL(), nil
	case base.DXDatabaseTypeOracle:
		return t.createOracleDDL(), nil
	default:
		return "", fmt.Errorf("unsupported database type: %v", dbType)
	}
}

func (t *ModelDBTrigger) createPostgreSQLDDL() string {
	var sb strings.Builder

	sb.WriteString("CREATE TRIGGER ")
	sb.WriteString(t.Name)
	sb.WriteString("\n    ")
	sb.WriteString(string(t.Timing))
	sb.WriteString(" ")

	// Events
	var eventStrs []string
	for _, event := range t.Events {
		eventStr := string(event)
		if event == ModelDBTriggerEventUpdate && len(t.UpdateColumns) > 0 {
			eventStr += " OF " + strings.Join(t.UpdateColumns, ", ")
		}
		eventStrs = append(eventStrs, eventStr)
	}
	sb.WriteString(strings.Join(eventStrs, " OR "))

	sb.WriteString("\n    ON ")
	sb.WriteString(t.GetOwnerName())

	// Constraint trigger options
	if t.IsConstraint {
		sb.WriteString("\n    ")
		if t.Deferrable {
			sb.WriteString("DEFERRABLE ")
			if t.InitiallyDeferred {
				sb.WriteString("INITIALLY DEFERRED ")
			} else {
				sb.WriteString("INITIALLY IMMEDIATE ")
			}
		}
	}

	sb.WriteString("\n    ")
	sb.WriteString(string(t.Scope))

	// WHEN condition
	if t.When != "" {
		sb.WriteString("\n    WHEN (")
		sb.WriteString(t.When)
		sb.WriteString(")")
	}

	sb.WriteString("\nEXECUTE FUNCTION ")
	sb.WriteString(t.ExecuteFunctionName)
	sb.WriteString("();\n")

	return sb.String()
}

func (t *ModelDBTrigger) createSQLServerDDL() string {
	var sb strings.Builder

	sb.WriteString("CREATE TRIGGER ")
	sb.WriteString(t.FullName())
	sb.WriteString("\nON ")
	sb.WriteString(t.GetOwnerName())
	sb.WriteString("\n")
	sb.WriteString(string(t.Timing))
	sb.WriteString(" ")

	// Events
	var eventStrs []string
	for _, event := range t.Events {
		eventStrs = append(eventStrs, string(event))
	}
	sb.WriteString(strings.Join(eventStrs, ", "))
	sb.WriteString("\nAS\n")
	sb.WriteString("BEGIN\n")
	sb.WriteString("    EXEC ")
	sb.WriteString(t.ExecuteFunctionName)
	sb.WriteString(";\n")
	sb.WriteString("END;\n")

	return sb.String()
}

func (t *ModelDBTrigger) createMariaDBDDL() string {
	var sb strings.Builder

	// MariaDB/MySQL triggers: one trigger per timing/event combination
	// We'll generate for the first event only, user should create multiple triggers for multiple events
	sb.WriteString("CREATE TRIGGER ")
	sb.WriteString(t.Name)
	sb.WriteString("\n")
	sb.WriteString(string(t.Timing))
	sb.WriteString(" ")
	if len(t.Events) > 0 {
		sb.WriteString(string(t.Events[0]))
	}
	sb.WriteString("\nON ")
	sb.WriteString(t.GetOwnerName())
	sb.WriteString("\n")
	sb.WriteString(string(t.Scope))
	sb.WriteString("\n")
	sb.WriteString("CALL ")
	sb.WriteString(t.ExecuteFunctionName)
	sb.WriteString("();\n")

	return sb.String()
}

func (t *ModelDBTrigger) createOracleDDL() string {
	var sb strings.Builder

	sb.WriteString("CREATE OR REPLACE TRIGGER ")
	sb.WriteString(t.FullName())
	sb.WriteString("\n")
	sb.WriteString(string(t.Timing))
	sb.WriteString(" ")

	// Events
	var eventStrs []string
	for _, event := range t.Events {
		eventStr := string(event)
		if event == ModelDBTriggerEventUpdate && len(t.UpdateColumns) > 0 {
			eventStr += " OF " + strings.Join(t.UpdateColumns, ", ")
		}
		eventStrs = append(eventStrs, eventStr)
	}
	sb.WriteString(strings.Join(eventStrs, " OR "))

	sb.WriteString("\nON ")
	sb.WriteString(t.GetOwnerName())
	sb.WriteString("\n")

	// Oracle uses FOR EACH ROW or nothing (statement level is default)
	if t.Scope == ModelDBTriggerScopeRow {
		sb.WriteString("FOR EACH ROW\n")
	}

	// WHEN condition
	if t.When != "" {
		sb.WriteString("WHEN (")
		sb.WriteString(t.When)
		sb.WriteString(")\n")
	}

	sb.WriteString("BEGIN\n")
	sb.WriteString("    ")
	sb.WriteString(t.ExecuteFunctionName)
	sb.WriteString(";\n")
	sb.WriteString("END;\n/\n")

	return sb.String()
}
