package database3

import (
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
)

// ============================================================================
// DBTrigger - Database trigger entity
// ============================================================================

// DBTriggerTiming represents when the trigger fires
type DBTriggerTiming string

const (
	DBTriggerTimingBefore  DBTriggerTiming = "BEFORE"
	DBTriggerTimingAfter   DBTriggerTiming = "AFTER"
	DBTriggerTimingInsteadOf DBTriggerTiming = "INSTEAD OF"
)

// DBTriggerEvent represents the event that fires the trigger
type DBTriggerEvent string

const (
	DBTriggerEventInsert DBTriggerEvent = "INSERT"
	DBTriggerEventUpdate DBTriggerEvent = "UPDATE"
	DBTriggerEventDelete DBTriggerEvent = "DELETE"
	DBTriggerEventTruncate DBTriggerEvent = "TRUNCATE"
)

// DBTriggerScope represents the trigger scope
type DBTriggerScope string

const (
	DBTriggerScopeRow       DBTriggerScope = "FOR EACH ROW"
	DBTriggerScopeStatement DBTriggerScope = "FOR EACH STATEMENT"
)

// DBTrigger represents a database trigger
type DBTrigger struct {
	DBEntity
	Timing           DBTriggerTiming
	Events           []DBTriggerEvent      // INSERT, UPDATE, DELETE, TRUNCATE
	UpdateColumns    []string              // Columns for UPDATE OF (optional)
	Scope            DBTriggerScope        // FOR EACH ROW or FOR EACH STATEMENT
	When             string                // WHEN condition (optional)
	ExecuteFunction  *DBFunction           // Reference to the function to execute
	ExecuteFunctionName string             // Or just the function name if not using DBFunction reference
	IsConstraint     bool                  // Constraint trigger (PostgreSQL)
	Deferrable       bool                  // DEFERRABLE (constraint triggers)
	InitiallyDeferred bool                 // INITIALLY DEFERRED (constraint triggers)

	// Owner reference - the table this trigger is on
	OwnerTable *DBTable
}

// NewDBTrigger creates a new trigger for a table
func NewDBTrigger(table *DBTable, name string, order int, timing DBTriggerTiming, events []DBTriggerEvent, scope DBTriggerScope, executeFunctionName string) *DBTrigger {
	trigger := &DBTrigger{
		DBEntity: DBEntity{
			Name:   name,
			Type:   DBEntityTypeTrigger,
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
func (t *DBTrigger) SetExecuteFunction(fn *DBFunction) *DBTrigger {
	t.ExecuteFunction = fn
	t.ExecuteFunctionName = fn.FullName()
	return t
}

// SetWhen sets the WHEN condition
func (t *DBTrigger) SetWhen(condition string) *DBTrigger {
	t.When = condition
	return t
}

// SetUpdateColumns sets the columns for UPDATE OF
func (t *DBTrigger) SetUpdateColumns(columns []string) *DBTrigger {
	t.UpdateColumns = columns
	return t
}

// GetOwnerName returns the name of the table that owns this trigger
func (t *DBTrigger) GetOwnerName() string {
	if t.OwnerTable != nil {
		return t.OwnerTable.FullTableName()
	}
	return ""
}

// CreateDDL generates DDL script for the trigger based on database type
func (t *DBTrigger) CreateDDL(dbType base.DXDatabaseType) (string, error) {
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

func (t *DBTrigger) createPostgreSQLDDL() string {
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
		if event == DBTriggerEventUpdate && len(t.UpdateColumns) > 0 {
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

func (t *DBTrigger) createSQLServerDDL() string {
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

func (t *DBTrigger) createMariaDBDDL() string {
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

func (t *DBTrigger) createOracleDDL() string {
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
		if event == DBTriggerEventUpdate && len(t.UpdateColumns) > 0 {
			eventStr += " OF " + strings.Join(t.UpdateColumns, ", ")
		}
		eventStrs = append(eventStrs, eventStr)
	}
	sb.WriteString(strings.Join(eventStrs, " OR "))

	sb.WriteString("\nON ")
	sb.WriteString(t.GetOwnerName())
	sb.WriteString("\n")

	// Oracle uses FOR EACH ROW or nothing (statement level is default)
	if t.Scope == DBTriggerScopeRow {
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
