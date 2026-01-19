package database3

// ============================================================================
// DBEntity Type Constants
// ============================================================================

type DBEntityType int

const (
	DBEntityTypeTable DBEntityType = iota
	DBEntityTypeView
	DBEntityTypeMaterializedView
)

// ============================================================================
// DBEntity - Base struct for all database entities (tables, views)
// ============================================================================

type DBEntity struct {
	Name   string
	Type   DBEntityType
	Order  int
	Schema *DBSchema
}

// FullName returns the entity name with schema prefix
func (t *DBEntity) FullName() string {
	if t.Schema != nil && t.Schema.Name != "" {
		return t.Schema.Name + "." + t.Name
	}
	return t.Name
}
