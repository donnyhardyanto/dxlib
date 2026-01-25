package models

// ============================================================================
// ModelDBEntity Type Constants
// ============================================================================

type ModelDBEntityType int

const (
	ModelDBEntityTypeTable ModelDBEntityType = iota
	ModelDBEntityTypeView
	ModelDBEntityTypeMaterializedView
	ModelDBEntityTypeFunction
	ModelDBEntityTypeIndex
	ModelDBEntityTypeTrigger
)

// ============================================================================
// ModelDBEntity - Base struct for all database entities (tables, views)
// ============================================================================

type ModelDBEntity struct {
	Name   string
	Type   ModelDBEntityType
	Order  int
	Schema *ModelDBSchema
}

// FullName returns the entity name with schema prefix
func (t *ModelDBEntity) FullName() string {
	if t.Schema != nil && t.Schema.Name != "" {
		return t.Schema.Name + "." + t.Name
	}
	return t.Name
}
