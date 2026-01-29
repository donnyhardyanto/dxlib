package models

import (
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
)

// ============================================================================
// ModelDBIndex - Database index entity
// ============================================================================

// ModelDBIndexMethod represents the index method/type
type ModelDBIndexMethod string

const (
	ModelDBIndexMethodBTree  ModelDBIndexMethod = "BTREE"
	ModelDBIndexMethodHash   ModelDBIndexMethod = "HASH"
	ModelDBIndexMethodGiST   ModelDBIndexMethod = "GIST"
	ModelDBIndexMethodGIN    ModelDBIndexMethod = "GIN"
	ModelDBIndexMethodSPGiST ModelDBIndexMethod = "SPGIST"
	ModelDBIndexMethodBRIN   ModelDBIndexMethod = "BRIN"
)

// ModelDBIndexColumn represents a column in the index
type ModelDBIndexColumn struct {
	Name       string
	Order      string // ASC, DESC, empty for default
	NullsOrder string // NULLS FIRST, NULLS LAST, empty for default
}

// ModelDBIndex represents a databases index
type ModelDBIndex struct {
	ModelDBEntity
	Columns     []ModelDBIndexColumn
	IsUnique    bool
	Method      ModelDBIndexMethod // BTREE, HASH, GIST, etc.
	Where       string             // Partial index condition (PostgreSQL)
	Include     []string           // INCLUDE columns (PostgreSQL 11+)
	Tablespace  string             // Optional tablespace
	Concurrent  bool               // CREATE INDEX CONCURRENTLY (PostgreSQL)
	IfNotExists bool               // CREATE INDEX IF NOT EXISTS

	// Owner reference - either Table or MaterializedView (set by NewDBIndex*)
	OwnerTable            *ModelDBTable
	OwnerMaterializedView *ModelDBMaterializedView
}

// NewDBIndexForTable creates a new index for a table
func NewModelDBIndexForTable(table *ModelDBTable, name string, order int, columns []ModelDBIndexColumn, isUnique bool) *ModelDBIndex {
	idx := &ModelDBIndex{
		ModelDBEntity: ModelDBEntity{
			Name:   name,
			Type:   ModelDBEntityTypeIndex,
			Order:  order,
			Schema: table.Schema,
		},
		Columns:    columns,
		IsUnique:   isUnique,
		Method:     ModelDBIndexMethodBTree,
		OwnerTable: table,
	}
	table.Indexes = append(table.Indexes, idx)
	return idx
}

// NewDBIndexForMaterializedView creates a new index for a materialized view
func NewModelDBIndexForMaterializedView(mv *ModelDBMaterializedView, name string, order int, columns []ModelDBIndexColumn, isUnique bool) *ModelDBIndex {
	idx := &ModelDBIndex{
		ModelDBEntity: ModelDBEntity{
			Name:   name,
			Type:   ModelDBEntityTypeIndex,
			Order:  order,
			Schema: mv.Schema,
		},
		Columns:               columns,
		IsUnique:              isUnique,
		Method:                ModelDBIndexMethodBTree,
		OwnerMaterializedView: mv,
	}
	mv.Indexes = append(mv.Indexes, idx)
	return idx
}

// SetMethod sets the index method
func (i *ModelDBIndex) SetMethod(method ModelDBIndexMethod) *ModelDBIndex {
	i.Method = method
	return i
}

// SetWhere sets the partial index condition
func (i *ModelDBIndex) SetWhere(condition string) *ModelDBIndex {
	i.Where = condition
	return i
}

// SetInclude sets the INCLUDE columns
func (i *ModelDBIndex) SetInclude(columns []string) *ModelDBIndex {
	i.Include = columns
	return i
}

// GetOwnerName returns the name of the table or materialized view that owns this index
func (i *ModelDBIndex) GetOwnerName() string {
	if i.OwnerTable != nil {
		return i.OwnerTable.FullTableName()
	}
	if i.OwnerMaterializedView != nil {
		return i.OwnerMaterializedView.FullName()
	}
	return ""
}

// CreateDDL generates DDL script for the index based on databases type
func (i *ModelDBIndex) CreateDDL(dbType base.DXDatabaseType) (string, error) {
	switch dbType {
	case base.DXDatabaseTypePostgreSQL:
		return i.createPostgreSQLDDL(), nil
	case base.DXDatabaseTypeSQLServer:
		return i.createSQLServerDDL(), nil
	case base.DXDatabaseTypeMariaDB:
		return i.createMariaDBDDL(), nil
	case base.DXDatabaseTypeOracle:
		return i.createOracleDDL(), nil
	default:
		return "", fmt.Errorf("unsupported databases type: %v", dbType)
	}
}

func (i *ModelDBIndex) createPostgreSQLDDL() string {
	var sb strings.Builder

	sb.WriteString("CREATE ")
	if i.IsUnique {
		sb.WriteString("UNIQUE ")
	}
	sb.WriteString("INDEX ")
	if i.Concurrent {
		sb.WriteString("CONCURRENTLY ")
	}
	if i.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	sb.WriteString(i.Name)
	sb.WriteString(" ON ")
	sb.WriteString(i.GetOwnerName())

	// Method
	if i.Method != "" && i.Method != ModelDBIndexMethodBTree {
		sb.WriteString(" USING ")
		sb.WriteString(string(i.Method))
	}

	// Columns
	sb.WriteString(" (")
	var cols []string
	for _, col := range i.Columns {
		colStr := col.Name
		if col.Order != "" {
			colStr += " " + col.Order
		}
		if col.NullsOrder != "" {
			colStr += " " + col.NullsOrder
		}
		cols = append(cols, colStr)
	}
	sb.WriteString(strings.Join(cols, ", "))
	sb.WriteString(")")

	// Include columns
	if len(i.Include) > 0 {
		sb.WriteString(" INCLUDE (")
		sb.WriteString(strings.Join(i.Include, ", "))
		sb.WriteString(")")
	}

	// Tablespace
	if i.Tablespace != "" {
		sb.WriteString(" TABLESPACE ")
		sb.WriteString(i.Tablespace)
	}

	// Where clause for partial index
	if i.Where != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(i.Where)
	}

	sb.WriteString(";\n")

	return sb.String()
}

func (i *ModelDBIndex) createSQLServerDDL() string {
	var sb strings.Builder

	sb.WriteString("CREATE ")
	if i.IsUnique {
		sb.WriteString("UNIQUE ")
	}
	// SQL Server supports CLUSTERED/NONCLUSTERED
	sb.WriteString("NONCLUSTERED INDEX ")
	sb.WriteString(i.Name)
	sb.WriteString(" ON ")
	sb.WriteString(i.GetOwnerName())

	// Columns
	sb.WriteString(" (")
	var cols []string
	for _, col := range i.Columns {
		colStr := col.Name
		if col.Order != "" {
			colStr += " " + col.Order
		}
		cols = append(cols, colStr)
	}
	sb.WriteString(strings.Join(cols, ", "))
	sb.WriteString(")")

	// Include columns
	if len(i.Include) > 0 {
		sb.WriteString(" INCLUDE (")
		sb.WriteString(strings.Join(i.Include, ", "))
		sb.WriteString(")")
	}

	// Where clause for filtered index
	if i.Where != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(i.Where)
	}

	sb.WriteString(";\n")

	return sb.String()
}

func (i *ModelDBIndex) createMariaDBDDL() string {
	var sb strings.Builder

	sb.WriteString("CREATE ")
	if i.IsUnique {
		sb.WriteString("UNIQUE ")
	}
	sb.WriteString("INDEX ")
	sb.WriteString(i.Name)
	sb.WriteString(" ON ")
	sb.WriteString(i.GetOwnerName())

	// Method (MySQL/MariaDB supports BTREE and HASH for some storage engines)
	if i.Method != "" && i.Method != ModelDBIndexMethodBTree {
		sb.WriteString(" USING ")
		sb.WriteString(string(i.Method))
	}

	// Columns
	sb.WriteString(" (")
	var cols []string
	for _, col := range i.Columns {
		colStr := col.Name
		if col.Order != "" {
			colStr += " " + col.Order
		}
		cols = append(cols, colStr)
	}
	sb.WriteString(strings.Join(cols, ", "))
	sb.WriteString(")")

	sb.WriteString(";\n")

	return sb.String()
}

func (i *ModelDBIndex) createOracleDDL() string {
	var sb strings.Builder

	sb.WriteString("CREATE ")
	if i.IsUnique {
		sb.WriteString("UNIQUE ")
	}
	sb.WriteString("INDEX ")
	sb.WriteString(i.FullName())
	sb.WriteString(" ON ")
	sb.WriteString(i.GetOwnerName())

	// Columns
	sb.WriteString(" (")
	var cols []string
	for _, col := range i.Columns {
		colStr := col.Name
		if col.Order != "" {
			colStr += " " + col.Order
		}
		if col.NullsOrder != "" {
			colStr += " " + col.NullsOrder
		}
		cols = append(cols, colStr)
	}
	sb.WriteString(strings.Join(cols, ", "))
	sb.WriteString(")")

	// Tablespace
	if i.Tablespace != "" {
		sb.WriteString(" TABLESPACE ")
		sb.WriteString(i.Tablespace)
	}

	sb.WriteString(";\n")

	return sb.String()
}
