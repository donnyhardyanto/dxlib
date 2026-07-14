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

// qualifiedOwnerName returns the index's owner table, schema-qualified + quoted
// for EXECUTED DDL (per engine; MariaDB virtual-schema single identifier).
func (i *ModelDBIndex) qualifiedOwnerName(dbType base.DXDatabaseType) string {
	if i.OwnerTable != nil {
		schema := ""
		if i.OwnerTable.Schema != nil {
			schema = i.OwnerTable.Schema.Name
		}
		return qualifiedTableName(dbType, schema, i.OwnerTable.TableName())
	}
	if i.OwnerMaterializedView != nil {
		return i.OwnerMaterializedView.FullName()
	}
	return ""
}

// quotedColumns returns the index's column list, each name quoted per engine,
// preserving any per-column Order / NullsOrder.
func (i *ModelDBIndex) quotedColumns(dbType base.DXDatabaseType, withNulls bool) string {
	cols := make([]string, 0, len(i.Columns))
	for _, col := range i.Columns {
		s := quoteIdent(dbType, col.Name)
		if col.Order != "" {
			s += " " + col.Order
		}
		if withNulls && col.NullsOrder != "" {
			s += " " + col.NullsOrder
		}
		cols = append(cols, s)
	}
	return strings.Join(cols, ", ")
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
	sb.WriteString(quoteIdent(base.DXDatabaseTypePostgreSQL, i.Name))
	sb.WriteString(" ON ")
	sb.WriteString(i.qualifiedOwnerName(base.DXDatabaseTypePostgreSQL))

	// Method
	if i.Method != "" && i.Method != ModelDBIndexMethodBTree {
		sb.WriteString(" USING ")
		sb.WriteString(string(i.Method))
	}

	// Columns
	sb.WriteString(" (")
	sb.WriteString(i.quotedColumns(base.DXDatabaseTypePostgreSQL, true))
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
	sb.WriteString(quoteIdent(base.DXDatabaseTypeSQLServer, i.Name))
	sb.WriteString(" ON ")
	sb.WriteString(i.qualifiedOwnerName(base.DXDatabaseTypeSQLServer))

	// Columns
	sb.WriteString(" (")
	sb.WriteString(i.quotedColumns(base.DXDatabaseTypeSQLServer, false))
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
	sb.WriteString(quoteIdent(base.DXDatabaseTypeMariaDB, i.Name))
	sb.WriteString(" ON ")
	sb.WriteString(i.qualifiedOwnerName(base.DXDatabaseTypeMariaDB))

	// Method (MySQL/MariaDB supports BTREE and HASH for some storage engines)
	if i.Method != "" && i.Method != ModelDBIndexMethodBTree {
		sb.WriteString(" USING ")
		sb.WriteString(string(i.Method))
	}

	// Columns
	sb.WriteString(" (")
	sb.WriteString(i.quotedColumns(base.DXDatabaseTypeMariaDB, false))
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
	// Index names are NOT schema-qualified on Oracle (ORA-00953) — bare (quoted).
	sb.WriteString(quoteIdent(base.DXDatabaseTypeOracle, i.Name))
	sb.WriteString(" ON ")
	sb.WriteString(i.qualifiedOwnerName(base.DXDatabaseTypeOracle))

	// Columns
	sb.WriteString(" (")
	sb.WriteString(i.quotedColumns(base.DXDatabaseTypeOracle, true))
	sb.WriteString(")")

	// Tablespace
	if i.Tablespace != "" {
		sb.WriteString(" TABLESPACE ")
		sb.WriteString(i.Tablespace)
	}

	sb.WriteString(";\n")

	return sb.String()
}
