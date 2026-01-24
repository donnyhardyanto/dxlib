package database3

import (
	"database/sql"

	"github.com/donnyhardyanto/dxlib/database2"
	"github.com/donnyhardyanto/dxlib/database2/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

// ============================================================================
// DXDatabase3 - Runtime database wrapper for database3
// ============================================================================

// DXDatabase3 wraps database2.DXDatabase for runtime operations
// It provides a cleaner interface and bridges database3 schema with runtime
type DXDatabase3 struct {
	NameId   string
	Database *database2.DXDatabase
}

// NewDXDatabase3 creates a new DXDatabase3 from an existing database2.DXDatabase
func NewDXDatabase3(d *database2.DXDatabase) *DXDatabase3 {
	return &DXDatabase3{
		NameId:   d.NameId,
		Database: d,
	}
}

// NewDXDatabase3ByNameId creates a new DXDatabase3 by looking up database by name
func NewDXDatabase3ByNameId(nameId string) *DXDatabase3 {
	return &DXDatabase3{
		NameId:   nameId,
		Database: nil, // Will be resolved on first use
	}
}

// EnsureConnection ensures database connection is ready
func (d *DXDatabase3) EnsureConnection() error {
	if d.Database == nil {
		d.Database = database2.Manager.Databases[d.NameId]
		if d.Database == nil {
			return errors.Errorf("database not found: %s", d.NameId)
		}
	}
	return d.Database.EnsureConnection()
}

// GetConnection returns the underlying sqlx.DB connection
func (d *DXDatabase3) GetConnection() error {
	return d.EnsureConnection()
}

// ============================================================================
// Transaction Support
// ============================================================================

// DXDatabaseTx3 wraps database2.DXDatabaseTx for transaction operations
type DXDatabaseTx3 struct {
	*database2.DXDatabaseTx
}

// TransactionBegin starts a new transaction
func (d *DXDatabase3) TransactionBegin(isolationLevel sql.IsolationLevel) (*DXDatabaseTx3, error) {
	if err := d.EnsureConnection(); err != nil {
		return nil, err
	}

	dtx, err := d.Database.TransactionBegin(isolationLevel)
	if err != nil {
		return nil, err
	}

	return &DXDatabaseTx3{DXDatabaseTx: dtx}, nil
}

// Tx executes a function within a transaction
func (d *DXDatabase3) Tx(l *log.DXLog, isolationLevel sql.IsolationLevel, fn func(tx *DXDatabaseTx3) error) error {
	tx, err := d.TransactionBegin(isolationLevel)
	if err != nil {
		return err
	}

	err = fn(tx)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

// ============================================================================
// CRUD Operations
// ============================================================================

// Insert inserts a row and returns the result
func (d *DXDatabase3) Insert(tableName string, setFieldValues utils.JSON, returningFieldNames []string) (sql.Result, utils.JSON, error) {
	if err := d.EnsureConnection(); err != nil {
		return nil, nil, err
	}
	return d.Database.Insert(tableName, setFieldValues, returningFieldNames)
}

// Update updates rows matching the where condition
func (d *DXDatabase3) Update(tableName string, setFieldValues utils.JSON, whereAndFieldNameValues utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	if err := d.EnsureConnection(); err != nil {
		return nil, nil, err
	}
	return d.Database.Update(tableName, setFieldValues, whereAndFieldNameValues, returningFieldNames)
}

// Delete deletes rows matching the where condition
func (d *DXDatabase3) Delete(tableName string, whereAndFieldNameValues utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	if err := d.EnsureConnection(); err != nil {
		return nil, nil, err
	}
	return d.Database.Delete(tableName, whereAndFieldNameValues, returningFieldNames)
}

// SelectOne selects a single row
func (d *DXDatabase3) SelectOne(tableName string, fieldTypeMapping db.DXDatabaseTableFieldTypeMapping, fieldNames []string,
	where utils.JSON, joinSQLPart any, groupBy []string, havingClause utils.JSON,
	orderBy db.DXDatabaseTableFieldsOrderBy, offset any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if err := d.EnsureConnection(); err != nil {
		return nil, nil, err
	}
	return d.Database.SelectOne(tableName, fieldTypeMapping, fieldNames, where, joinSQLPart, groupBy, havingClause, orderBy, offset, forUpdatePart)
}

// ShouldSelectOne selects a single row and returns error if not found
func (d *DXDatabase3) ShouldSelectOne(tableName string, fieldTypeMapping db.DXDatabaseTableFieldTypeMapping, fieldNames []string,
	where utils.JSON, joinSQLPart any, groupBy []string, havingClause utils.JSON,
	orderBy db.DXDatabaseTableFieldsOrderBy, offset any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	if err := d.EnsureConnection(); err != nil {
		return nil, nil, err
	}
	return d.Database.ShouldSelectOne(tableName, fieldTypeMapping, fieldNames, where, joinSQLPart, groupBy, havingClause, orderBy, offset, forUpdatePart)
}

// Select selects multiple rows
func (d *DXDatabase3) Select(tableName string, fieldTypeMapping db.DXDatabaseTableFieldTypeMapping, fieldNames []string,
	where utils.JSON, joinSQLPart any, groupBy []string, havingClause utils.JSON,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, offset any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	if err := d.EnsureConnection(); err != nil {
		return nil, nil, err
	}
	return d.Database.Select(tableName, fieldTypeMapping, fieldNames, where, joinSQLPart, groupBy, havingClause, orderBy, limit, offset, forUpdatePart)
}

// Count returns the count of rows
func (d *DXDatabase3) Count(tableName string, where utils.JSON, joinSQLPart any) (int64, error) {
	if err := d.EnsureConnection(); err != nil {
		return 0, err
	}
	return d.Database.Count(tableName, where, joinSQLPart)
}

// SelectPaging executes a paging query
func (d *DXDatabase3) SelectPaging(pageIndex int64, rowsPerPage int64, tableName string, fieldTypeMapping db.DXDatabaseTableFieldTypeMapping,
	fieldNames []string, where utils.JSON, joinSQLPart any, groupBy []string, havingClause utils.JSON,
	orderBy db.DXDatabaseTableFieldsOrderBy) (totalRowCount int64, rowsInfo *db.DXDatabaseTableRowsInfo, resultDataRows []utils.JSON, err error) {
	if err := d.EnsureConnection(); err != nil {
		return 0, nil, nil, err
	}
	return d.Database.SelectPaging(pageIndex, rowsPerPage, tableName, fieldTypeMapping, fieldNames, where, joinSQLPart, groupBy, havingClause, orderBy)
}

// ============================================================================
// Transaction CRUD Operations
// ============================================================================

// Insert inserts a row within transaction
func (tx *DXDatabaseTx3) Insert(tableName string, setFieldValues utils.JSON, returningFieldNames []string) (sql.Result, utils.JSON, error) {
	return tx.DXDatabaseTx.Insert(tableName, setFieldValues, returningFieldNames)
}

// Update updates rows within transaction
func (tx *DXDatabaseTx3) Update(tableName string, setFieldValues utils.JSON, whereAndFieldNameValues utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	return tx.DXDatabaseTx.Update(tableName, setFieldValues, whereAndFieldNameValues, returningFieldNames)
}

// Delete deletes rows within transaction
func (tx *DXDatabaseTx3) Delete(tableName string, whereAndFieldNameValues utils.JSON, returningFieldNames []string) (sql.Result, []utils.JSON, error) {
	return tx.DXDatabaseTx.TxDelete(tableName, whereAndFieldNameValues, returningFieldNames)
}

// SelectOne selects a single row within transaction
func (tx *DXDatabaseTx3) SelectOne(tableName string, fieldTypeMapping db.DXDatabaseTableFieldTypeMapping, fieldNames []string,
	where utils.JSON, joinSQLPart any, groupBy []string, havingClause utils.JSON,
	orderBy db.DXDatabaseTableFieldsOrderBy, offset any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return tx.DXDatabaseTx.SelectOne(tableName, fieldTypeMapping, fieldNames, where, joinSQLPart, groupBy, havingClause, orderBy, offset, forUpdatePart)
}

// ShouldSelectOne selects a single row within transaction and returns error if not found
func (tx *DXDatabaseTx3) ShouldSelectOne(tableName string, fieldTypeMapping db.DXDatabaseTableFieldTypeMapping, fieldNames []string,
	where utils.JSON, joinSQLPart any, groupBy []string, havingClause utils.JSON,
	orderBy db.DXDatabaseTableFieldsOrderBy, offset any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {
	return tx.DXDatabaseTx.ShouldSelectOne(tableName, fieldTypeMapping, fieldNames, where, joinSQLPart, groupBy, havingClause, orderBy, offset, forUpdatePart)
}

// Select selects multiple rows within transaction
func (tx *DXDatabaseTx3) Select(tableName string, fieldTypeMapping db.DXDatabaseTableFieldTypeMapping, fieldNames []string,
	where utils.JSON, joinSQLPart any, groupBy []string, havingClause utils.JSON,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, offset any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {
	return tx.DXDatabaseTx.Select(tableName, fieldTypeMapping, fieldNames, where, joinSQLPart, groupBy, havingClause, orderBy, limit, offset, forUpdatePart)
}

// Count returns the count of rows within transaction
func (tx *DXDatabaseTx3) Count(tableName string, where utils.JSON, joinSQLPart any, groupBy []string, havingClause utils.JSON) (int64, error) {
	return tx.DXDatabaseTx.Count(tableName, where, joinSQLPart, groupBy, havingClause)
}

// Finish commits if no error, rollbacks if error
func (tx *DXDatabaseTx3) Finish(l *log.DXLog, err error) {
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			l.Errorf(err, "ROLLBACK_ERROR:%+v", err2)
		}
	} else {
		err2 := tx.Commit()
		if err2 != nil {
			l.Errorf(err2, "COMMIT_ERROR:%+v", err2)
		}
	}
}

// ============================================================================
// Database3 Manager
// ============================================================================

// DXDatabase3Manager manages database3 instances
type DXDatabase3Manager struct {
	Databases map[string]*DXDatabase3
}

// Manager3 is the global database3 manager
var Manager3 = DXDatabase3Manager{
	Databases: make(map[string]*DXDatabase3),
}

// GetOrCreate gets an existing DXDatabase3 or creates a new one
func (m *DXDatabase3Manager) GetOrCreate(nameId string) *DXDatabase3 {
	if db, exists := m.Databases[nameId]; exists {
		return db
	}

	db := NewDXDatabase3ByNameId(nameId)
	m.Databases[nameId] = db
	return db
}

// Get gets an existing DXDatabase3
func (m *DXDatabase3Manager) Get(nameId string) *DXDatabase3 {
	return m.Databases[nameId]
}
