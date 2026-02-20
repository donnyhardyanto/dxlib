package tables

import (
	"database/sql"

	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/utils"
)

// TxDeleteAuto deletes rows using table's EncryptionColumnDefs within a transaction
// For delete operations, encryption handling is typically not needed for the where clause,
// but this provides a consistent API pattern with other Auto methods
func (t *DXRawTable) TxDeleteAuto(
	dtx *databases.DXDatabaseTx,
	where utils.JSON,
	returningFieldNames []string,
) (sql.Result, []utils.JSON, error) {
	// For delete operations, we don't need special encryption handling
	// since we're just matching on the where clause
	return t.TxDelete(dtx, where, returningFieldNames)
}

// TxHardDeleteAuto is a wrapper for TxDeleteAuto on DXTable
// It provides a consistent API for hard delete operations
func (t *DXTable) TxHardDeleteAuto(dtx *databases.DXDatabaseTx, where utils.JSON) (sql.Result, error) {
	result, _, err := t.DXRawTable.TxDeleteAuto(dtx, where, nil)
	return result, err
}

// HardDeleteAuto deletes rows permanently (non-transaction version)
func (t *DXTable) HardDeleteAuto(where utils.JSON) (sql.Result, error) {
	result, _, err := t.DXRawTable.Delete(nil, where, nil)
	return result, err
}
