package tables

import (
	"database/sql"

	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

// UpdateWithBuilder executes an UPDATE using TableQueryBuilder for safe SQL construction
// The TableQueryBuilder provides safe WHERE clause building and RETURNING field validation
func (t *DXRawTable) UpdateWithBuilder(l *log.DXLog, setFieldNameValues utils.JSON, tqb *TableQueryBuilder) (sql.Result, []utils.JSON, error) {
	if err := t.EnsureDatabase(); err != nil {
		return nil, nil, err
	}

	// Check for errors accumulated in QueryBuilder
	if tqb.Error != nil {
		return nil, nil, tqb.Error
	}

	// Build WHERE clause from QueryBuilder conditions
	whereClause, whereArgs, err := tqb.Build()
	if err != nil {
		return nil, nil, err
	}

	// Build RETURNING clause (with validated field names)
	returningClause, err := tqb.BuildReturningClause()
	if err != nil {
		return nil, nil, err
	}

	// Create where JSON with raw where clause if we have conditions
	var where utils.JSON
	if whereClause != "" {
		where = utils.JSON{}
		where["__sql_where__"] = db.SQLExpression{Expression: "(" + whereClause + ")"}
		for k, v := range whereArgs {
			where[k] = v
		}
	} else if len(whereArgs) > 0 {
		where = whereArgs
	}

	// Get returning field names from QueryBuilder
	var returningFields []string
	if returningClause != "" {
		returningFields = tqb.OutFields
	}

	// Handle encryption paths
	if len(t.EncryptionKeyDefs) > 0 || len(t.EncryptionColumnDefs) > 0 {
		dtx, err := t.Database.TransactionBegin(databases.LevelReadCommitted)
		if err != nil {
			return nil, nil, err
		}
		defer dtx.Finish(l, err)
		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, nil, err
		}
		return dtx.Update(t.GetFullTableName(), setFieldNameValues, where, returningFields)
	}

	return t.Database.Update(t.GetFullTableName(), setFieldNameValues, where, returningFields)
}

// TxUpdateWithBuilder executes an UPDATE within a transaction using TableQueryBuilder
func (t *DXRawTable) TxUpdateWithBuilder(dtx *databases.DXDatabaseTx, setFieldNameValues utils.JSON, tqb *TableQueryBuilder) (sql.Result, []utils.JSON, error) {
	// Check for errors accumulated in QueryBuilder
	if tqb.Error != nil {
		return nil, nil, tqb.Error
	}

	// Build WHERE clause from QueryBuilder conditions
	whereClause, whereArgs, err := tqb.Build()
	if err != nil {
		return nil, nil, err
	}

	// Build RETURNING clause (validates field names)
	returningClause, err := tqb.BuildReturningClause()
	if err != nil {
		return nil, nil, err
	}

	// Create where JSON
	var where utils.JSON
	if whereClause != "" {
		where = utils.JSON{}
		where["__sql_where__"] = db.SQLExpression{Expression: "(" + whereClause + ")"}
		for k, v := range whereArgs {
			where[k] = v
		}
	} else if len(whereArgs) > 0 {
		where = whereArgs
	}

	// Get returning field names from QueryBuilder
	var returningFields []string
	if returningClause != "" {
		returningFields = tqb.OutFields
	}

	return dtx.Update(t.GetFullTableName(), setFieldNameValues, where, returningFields)
}

// UpdateByIdWithBuilder executes an UPDATE by ID using TableQueryBuilder for RETURNING
func (t *DXRawTable) UpdateByIdWithBuilder(l *log.DXLog, id int64, setFieldNameValues utils.JSON, tqb *TableQueryBuilder) (sql.Result, []utils.JSON, error) {
	// Add ID condition to the QueryBuilder
	// Note: We don't use tqb.Eq because the ID field might not be in SearchTextFieldNames
	tqb.Conditions = append(tqb.Conditions, t.FieldNameForRowId+" = :__update_id__")
	tqb.Args["__update_id__"] = id

	return t.UpdateWithBuilder(l, setFieldNameValues, tqb)
}

// TxUpdateByIdWithBuilder executes an UPDATE by ID within a transaction using TableQueryBuilder
func (t *DXRawTable) TxUpdateByIdWithBuilder(dtx *databases.DXDatabaseTx, id int64, setFieldNameValues utils.JSON, tqb *TableQueryBuilder) (sql.Result, []utils.JSON, error) {
	// Add ID condition to the QueryBuilder
	tqb.Conditions = append(tqb.Conditions, t.FieldNameForRowId+" = :__update_id__")
	tqb.Args["__update_id__"] = id

	return t.TxUpdateWithBuilder(dtx, setFieldNameValues, tqb)
}
