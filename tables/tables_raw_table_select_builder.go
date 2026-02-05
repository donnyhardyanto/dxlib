package tables

import (
	"strings"

	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	tableQueryBuilder "github.com/donnyhardyanto/dxlib/tables/query_builder"
	"github.com/donnyhardyanto/dxlib/utils"
)

// SelectWithBuilder returns multiple rows using TableSelectQueryBuilder for safe SQL construction
// This is the preferred method over Select() when using JOIN, GROUP BY, HAVING, or ORDER BY clauses
// Note: If tqb.OrderByDefs is populated (via OrderBy/OrderByAsc/OrderByDesc), it takes precedence over orderBy parameter
// Note: For tables with encryption, this falls back to standard Select with the built where clause
func (t *DXRawTable) SelectWithBuilder(l *log.DXLog, fieldNames []string, tqb *tableQueryBuilder.TableSelectQueryBuilder,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {

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

	// Build JOIN clause (safely constructed)
	joinClause, err := tqb.BuildJoinClause()
	if err != nil {
		return nil, nil, err
	}

	// Build HAVING clause
	havingClauseStr, havingArgs, err := tqb.BuildHavingClause()
	if err != nil {
		return nil, nil, err
	}

	// Build ORDER BY - if QueryBuilder has OrderByDefs, use those instead of the parameter
	// This allows using the fluent OrderBy/OrderByAsc/OrderByDesc methods
	effectiveOrderBy := orderBy
	if len(tqb.OrderByDefs) > 0 {
		// Convert OrderByDefs to DXDatabaseTableFieldsOrderBy format
		// Note: NULLS FIRST/LAST is handled differently - we'll use raw ORDER BY clause
		effectiveOrderBy = db.DXDatabaseTableFieldsOrderBy{}
		for _, o := range tqb.OrderByDefs {
			direction := strings.ToUpper(o.Direction)
			if o.NullPlacement != "" {
				// Append NULLS placement to direction for proper SQL generation
				direction += " NULLS " + strings.ToUpper(o.NullPlacement)
			}
			effectiveOrderBy[o.FieldName] = direction
		}
	}

	// Create where JSON with raw where clause if we have conditions
	var where utils.JSON
	if whereClause != "" {
		// Use SQLExpression for the raw where clause
		where = utils.JSON{}
		// Add the raw WHERE condition using proper struct
		where["__sql_where__"] = db.SQLExpression{Expression: "(" + whereClause + ")"}
		// Add args for parameter binding
		for k, v := range whereArgs {
			where[k] = v
		}
	} else if len(whereArgs) > 0 {
		where = whereArgs
	}

	// Join clause as any (string or nil)
	var joinPart any
	if joinClause != "" {
		joinPart = joinClause
	}

	// Group by fields
	var groupByFields []string
	if len(tqb.GroupByFields) > 0 {
		groupByFields = tqb.GroupByFields
	}

	// Having clause as utils.JSON
	var havingJSON utils.JSON
	if havingClauseStr != "" {
		havingJSON = utils.JSON{}
		// Strip "HAVING " prefix since the db layer adds it
		havingCondition := havingClauseStr
		if len(havingCondition) > 7 && havingCondition[:7] == "HAVING " {
			havingCondition = havingCondition[7:]
		}
		havingJSON["__sql_having__"] = db.SQLExpression{Expression: havingCondition}
		for k, v := range havingArgs {
			havingJSON[k] = v
		}
	}

	// Handle encryption paths - fall back to standard Select with built where clause
	if len(t.EncryptionColumnDefs) > 0 {
		encryptionColumns := t.convertEncryptionColumnDefsForSelect()
		return t.SelectWithEncryption(l, fieldNames, encryptionColumns, where, joinPart, effectiveOrderBy, limit, forUpdatePart)
	}

	if len(t.EncryptionKeyDefs) > 0 {
		dtx, err := t.Database.TransactionBegin(databases.LevelReadCommitted)
		if err != nil {
			return nil, nil, err
		}
		defer dtx.Finish(l, err)
		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return nil, nil, err
		}
		return dtx.Select(t.GetListViewName(), t.FieldTypeMapping, fieldNames, where, joinPart, groupByFields, havingJSON, effectiveOrderBy, limit, nil, forUpdatePart)
	}

	// Call database with safely built clauses
	return t.Database.Select(t.GetListViewName(), t.FieldTypeMapping, fieldNames, where, joinPart, groupByFields, havingJSON, effectiveOrderBy, limit, nil, forUpdatePart)
}

// SelectOneWithBuilder returns a single row using TableSelectQueryBuilder
func (t *DXRawTable) SelectOneWithBuilder(l *log.DXLog, fieldNames []string, tqb *tableQueryBuilder.TableSelectQueryBuilder,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {

	rowsInfo, rows, err := t.SelectWithBuilder(l, fieldNames, tqb, orderBy, 1, nil)
	if err != nil {
		return nil, nil, err
	}

	if len(rows) == 0 {
		return rowsInfo, nil, nil
	}

	return rowsInfo, rows[0], nil
}

// ShouldSelectOneWithBuilder returns a single row or error if not found
func (t *DXRawTable) ShouldSelectOneWithBuilder(l *log.DXLog, fieldNames []string, tqb *tableQueryBuilder.TableSelectQueryBuilder,
	orderBy db.DXDatabaseTableFieldsOrderBy) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {

	rowsInfo, row, err := t.SelectOneWithBuilder(l, fieldNames, tqb, orderBy)
	if err != nil {
		return nil, nil, err
	}

	if row == nil {
		return nil, nil, errors.New("RECORD_NOT_FOUND")
	}

	return rowsInfo, row, nil
}

// TxSelectWithBuilder returns multiple rows within a transaction using TableSelectQueryBuilder
// Note: If tqb.OrderByDefs is populated (via OrderBy/OrderByAsc/OrderByDesc), it takes precedence over orderBy parameter
func (t *DXRawTable) TxSelectWithBuilder(dtx *databases.DXDatabaseTx, fieldNames []string, tqb *tableQueryBuilder.TableSelectQueryBuilder,
	orderBy db.DXDatabaseTableFieldsOrderBy, limit any, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, []utils.JSON, error) {

	// Check for errors accumulated in QueryBuilder
	if tqb.Error != nil {
		return nil, nil, tqb.Error
	}

	// Build WHERE clause
	whereClause, whereArgs, err := tqb.Build()
	if err != nil {
		return nil, nil, err
	}

	// Build JOIN clause
	joinClause, err := tqb.BuildJoinClause()
	if err != nil {
		return nil, nil, err
	}

	// Build HAVING clause
	havingClauseStr, havingArgs, err := tqb.BuildHavingClause()
	if err != nil {
		return nil, nil, err
	}

	// Build ORDER BY - if QueryBuilder has OrderByDefs, use those instead of the parameter
	effectiveOrderBy := orderBy
	if len(tqb.OrderByDefs) > 0 {
		effectiveOrderBy = db.DXDatabaseTableFieldsOrderBy{}
		for _, o := range tqb.OrderByDefs {
			direction := strings.ToUpper(o.Direction)
			if o.NullPlacement != "" {
				direction += " NULLS " + strings.ToUpper(o.NullPlacement)
			}
			effectiveOrderBy[o.FieldName] = direction
		}
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

	var joinPart any
	if joinClause != "" {
		joinPart = joinClause
	}

	var groupByFields []string
	if len(tqb.GroupByFields) > 0 {
		groupByFields = tqb.GroupByFields
	}

	var havingJSON utils.JSON
	if havingClauseStr != "" {
		havingJSON = utils.JSON{}
		havingCondition := havingClauseStr
		if len(havingCondition) > 7 && havingCondition[:7] == "HAVING " {
			havingCondition = havingCondition[7:]
		}
		havingJSON["__sql_having__"] = db.SQLExpression{Expression: havingCondition}
		for k, v := range havingArgs {
			havingJSON[k] = v
		}
	}

	return dtx.Select(t.GetListViewName(), t.FieldTypeMapping, fieldNames, where, joinPart, groupByFields, havingJSON, effectiveOrderBy, limit, nil, forUpdatePart)
}

// TxSelectOneWithBuilder returns a single row within a transaction using TableSelectQueryBuilder
func (t *DXRawTable) TxSelectOneWithBuilder(dtx *databases.DXDatabaseTx, fieldNames []string, tqb *tableQueryBuilder.TableSelectQueryBuilder,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {

	rowsInfo, rows, err := t.TxSelectWithBuilder(dtx, fieldNames, tqb, orderBy, 1, forUpdatePart)
	if err != nil {
		return nil, nil, err
	}

	if len(rows) == 0 {
		return rowsInfo, nil, nil
	}

	return rowsInfo, rows[0], nil
}

// TxShouldSelectOneWithBuilder returns a single row or error if not found within a transaction
func (t *DXRawTable) TxShouldSelectOneWithBuilder(dtx *databases.DXDatabaseTx, fieldNames []string, tqb *tableQueryBuilder.TableSelectQueryBuilder,
	orderBy db.DXDatabaseTableFieldsOrderBy, forUpdatePart any) (*db.DXDatabaseTableRowsInfo, utils.JSON, error) {

	rowsInfo, row, err := t.TxSelectOneWithBuilder(dtx, fieldNames, tqb, orderBy, forUpdatePart)
	if err != nil {
		return nil, nil, err
	}

	if row == nil {
		return nil, nil, errors.New("RECORD_NOT_FOUND")
	}

	return rowsInfo, row, nil
}

// CountWithBuilder returns total row count using TableSelectQueryBuilder
func (t *DXRawTable) CountWithBuilder(l *log.DXLog, tqb *tableQueryBuilder.TableSelectQueryBuilder) (int64, error) {
	if err := t.EnsureDatabase(); err != nil {
		return 0, err
	}

	// Check for errors accumulated in QueryBuilder
	if tqb.Error != nil {
		return 0, tqb.Error
	}

	// Build WHERE clause
	whereClause, whereArgs, err := tqb.Build()
	if err != nil {
		return 0, err
	}

	// Build JOIN clause
	joinClause, err := tqb.BuildJoinClause()
	if err != nil {
		return 0, err
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

	var joinPart any
	if joinClause != "" {
		joinPart = joinClause
	}

	if len(t.EncryptionKeyDefs) > 0 || len(t.EncryptionColumnDefs) > 0 {
		dtx, err := t.Database.TransactionBegin(databases.LevelReadCommitted)
		if err != nil {
			return 0, err
		}
		defer dtx.Finish(l, err)
		if err := t.TxSetAllEncryptionSessionKeys(dtx); err != nil {
			return 0, err
		}
		return dtx.Count(t.GetListViewName(), where, joinPart, nil, nil)
	}

	return t.Database.Count(t.GetListViewName(), where, joinPart)
}
