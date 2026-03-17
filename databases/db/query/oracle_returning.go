package query

import (
	"context"
	"database/sql"
	"strings"

	"github.com/donnyhardyanto/dxlib/databases"
	databaseDb "github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/databases/db/query/builder"
	"github.com/donnyhardyanto/dxlib/databases/db/query/named"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
)

// Oracle RETURNING support:
// - INSERT: Uses RETURNING ... INTO :bind with sql.Out for output parameters (single row).
//   Executes via RawExec/RawTxExec bypassing the named parameter conversion layer.
// - UPDATE/DELETE: Uses two-step SELECT-then-DML because Oracle RETURNING INTO only
//   supports single-row results. For UPDATE, returns pre-update values.
//   For DELETE, returns pre-delete values.

// oracleInsertWithReturningInto executes an Oracle INSERT with RETURNING INTO using sql.Out binds.
func oracleInsertWithReturningInto(ctx context.Context, db *sqlx.DB, qb *builder.InsertQueryBuilder) (sql.Result, utils.JSON, error) {
	// Build INSERT SQL without RETURNING (oracle case is no-op in buildInsertSQL)
	query, setArgs, err := buildInsertSQL("oracle", qb)
	if err != nil {
		return nil, nil, err
	}

	// Append RETURNING ... INTO ... clause for go-ora
	var returningCols []string
	var intoBinds []string
	for _, field := range qb.OutFields {
		returningCols = append(returningCols, field)
		intoBinds = append(intoBinds, ":__ret_"+field)
	}
	query += " RETURNING " + strings.Join(returningCols, ", ") + " INTO " + strings.Join(intoBinds, ", ")

	// Build args for go-ora: input params as sql.Named, output params as sql.Named with sql.Out
	// go-ora matches :param_name in SQL with sql.Named("param_name", value) args
	execArgs := make([]any, 0, len(setArgs)+len(qb.OutFields))
	for name, value := range setArgs {
		execArgs = append(execArgs, sql.Named(name, value))
	}

	// Output destinations: go-ora will populate these via RETURNING INTO
	// Using *any as destination type — go-ora determines the Go type from the Oracle column type
	// Prefix __ret_ avoids collision with SET field names
	destPtrs := make([]*any, len(qb.OutFields))
	for i, field := range qb.OutFields {
		dest := new(any)
		destPtrs[i] = dest
		execArgs = append(execArgs, sql.Named("__ret_"+field, sql.Out{Dest: dest}))
	}

	// Execute directly via RawExec (bypasses named parameter conversion layer)
	_, err = databaseDb.RawExec(ctx, db, query, execArgs)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "ORACLE_INSERT_WITH_RETURNING_INTO_ERROR")
	}

	// Build result row from output destinations
	row := utils.JSON{}
	for i, field := range qb.OutFields {
		row[field] = *destPtrs[i]
	}
	return nil, row, nil
}

// oracleTxInsertWithReturningInto executes an Oracle INSERT with RETURNING INTO within a transaction.
func oracleTxInsertWithReturningInto(ctx context.Context, dtx *databases.DXDatabaseTx, qb *builder.InsertQueryBuilder) (sql.Result, utils.JSON, error) {
	query, setArgs, err := buildInsertSQL("oracle", qb)
	if err != nil {
		return nil, nil, err
	}

	var returningCols []string
	var intoBinds []string
	for _, field := range qb.OutFields {
		returningCols = append(returningCols, field)
		intoBinds = append(intoBinds, ":__ret_"+field)
	}
	query += " RETURNING " + strings.Join(returningCols, ", ") + " INTO " + strings.Join(intoBinds, ", ")

	execArgs := make([]any, 0, len(setArgs)+len(qb.OutFields))
	for name, value := range setArgs {
		execArgs = append(execArgs, sql.Named(name, value))
	}

	destPtrs := make([]*any, len(qb.OutFields))
	for i, field := range qb.OutFields {
		dest := new(any)
		destPtrs[i] = dest
		execArgs = append(execArgs, sql.Named("__ret_"+field, sql.Out{Dest: dest}))
	}

	_, err = databaseDb.RawTxExec(ctx, dtx.Tx, query, execArgs)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "ORACLE_TX_INSERT_WITH_RETURNING_INTO_ERROR")
	}

	row := utils.JSON{}
	for i, field := range qb.OutFields {
		row[field] = *destPtrs[i]
	}
	return nil, row, nil
}

// oracleUpdateWithReturning executes an Oracle UPDATE with two-step SELECT-then-UPDATE.
// Returns pre-update values from the SELECT step.
func oracleUpdateWithReturning(ctx context.Context, db *sqlx.DB, qb *builder.UpdateQueryBuilder) (sql.Result, []utils.JSON, error) {
	whereClause, whereArgs, err := qb.BuildWhereClause()
	if err != nil {
		return nil, nil, err
	}

	// Step 1: SELECT outfields with same WHERE clause (pre-update values)
	selectQuery := "SELECT " + strings.Join(qb.OutFields, ", ") + " FROM " + qb.SourceName
	if whereClause != "" {
		selectQuery += " WHERE " + whereClause
	}
	_, rows, err := named.NamedQueryRows2(ctx, db, selectQuery, whereArgs, nil)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "ORACLE_UPDATE_PRE_SELECT_ERROR")
	}

	// Step 2: Execute UPDATE without RETURNING
	updateQuery, updateArgs, err := buildUpdateSQL("oracle", qb)
	if err != nil {
		return nil, nil, err
	}
	result, err := named.NamedExec2(ctx, db, updateQuery, updateArgs)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "ORACLE_UPDATE_ERROR")
	}

	return result, rows, nil
}

// oracleTxUpdateWithReturning executes an Oracle UPDATE within a transaction with two-step SELECT-then-UPDATE.
// Returns pre-update values from the SELECT step.
func oracleTxUpdateWithReturning(ctx context.Context, dtx *databases.DXDatabaseTx, qb *builder.UpdateQueryBuilder) (sql.Result, []utils.JSON, error) {
	whereClause, whereArgs, err := qb.BuildWhereClause()
	if err != nil {
		return nil, nil, err
	}

	selectQuery := "SELECT " + strings.Join(qb.OutFields, ", ") + " FROM " + qb.SourceName
	if whereClause != "" {
		selectQuery += " WHERE " + whereClause
	}
	_, rows, err := named.TxNamedQueryRows2(ctx, dtx, selectQuery, whereArgs, nil)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "ORACLE_TX_UPDATE_PRE_SELECT_ERROR")
	}

	updateQuery, updateArgs, err := buildUpdateSQL("oracle", qb)
	if err != nil {
		return nil, nil, err
	}
	result, err := named.TxNamedExec2(ctx, dtx, updateQuery, updateArgs)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "ORACLE_TX_UPDATE_ERROR")
	}

	return result, rows, nil
}

// oracleDeleteWithReturning executes an Oracle DELETE with two-step SELECT-then-DELETE.
// Returns pre-delete values from the SELECT step.
func oracleDeleteWithReturning(ctx context.Context, db *sqlx.DB, qb *builder.DeleteQueryBuilder) (sql.Result, []utils.JSON, error) {
	whereClause, whereArgs, err := qb.BuildWhereClause()
	if err != nil {
		return nil, nil, err
	}

	// Step 1: SELECT outfields before delete
	selectQuery := "SELECT " + strings.Join(qb.OutFields, ", ") + " FROM " + qb.SourceName
	if whereClause != "" {
		selectQuery += " WHERE " + whereClause
	}
	_, rows, err := named.NamedQueryRows2(ctx, db, selectQuery, whereArgs, nil)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "ORACLE_DELETE_PRE_SELECT_ERROR")
	}

	// Step 2: Execute DELETE without RETURNING
	deleteQuery, deleteArgs, err := buildDeleteSQL("oracle", qb)
	if err != nil {
		return nil, nil, err
	}
	result, err := named.NamedExec2(ctx, db, deleteQuery, deleteArgs)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "ORACLE_DELETE_ERROR")
	}

	return result, rows, nil
}

// oracleTxDeleteWithReturning executes an Oracle DELETE within a transaction with two-step SELECT-then-DELETE.
// Returns pre-delete values from the SELECT step.
func oracleTxDeleteWithReturning(ctx context.Context, dtx *databases.DXDatabaseTx, qb *builder.DeleteQueryBuilder) (sql.Result, []utils.JSON, error) {
	whereClause, whereArgs, err := qb.BuildWhereClause()
	if err != nil {
		return nil, nil, err
	}

	selectQuery := "SELECT " + strings.Join(qb.OutFields, ", ") + " FROM " + qb.SourceName
	if whereClause != "" {
		selectQuery += " WHERE " + whereClause
	}
	_, rows, err := named.TxNamedQueryRows2(ctx, dtx, selectQuery, whereArgs, nil)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "ORACLE_TX_DELETE_PRE_SELECT_ERROR")
	}

	deleteQuery, deleteArgs, err := buildDeleteSQL("oracle", qb)
	if err != nil {
		return nil, nil, err
	}
	result, err := named.TxNamedExec2(ctx, dtx, deleteQuery, deleteArgs)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "ORACLE_TX_DELETE_ERROR")
	}

	return result, rows, nil
}
