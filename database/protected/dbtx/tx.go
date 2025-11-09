package dbtx

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/donnyhardyanto/dxlib/database/sqlchecker"
	utils2 "github.com/donnyhardyanto/dxlib/database2/utils"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"

	"github.com/donnyhardyanto/dxlib/database/protected/db"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
)

type TxCallback func(tx *sqlx.Tx, log *log.DXLog) (err error)

func Tx(log *log.DXLog, db *sqlx.DB, isolationLevel sql.IsolationLevel, callback TxCallback) (err error) {
	driverName := db.DriverName()
	switch driverName {
	case "oracle":
		tx, err := db.BeginTxx(log.Context, &sql.TxOptions{
			Isolation: isolationLevel,
			ReadOnly:  false,
		})
		if err != nil {
			log.Error("error occurred", err)
			return err
		}
		err = callback(tx, log)
		if err != nil {
			log.Errorf(err, "TX_ERROR_IN_CALLBACK: (%+v)", err)
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(errTx, "SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%+v)", errTx)
			}
			return err
		}
		err = tx.Commit()
		if err != nil {
			log.Errorf(err, "TX_ERROR_IN_COMMITT: (%+v)", err)
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(errTx, "ErrorInCommitRollback: (%+v)", errTx)
			}
			return err
		}

		return nil
	}
	tx, err := db.BeginTxx(log.Context, &sql.TxOptions{
		Isolation: isolationLevel,
		ReadOnly:  false,
	})
	if err != nil {
		log.Error(err.Error(), err)
		return errors.Wrap(err, "error occurred")
	}
	err = callback(tx, log)
	if err != nil {
		log.Errorf(err, "TX_ERROR_IN_CALLBACK: (%v)", err.Error())
		errTx := tx.Rollback()
		if errTx != nil {
			log.Errorf(errTx, "SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)", errTx.Error())
		}
		return errors.Wrap(err, "error occurred")
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf(err, "TX_ERROR_IN_COMMITT: (%v)", err.Error())
		errTx := tx.Rollback()
		if errTx != nil {
			log.Errorf(errTx, "ErrorInCommitRollback: (%v)", errTx.Error())
		}
		return errors.Wrap(err, "error occurred")
	}

	return nil
}

func TxNamedQuery(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, query string, args any) (rows *sqlx.Rows, err error) {
	err = sqlchecker.CheckAll(tx.DriverName(), query, args)
	if err != nil {
		return nil, errors.Errorf("SQL_INJECTION_DETECTED:VALIDATION_FAILED: %+v", err)
	}

	rows, err = tx.NamedQuery(query, args)
	if err != nil {
		if autoRollback {
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(errTx, "SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)", errTx.Error())
			}
		}
		return nil, err
	}
	return rows, nil
}

func TxNamedExec(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, query string, args any) (r sql.Result, err error) {
	err = sqlchecker.CheckAll(tx.DriverName(), query, args)
	if err != nil {
		return nil, errors.Errorf("SQL_INJECTION_DETECTED:VALIDATION_FAILED: %+v", err)
	}

	r, err = tx.NamedExec(query, args)
	if err != nil {
		if autoRollback {
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(errTx, "SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)", errTx.Error())
			}
		}
		return nil, err
	}
	return r, nil
}

func TxShouldNamedQueryIdBig(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, query string, args any) (int64, error) {
	rows, err := TxNamedQuery(log, autoRollback, tx, query, args)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var returningId int64
	if rows.Next() {
		err := rows.Scan(&returningId)
		if err != nil {
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(errTx, "SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)", errTx.Error())
			}
			return 0, err
		}
	} else {
		err := errors.New("NO_ID_RETURNED:" + query)
		errTx := tx.Rollback()
		if errTx != nil {
			log.Errorf(errTx, "SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)", errTx.Error())
		}
		return 0, err
	}
	return returningId, nil
}

func TxNamedQueryRows(log *log.DXLog, fieldTypeMapping db.FieldTypeMapping, autoRollback bool, tx *sqlx.Tx, query string, arg any) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {
	rows, err := TxNamedQuery(log, autoRollback, tx, query, arg)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	rowsInfo = &db.RowsInfo{}
	rowsInfo.Columns, err = rows.Columns()
	if err != nil {
		return nil, r, err
	}
	//rowsInfo.ColumnTypes, err = rows.ColumnTypes()
	//if err != nil {
	//	return rowsInfo, r, err
	//}
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(errTx, "SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)", errTx.Error())
			}
			return nil, nil, err
		}
		rowJSON, err = db.DeformatKeys(rowJSON, tx.DriverName(), fieldTypeMapping)
		if err != nil {
			return nil, nil, err
		}
		r = append(r, rowJSON)
	}

	return rowsInfo, r, nil
}

func TxNamedQueryRow(log *log.DXLog, fieldTypeMapping db.FieldTypeMapping, autoRollback bool, tx *sqlx.Tx, query string, arg any) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rows, err := TxNamedQuery(log, autoRollback, tx, query, arg)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	rowsInfo = &db.RowsInfo{}
	rowsInfo.Columns, err = rows.Columns()
	if err != nil {
		return nil, r, err
	}
	//rowsInfo.ColumnTypes, err = rows.ColumnTypes()
	//if err != nil {
	//	return rowsInfo, r, err
	//}
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(errTx, "SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)", errTx.Error())
			}
			return rowsInfo, nil, err
		}
		rowJSON, err = db.DeformatKeys(rowJSON, tx.DriverName(), fieldTypeMapping)
		if err != nil {
			return nil, nil, err
		}
		return rowsInfo, rowJSON, nil
	}

	return rowsInfo, nil, nil
}

func TxShouldNamedQueryRow(log *log.DXLog, fieldTypeMapping db.FieldTypeMapping, autoRollback bool, tx *sqlx.Tx, query string, arg any) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, row, err := TxNamedQueryRow(log, fieldTypeMapping, autoRollback, tx, query, arg)
	if err != nil {
		return rowsInfo, row, err
	}
	if row == nil {
		err := errors.New("ROW_MUST_EXIST:" + query)
		errTx := tx.Rollback()
		if errTx != nil {
			log.Errorf(errTx, "SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)", errTx.Error())
		}
		return rowsInfo, nil, err
	}
	return rowsInfo, row, err
}

/*func TxSelectWhereKeyValuesRows(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderGyFieldNameDirections db.FieldsOrderBy, forUpdatePart any) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {
	driverName := tx.DriverName()
	s, err := db.SQLPartConstructSelect(driverName, tableName, fieldNames, whereAndFieldNameValues, joinSQLPart, orderGyFieldNameDirections, nil, forUpdatePart)
	if err != nil {
		return nil, nil, err
	}
	wKV := db.ExcludeSQLExpression(whereAndFieldNameValues, driverName)
	rowsInfo, r, err = TxNamedQueryRows(log, autoRollback, tx, s, wKV)
	return rowsInfo, r, err
}*/

func TxShouldSelectOne(log *log.DXLog, fieldTypeMapping db.FieldTypeMapping, autoRollback bool, tx *sqlx.Tx, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderByFieldNameDirections db.FieldsOrderBy, forUpdatePart any) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	driverName := tx.DriverName()
	s, err := db.SQLPartConstructSelect(driverName, tableName, fieldNames, whereAndFieldNameValues, joinSQLPart, orderByFieldNameDirections, 1, forUpdatePart)
	if err != nil {
		err := errors.Errorf("%s:%s", err, tableName)
		return rowsInfo, nil, err
	}
	wKV, err := db.ExcludeSQLExpression(whereAndFieldNameValues, driverName)
	if err != nil {
		return nil, nil, err
	}

	rowsInfo, r, err = TxShouldNamedQueryRow(log, fieldTypeMapping, autoRollback, tx, s, wKV)
	if err != nil {
		err := errors.Errorf("%s:%s", err, tableName)
		return rowsInfo, nil, err
	}
	return rowsInfo, r, err
}

func TxSelect(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, fieldTypeMapping db.FieldTypeMapping, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderByFieldNameDirections db.FieldsOrderBy, limit any, forUpdatePart any) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {
	driverName := tx.DriverName()
	s, err := db.SQLPartConstructSelect(driverName, tableName, fieldNames, whereAndFieldNameValues, joinSQLPart, orderByFieldNameDirections, limit, forUpdatePart)
	if err != nil {
		return nil, nil, err
	}
	wKV, err := db.ExcludeSQLExpression(whereAndFieldNameValues, driverName)
	if err != nil {
		return nil, nil, err
	}

	rowsInfo, r, err = TxNamedQueryRows(log, fieldTypeMapping, autoRollback, tx, s, wKV)
	return rowsInfo, r, err
}

func TxSelectOne(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, fieldTypeMapping db.FieldTypeMapping, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderByFieldNameDirections db.FieldsOrderBy, forUpdatePart any) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	driverName := tx.DriverName()
	s, err := db.SQLPartConstructSelect(driverName, tableName, fieldNames, whereAndFieldNameValues, joinSQLPart, orderByFieldNameDirections, 1, forUpdatePart)
	if err != nil {
		return nil, nil, err
	}
	wKV, err := db.ExcludeSQLExpression(whereAndFieldNameValues, driverName)
	if err != nil {
		return nil, nil, err
	}

	rowsInfo, r, err = TxNamedQueryRow(log, fieldTypeMapping, autoRollback, tx, s, wKV)
	return rowsInfo, r, err
}

func OracleTxInsertReturning(tx *sqlx.Tx, tableName string, fieldNameForRowId string, keyValues map[string]interface{}) (int64, error) {
	tableName = strings.ToUpper(tableName)
	fieldNameForRowId = strings.ToUpper(fieldNameForRowId)
	returningClause := fmt.Sprintf("RETURNING %s INTO :new_id", fieldNameForRowId)

	fieldNames, fieldValues, fieldArgs := utils2.PrepareArrayArgs(keyValues, tx.DriverName())

	query := "INSERT INTO" + " " + fmt.Sprintf("%s(%s) VALUES( % s) %s", tableName, fieldNames, fieldValues, returningClause)
	stmt, err := tx.Prepare(query)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = stmt.Close()
	}()

	// Add the returning parameter
	newId := int64(99)
	fieldArgs = append(fieldArgs, sql.Named("new_id", sql.Out{Dest: &newId}))

	err = sqlchecker.CheckAll(tx.DriverName(), query, fieldArgs)
	if err != nil {
		return 0, errors.Errorf("SQL_INJECTION_DETECTED:VALIDATION_FAILED: %+v", err)
	}

	// Execute the statement
	_, err = stmt.Exec(fieldArgs...)
	if err != nil {
		return 0, err
	}

	return newId, nil
}

func TxInsert(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, keyValues utils.JSON) (id int64, err error) {
	driverName := tx.DriverName()
	fn, fv := db.SQLPartInsertFieldNamesFieldValues(keyValues, driverName)
	s := ""
	switch driverName {
	case "postgres":
		s = "INSERT INTO" + " " + tableName + " (" + fn + ") VALUES (" + fv + ") RETURNING id"
	case "sqlserver":
		s = "INSERT INTO" + " " + tableName + " (" + fn + ") OUTPUT INSERTED.id VALUES (" + fv + ")"
	case "oracle":
		id, err = OracleTxInsertReturning(tx, tableName, "id", keyValues)
		if err != nil {
			return 0, err
		}
		return id, nil
	default:
		fmt.Println("Unknown database type. Using Postgresql Dialect")
		s = "INSERT INTO" + " " + tableName + " (" + fn + ") values (" + fv + ") returning id"
	}
	kv, err := db.ExcludeSQLExpression(keyValues, driverName)
	if err != nil {
		return 0, err
	}

	id, err = TxShouldNamedQueryIdBig(log, autoRollback, tx, s, kv)
	return id, err
}

func TxUpdate(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, setKeyValues utils.JSON, whereKeyValues utils.JSON) (result sql.Result, err error) {
	driveName := tx.DriverName()
	setKeyValues, u := db.SQLPartSetFieldNameValues(setKeyValues, driveName)
	w := db.SQLPartWhereAndFieldNameValues(whereKeyValues, driveName)
	joinedKeyValues := db.MergeMapExcludeSQLExpression(setKeyValues, whereKeyValues, driveName)
	driverName := tx.DriverName()
	var s string
	switch driverName {
	case "postgres":
		s = "update " + tableName + " set " + u + " where " + w
	case "sqlserver":
		s = "update " + tableName + " set " + u + " where " + w
	case "oracle":
		return nil, errors.New("unknown database type, using Postgresql Dialect")
	default:
		return nil, errors.New("unknown database type, using Postgresql Dialect")
	}
	result, err = TxNamedExec(log, autoRollback, tx, s, joinedKeyValues)
	return result, err
}

func TxDelete(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, whereAndFieldNameValues utils.JSON) (r sql.Result, err error) {
	if whereAndFieldNameValues == nil {
		whereAndFieldNameValues = utils.JSON{}
	}

	driverName := tx.DriverName()
	w := db.SQLPartWhereAndFieldNameValues(whereAndFieldNameValues, driverName)
	s := "delete from" + " " + tableName + " where " + w
	wKV, err := db.ExcludeSQLExpression(whereAndFieldNameValues, driverName)
	if err != nil {
		return nil, err
	}

	r, err = TxNamedExec(log, autoRollback, tx, s, wKV)
	return r, err
}
