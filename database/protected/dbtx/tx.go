package dbtx

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"

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
			log.Error(err.Error())
			return err
		}
		err = callback(tx, log)
		if err != nil {
			log.Errorf(`TX_ERROR_IN_CALLBACK: (%v)`, err.Error())
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(`SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)`, errTx.Error())
			}
			return err
		}
		err = tx.Commit()
		if err != nil {
			log.Errorf(`TX_ERROR_IN_COMMITT: (%v)`, err.Error())
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(`ErrorInCommitRollback: (%v)`, errTx.Error())
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
		log.Error(err.Error())
		return err
	}
	err = callback(tx, log)
	if err != nil {
		log.Errorf(`TX_ERROR_IN_CALLBACK: (%v)`, err.Error())
		errTx := tx.Rollback()
		if errTx != nil {
			log.Errorf(`SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)`, errTx.Error())
		}
		return err
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf(`TX_ERROR_IN_COMMITT: (%v)`, err.Error())
		errTx := tx.Rollback()
		if errTx != nil {
			log.Errorf(`ErrorInCommitRollback: (%v)`, errTx.Error())
		}
		return err
	}

	return nil
}

func TxNamedQuery(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, query string, args any) (rows *sqlx.Rows, err error) {
	rows, err = tx.NamedQuery(query, args)
	if err != nil {
		if autoRollback {
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(`SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)`, errTx.Error())
			}
		}
		return nil, err
	}
	return rows, nil
}

func TxNamedExec(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, query string, args any) (r sql.Result, err error) {
	r, err = tx.NamedExec(query, args)
	if err != nil {
		if autoRollback {
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(`SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)`, errTx.Error())
			}
		}
		return nil, err
	}
	return r, nil
}

func TxMustNamedQueryIdBig(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, query string, args any) (int64, error) {
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
				log.Errorf(`SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)`, errTx.Error())
			}
			return 0, err
		}
	} else {
		err := errors.New(`NO_ID_RETURNED:` + query)
		errTx := tx.Rollback()
		if errTx != nil {
			log.Errorf(`SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)`, errTx.Error())
		}
		return 0, err
	}
	return returningId, nil
}

func TxNamedQueryRows(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, query string, arg any) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {
	rows, err := tx.NamedQuery(query, arg)
	if err != nil {
		if autoRollback {
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(`SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)`, errTx.Error())
			}
		}
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
	rowsInfo.ColumnTypes, err = rows.ColumnTypes()
	if err != nil {
		return rowsInfo, r, err
	}
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(`SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)`, errTx.Error())
			}
			return nil, nil, err
		}
		rowJSON = db.DeformatKeys(rowJSON, tx.DriverName())
		r = append(r, rowJSON)
	}

	return rowsInfo, r, nil
}

func TxNamedQueryRow(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, query string, arg any) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
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
	rowsInfo.ColumnTypes, err = rows.ColumnTypes()
	if err != nil {
		return rowsInfo, r, err
	}
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(`SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)`, errTx.Error())
			}
			return rowsInfo, nil, err
		}
		rowJSON = db.DeformatKeys(rowJSON, tx.DriverName())
		return rowsInfo, rowJSON, nil
	}

	return rowsInfo, nil, nil
}

func TxMustNamedQueryRow(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, query string, arg any) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	rowsInfo, row, err := TxNamedQueryRow(log, autoRollback, tx, query, arg)
	if err != nil {
		return rowsInfo, row, err
	}
	if row == nil {
		err := errors.New(`ROW_MUST_EXIST:` + query)
		errTx := tx.Rollback()
		if errTx != nil {
			log.Errorf(`SHOULD_NOT_HAPPEN:ERROR_IN_ROLLBACK(%v)`, errTx.Error())
		}
		return rowsInfo, nil, err
	}
	return rowsInfo, row, err
}

func TxSelectWhereKeyValuesRows(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections map[string]string, forUpdatePart any) (rowsInfo *db.RowsInfo, r []utils.JSON, err error) {
	driverName := tx.DriverName()
	s, err := db.SQLPartConstructSelect(driverName, tableName, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, nil, forUpdatePart)
	if err != nil {
		return nil, nil, err
	}
	wKV := db.ExcludeSQLExpression(whereAndFieldNameValues, driverName)
	rowsInfo, r, err = TxNamedQueryRows(log, autoRollback, tx, s, wKV)
	return rowsInfo, r, err
}

func TxMustSelectOne(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections map[string]string, forUpdatePart any) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	driverName := tx.DriverName()
	s, err := db.SQLPartConstructSelect(driverName, tableName, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, 1, forUpdatePart)
	if err != nil {
		err := fmt.Errorf(`%s:%s`, err, tableName)
		return rowsInfo, nil, err
	}
	wKV := db.ExcludeSQLExpression(whereAndFieldNameValues, driverName)
	rowsInfo, r, err = TxMustNamedQueryRow(log, autoRollback, tx, s, wKV)
	if err != nil {
		err := fmt.Errorf(`%s:%s`, err, tableName)
		return rowsInfo, nil, err
	}
	return rowsInfo, r, err
}

func TxSelectOne(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections map[string]string, forUpdatePart any) (rowsInfo *db.RowsInfo, r utils.JSON, err error) {
	driverName := tx.DriverName()
	s, err := db.SQLPartConstructSelect(driverName, tableName, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, 1, forUpdatePart)
	if err != nil {
		return nil, nil, err
	}
	wKV := db.ExcludeSQLExpression(whereAndFieldNameValues, driverName)
	rowsInfo, r, err = TxNamedQueryRow(log, autoRollback, tx, s, wKV)
	return rowsInfo, r, err
}

func TxInsert(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, keyValues utils.JSON) (id int64, err error) {
	driverName := tx.DriverName()
	fn, fv := db.SQLPartInsertFieldNamesFieldValues(keyValues, driverName)
	s := ``
	switch driverName {
	case "postgres":
		s = `INSERT INTO ` + tableName + ` (` + fn + `) VALUES (` + fv + `) RETURNING id`
	case "sqlserver":
		s = `INSERT INTO ` + tableName + ` (` + fn + `) OUTPUT INSERTED.id VALUES (` + fv + `)`
	case "oracle":
		s = `INSERT INTO ` + tableName + ` (` + fn + `) VALUES (` + fv + `) RETURNING id`
	default:
		fmt.Println("Unknown database type. Using Postgresql Dialect")
		s = `INSERT INTO ` + tableName + ` (` + fn + `) values (` + fv + `) returning id`
	}
	kv := db.ExcludeSQLExpression(keyValues, driverName)
	id, err = TxMustNamedQueryIdBig(log, autoRollback, tx, s, kv)
	return id, err
}

func TxUpdateWhereKeyValues(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, setKeyValues utils.JSON, whereKeyValues utils.JSON) (result sql.Result, err error) {
	driveName := tx.DriverName()
	setKeyValues, u := db.SQLPartSetFieldNameValues(setKeyValues, driveName)
	w := db.SQLPartWhereAndFieldNameValues(whereKeyValues, driveName)
	joinedKeyValues := db.MergeMapExcludeSQLExpression(setKeyValues, whereKeyValues, driveName)
	s := `update ` + tableName + ` set ` + u + ` where ` + w

	result, err = TxNamedExec(log, autoRollback, tx, s, joinedKeyValues)
	return result, err
}

func TxUpdateOne(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, setKeyValues utils.JSON, whereKeyValues utils.JSON) (
	result utils.JSON, err error) {
	driveName := tx.DriverName()
	setKeyValues, u := db.SQLPartSetFieldNameValues(setKeyValues, driveName)
	w := db.SQLPartWhereAndFieldNameValues(whereKeyValues, driveName)
	joinedKeyValues := db.MergeMapExcludeSQLExpression(setKeyValues, whereKeyValues, driveName)
	s := `update ` + tableName + ` set ` + u + ` where ` + w + ` returning *`

	_, result, err = TxNamedQueryRow(log, autoRollback, tx, s, joinedKeyValues)
	return result, err
}

func TxDeleteWhereKeyValues(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, whereAndFieldNameValues utils.JSON) (r sql.Result, err error) {
	driverName := tx.DriverName()
	w := db.SQLPartWhereAndFieldNameValues(whereAndFieldNameValues, driverName)
	s := `delete from ` + tableName + ` where ` + w
	wKV := db.ExcludeSQLExpression(whereAndFieldNameValues, driverName)
	r, err = TxNamedExec(log, autoRollback, tx, s, wKV)
	return r, err
}
