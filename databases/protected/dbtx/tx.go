package dbtx

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"

	"dxlib/v3/databases/protected/db"
	"dxlib/v3/log"
	"dxlib/v3/utils"
)

type TxCallback func(tx *sqlx.Tx, log *log.DXLog) (err error)

func Tx(log *log.DXLog, db *sqlx.DB, isolationLevel sql.IsolationLevel, callback TxCallback) (err error) {
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
		log.Errorf(`ErrorInCallback: (%v)`, err.Error())
		errTx := tx.Rollback()
		if errTx != nil {
			log.Errorf(`ErrorInRollback: (%v)`, errTx.Error())
		}
		return err
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf(`ErrorInCommit: (%v)`, err.Error())
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
				log.Errorf(`ErrorInRollback (%v)`, errTx.Error())
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
				log.Errorf(`ErrorInRollback: (%v)`, errTx.Error())
			}
		}
		return nil, err
	}
	return r, nil
}

func TxNamedQueryIdBigMustExist(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, query string, args any) (int64, error) {
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
				log.Errorf(`ErrorInRollback: (%v)`, errTx.Error())
			}
			return 0, err
		}
	} else {
		err := errors.New(`QueryReturnEmpty`)
		errTx := tx.Rollback()
		if errTx != nil {
			log.Errorf(`ErrorInRollback: (%v)`, errTx.Error())
		}
		return 0, err
	}
	return returningId, nil
}

func TxNamedQueryRows(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, query string, arg any) (r []utils.JSON, err error) {
	rows, err := tx.NamedQuery(query, arg)
	if err != nil {
		if autoRollback {
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(`ErrorInRollback: (%v)`, errTx.Error())
			}
		}
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(`ErrorInRollback: (%v)`, errTx.Error())
			}
			return nil, err
		}
		r = append(r, rowJSON)
	}
	return r, nil
}

func TxNamedQueryRow(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, query string, arg any) (r utils.JSON, err error) {
	rows, err := TxNamedQuery(log, autoRollback, tx, query, arg)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	for rows.Next() {
		rowJSON := make(utils.JSON)
		err = rows.MapScan(rowJSON)
		if err != nil {
			errTx := tx.Rollback()
			if errTx != nil {
				log.Errorf(`ErrorInRollback: (%v)`, errTx.Error())
			}
			return nil, err
		}
		return rowJSON, nil
	}
	return nil, nil
}

func TxNamedQueryRowMustExist(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, query string, arg any) (r utils.JSON, err error) {
	row, err := TxNamedQueryRow(log, autoRollback, tx, query, arg)
	if err != nil {
		return nil, err
	}
	if row == nil {
		err := errors.New(`QueryRowResultMustExist`)
		errTx := tx.Rollback()
		if errTx != nil {
			log.Errorf(`ErrorInRollback: (%v)`, errTx.Error())
		}
		return nil, err
	}
	return row, err
}

func TxSelectWhereKeyValuesRows(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections map[string]string, forUpdatePart any) (r []utils.JSON, err error) {
	s, err := db.SQLPartConstructSelect(tableName, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, nil, forUpdatePart)
	if err != nil {
		return nil, err
	}
	wKV := db.ExcludeSQLExpression(whereAndFieldNameValues)
	r, err = TxNamedQueryRows(log, autoRollback, tx, s, wKV)
	return r, err
}

func TxSelectOneMustExist(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any, orderbyFieldNameDirections map[string]string, forUpdatePart any) (r utils.JSON,
	err error) {
	s, err := db.SQLPartConstructSelect(tableName, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, 1, forUpdatePart)
	if err != nil {
		err := fmt.Errorf(`%s:%s`, err, tableName)
		return nil, err
	}
	wKV := db.ExcludeSQLExpression(whereAndFieldNameValues)
	r, err = TxNamedQueryRowMustExist(log, autoRollback, tx, s, wKV)
	if err != nil {
		err := fmt.Errorf(`%s:%s`, err, tableName)
		return nil, err
	}
	return r, err
}

func TxSelectOne(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, fieldNames []string, whereAndFieldNameValues utils.JSON, joinSQLPart any,
	orderbyFieldNameDirections map[string]string, forUpdatePart any) (r utils.JSON, err error) {
	s, err := db.SQLPartConstructSelect(tableName, fieldNames, whereAndFieldNameValues, joinSQLPart, orderbyFieldNameDirections, 1, forUpdatePart)
	if err != nil {
		return nil, err
	}
	wKV := db.ExcludeSQLExpression(whereAndFieldNameValues)
	r, err = TxNamedQueryRow(log, autoRollback, tx, s, wKV)
	return r, err
}

func TxInsert(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, keyValues utils.JSON) (id int64, err error) {
	fn, fv := db.SQLPartInsertFieldNamesFieldValues(keyValues)
	s := ``
	switch tx.DriverName() {
	case "postgres":
		s = `INSERT INTO ` + tableName + ` (` + fn + `) VALUES (` + fv + `) RETURNING id`
	case "sqlserver":
		s = `INSERT INTO ` + tableName + ` (` + fn + `) OUTPUT INSERTED.id VALUES (` + fv + `)`
	default:
		fmt.Println("Unknown database type. Using Postgresql Dialect")
		s = `INSERT INTO ` + tableName + ` (` + fn + `) values (` + fv + `) returning id`
	}
	//s := `insert into ` + tableName + ` (` + fn + `) values (` + fv + `) returning id`
	kv := db.ExcludeSQLExpression(keyValues)
	id, err = TxNamedQueryIdBigMustExist(log, autoRollback, tx, s, kv)
	return id, err
}

func TxUpdateWhereKeyValues(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, setKeyValues utils.JSON, whereKeyValues utils.JSON) (result sql.Result, err error) {
	u := db.SQLPartSetFieldNameValues(setKeyValues)
	w := db.SQLPartWhereAndFieldNameValues(whereKeyValues)
	joinedKeyValues := db.MergeMapExcludeSQLExpression(setKeyValues, whereKeyValues)
	s := `update ` + tableName + ` set ` + u + ` where ` + w

	result, err = TxNamedExec(log, autoRollback, tx, s, joinedKeyValues)
	return result, err
}

func TxUpdateOne(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, setKeyValues utils.JSON, whereKeyValues utils.JSON) (result utils.JSON, err error) {
	u := db.SQLPartSetFieldNameValues(setKeyValues)
	w := db.SQLPartWhereAndFieldNameValues(whereKeyValues)
	joinedKeyValues := db.MergeMapExcludeSQLExpression(setKeyValues, whereKeyValues)
	s := `update ` + tableName + ` set ` + u + ` where ` + w + ` returning *`

	result, err = TxNamedQueryRow(log, autoRollback, tx, s, joinedKeyValues)
	return result, err
}

func TxDeleteWhereKeyValues(log *log.DXLog, autoRollback bool, tx *sqlx.Tx, tableName string, whereAndFieldNameValues utils.JSON) (r sql.Result, err error) {
	w := db.SQLPartWhereAndFieldNameValues(whereAndFieldNameValues)
	s := `delete from ` + tableName + ` where ` + w
	wKV := db.ExcludeSQLExpression(whereAndFieldNameValues)
	r, err = TxNamedExec(log, autoRollback, tx, s, wKV)
	return r, err
}
