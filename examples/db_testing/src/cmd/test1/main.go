package main

import (
	"database/sql"

	"github.com/donnyhardyanto/dxlib/app"
	"github.com/donnyhardyanto/dxlib/configuration"
	"github.com/donnyhardyanto/dxlib/database"
	"github.com/donnyhardyanto/dxlib/database/utils"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/tables"
	utilsOs "github.com/donnyhardyanto/dxlib/utils/os"
)

const (
	ConfirmationKey1 = "sudah makan?"
	ConfirmationKey2 = "pluto"
)

func doOnDefineConfiguration() (err error) {
	createScriptFileFolder := utilsOs.GetEnvDefaultValue("CREATE_SCRIPT_FILE_FOLDER", "./../src/sql")
	configuration.Manager.NewIfNotExistConfiguration("storage", "storage.json", "json", false, false, map[string]any{
		"postgresql-system": map[string]any{
			"nameid":              "postgresql-system",
			"database_type":       utilsOs.GetEnvDefaultValue("DB_SYSTEM_DATABASE_TYPE", "postgres"),
			"address":             utilsOs.GetEnvDefaultValue("DB_SYSTEM_ADDRESS", "127.0.0.1:5432"),
			"user_name":           utilsOs.GetEnvDefaultValue("DB_SYSTEM_USER_NAME", "postgres"),
			"user_password":       utilsOs.GetEnvDefaultValue("DB_SYSTEM_USER_PASSWORD", "postgres"),
			"database_name":       utilsOs.GetEnvDefaultValue("DB_SYSTEM_DATABASE_NAME", "postgres"),
			"connection_options":  "sslmode=disable",
			"must_connected":      true,
			"is_connect_at_start": true,
		},
		"p1": map[string]any{
			"nameid":              "p1",
			"database_type":       utilsOs.GetEnvDefaultValue("DB_SYSTEM_DATABASE_TYPE", "postgres"),
			"address":             utilsOs.GetEnvDefaultValue("DB_SYSTEM_ADDRESS", "127.0.0.1:5432"),
			"user_name":           utilsOs.GetEnvDefaultValue("DB_SYSTEM_USER_NAME", "postgres"),
			"user_password":       utilsOs.GetEnvDefaultValue("DB_SYSTEM_USER_PASSWORD", "postgres"),
			"database_name":       utilsOs.GetEnvDefaultValue("DB_SYSTEM_DATABASE_NAME", "test1"),
			"connection_options":  "sslmode=disable",
			"must_connected":      true,
			"is_connect_at_start": true,
			"create_script_files": []string{
				createScriptFileFolder + "/postgresql/test1-ddl.sql",
			},
		},
		"sqlserver-system": map[string]any{
			"nameid":              "sqlserver-system",
			"database_type":       utilsOs.GetEnvDefaultValue("DB_SYSTEM_DATABASE_TYPE", "sqlserver"),
			"address":             utilsOs.GetEnvDefaultValue("DB_SYSTEM_ADDRESS", "127.0.0.1:1433"),
			"user_name":           utilsOs.GetEnvDefaultValue("DB_SYSTEM_USER_NAME", "sa"),
			"user_password":       utilsOs.GetEnvDefaultValue("DB_SYSTEM_USER_PASSWORD", "12345678_Sa"),
			"database_name":       utilsOs.GetEnvDefaultValue("DB_SYSTEM_DATABASE_NAME", "master"),
			"connection_options":  "sslmode=disable",
			"must_connected":      true,
			"is_connect_at_start": true,
		},
		"p2": map[string]any{
			"nameid":              "p2",
			"database_type":       utilsOs.GetEnvDefaultValue("DB_SYSTEM_DATABASE_TYPE", "sqlserver"),
			"address":             utilsOs.GetEnvDefaultValue("DB_SYSTEM_ADDRESS", "127.0.0.1:1433"),
			"user_name":           utilsOs.GetEnvDefaultValue("DB_SYSTEM_USER_NAME", "sa"),
			"user_password":       utilsOs.GetEnvDefaultValue("DB_SYSTEM_USER_PASSWORD", "12345678_Sa"),
			"database_name":       utilsOs.GetEnvDefaultValue("DB_SYSTEM_DATABASE_NAME", "test1"),
			"connection_options":  "sslmode=disable",
			"must_connected":      true,
			"is_connect_at_start": true,
			"create_script_files": []string{
				createScriptFileFolder + "/sqlserver/test1-ddl.sql",
			},
		},
		"oracle-system": map[string]any{
			"nameid":              "oracle-system",
			"database_type":       utilsOs.GetEnvDefaultValue("DB_SYSTEM_DATABASE_TYPE", "oracle"),
			"address":             utilsOs.GetEnvDefaultValue("DB_SYSTEM_ADDRESS", "127.0.0.1:1521"),
			"user_name":           utilsOs.GetEnvDefaultValue("DB_SYSTEM_USER_NAME", "system"),
			"user_password":       utilsOs.GetEnvDefaultValue("DB_SYSTEM_USER_PASSWORD", "oraORAora1000"),
			"database_name":       utilsOs.GetEnvDefaultValue("DB_SYSTEM_DATABASE_NAME", "FREEPDB1"),
			"connection_options":  "sslmode=disable",
			"must_connected":      true,
			"is_connect_at_start": true,
		},
		"p3": map[string]any{
			"nameid":              "p3",
			"database_type":       utilsOs.GetEnvDefaultValue("DB_SYSTEM_DATABASE_TYPE", "oracle"),
			"address":             utilsOs.GetEnvDefaultValue("DB_SYSTEM_ADDRESS", "127.0.0.1:1521"),
			"user_name":           utilsOs.GetEnvDefaultValue("DB_SYSTEM_USER_NAME", "SYSTEM"),
			"user_password":       utilsOs.GetEnvDefaultValue("DB_SYSTEM_USER_PASSWORD", "oraORAora1000"),
			"database_name":       utilsOs.GetEnvDefaultValue("DB_SYSTEM_DATABASE_NAME", "FREEPDB1"),
			"connection_options":  "sslmode=disable",
			"must_connected":      true,
			"is_connect_at_start": false,
			"create_script_files": []string{
				createScriptFileFolder + "/oracle/test1-ddl.sql",
			},
		},
	}, []string{"postgres.user_name", "postgres.user_password"})
	return nil

}

func testTableFunction(db *database.DXDatabase) (err error) {
	var dtx *database.DXDatabaseTx
	dtx, err = db.TransactionBegin(sql.LevelReadCommitted)
	if err != nil {
		return err
	}

	defer dtx.Finish(&log.Log, err)

	var aId int64
	_, returningData, err := dtx.Insert("test1.test1_table", map[string]any{
		"name":  "abc",
		"at":    "2024-01-10 15:16:17.001+07:00",
		"is_ok": true,
	}, []string{"id"})
	if err != nil {
		return err
	}
	aId = returningData["id"].(int64)

	log.Log.Infof("Update result aId: %v", aId)

	table1 := tables.NewDXTableSimple(db.NameId, "test1.test1_table", "test1.test1_table", "test1.test1_table",
		"id", "", "", "data", nil)

	_, returningData2, err := table1.TxInsert(dtx, map[string]any{
		"name":  "zfx",
		"at":    "2024-01-10 15:16:17.001+07:00",
		"is_ok": false,
	}, []string{"id"})
	if err != nil {
		return err
	}
	aId2 := returningData2["id"].(int64)

	log.Log.Infof("Update result aId2: %v", aId2)

	var r sql.Result
	r, _, err = table1.TxUpdate(dtx, map[string]any{
		"name": "bc1",
	}, map[string]any{
		"id": aId,
	}, nil)
	if err != nil {
		return err
	}

	_, err = table1.TxHardDelete(dtx, map[string]any{
		"id": aId2,
	})
	log.Log.Infof("Update result r after update: %v", r)

	return nil
}

func doOnAfterConfigurationStartAll() (err error) {

	log.Log.Warn("Executing wipe... START")

	/*
	   Example to execute stored procedure
	   var dbP1 *database.DXDatabase

	   	dbP1 := database.Manager.Databases["p1"]
	   	defer dbP1.Disconnect()

	   	err = dbP1.Connect()
	   	if err != nil {
	   		log.Log.Errorf("Failed to connect to database %s: %s", dbP1.DatabaseName, err.Error())
	   		return err
	   	}
	   	_, err = dbP1.Execute("EXEC inv.CreateTransaction @stagTransactionId=1", nil)
	   	if err != nil {
	   		return err
	   	}*/

	dbP1 := database.Manager.Databases["p1"]
	dbSystem := database.Manager.Databases["postgresql-system"]

	err = dbP1.Connect()
	if err != nil {
		log.Log.Errorf(err, "Failed to connect to database %s: %s", dbP1.DatabaseName, err.Error())
		return err
	}

	_ = utils.DropDatabase(dbSystem.Connection, dbP1.DatabaseName)
	_ = utils.CreateDatabase(dbSystem.Connection, dbP1.DatabaseName)

	_, err = dbP1.ExecuteCreateScripts()

	if err != nil {
		log.Log.Errorf(err, "Failed to connect/execute to database %s: %s", dbP1.DatabaseName, err.Error())
	}

	err = testTableFunction(dbP1)
	if err != nil {
		return err
	}
	/*var dtx1 *database.DXDatabaseTx
	dtx1, err = dbP1.TransactionBegin(sql.LevelReadCommitted)
	if err != nil {
		return err
	}
	{
		defer dtx1.Finish(&log.Log, err)

		var aId int64
		aId, err = dbtx.TxInsert(&log.Log, true, dtx1.Tx, "test1.test1_table", map[string]any{
			"name":  "abc",
			"at":    "2024-01-10 15:16:17.001+07:00",
			"is_ok": true,
		})
		if err != nil {
			return err
		}

		tableTable1 := table.Manager.NewTable(dbP1.NameId, "test1.test1_table", "test1",
			"test1_table", "id", "id")
		var r map[string]any
		r, err = tableTable1.TxUpdate(dtx1, map[string]any{
			"name": "bc1",
		}, map[string]any{
			"id": aId,
		})
		if err != nil {
			return err
		}

		log.Log.Infof("Update result: %v", r)
	}*/

	dbP2 := database.Manager.Databases["p2"]
	dbP2System := database.Manager.Databases["sqlserver-system"]

	err = dbP2.Connect()
	if err != nil {
		log.Log.Errorf(err, "Failed to connect to database %s: %s", dbP2.DatabaseName, err.Error())
		return err
	}

	_ = utils.DropDatabase(dbP2System.Connection, dbP2.DatabaseName)
	_ = utils.CreateDatabase(dbP2System.Connection, dbP2.DatabaseName)

	_, err = dbP2.ExecuteCreateScripts()
	if err != nil {
		log.Log.Errorf(err, "Failed to connect/execute to database %s: %s", dbP2.DatabaseName, err.Error())
	}

	err = testTableFunction(dbP2)
	if err != nil {
		return err
	}
	/*
		var dtx2 *database.DXDatabaseTx
		dtx2, err = dbP2.TransactionBegin(sql.LevelReadCommitted)
		if err != nil {
			return err
		}
		{
			defer dtx2.Finish(&log.Log, err)

			var aId int64
			aId, err = dbtx.TxInsert(&log.Log, true, dtx2.Tx, "test1.test1_table", map[string]any{
				"name":  "abc",
				"at":    "2024-01-10 15:16:17.001+07:00",
				"is_ok": true,
			})
			if err != nil {
				return err
			}

			tableTable2 := table.Manager.NewTable(dbP2.NameId, "test1.test1_table", "test1",
				"test1_table", "id", "id")
			var r map[string]any
			r, err = tableTable2.TxUpdate(dtx2, map[string]any{
				"name": "bc1",
			}, map[string]any{
				"id": aId,
			})
			if err != nil {
				return err
			}

			log.Log.Infof("Update result: %v", r)
		}*/

	dbP3 := database.Manager.Databases["p3"]

	dbP3System := database.Manager.Databases["oracle-system"]

	err = dbP3.Connect()
	if err != nil {
		log.Log.Errorf(err, "Failed to connect to database %s: %s", dbP2.DatabaseName, err.Error())
		return err
	}

	_ = utils.DropDatabase(dbP3System.Connection, dbP3.DatabaseName)
	_ = utils.CreateDatabase(dbP3System.Connection, dbP3.DatabaseName)

	_, err = dbP3.ExecuteCreateScripts()

	if err != nil {
		log.Log.Errorf(err, "Failed to connect/execute to database %s: %s", dbP3.DatabaseName, err.Error())
	}

	log.Log.Warn("Executing wipe... DONE")
	return nil
}

func main() {
	log.SetFormatText()
	app.Set("dxlib-test1-reset",
		"DxLib Test1 Reset CLI",
		"DxLib Test1 Reset CLI",
		false,
		"dxlib-test1-reset-debug",
		"abc",
	)
	app.App.OnDefineConfiguration = doOnDefineConfiguration
	app.App.OnAfterConfigurationStartAll = doOnAfterConfigurationStartAll
	_ = app.App.Run()
}
