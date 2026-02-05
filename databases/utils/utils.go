package utils

import (
	"database/sql"
	"fmt"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/jmoiron/sqlx"
	"strings"
)

func FormatIdentifier(identifier string, driverName string) string {
	// Convert the identifier to lowercase as the base case
	formattedIdentifier := strings.ToLower(identifier)

	// Apply databases-specific formatting
	switch driverName {
	case "oracle", "db2":
		formattedIdentifier = strings.ToUpper(formattedIdentifier)
		return formattedIdentifier
	default:
		// do nothing
	}

	// Wrap the identifier in quotes to preserve case in the SQL statement
	return `"` + formattedIdentifier + `"`
}

func PrepareArrayArgs(keyValues map[string]any, driverName string) (fieldNames string, fieldValues string, fieldArgs []any) {
	for k, v := range keyValues {
		if fieldNames != "" {
			fieldNames += ", "
			fieldValues += ", "
		}

		fieldName := FormatIdentifier(k, driverName)
		fieldNames += fieldName
		fieldValues += ":" + fieldName

		var s sql.NamedArg
		switch v.(type) {
		case bool:
			switch driverName {
			case "oracle", "sqlserver":
				if v.(bool) == true {
					keyValues[k] = 1
				} else {
					keyValues[k] = 0
				}

			default:
			}

		default:
		}
		s = sql.Named(fieldName, keyValues[k])
		fieldArgs = append(fieldArgs, s)
	}

	return fieldNames, fieldValues, fieldArgs
}

// Function to kill all connections to a specific databases

func KillConnections(db *sqlx.DB, dbName string) (err error) {
	driverName := db.DriverName()
	switch driverName {
	case "postgres":
		query := fmt.Sprintf(`
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = '%s'
          AND pid <> pg_backend_pid();
    `, dbName)
		_, err = db.Exec(query)
		if err != nil {
			return errors.Errorf("failed to kill connections: %+v", err)
		}
	case "sqlserver":
		query := fmt.Sprintf(`
            USE master;
            DECLARE @kill varchar(8000) = '';
            SELECT @kill = @kill + 'kill ' + CONVERT(varchar(5), session_id) + ';'
            FROM sys.dm_exec_sessions
            WHERE database_id = DB_ID('%s')
              AND session_id != @@SPID;
            EXEC(@kill);
        `, dbName)
		_, err = db.Exec(query)
		if err != nil {
			return errors.Errorf("failed to kill connections: %+v", err)
		}
	case "godror", "oracle":
		// For Oracle, we use ALTER SYSTEM KILL SESSION
		query := `
            BEGIN
                FOR s IN (SELECT sid, serial# FROM v$session WHERE username = UPPER(:1))
                LOOP
                    EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || s.sid || ',' || s.serial# || ''' IMMEDIATE';
                END LOOP;
            END;
        `
		_, err = db.Exec(query, dbName)
		if err != nil {
			return errors.Errorf("failed to kill connections: %+v", err)
		}
	case "mysql":
		query := fmt.Sprintf(`
            SELECT CONCAT('KILL ', id, ';')
            FROM information_schema.processlist
            WHERE db = '%s'
              AND id != CONNECTION_ID();
        `, dbName)
		rows, err := db.Query(query)
		if err != nil {
			return errors.Errorf("failed to get connections: %+v", err)
		}
		defer rows.Close()
		for rows.Next() {
			var killStmt string
			if err := rows.Scan(&killStmt); err != nil {
				continue
			}
			_, _ = db.Exec(killStmt)
		}
	default:
		return errors.Errorf("unsupported databases driver: %s", driverName)
	}

	return nil
}

func DropDatabase(db *sqlx.DB, dbName string) (err error) {
	defer func() {
		if err != nil {
			log.Log.Warnf("Error dropping databases %s: %s", dbName, err.Error())
		}
	}()

	driverName := db.DriverName()

	// Kill all connections to the target databases
	err = KillConnections(db, dbName)
	if err != nil {
		log.Log.Errorf(err, "Failed to kill connections")
		return err
	}

	var query string
	switch driverName {
	case "postgres":
		query = fmt.Sprintf(`DROP DATABASE IF EXISTS "%s"`, dbName)
	case "sqlserver":
		query = fmt.Sprintf(`
            IF EXISTS (SELECT name FROM sys.databases WHERE name = N'%s')
            BEGIN
                ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [%s];
            END
        `, dbName, dbName, dbName)
	case "godror", "oracle":
		// Oracle doesn't support DROP DATABASE. Instead, we'll drop all objects in the schema.
		query = fmt.Sprintf(`
            BEGIN
                FOR obj IN (SELECT object_name, object_type FROM all_objects WHERE owner = UPPER('%s'))
                LOOP
                    IF obj.object_type = 'TABLE' THEN
                        EXECUTE IMMEDIATE 'DROP ' || obj.object_type || ' "' || UPPER('%s') || '"."' || obj.object_name || '" CASCADE CONSTRAINTS';
                    ELSE
                        EXECUTE IMMEDIATE 'DROP ' || obj.object_type || ' "' || UPPER('%s') || '"."' || obj.object_name || '"';
                    END IF;
                END LOOP;
            END;
        `, dbName, dbName, dbName)
	case "mysql":
		query = fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName)
	default:
		return errors.Errorf("unsupported databases driver: %s", driverName)
	}

	_, err = db.Exec(query)
	if err != nil {
		return errors.Errorf("failed to drop databases: %+v", err)
	}

	return nil
}

func CreateDatabase(db *sqlx.DB, dbName string) error {
	driverName := db.DriverName()

	var query string
	switch driverName {
	case "postgres":
		query = fmt.Sprintf(`CREATE DATABASE "%s"`, dbName)
	case "sqlserver":
		query = fmt.Sprintf(`CREATE DATABASE [%s]`, dbName)
	case "godror", "oracle":
		// In Oracle, we create a user (schema) instead of a databases
		// Note: You may want to replace 'identified by password' with a more secure method
		query = fmt.Sprintf(`
            BEGIN
                EXECUTE IMMEDIATE 'CREATE USER %s IDENTIFIED BY "TemporaryPassword123!"';
                EXECUTE IMMEDIATE 'GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW TO %s';
                EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO %s';
            END;
        `, dbName, dbName, dbName)
	case "mysql":
		query = fmt.Sprintf("CREATE DATABASE `%s`", dbName)
	default:
		return errors.Errorf("unsupported databases driver: %s", driverName)
	}

	_, err := db.Exec(query)
	if err != nil {
		return errors.Errorf("failed to create databases/user: %+v", err)
	}

	return nil

}
