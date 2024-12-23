package utils

import (
	"database/sql"
	"fmt"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/jmoiron/sqlx"
	"strings"
)

type FieldTypeMapping map[string]string

func FormatIdentifier(identifier string, driverName string) string {
	// Convert the identifier to lowercase as the base case
	formattedIdentifier := strings.ToLower(identifier)

	// Apply database-specific formatting
	switch driverName {
	case "oracle", "db2":
		formattedIdentifier = strings.ToUpper(formattedIdentifier)
		return formattedIdentifier
	}

	// Wrap the identifier in quotes to preserve case in the SQL statement
	return `"` + formattedIdentifier + `"`
}

func DeformatIdentifier(identifier string, driverName string) string {
	// Remove the quotes from the identifier
	deformattedIdentifier := strings.Trim(identifier, `"`)
	deformattedIdentifier = strings.ToLower(deformattedIdentifier)
	return deformattedIdentifier
}

func DeformatKeys(kv map[string]interface{}, driverName string, fieldTypeMapping FieldTypeMapping) (r map[string]interface{}, err error) {
	r = map[string]interface{}{}
	for k, v := range kv {
		newKey := DeformatIdentifier(k, driverName)
		if fieldTypeMapping != nil {
			fieldValueType, isExist := fieldTypeMapping[newKey]
			if isExist {
				switch fieldValueType {
				case "array-string":
					v, err = utils.GetArrayFromV(v)
					if err != nil {
						return nil, err
					}
				}
			}
		}
		r[newKey] = v
	}
	return r, nil
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

// Function to kill all connections to a specific database

func KillConnections(db *sqlx.DB, dbName string) (err error) {
	driverName := db.DriverName()
	switch driverName {
	case `postgres`:
		query := fmt.Sprintf(`
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = '%s'
          AND pid <> pg_backend_pid();
    `, dbName)
		_, err = db.Exec(query)

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
	default:
		return fmt.Errorf("unsupported database driver: %s", driverName)
	}

	if err != nil {
		return fmt.Errorf("failed to kill connections: %w", err)
	}
	return nil
}

/*func DropDatabase(db *sqlx.DB, dbName string) (err error) {
	defer func() {
		if err != nil {
			log.Log.Warnf(`Error drop database %s:%s`, dbName, err.Error())
		}
	}()

	// Kill all connections to the target database
	err = KillConnections(db, dbName)
	if err != nil {
		log.Log.Errorf("Failed to kill connections: %s", err.Error())
		return err
	}

	query := fmt.Sprintf(`DROP DATABASE "%s"`, dbName)
	_, err = db.Exec(query)
	if err != nil {
		return err
	}

	return nil
}*/

func DropDatabase(db *sqlx.DB, dbName string) (err error) {
	defer func() {
		if err != nil {
			log.Log.Warnf(`Error dropping database %s: %s`, dbName, err.Error())
		}
	}()

	driverName := db.DriverName()

	// Kill all connections to the target database
	err = KillConnections(db, dbName)
	if err != nil {
		log.Log.Errorf("Failed to kill connections: %s", err.Error())
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
	default:
		return fmt.Errorf("unsupported database driver: %s", driverName)
	}

	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
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
		// In Oracle, we create a user (schema) instead of a database
		// Note: You may want to replace 'identified by password' with a more secure method
		query = fmt.Sprintf(`
            BEGIN
                EXECUTE IMMEDIATE 'CREATE USER %s IDENTIFIED BY "TemporaryPassword123!"';
                EXECUTE IMMEDIATE 'GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW TO %s';
                EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO %s';
            END;
        `, dbName, dbName, dbName)
	default:
		return fmt.Errorf("unsupported database driver: %s", driverName)
	}

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create database/user: %w", err)
	}

	return nil
	/*
		query := fmt.Sprintf(`CREATE DATABASE "%s"`, dbName)
		_, err := db.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to create database: %w", err)
		}
		return nil*/
}

func SQLBuildWhereInClause(fieldName string, values []string) string {
	// Quote each status and join them with commas
	quotedStatuses := make([]string, len(values))
	for i, status := range values {
		quotedStatuses[i] = fmt.Sprintf("'%s'", status)
	}

	return fieldName + " IN (" + strings.Join(quotedStatuses, ",") + ")"
}

func SQLBuildWhereInClauseInt64(fieldName string, values []int64) string {
	// Quote each status and join them with commas
	quotedStatuses := make([]string, len(values))
	for i, status := range values {
		quotedStatuses[i] = fmt.Sprintf("%d", status)
	}

	return fieldName + " IN (" + strings.Join(quotedStatuses, ",") + ")"
}

func SQLBuildWhereInClauseBool(fieldName string, values []bool) string {
	// Quote each status and join them with commas
	quotedStatuses := make([]string, len(values))
	for i, status := range values {
		if status {
			quotedStatuses[i] = "1"
		} else {
			quotedStatuses[i] = "0"
		}
	}

	return fieldName + " IN (" + strings.Join(quotedStatuses, ",") + ")"
}
