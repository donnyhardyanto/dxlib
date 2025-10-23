package utils

import (
	"database/sql"
	"fmt"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"strings"
)

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
	default:
		return errors.Errorf("unsupported database driver: %s", driverName)
	}

	return nil
}

func DropDatabase(db *sqlx.DB, dbName string) (err error) {
	defer func() {
		if err != nil {
			log.Log.Warnf("Error dropping database %s: %s", dbName, err.Error())
		}
	}()

	driverName := db.DriverName()

	// Kill all connections to the target database
	err = KillConnections(db, dbName)
	if err != nil {
		log.Log.Errorf(err, "Failed to kill connections")
		return errors.Wrap(err, "error occured")
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
		return errors.Errorf("unsupported database driver: %s", driverName)
	}

	_, err = db.Exec(query)
	if err != nil {
		return errors.Errorf("failed to drop database: %+v", err)
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
		return errors.Errorf("unsupported database driver: %s", driverName)
	}

	_, err := db.Exec(query)
	if err != nil {
		return errors.Errorf("failed to create database/user: %+v", err)
	}

	return nil
	/*
		query := fmt.Sprintf(`CREATE DATABASE "%s"`, dbName)
		_, err := db.Exec(query)
		if err != nil {
			return errors.Wrap(err, "error occured")ors.Errorf("failed to create database: %+v", err)
		}
		return nil*/
}

func SQLBuildParameterizedWhereClauseT[T comparable](driverName string, pv *[]any, template string, values ...T) string {
	if len(values) == 0 {
		return ""
	}

	p := len(*pv)
	result := template

	for _, value := range values {
		p++
		var placeholder string
		switch driverName {
		case "postgres":
			placeholder = fmt.Sprintf("$%d", p)
		case "sqlserver":
			placeholder = fmt.Sprintf("@%d", p)
		case "oracle":
			placeholder = fmt.Sprintf(":%d", p)
		default: // mysql and others
			placeholder = "?"
		}

		result = strings.Replace(result, "?", placeholder, 1)
		*pv = append(*pv, value)
	}

	return result
}
func SQLBuildParameterizedWhereClauseString(driverName string, pv *[]any, template string, values ...string) string {
	return SQLBuildParameterizedWhereClauseT[string](driverName, pv, template, values...)
}

func SQLBuildParameterizedWhereClauseBool(driverName string, pv *[]any, template string, values ...bool) string {
	return SQLBuildParameterizedWhereClauseT(driverName, pv, template, values...)
}

func SQLBuildParameterizedWhereClauseInt64(driverName string, pv *[]any, template string, values ...int64) string {
	return SQLBuildParameterizedWhereClauseT(driverName, pv, template, values...)
}

func SQLBuildWhereInClauseStrings(fieldName string, values []string) string {
	l := len(values)
	if l == 0 {
		return ""
	}
	quotedStatuses := make([]string, l)
	for i, status := range values {
		quotedStatuses[i] = fmt.Sprintf("'%s'", status)
	}
	if l == 1 {
		return fieldName + " = " + quotedStatuses[0]
	}
	return "(" + fieldName + " IN (" + strings.Join(quotedStatuses, ",") + "))"
}

func SQLBuildParameterizedWhereInClauseStrings(driverName string, pv *[]any, fieldName string, values []string) string {
	l := len(values)
	if l == 0 {
		return ""
	}
	p := len(*pv)
	quotedStatuses := make([]string, l)
	for i, value := range values {
		p = p + 1
		switch driverName {
		case "postgres":
			quotedStatuses[i] = fmt.Sprintf("$%d", p)
		case "sqlserver":
			quotedStatuses[i] = fmt.Sprintf("@%d", p)
		case "oracle":
			quotedStatuses[i] = fmt.Sprintf(":%d", p)
		case "mysql":
			quotedStatuses[i] = "?"
		default:
			quotedStatuses[i] = "?"
		}
		*pv = append(*pv, value)
	}
	if l == 1 {
		return fieldName + " =" + quotedStatuses[0]
	}
	return fieldName + " IN (" + strings.Join(quotedStatuses, ",") + ")"
}

func SQLBuildWhereInClauseInt64(fieldName string, values []int64) string {
	l := len(values)
	if l == 0 {
		return ""
	}
	valueAsStrings := make([]string, l)
	for i, value := range values {
		valueAsStrings[i] = fmt.Sprintf("%d", value)
	}
	if l == 1 {
		return fieldName + " = " + valueAsStrings[0]
	}
	return fieldName + " IN (" + strings.Join(valueAsStrings, ",") + ")"
}

func SQLBuildParameterizedWhereInClauseInt64(pv *[]any, fieldName string, values []int64) string {
	return SQLBuildWhereInClauseInt64(fieldName, values)
}

func SQLBuildWhereInClauseBool(fieldName string, values []bool) string {
	l := len(values)
	quotedStatuses := make([]string, l)
	for i, status := range values {
		if status {
			quotedStatuses[i] = "1"
		} else {
			quotedStatuses[i] = "0"
		}
	}
	if l == 0 {
		return ""
	}
	if l == 1 {
		return fieldName + " = " + fmt.Sprintf("%t", values[0])
	}
	return fieldName + " IN (" + strings.Join(quotedStatuses, ",") + ")"
}
