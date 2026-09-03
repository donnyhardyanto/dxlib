package utils

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/jmoiron/sqlx"
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

// --- database name safety -------------------------------------------------------
//
// KillConnections, DropDatabase and CreateDatabase take a database name and put
// it into SQL fourteen times across four engines, in two different syntactic
// positions: as a value compared against a catalogue column, and as an
// identifier in DDL. Both were interpolated with fmt.Sprintf and no escaping,
// so a name carrying the engine's own quote character escaped its quoting -- in
// statements whose verbs are DROP DATABASE and CREATE USER.
//
// Three layers, in order of how much they are relied on:
//
//  1. The name is validated once on entry, against an allowlist that contains
//     none of the four engines' quote characters, no semicolon, backslash,
//     whitespace or NUL. This is the primary defence, because DDL cannot be
//     parameterized: an identifier is part of the statement's grammar, so no
//     driver will bind it. CreateOracleUser below already took this approach
//     for the same reason, and this brings the rest of the file in line with it.
//  2. Where the name appears as a *value*, it is bound as a parameter rather
//     than pasted in. Oracle's KillConnections already did this (UPPER(:1));
//     the other three engines now do too.
//  3. Where it appears as an identifier, it is quoted the way that engine
//     quotes identifiers.
//
// The layers are deliberately redundant. Layer 1 alone would be enough today,
// but it is one edit away from being loosened by someone who needs a hyphen or
// a unicode name, and layers 2 and 3 keep the statements correct if that
// happens.
//
// databaseNamePattern is intentionally conservative rather than a
// transcription of what each engine permits. A database name here comes from
// configuration, not from a request, so the cost of refusing an exotic-but-legal
// name is a startup error someone reads once; the cost of accepting one is a
// DROP DATABASE that does not mean what it says. Letters, digits, underscore,
// dollar, hash, hyphen and dot, starting with a letter or underscore, up to 63
// characters -- which is also PostgreSQL's identifier limit.
//
// The dot is there because a per-tenant database is named after an app-instance
// nameid, and those carry dots in practice (dcc.mareca.vc-development). It is
// safe in every quoted context: a dot is not a delimiter in any of the four
// engines' identifier quoting, so it cannot end the quoting early. It is NOT
// safe unquoted in Oracle, where a dot separates schema from object -- which is
// why the Oracle branch of CreateDatabase applies oracleUserNamePattern on top,
// that pattern excluding both dot and hyphen.
var databaseNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$#.-]{0,62}$`)

// ValidateDatabaseName reports whether a name may be embedded in the
// provisioning statements in this file. Callers that build their own DDL from a
// configured name should use it too.
func ValidateDatabaseName(dbName string) error {
	if dbName == "" {
		return errors.Errorf("invalid database name: empty")
	}
	if !databaseNamePattern.MatchString(dbName) {
		return errors.Errorf("invalid database name: %q: expected %s", dbName, databaseNamePattern.String())
	}
	return nil
}

// quoteDatabaseIdentifier quotes a validated database name as an identifier for
// one engine. The rules differ per engine and getting them wrong is silent:
// MariaDB without ANSI_QUOTES reads "name" as a string literal rather than an
// identifier, and Oracle folds an unquoted name to upper case while treating a
// quoted one as case-sensitive.
//
// It re-validates rather than trusting the caller: it is cheap, and a future
// caller that forgets the entry check should not be the reason a quote
// character reaches a DROP statement.
func quoteDatabaseIdentifier(driverName string, dbName string) (string, error) {
	if err := ValidateDatabaseName(dbName); err != nil {
		return "", err
	}
	switch driverName {
	case "postgres":
		return `"` + strings.ReplaceAll(dbName, `"`, `""`) + `"`, nil
	case "sqlserver":
		return "[" + strings.ReplaceAll(dbName, "]", "]]") + "]", nil
	case "mariadb":
		return "`" + strings.ReplaceAll(dbName, "`", "``") + "`", nil
	case "godror", "oracle":
		// Oracle quoted identifiers are case-sensitive and its DDL creates
		// upper-case objects, so a quoted lower-case name never resolves
		// (ORA-00904). Fold before quoting, as query/utils does.
		return `"` + strings.ReplaceAll(strings.ToUpper(dbName), `"`, `""`) + `"`, nil
	default:
		return "", errors.Errorf("unsupported databases driver: %s", driverName)
	}
}

func KillConnections(db *sqlx.DB, dbName string) (err error) {
	if err = ValidateDatabaseName(dbName); err != nil {
		return err
	}
	driverName := base.NormalizeDriverName(db.DriverName())
	switch driverName {
	case "postgres":
		// datname is compared as a value, so it is bound rather than pasted.
		query := `
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = $1
          AND pid <> pg_backend_pid();
    `
		_, err = db.Exec(query, dbName)
		if err != nil {
			return errors.Errorf("failed to kill connections: %+v", err)
		}
	case "sqlserver":
		query := `
            USE master;
            DECLARE @kill varchar(8000) = '';
            SELECT @kill = @kill + 'kill ' + CONVERT(varchar(5), session_id) + ';'
            FROM sys.dm_exec_sessions
            WHERE database_id = DB_ID(@p1)
              AND session_id != @@SPID;
            EXEC(@kill);
        `
		_, err = db.Exec(query, dbName)
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
	case "mariadb":
		query := `
            SELECT CONCAT('KILL ', id, ';')
            FROM information_schema.processlist
            WHERE db = ?
              AND id != CONNECTION_ID();
        `
		rows, err := db.Query(query, dbName)
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
	if err = ValidateDatabaseName(dbName); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			log.Log.Warnf("Error dropping databases %s: %s", dbName, err.Error())
		}
	}()

	driverName := base.NormalizeDriverName(db.DriverName())

	// Kill all connections to the target databases
	err = KillConnections(db, dbName)
	if err != nil {
		log.Log.Errorf(err, "Failed to kill connections")
		return err
	}

	quoted, err := quoteDatabaseIdentifier(driverName, dbName)
	if err != nil {
		return err
	}

	var query string
	var args []any
	switch driverName {
	case "postgres":
		query = `DROP DATABASE IF EXISTS ` + quoted
	case "sqlserver":
		// The catalogue lookup is a value and is bound; the two ALTER/DROP
		// targets are identifiers and are quoted, because no driver will bind
		// the object a DDL statement acts on.
		query = `
            IF EXISTS (SELECT name FROM sys.databases WHERE name = @p1)
            BEGIN
                ALTER DATABASE ` + quoted + ` SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE ` + quoted + `;
            END
        `
		args = []any{dbName}
	case "godror", "oracle":
		// Oracle has no DROP DATABASE; a schema is a user, so its objects are
		// dropped instead. The owner comparison is bound. The object names come
		// from the catalogue and are re-quoted inside the dynamic statement.
		query = `
            BEGIN
                FOR obj IN (SELECT object_name, object_type FROM all_objects WHERE owner = UPPER(:1))
                LOOP
                    IF obj.object_type = 'TABLE' THEN
                        EXECUTE IMMEDIATE 'DROP ' || obj.object_type || ' "' || UPPER(:2) || '"."' || obj.object_name || '" CASCADE CONSTRAINTS';
                    ELSE
                        EXECUTE IMMEDIATE 'DROP ' || obj.object_type || ' "' || UPPER(:3) || '"."' || obj.object_name || '"';
                    END IF;
                END LOOP;
            END;
        `
		args = []any{dbName, dbName, dbName}
	case "mariadb":
		query = "DROP DATABASE IF EXISTS " + quoted
	default:
		return errors.Errorf("unsupported databases driver: %s", driverName)
	}

	_, err = db.Exec(query, args...)
	if err != nil {
		return errors.Errorf("failed to drop databases: %+v", err)
	}

	return nil
}

func CreateDatabase(db *sqlx.DB, dbName string) error {
	if err := ValidateDatabaseName(dbName); err != nil {
		return err
	}
	driverName := base.NormalizeDriverName(db.DriverName())

	quoted, err := quoteDatabaseIdentifier(driverName, dbName)
	if err != nil {
		return err
	}

	var query string
	switch driverName {
	case "postgres":
		query = `CREATE DATABASE ` + quoted
	case "sqlserver":
		query = `CREATE DATABASE ` + quoted
	case "godror", "oracle":
		// Oracle has no CREATE DATABASE: a schema is a user. The name goes into
		// DDL nested inside a PL/SQL string literal, where nothing can be
		// bound -- EXECUTE IMMEDIATE takes a string, and the name is part of
		// that string's grammar. So this branch rests entirely on the entry
		// validation, which is why the stricter Oracle pattern is applied on
		// top: an unquoted Oracle identifier cannot carry a hyphen, and this
		// emits the name unquoted so Oracle folds it to upper case, matching
		// the quoted upper-case identifiers the model DDL creates.
		if !oracleUserNamePattern.MatchString(dbName) {
			return errors.Errorf("invalid oracle schema name: %q: expected %s", dbName, oracleUserNamePattern.String())
		}
		// CreateOracleUser is the maintained path for this and takes a password
		// rather than embedding one. This branch is kept for callers that reach
		// CreateDatabase generically; the placeholder password is unchanged
		// behaviour and should be rotated by the caller.
		query = fmt.Sprintf(`
            BEGIN
                EXECUTE IMMEDIATE 'CREATE USER %s IDENTIFIED BY "TemporaryPassword123!"';
                EXECUTE IMMEDIATE 'GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW TO %s';
                EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO %s';
            END;
        `, dbName, dbName, dbName)
	case "mariadb":
		query = "CREATE DATABASE " + quoted
	default:
		return errors.Errorf("unsupported databases driver: %s", driverName)
	}

	_, err = db.Exec(query)
	if err != nil {
		return errors.Errorf("failed to create databases/user: %+v", err)
	}

	return nil

}

// oracleUserNamePattern validates a user/schema name before it is embedded in
// provisioning DDL (DDL cannot be parameterized) — anti-SQLI.
var oracleUserNamePattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_$#]*$`)

// CreateOracleUser provisions an Oracle schema USER — Oracle's equivalent of
// "create database": one schema == one user inside the shared service/PDB, so
// there is no CREATE DATABASE. The name is emitted UNQUOTED and Oracle folds it
// to UPPERCASE, matching the quoted-UPPERCASE identifiers models.CreateDDL
// emits. Grants cover the object types the model DDL creates, plus quota.
// Requires an admin connection (e.g. SYSTEM) on the same service.
func CreateOracleUser(db *sqlx.DB, userName string, password string) error {
	if !oracleUserNamePattern.MatchString(userName) {
		return errors.Errorf("invalid oracle user name: %q", userName)
	}
	statements := []string{
		fmt.Sprintf(`CREATE USER %s IDENTIFIED BY "%s"`, userName, strings.ReplaceAll(password, `"`, `""`)),
		fmt.Sprintf(`GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE PROCEDURE, CREATE TRIGGER TO %s`, userName),
		fmt.Sprintf(`GRANT UNLIMITED TABLESPACE TO %s`, userName),
	}
	for _, statement := range statements {
		_, err := db.Exec(statement)
		if err != nil {
			return errors.Errorf("failed to provision oracle user %s: %+v", userName, err)
		}
	}
	return nil
}

// DropOracleUser drops an Oracle schema user and every object it owns (the
// Oracle equivalent of DROP DATABASE). Sessions held by the user are killed
// first — DROP USER fails with ORA-01940 while the user is connected.
func DropOracleUser(db *sqlx.DB, userName string) (err error) {
	defer func() {
		if err != nil {
			log.Log.Warnf("Error dropping oracle user %s: %s", userName, err.Error())
		}
	}()
	if !oracleUserNamePattern.MatchString(userName) {
		return errors.Errorf("invalid oracle user name: %q", userName)
	}
	err = KillConnections(db, userName)
	if err != nil {
		return err
	}
	_, err = db.Exec(fmt.Sprintf(`DROP USER %s CASCADE`, userName))
	if err != nil {
		return errors.Errorf("failed to drop oracle user %s: %+v", userName, err)
	}
	return nil
}
