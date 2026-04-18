package db

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	utilsJson "github.com/donnyhardyanto/dxlib/utils/json"
	"github.com/jmoiron/sqlx"
)

// Upsert performs an atomic single-row UPSERT using the database's native syntax.
//
// Per dialect:
//   - PostgreSQL / PostgresSQLV2 : INSERT ... ON CONFLICT (whereKeys) DO UPDATE ... RETURNING id, (xmax = 0)
//   - MariaDB / MySQL            : INSERT ... ON DUPLICATE KEY UPDATE ..., id = LAST_INSERT_ID(id)
//   - SQL Server                 : MERGE ... WITH (HOLDLOCK) ... OUTPUT inserted.id, $action
//   - Oracle                     : PL/SQL block - try INSERT ... EXCEPTION WHEN DUP_VAL_ON_INDEX THEN UPDATE
//
// PRECONDITION: whereKeys columns MUST have a UNIQUE (or PRIMARY KEY) constraint.
//   - PostgreSQL / SQLite : missing constraint -> statement rejected with clear error
//   - SQL Server / Oracle : missing constraint -> caller responsibility; MERGE syntax still runs
//   - MariaDB / MySQL     : missing constraint -> SILENT duplicate insert (DB-level foot-gun)
//
// Affects exactly ONE row per call (never multi-row). Single SQL statement, single round-trip.
// No SELECT-then-INSERT-or-UPDATE race window.
//
// Returns:
//   - sql.Result : non-nil for MariaDB and Oracle paths; nil for PG / MSSQL paths (which use QueryRows)
//   - id         : newly-inserted row id OR existing row id on update
//   - isInsert   : true if a new row was inserted, false if an existing row was updated
//
// insertData : full column set for INSERT (typically data ∪ whereKeys, plus insert-audit fields)
// updateData : columns to assign on UPDATE (omit immutable fields like created_at)
// whereKeys  : identity columns; must be a UNIQUE or PK on the table
// idField    : the row-id column (returned in id)
func Upsert(
	ctx context.Context,
	db *sqlx.DB,
	tableName string,
	insertData utils.JSON,
	updateData utils.JSON,
	whereKeys utils.JSON,
	idField string,
) (result sql.Result, id int64, isInsert bool, err error) {
	defer func() {
		if err != nil {
			err = NewDBOperationError("UPSERT", tableName, insertData, err)
		} else {
			log.Log.Debugf("DB_UPSERT table=%s data=%s where=%s id=%d inserted=%t",
				tableName, formatJSONForLog(insertData), formatJSONForLog(whereKeys), id, isInsert)
		}
	}()

	dbType, insertCols, updateCols, whereCols, mergedArgs, err := prepareUpsert(
		db.DriverName(), tableName, insertData, updateData, whereKeys, idField,
	)
	if err != nil {
		return nil, 0, false, err
	}

	switch dbType {
	case base.DXDatabaseTypePostgreSQL, base.DXDatabaseTypePostgresSQLV2:
		return execUpsertPostgres(ctx, db, nil, tableName, insertCols, updateCols, whereCols, mergedArgs, idField)
	case base.DXDatabaseTypeMariaDB:
		return execUpsertMariaDB(ctx, db, nil, tableName, insertCols, updateCols, mergedArgs, idField)
	case base.DXDatabaseTypeSQLServer:
		return execUpsertSQLServer(ctx, db, nil, tableName, insertCols, updateCols, whereCols, mergedArgs, idField)
	case base.DXDatabaseTypeOracle:
		return execUpsertOracle(ctx, db, nil, tableName, insertCols, updateCols, whereCols, mergedArgs, idField)
	default:
		return nil, 0, false, errors.Errorf("unsupported databases driver: %s", db.DriverName())
	}
}

// TxUpsert is the transactional variant of Upsert. Same contract and dialect matrix.
func TxUpsert(
	ctx context.Context,
	tx *sqlx.Tx,
	tableName string,
	insertData utils.JSON,
	updateData utils.JSON,
	whereKeys utils.JSON,
	idField string,
) (result sql.Result, id int64, isInsert bool, err error) {
	defer func() {
		if err != nil {
			err = NewDBOperationError("UPSERT", tableName, insertData, err)
		} else {
			log.Log.Debugf("DB_UPSERT table=%s data=%s where=%s id=%d inserted=%t",
				tableName, formatJSONForLog(insertData), formatJSONForLog(whereKeys), id, isInsert)
		}
	}()

	dbType, insertCols, updateCols, whereCols, mergedArgs, err := prepareUpsert(
		tx.DriverName(), tableName, insertData, updateData, whereKeys, idField,
	)
	if err != nil {
		return nil, 0, false, err
	}

	switch dbType {
	case base.DXDatabaseTypePostgreSQL, base.DXDatabaseTypePostgresSQLV2:
		return execUpsertPostgres(ctx, nil, tx, tableName, insertCols, updateCols, whereCols, mergedArgs, idField)
	case base.DXDatabaseTypeMariaDB:
		return execUpsertMariaDB(ctx, nil, tx, tableName, insertCols, updateCols, mergedArgs, idField)
	case base.DXDatabaseTypeSQLServer:
		return execUpsertSQLServer(ctx, nil, tx, tableName, insertCols, updateCols, whereCols, mergedArgs, idField)
	case base.DXDatabaseTypeOracle:
		return execUpsertOracle(ctx, nil, tx, tableName, insertCols, updateCols, whereCols, mergedArgs, idField)
	default:
		return nil, 0, false, errors.Errorf("unsupported databases driver: %s", tx.DriverName())
	}
}

// prepareUpsert validates identifiers, normalizes values, and returns sorted column slices plus
// a merged named-parameter map that covers every :col placeholder used by the dialect-specific SQL.
func prepareUpsert(
	driverNameRaw string,
	tableName string,
	insertData utils.JSON,
	updateData utils.JSON,
	whereKeys utils.JSON,
	idField string,
) (dbType base.DXDatabaseType, insertCols, updateCols, whereCols []string, mergedArgs utils.JSON, err error) {
	driverName := base.NormalizeDriverName(strings.ToLower(driverNameRaw))
	dbType = base.StringToDXDatabaseType(driverName)

	if tableName == "" {
		return 0, nil, nil, nil, nil, errors.New("table name cannot be empty")
	}
	if idField == "" {
		return 0, nil, nil, nil, nil, errors.New("idField cannot be empty")
	}
	if len(insertData) == 0 {
		return 0, nil, nil, nil, nil, errors.New("insertData cannot be empty")
	}
	if len(whereKeys) == 0 {
		return 0, nil, nil, nil, nil, errors.New("whereKeys cannot be empty")
	}
	if len(updateData) == 0 {
		return 0, nil, nil, nil, nil, errors.New("updateData cannot be empty")
	}

	if err := CheckIdentifier(dbType, tableName); err != nil {
		return 0, nil, nil, nil, nil, errors.Wrap(err, "invalid table name")
	}
	if err := CheckIdentifier(dbType, idField); err != nil {
		return 0, nil, nil, nil, nil, errors.Wrap(err, "invalid idField")
	}

	mergedArgs = utils.JSON{}

	collect := func(src utils.JSON) error {
		for k, v := range src {
			if err := CheckIdentifier(dbType, k); err != nil {
				return errors.Wrapf(err, "invalid field name: %s", k)
			}
			converted, convErr := DbDriverConvertValueTypeToDBCompatible(driverName, v)
			if convErr != nil {
				return errors.Wrapf(convErr, "failed to convert field value: %s", k)
			}
			mergedArgs[k] = converted
		}
		return nil
	}
	if err := collect(insertData); err != nil {
		return 0, nil, nil, nil, nil, err
	}
	if err := collect(updateData); err != nil {
		return 0, nil, nil, nil, nil, err
	}
	if err := collect(whereKeys); err != nil {
		return 0, nil, nil, nil, nil, err
	}

	insertCols = sortedKeys(insertData)
	updateCols = sortedKeys(updateData)
	whereCols = sortedKeys(whereKeys)

	return dbType, insertCols, updateCols, whereCols, mergedArgs, nil
}

func sortedKeys(m utils.JSON) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ---------- PostgreSQL ----------

func buildUpsertPostgresSQL(tableName string, insertCols, updateCols, whereCols []string, idField string) string {
	valueRefs := make([]string, len(insertCols))
	for i, c := range insertCols {
		valueRefs[i] = ":" + c
	}
	updateAssigns := make([]string, 0, len(updateCols))
	for _, c := range updateCols {
		updateAssigns = append(updateAssigns, fmt.Sprintf("%s = EXCLUDED.%s", c, c))
	}
	return fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s RETURNING %s, (xmax = 0) AS was_inserted`,
		tableName,
		strings.Join(insertCols, ", "),
		strings.Join(valueRefs, ", "),
		strings.Join(whereCols, ", "),
		strings.Join(updateAssigns, ", "),
		idField,
	)
}

func execUpsertPostgres(
	ctx context.Context,
	db *sqlx.DB, tx *sqlx.Tx,
	tableName string, insertCols, updateCols, whereCols []string,
	args utils.JSON, idField string,
) (sql.Result, int64, bool, error) {
	sqlStmt := buildUpsertPostgresSQL(tableName, insertCols, updateCols, whereCols, idField)

	var (
		rows []utils.JSON
		err  error
	)
	if tx != nil {
		_, rows, err = TxQueryRows(ctx, tx, nil, sqlStmt, args)
	} else {
		_, rows, err = QueryRows(ctx, db, nil, sqlStmt, args)
	}
	if err != nil {
		return nil, 0, false, errors.Wrap(err, "postgres upsert failed")
	}
	if len(rows) == 0 {
		return nil, 0, false, errors.New("postgres upsert returned no rows")
	}

	id, err := utilsJson.GetInt64(rows[0], idField)
	if err != nil {
		return nil, 0, false, errors.Wrapf(err, "postgres upsert failed to read %s", idField)
	}
	isInsert, err := utilsJson.GetBool(rows[0], "was_inserted")
	if err != nil {
		return nil, 0, false, errors.Wrap(err, "postgres upsert failed to read was_inserted")
	}
	return nil, id, isInsert, nil
}

// ---------- MariaDB / MySQL ----------

func buildUpsertMariaDBSQL(tableName string, insertCols, updateCols []string, idField string) string {
	valueRefs := make([]string, len(insertCols))
	for i, c := range insertCols {
		valueRefs[i] = ":" + c
	}
	updateAssigns := make([]string, 0, len(updateCols)+1)
	for _, c := range updateCols {
		updateAssigns = append(updateAssigns, fmt.Sprintf("%s = VALUES(%s)", c, c))
	}
	// LAST_INSERT_ID(<id>) trick: on UPDATE path, makes result.LastInsertId() return the
	// existing row's id. On INSERT path, auto-increment naturally populates LastInsertId.
	updateAssigns = append(updateAssigns, fmt.Sprintf("%s = LAST_INSERT_ID(%s)", idField, idField))

	return fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s`,
		tableName,
		strings.Join(insertCols, ", "),
		strings.Join(valueRefs, ", "),
		strings.Join(updateAssigns, ", "),
	)
}

func execUpsertMariaDB(
	ctx context.Context,
	db *sqlx.DB, tx *sqlx.Tx,
	tableName string, insertCols, updateCols []string,
	args utils.JSON, idField string,
) (sql.Result, int64, bool, error) {
	sqlStmt := buildUpsertMariaDBSQL(tableName, insertCols, updateCols, idField)

	var (
		result sql.Result
		err    error
	)
	if tx != nil {
		result, err = TxExec(ctx, tx, sqlStmt, args)
	} else {
		result, err = Exec(ctx, db, sqlStmt, args)
	}
	if err != nil {
		return nil, 0, false, errors.Wrap(err, "mariadb upsert failed")
	}

	id, idErr := result.LastInsertId()
	if idErr != nil {
		return result, 0, false, errors.Wrap(idErr, "mariadb upsert failed to read LastInsertId")
	}
	ra, raErr := result.RowsAffected()
	if raErr != nil {
		return result, id, false, errors.Wrap(raErr, "mariadb upsert failed to read RowsAffected")
	}
	// MySQL/MariaDB RowsAffected convention for ON DUPLICATE KEY UPDATE:
	//   1 = new row inserted
	//   2 = existing row updated (counts DELETE + INSERT internally)
	//   0 = existing row, update produced no actual change (identical values)
	isInsert := ra == 1
	return result, id, isInsert, nil
}

// ---------- SQL Server ----------

func buildUpsertSQLServerSQL(tableName string, insertCols, updateCols, whereCols []string, idField string) string {
	sourceSelects := make([]string, len(insertCols))
	for i, c := range insertCols {
		sourceSelects[i] = fmt.Sprintf(":%s AS %s", c, c)
	}
	srcInsertCols := make([]string, len(insertCols))
	for i, c := range insertCols {
		srcInsertCols[i] = "src." + c
	}
	updateAssigns := make([]string, len(updateCols))
	for i, c := range updateCols {
		updateAssigns[i] = fmt.Sprintf("tgt.%s = src.%s", c, c)
	}
	onConditions := make([]string, len(whereCols))
	for i, c := range whereCols {
		onConditions[i] = fmt.Sprintf("tgt.%s = src.%s", c, c)
	}
	// HOLDLOCK is critical for race safety: plain MERGE in SQL Server has known race bugs
	// under concurrent writers. The hint serializes via key-range locks.
	return fmt.Sprintf(
		`MERGE INTO %s WITH (HOLDLOCK) AS tgt USING (SELECT %s) AS src ON %s WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s) OUTPUT inserted.%s AS id, $action AS action;`,
		tableName,
		strings.Join(sourceSelects, ", "),
		strings.Join(onConditions, " AND "),
		strings.Join(updateAssigns, ", "),
		strings.Join(insertCols, ", "),
		strings.Join(srcInsertCols, ", "),
		idField,
	)
}

func execUpsertSQLServer(
	ctx context.Context,
	db *sqlx.DB, tx *sqlx.Tx,
	tableName string, insertCols, updateCols, whereCols []string,
	args utils.JSON, idField string,
) (sql.Result, int64, bool, error) {
	sqlStmt := buildUpsertSQLServerSQL(tableName, insertCols, updateCols, whereCols, idField)

	var (
		rows []utils.JSON
		err  error
	)
	if tx != nil {
		_, rows, err = TxQueryRows(ctx, tx, nil, sqlStmt, args)
	} else {
		_, rows, err = QueryRows(ctx, db, nil, sqlStmt, args)
	}
	if err != nil {
		return nil, 0, false, errors.Wrap(err, "sqlserver upsert failed")
	}
	if len(rows) == 0 {
		return nil, 0, false, errors.New("sqlserver upsert returned no rows")
	}

	id, err := utilsJson.GetInt64(rows[0], "id")
	if err != nil {
		return nil, 0, false, errors.Wrapf(err, "sqlserver upsert failed to read id")
	}
	actionRaw, _ := rows[0]["action"]
	action, _ := actionRaw.(string)
	isInsert := strings.EqualFold(action, "INSERT")
	return nil, id, isInsert, nil
}

// ---------- Oracle ----------

func buildUpsertOraclePLSQL(tableName string, insertCols, updateCols, whereCols []string, idField string) string {
	valueRefs := make([]string, len(insertCols))
	for i, c := range insertCols {
		valueRefs[i] = ":" + c
	}
	updateAssigns := make([]string, len(updateCols))
	for i, c := range updateCols {
		updateAssigns[i] = fmt.Sprintf("%s = :%s", c, c)
	}
	whereAssigns := make([]string, len(whereCols))
	for i, c := range whereCols {
		whereAssigns[i] = fmt.Sprintf("%s = :%s", c, c)
	}
	// Oracle MERGE does not expose $action or support RETURNING on MERGE portably,
	// so use the canonical try-INSERT / catch-DUP_VAL_ON_INDEX / UPDATE idiom.
	// Atomic within the enclosing transaction: UNIQUE constraint check is synchronous.
	return fmt.Sprintf(
		`DECLARE v_id NUMBER; v_action VARCHAR2(1); BEGIN BEGIN INSERT INTO %s (%s) VALUES (%s) RETURNING %s INTO v_id; v_action := 'I'; EXCEPTION WHEN DUP_VAL_ON_INDEX THEN UPDATE %s SET %s WHERE %s RETURNING %s INTO v_id; IF SQL%%ROWCOUNT = 0 THEN RAISE_APPLICATION_ERROR(-20001, 'UPSERT_CONCURRENT_DELETE'); END IF; v_action := 'U'; END; :out_id := v_id; :out_action := v_action; END;`,
		tableName,
		strings.Join(insertCols, ", "),
		strings.Join(valueRefs, ", "),
		idField,
		tableName,
		strings.Join(updateAssigns, ", "),
		strings.Join(whereAssigns, " AND "),
		idField,
	)
}

func execUpsertOracle(
	ctx context.Context,
	db *sqlx.DB, tx *sqlx.Tx,
	tableName string, insertCols, updateCols, whereCols []string,
	args utils.JSON, idField string,
) (sql.Result, int64, bool, error) {
	sqlStmt := buildUpsertOraclePLSQL(tableName, insertCols, updateCols, whereCols, idField)

	namedArgs := make([]interface{}, 0, len(args)+2)
	for name, value := range args {
		namedArgs = append(namedArgs, sql.Named(name, value))
	}

	var (
		outID     int64
		outAction string
	)
	namedArgs = append(namedArgs, sql.Named("out_id", sql.Out{Dest: &outID}))
	namedArgs = append(namedArgs, sql.Named("out_action", sql.Out{Dest: &outAction}))

	var (
		result sql.Result
		err    error
	)
	if tx != nil {
		result, err = tx.ExecContext(ctx, sqlStmt, namedArgs...)
	} else {
		result, err = db.ExecContext(ctx, sqlStmt, namedArgs...)
	}
	if err != nil {
		return nil, 0, false, errors.Wrap(err, "oracle upsert plsql failed")
	}

	isInsert := outAction == "I"
	return result, outID, isInsert, nil
}
