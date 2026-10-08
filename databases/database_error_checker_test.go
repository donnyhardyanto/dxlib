package databases

import (
	"testing"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/sijms/go-ora/v2/network"
)

// A failed write is one of three things: a duplicate key, a row the schema
// refuses (foreign key, check, not null), or something else. Each driver
// reports them differently, typed and as text, and the two predicates have to
// agree on every form.
func TestDuplicateAndConstraintPredicatesPerDialect(t *testing.T) {
	for _, c := range []struct {
		name       string
		err        error
		duplicate  bool
		constraint bool
	}{
		// PostgreSQL, typed
		{"pg unique", &pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint"}, true, false},
		{"pg foreign key", &pgconn.PgError{Code: "23503", Message: "insert or update violates foreign key constraint"}, false, true},
		{"pg check", &pgconn.PgError{Code: "23514", Message: "new row violates check constraint"}, false, true},
		{"pg not null", &pgconn.PgError{Code: "23502", Message: "null value violates not-null constraint"}, false, true},
		{"pg syntax", &pgconn.PgError{Code: "42601", Message: "syntax error"}, false, false},
		{"pg undefined table", &pgconn.PgError{Code: "42P01", Message: "relation does not exist"}, false, false},
		// MySQL/MariaDB, typed
		{"mysql duplicate", &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'x' for key 'k'"}, true, false},
		{"mysql foreign key", &mysql.MySQLError{Number: 1452, Message: "Cannot add or update a child row"}, false, true},
		{"mysql child rows", &mysql.MySQLError{Number: 1451, Message: "Cannot delete or update a parent row"}, false, true},
		{"mysql check", &mysql.MySQLError{Number: 3819, Message: "Check constraint 'c' is violated."}, false, true},
		{"mariadb check", &mysql.MySQLError{Number: 4025, Message: "CONSTRAINT `c` failed for `s`.`t`"}, false, true},
		{"mysql not null", &mysql.MySQLError{Number: 1048, Message: "Column 'a' cannot be null"}, false, true},
		{"mysql syntax", &mysql.MySQLError{Number: 1064, Message: "You have an error in your SQL syntax"}, false, false},
		// SQL Server, typed
		{"mssql unique constraint", mssql.Error{Number: 2627, Message: "Violation of UNIQUE KEY constraint"}, true, false},
		{"mssql unique index", mssql.Error{Number: 2601, Message: "Cannot insert duplicate key row"}, true, false},
		{"mssql foreign key or check", mssql.Error{Number: 547, Message: "conflicted with the FOREIGN KEY constraint"}, false, true},
		{"mssql not null", mssql.Error{Number: 515, Message: "Cannot insert the value NULL into column"}, false, true},
		{"mssql syntax", mssql.Error{Number: 102, Message: "Incorrect syntax near"}, false, false},
		// Oracle, typed
		{"oracle unique", &network.OracleError{ErrCode: 1, ErrMsg: "ORA-00001: unique constraint violated"}, true, false},
		{"oracle parent missing", &network.OracleError{ErrCode: 2291, ErrMsg: "ORA-02291: integrity constraint violated - parent key not found"}, false, true},
		{"oracle child found", &network.OracleError{ErrCode: 2292, ErrMsg: "ORA-02292: integrity constraint violated - child record found"}, false, true},
		{"oracle check", &network.OracleError{ErrCode: 2290, ErrMsg: "ORA-02290: check constraint violated"}, false, true},
		{"oracle not null", &network.OracleError{ErrCode: 1400, ErrMsg: "ORA-01400: cannot insert NULL"}, false, true},
		{"oracle table missing", &network.OracleError{ErrCode: 942, ErrMsg: "ORA-00942: table or view does not exist"}, false, false},
		// Text only, as a driver that is not typed here, or a logged message, would give it
		{"text pg duplicate", errors.New(`ERROR: duplicate key value violates unique constraint "t_pkey" (SQLSTATE 23505)`), true, false},
		{"text pg foreign key", errors.New(`ERROR: insert or update on table "t" violates foreign key constraint "t_fk" (SQLSTATE 23503)`), false, true},
		{"text pg check", errors.New(`ERROR: new row for relation "t" violates check constraint "t_c" (SQLSTATE 23514)`), false, true},
		{"text pg not null", errors.New(`ERROR: null value in column "a" violates not-null constraint (SQLSTATE 23502)`), false, true},
		{"text mysql duplicate", errors.New("Error 1062 (23000): Duplicate entry '1' for key 't.PRIMARY'"), true, false},
		{"text mysql foreign key", errors.New("Error 1452 (23000): Cannot add or update a child row: a foreign key constraint fails"), false, true},
		{"text mysql check", errors.New("Error 3819 (HY000): Check constraint 't_chk_1' is violated."), false, true},
		{"text mariadb check", errors.New("Error 4025 (23000): CONSTRAINT `t_chk_1` failed for `s`.`t`"), false, true},
		{"text mysql not null", errors.New("Error 1048 (23000): Column 'a' cannot be null"), false, true},
		{"text mssql foreign key", errors.New("mssql: The INSERT statement conflicted with the FOREIGN KEY constraint \"FK_t\""), false, true},
		{"text mssql check", errors.New("mssql: The INSERT statement conflicted with the CHECK constraint \"CK_t\""), false, true},
		{"text mssql not null", errors.New("mssql: Cannot insert the value NULL into column 'a', table 't'; column does not allow nulls."), false, true},
		{"text oracle unique", errors.New("ORA-00001: unique constraint (S.T_PK) violated"), true, false},
		{"text oracle foreign key", errors.New("ORA-02291: integrity constraint (S.T_FK) violated - parent key not found"), false, true},
		{"text oracle check", errors.New("ORA-02290: check constraint (S.T_CK) violated"), false, true},
		{"text oracle not null", errors.New(`ORA-01400: cannot insert NULL into ("S"."T"."A")`), false, true},
		{"text sqlite foreign key", errors.New("FOREIGN KEY constraint failed"), false, true},
		{"text connection lost", errors.New("read tcp 10.0.0.1:5432: connection reset by peer"), false, false},
		{"text syntax", errors.New(`ERROR: syntax error at or near "FORM" (SQLSTATE 42601)`), false, false},
		{"nil", nil, false, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := IsDuplicateKeyError(c.err); got != c.duplicate {
				t.Errorf("IsDuplicateKeyError = %v, want %v", got, c.duplicate)
			}
			if got := IsConstraintViolationError(c.err); got != c.constraint {
				t.Errorf("IsConstraintViolationError = %v, want %v", got, c.constraint)
			}
		})
	}
}

// Insert hands its errors through CheckDatabaseError, which relabels a duplicate
// key but keeps the driver error as the cause; the predicates must still read
// the wrapped form, and a wrapped constraint violation must not turn into a
// duplicate.
func TestPredicatesReadTheWrappedForm(t *testing.T) {
	dup := CheckDatabaseError(errors.Wrap(&pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint"}, "insert"))
	if !IsDuplicateKeyError(dup) || IsConstraintViolationError(dup) {
		t.Errorf("wrapped duplicate: duplicate=%v constraint=%v, want true/false (%v)", IsDuplicateKeyError(dup), IsConstraintViolationError(dup), dup)
	}
	fk := CheckDatabaseError(errors.Wrap(&mysql.MySQLError{Number: 1452, Message: "Cannot add or update a child row"}, "insert"))
	if IsDuplicateKeyError(fk) || !IsConstraintViolationError(fk) {
		t.Errorf("wrapped foreign key: duplicate=%v constraint=%v, want false/true (%v)", IsDuplicateKeyError(fk), IsConstraintViolationError(fk), fk)
	}
}
