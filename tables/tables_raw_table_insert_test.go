package tables

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/sijms/go-ora/v2/network"
)

// DXRawTable.DoCreate writes its own error response and returns nil. A row the
// schema refused answers 409 or 422 with only the reason code, as the DXTable
// paths do; a database that failed answers the sanitized 500. No answer
// carries the table, a column, the constraint or the driver message, which
// all go to the log. Each case runs the real chain over a driver that fails
// the statement, under each dialect's driver name.
func TestRawTableDoCreateAnswersAFixedReasonCode(t *testing.T) {
	const tableName = "widget"
	cases := []struct {
		name      string
		driver    string
		driverErr error
		leaks     []string // driver-message words that must stay in the log
		status    int
		reason    string
	}{
		{"pg duplicate", "postgres", &pgconn.PgError{Code: "23505", Message: `duplicate key value violates unique constraint "widget_name_key"`, ConstraintName: "widget_name_key"}, []string{"widget_name_key", "duplicate key"}, http.StatusConflict, InsertRefusedReasonDuplicateKey},
		{"pg foreign key", "postgres", &pgconn.PgError{Code: "23503", Message: `insert or update on table "widget" violates foreign key constraint "widget_owner_fk"`}, []string{"widget_owner_fk", "foreign key"}, http.StatusUnprocessableEntity, InsertRefusedReasonConstraintViolation},
		{"pg not null", "postgres", &pgconn.PgError{Code: "23502", Message: `null value in column "owner_ref" violates not-null constraint`}, []string{"owner_ref", "not-null"}, http.StatusUnprocessableEntity, InsertRefusedReasonConstraintViolation},
		{"mariadb duplicate", "mysql", &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'x' for key 'widget_name_key'"}, []string{"widget_name_key", "Duplicate entry"}, http.StatusConflict, InsertRefusedReasonDuplicateKey},
		{"mariadb check", "mysql", &mysql.MySQLError{Number: 4025, Message: "CONSTRAINT `widget_chk` failed for `s`.`widget`"}, []string{"widget_chk", "failed for"}, http.StatusUnprocessableEntity, InsertRefusedReasonConstraintViolation},
		{"mariadb foreign key", "mysql", &mysql.MySQLError{Number: 1452, Message: "Cannot add or update a child row: a foreign key constraint fails (`s`.`widget`, CONSTRAINT `widget_owner_fk`)"}, []string{"widget_owner_fk", "child row"}, http.StatusUnprocessableEntity, InsertRefusedReasonConstraintViolation},
		{"sqlserver duplicate", "sqlserver", mssql.Error{Number: 2627, Message: "Violation of UNIQUE KEY constraint 'widget_name_key'. Cannot insert duplicate key in object 'dbo.widget'."}, []string{"widget_name_key", "Violation"}, http.StatusConflict, InsertRefusedReasonDuplicateKey},
		{"sqlserver foreign key", "sqlserver", mssql.Error{Number: 547, Message: `The INSERT statement conflicted with the FOREIGN KEY constraint "widget_owner_fk".`}, []string{"widget_owner_fk", "conflicted"}, http.StatusUnprocessableEntity, InsertRefusedReasonConstraintViolation},
		{"sqlserver not null", "sqlserver", mssql.Error{Number: 515, Message: "Cannot insert the value NULL into column 'owner_ref', table 'dbo.widget'"}, []string{"owner_ref", "NULL"}, http.StatusUnprocessableEntity, InsertRefusedReasonConstraintViolation},
		{"oracle duplicate", "oracle", &network.OracleError{ErrCode: 1, ErrMsg: "ORA-00001: unique constraint (S.WIDGET_NAME_KEY) violated"}, []string{"WIDGET_NAME_KEY", "ORA-"}, http.StatusConflict, InsertRefusedReasonDuplicateKey},
		{"oracle parent missing", "oracle", &network.OracleError{ErrCode: 2291, ErrMsg: "ORA-02291: integrity constraint (S.WIDGET_OWNER_FK) violated - parent key not found"}, []string{"WIDGET_OWNER_FK", "ORA-"}, http.StatusUnprocessableEntity, InsertRefusedReasonConstraintViolation},
		{"oracle not null", "oracle", &network.OracleError{ErrCode: 1400, ErrMsg: `ORA-01400: cannot insert NULL into ("S"."WIDGET"."OWNER_REF")`}, []string{"OWNER_REF", "ORA-"}, http.StatusUnprocessableEntity, InsertRefusedReasonConstraintViolation},

		{"pg syntax", "postgres", &pgconn.PgError{Code: "42601", Message: `syntax error at or near "FORM"`}, []string{"FORM", "syntax"}, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR"},
		{"database down", "postgres", errors.New("connection refused to widget host"), []string{"connection refused"}, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := failingDatabaseOn(c.driver, c.driverErr)
			table := &DXRawTable{DatabaseNameId: d.NameId, Database: d, TableNameDirect: tableName, FieldNameForRowId: "id", FieldNameForRowUid: "uid", ResponseEnvelopeObjectName: "widget"}
			aepr, rec := newCreateRequest()

			id, err := table.DoCreate(aepr, utils.JSON{"name": "secret-value", "owner_ref": "other-value"})
			if err != nil || id != 0 {
				t.Fatalf("DoCreate returned (%d, %v), want (0, nil): it writes its own error response", id, err)
			}
			if rec.Code != c.status {
				t.Fatalf("status = %d, want %d: %s", rec.Code, c.status, rec.Body.String())
			}
			var body map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("body is not JSON: %v: %s", err, rec.Body.String())
			}
			if got, _ := body["status_code"].(float64); int(got) != c.status {
				t.Errorf("body status_code = %v, want %d", body["status_code"], c.status)
			}
			if body["reason"] != c.reason || body["reason_message"] != c.reason {
				t.Errorf("body = %v, want reason and reason_message %q", body, c.reason)
			}
			raw := rec.Body.String()
			forbidden := append([]string{tableName, "WIDGET", "name", "owner_ref", "secret-value", "other-value", "DB_INSERT_ERROR", "data="}, c.leaks...)
			for _, f := range forbidden {
				if strings.Contains(raw, f) {
					t.Errorf("body %s carries %q", raw, f)
				}
			}
		})
	}
}
