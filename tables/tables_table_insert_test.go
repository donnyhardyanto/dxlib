package tables

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/sijms/go-ora/v2/network"
)

// DXTable.DoCreate and DXTableAuditOnly.DoCreate write no error response of
// their own; they hand the error to the route handler. insertError decides
// what that handler sees: a domain error with the client's status for a row the
// schema refused, the plain error for everything else.
func TestInsertErrorClassifiesLikeTheRawPath(t *testing.T) {
	for _, c := range []struct {
		name   string
		err    error
		status int // 0: not a domain error, returned as it is
		reason string
	}{
		{"pg duplicate", &pgconn.PgError{Code: "23505"}, http.StatusConflict, InsertRefusedReasonDuplicateKey},
		{"mysql duplicate", &mysql.MySQLError{Number: 1062}, http.StatusConflict, InsertRefusedReasonDuplicateKey},
		{"mssql duplicate", mssql.Error{Number: 2627}, http.StatusConflict, InsertRefusedReasonDuplicateKey},
		{"oracle duplicate", &network.OracleError{ErrCode: 1}, http.StatusConflict, InsertRefusedReasonDuplicateKey},
		{"pg foreign key", &pgconn.PgError{Code: "23503"}, http.StatusUnprocessableEntity, InsertRefusedReasonConstraintViolation},
		{"mariadb check", &mysql.MySQLError{Number: 4025, Message: "CONSTRAINT `c` failed for `s`.`t`"}, http.StatusUnprocessableEntity, InsertRefusedReasonConstraintViolation},
		{"mssql not null", mssql.Error{Number: 515}, http.StatusUnprocessableEntity, InsertRefusedReasonConstraintViolation},
		{"oracle check", &network.OracleError{ErrCode: 2290}, http.StatusUnprocessableEntity, InsertRefusedReasonConstraintViolation},
		{"connection lost", errors.New("ERROR_DB_NOT_CONNECTED"), 0, ""},
		{"syntax", &pgconn.PgError{Code: "42601"}, 0, ""},
		{"unknown", errors.New("something else"), 0, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := insertError(c.err)
			var domainErr api.DXAPIDomainError
			isDomain := errors.As(got, &domainErr)
			if c.status == 0 {
				if isDomain {
					t.Fatalf("classified as a domain error %d, want it returned as it is", domainErr.DomainErrorHTTPStatusCode())
				}
				if got != c.err {
					t.Errorf("returned %v, want the same error", got)
				}
				return
			}
			if !isDomain {
				t.Fatalf("returned %T, want a domain error with status %d", got, c.status)
			}
			if domainErr.DomainErrorHTTPStatusCode() != c.status {
				t.Errorf("status = %d, want %d", domainErr.DomainErrorHTTPStatusCode(), c.status)
			}
			if domainErr.DomainErrorCode() != c.reason {
				t.Errorf("code = %q, want %q", domainErr.DomainErrorCode(), c.reason)
			}
			// Not errors.Is: mssql.Error is not comparable, so Is cannot match it.
			if inner := errors.Unwrap(got); inner == nil || inner.Error() != c.err.Error() {
				t.Errorf("the domain error unwraps to %v, want the insert error %v", inner, c.err)
			}
			if got.Error() == c.reason {
				t.Errorf("Error() = %q carries no detail for the server log", got.Error())
			}
		})
	}
	if insertError(nil) != nil {
		t.Errorf("insertError(nil) must be nil")
	}
}

// A database/sql driver whose every statement fails with one chosen driver
// error, so an insert runs the real chain (db.Insert -> DBOperationError ->
// CheckDatabaseError -> DoCreate) without a server.
type failingConnector struct{ err error }
type failingDriver struct{}
type failingConn struct{ err error }

func (c failingConnector) Connect(context.Context) (driver.Conn, error) {
	return failingConn{c.err}, nil
}
func (c failingConnector) Driver() driver.Driver          { return failingDriver{} }
func (failingDriver) Open(string) (driver.Conn, error)    { return nil, errors.New("not used") }
func (c failingConn) Prepare(string) (driver.Stmt, error) { return nil, c.err }
func (failingConn) Close() error                          { return nil }
func (failingConn) Begin() (driver.Tx, error)             { return nil, errors.New("not used") }
func (c failingConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return nil, c.err
}
func (c failingConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, c.err
}

func failingDatabase(err error) *databases.DXDatabase {
	return &databases.DXDatabase{
		NameId:     "failing",
		Connected:  true,
		Connection: sqlx.NewDb(sql.OpenDB(failingConnector{err}), "postgres"),
	}
}

func newCreateRequest() (*api.DXAPIEndPointRequest, *httptest.ResponseRecorder) {
	r := httptest.NewRequest(http.MethodPost, "/create", nil)
	rec := httptest.NewRecorder()
	var rw http.ResponseWriter = rec
	return &api.DXAPIEndPointRequest{
		Context:         r.Context(),
		Request:         r,
		ResponseWriter:  &rw,
		Log:             log.NewLog(nil, r.Context(), "insert-test"),
		ParameterValues: map[string]*api.DXAPIEndPointRequestParameterValue{},
		EndPoint:        &api.DXAPIEndPoint{Method: http.MethodPost, EndPointType: api.EndPointTypeHTTPJSON},
	}, rec
}

// The error each DoCreate returns through the real wrapped chain, and what the
// route handler would do with it: the status the client gets, a body that names
// only the reason, and the table, data and driver message kept for the log.
func TestTableDoCreateReturnsTheClientStatusThroughTheRealChain(t *testing.T) {
	const tableName = "widget"
	raw := func(d *databases.DXDatabase) DXRawTable {
		return DXRawTable{DatabaseNameId: d.NameId, Database: d, TableNameDirect: tableName, FieldNameForRowId: "id", FieldNameForRowUid: "uid", ResponseEnvelopeObjectName: "widget"}
	}
	creates := map[string]func(*databases.DXDatabase, *api.DXAPIEndPointRequest, utils.JSON) (int64, error){
		"DXTable": func(d *databases.DXDatabase, aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
			return (&DXTable{DXRawTable: raw(d)}).DoCreate(aepr, data)
		},
		"DXTableAuditOnly": func(d *databases.DXDatabase, aepr *api.DXAPIEndPointRequest, data utils.JSON) (int64, error) {
			return (&DXTableAuditOnly{DXRawTable: raw(d)}).DoCreate(aepr, data)
		},
	}
	cases := []struct {
		name       string
		driverErr  error
		constraint string // named in the driver message: must reach the log, never the client
		status     int    // 0: a server error, the route handler's 500
		reason     string
	}{
		{"duplicate", &pgconn.PgError{Code: "23505", Message: `duplicate key value violates unique constraint "widget_name_key"`}, "widget_name_key", http.StatusConflict, InsertRefusedReasonDuplicateKey},
		{"foreign key", &pgconn.PgError{Code: "23503", Message: `insert or update on table "widget" violates foreign key constraint "widget_owner_fk"`}, "widget_owner_fk", http.StatusUnprocessableEntity, InsertRefusedReasonConstraintViolation},
		{"mariadb check", &mysql.MySQLError{Number: 4025, Message: "CONSTRAINT `widget_chk` failed for `s`.`widget`"}, "widget_chk", http.StatusUnprocessableEntity, InsertRefusedReasonConstraintViolation},
		{"syntax", &pgconn.PgError{Code: "42601", Message: `syntax error at or near "FORM"`}, "", 0, ""},
	}
	for kind, create := range creates {
		for _, c := range cases {
			t.Run(kind+"/"+c.name, func(t *testing.T) {
				aepr, rec := newCreateRequest()
				id, err := create(failingDatabase(c.driverErr), aepr, utils.JSON{"name": "secret-value"})
				if err == nil {
					t.Fatalf("DoCreate returned id %d and no error", id)
				}
				if aepr.ResponseHeaderSent || rec.Body.Len() != 0 {
					t.Fatalf("DoCreate wrote a response itself (%d %s); the route handler owns the answer", rec.Code, rec.Body.String())
				}

				// The driver error is still the cause, however it was relabelled.
				var dbErr *db.DBOperationError
				if c.status != http.StatusConflict && !errors.As(err, &dbErr) {
					t.Errorf("the insert context (DBOperationError) is gone: %v", err)
				}

				var domainErr api.DXAPIDomainError
				if c.status == 0 {
					if errors.As(err, &domainErr) {
						t.Fatalf("a %s is answered %d, want the route handler's 500", c.name, domainErr.DomainErrorHTTPStatusCode())
					}
					return
				}
				if !errors.As(err, &domainErr) {
					t.Fatalf("returned %T (%v), want a domain error answering %d", err, err, c.status)
				}
				if domainErr.DomainErrorHTTPStatusCode() != c.status {
					t.Errorf("status = %d, want %d", domainErr.DomainErrorHTTPStatusCode(), c.status)
				}
				body := domainErr.DomainErrorResponseBody()
				if body["reason"] != c.reason || body["reason_message"] != c.reason {
					t.Errorf("body = %v, want reason and reason_message %q", body, c.reason)
				}
				for k, v := range body {
					s, _ := v.(string)
					if strings.Contains(s, tableName) || strings.Contains(s, "secret-value") || strings.Contains(s, c.constraint) {
						t.Errorf("body %s = %q leaks the table, the data or the driver message", k, s)
					}
				}
				// A duplicate is relabelled by CheckDatabaseError, which keeps the
				// driver's message but drops the insert context; a constraint
				// violation keeps the whole DBOperationError.
				details := domainErr.DomainErrorLogDetails()
				if !strings.Contains(details, c.constraint) {
					t.Errorf("log details %q lack the driver message naming %q", details, c.constraint)
				}
				if c.status != http.StatusConflict && !strings.Contains(details, "table="+tableName) {
					t.Errorf("log details %q lack the table", details)
				}
			})
		}
	}
}
