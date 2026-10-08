package tables

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/donnyhardyanto/dxlib/api"
	"github.com/donnyhardyanto/dxlib/databases"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/sijms/go-ora/v2/network"
)

// 409 is for a duplicate key only; a row the schema refuses is 422; a database
// that failed is 500. DoCreate used to answer 409 for all three.
func TestInsertErrorStatusCode(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want int
	}{
		{"pg duplicate", &pgconn.PgError{Code: "23505"}, http.StatusConflict},
		{"mysql duplicate", &mysql.MySQLError{Number: 1062}, http.StatusConflict},
		{"mssql duplicate", mssql.Error{Number: 2627}, http.StatusConflict},
		{"oracle duplicate", &network.OracleError{ErrCode: 1}, http.StatusConflict},
		{"duplicate relabelled by CheckDatabaseError", databases.CheckDatabaseError(&pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint"}), http.StatusConflict},

		{"pg foreign key", &pgconn.PgError{Code: "23503"}, http.StatusUnprocessableEntity},
		{"pg check", &pgconn.PgError{Code: "23514"}, http.StatusUnprocessableEntity},
		{"pg not null", &pgconn.PgError{Code: "23502"}, http.StatusUnprocessableEntity},
		{"mysql foreign key", &mysql.MySQLError{Number: 1452}, http.StatusUnprocessableEntity},
		{"mariadb check", &mysql.MySQLError{Number: 4025, Message: "CONSTRAINT `c` failed for `s`.`t`"}, http.StatusUnprocessableEntity},
		{"mssql foreign key or check", mssql.Error{Number: 547}, http.StatusUnprocessableEntity},
		{"oracle parent missing", &network.OracleError{ErrCode: 2291}, http.StatusUnprocessableEntity},
		{"oracle not null", &network.OracleError{ErrCode: 1400}, http.StatusUnprocessableEntity},

		{"connection lost", errors.New("ERROR_DB_NOT_CONNECTED"), http.StatusInternalServerError},
		{"syntax", &pgconn.PgError{Code: "42601"}, http.StatusInternalServerError},
		{"unknown", errors.New("something else"), http.StatusInternalServerError},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := insertErrorStatusCode(c.err); got != c.want {
				t.Errorf("status = %d, want %d", got, c.want)
			}
		})
	}
}

// The response the client gets: the chosen status in the header and in the
// body, with the error text as the reason, the same body shape as before.
func TestWriteInsertErrorAnswersWithTheChosenStatus(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want int
	}{
		{"duplicate", &pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint"}, http.StatusConflict},
		{"foreign key", &pgconn.PgError{Code: "23503", Message: "violates foreign key constraint"}, http.StatusUnprocessableEntity},
		{"database down", errors.New("ERROR_DB_NOT_CONNECTED"), http.StatusInternalServerError},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/create", nil)
			rec := httptest.NewRecorder()
			var rw http.ResponseWriter = rec
			aepr := &api.DXAPIEndPointRequest{
				Request:         r,
				ResponseWriter:  &rw,
				Log:             log.NewLog(nil, r.Context(), "insert-test"),
				ParameterValues: map[string]*api.DXAPIEndPointRequestParameterValue{},
				EndPoint:        &api.DXAPIEndPoint{Method: http.MethodPost, EndPointType: api.EndPointTypeHTTPJSON},
			}

			writeInsertError(aepr, c.err)

			if rec.Code != c.want {
				t.Fatalf("status = %d, want %d", rec.Code, c.want)
			}
			var body map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("body is not JSON: %v: %s", err, rec.Body.String())
			}
			if got, _ := body["status_code"].(float64); int(got) != c.want {
				t.Errorf("body status_code = %v, want %d", body["status_code"], c.want)
			}
			if body["reason"] == nil || body["reason"] == "" {
				t.Errorf("body carries no reason: %s", rec.Body.String())
			}
		})
	}
}
