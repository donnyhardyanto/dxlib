package export

import (
	"bytes"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"

	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/internal/xlsxtest"
	"github.com/donnyhardyanto/dxlib/utils"
)

// A spreadsheet reads a CSV cell that starts with = + - @, or a tab or carriage
// return, as a formula (CWE-1236). Security-event rows carry text the client chose,
// such as its User-Agent, so every such cell must reach the file neutralised.
var formulaCells = []string{
	"=HYPERLINK(\"http://x\",\"y\")",
	"+1+cmd|' /C calc'!A0",
	"-2+3",
	"@SUM(A1:A2)",
	"\t=1+1",
	"\r=1+1",
}

// Plain decimal numbers stay numbers: a leading sign is not a formula there.
var numericCells = []string{"-5", "+1.5", "-2e3", "-.5", "0", "12"}

func csvCells(t *testing.T, values []string) []string {
	t.Helper()
	rowsInfo := &db.DXDatabaseTableRowsInfo{Columns: []string{"user_agent"}}
	rows := make([]utils.JSON, len(values))
	for i, v := range values {
		rows[i] = utils.JSON{"user_agent": v}
	}
	data, _, err := ExportToStream(rowsInfo, rows, ExportOptions{Format: CSV})
	if err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(bytes.NewReader(data)).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	cells := make([]string, 0, len(values))
	for _, r := range records[1:] {
		cells = append(cells, r[0])
	}
	return cells
}

func TestCSVNeutralisesFormulaCells(t *testing.T) {
	for i, got := range csvCells(t, formulaCells) {
		if got != "'"+formulaCells[i] {
			t.Errorf("cell %q came out as %q, want it prefixed with a single quote", formulaCells[i], got)
		}
	}
}

func TestCSVKeepsNumbers(t *testing.T) {
	for i, got := range csvCells(t, numericCells) {
		if got != numericCells[i] {
			t.Errorf("number %q came out as %q", numericCells[i], got)
		}
	}
}

// The XLSX writer stores strings as text, never as a formula, so it keeps the
// raw value. xlsxtest.Read refuses any cell that is not a shared string or
// that carries a formula.
func TestXLSXKeepsRawValue(t *testing.T) {
	rowsInfo := &db.DXDatabaseTableRowsInfo{Columns: []string{"user_agent"}}
	rows := []utils.JSON{{"user_agent": formulaCells[0]}}
	data, _, err := ExportToStream(rowsInfo, rows, ExportOptions{Format: XLSX, SheetName: "Sheet1"})
	if err != nil {
		t.Fatal(err)
	}
	wb, err := xlsxtest.Read(data)
	if err != nil {
		t.Fatal(err)
	}
	if got := wb.Rows[1][0]; got != formulaCells[0] {
		t.Errorf("XLSX cell is %q, want %q", got, formulaCells[0])
	}
}

// The file writer takes the same path as the stream writer.
func TestCSVFileNeutralisesFormulaCells(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.csv")
	rowsInfo := &db.DXDatabaseTableRowsInfo{Columns: []string{"user_agent"}}
	rows := []utils.JSON{{"user_agent": formulaCells[0]}, {"user_agent": "-5"}}
	if err := ExportQueryResults(rowsInfo, rows, ExportOptions{Format: CSV, FilePath: path}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(bytes.NewReader(data)).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if records[1][0] != "'"+formulaCells[0] || records[2][0] != "-5" {
		t.Errorf("file cells are %q and %q", records[1][0], records[2][0])
	}
}
