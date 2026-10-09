package export

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/internal/xlsxtest"
	"github.com/donnyhardyanto/dxlib/utils"
)

// excelize.NewFile made only Sheet1, so any other SheetName failed with
// "sheet Data does not exist".
func TestXLSXHonoursSheetName(t *testing.T) {
	rowsInfo, rows := sample()
	data, _, err := ExportToStream(rowsInfo, rows, ExportOptions{Format: XLSX, SheetName: "Data"})
	if err != nil {
		t.Fatal(err)
	}
	wb, err := xlsxtest.Read(data)
	if err != nil {
		t.Fatal(err)
	}
	if wb.SheetName != "Data" {
		t.Errorf("sheet is %q, want Data", wb.SheetName)
	}
}

func TestXLSXEmptySheetNameIsSheet1(t *testing.T) {
	rowsInfo, rows := sample()
	data, _, err := ExportToStream(rowsInfo, rows, ExportOptions{Format: XLSX})
	if err != nil {
		t.Fatal(err)
	}
	wb, err := xlsxtest.Read(data)
	if err != nil {
		t.Fatal(err)
	}
	if wb.SheetName != "Sheet1" {
		t.Errorf("sheet is %q, want Sheet1", wb.SheetName)
	}
}

// The header row is bold, centred and filled as before, every column is 15
// wide, and the values are what formatValue gives.
func TestXLSXContent(t *testing.T) {
	rowsInfo := &db.DXDatabaseTableRowsInfo{Columns: []string{"fullname", "tags", "note"}}
	rows := []utils.JSON{
		{"fullname": "Budi", "tags": []string{"a", "b"}, "note": nil},
		{"fullname": "Siti", "tags": []string{}, "note": []byte("x")},
	}
	data, _, err := ExportToStream(rowsInfo, rows, ExportOptions{Format: XLS})
	if err != nil {
		t.Fatal(err)
	}
	wb, err := xlsxtest.Read(data)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"fullname", "tags", "note"}, {"Budi", "a, b", ""}, {"Siti", "All", "x"}}
	if !reflect.DeepEqual(wb.Rows, want) {
		t.Errorf("rows %q, want %q", wb.Rows, want)
	}
	if !reflect.DeepEqual(wb.ColumnWidths, []float64{15, 15, 15}) {
		t.Errorf("widths %v", wb.ColumnWidths)
	}
	if wb.RowStyles[0] != 1 || wb.CellStyles[0][2] != 1 || wb.CellStyles[1][0] != 0 {
		t.Errorf("styles: header row %d, cells %v / %v", wb.RowStyles[0], wb.CellStyles[0], wb.CellStyles[1])
	}
}

// ExportQueryResults knew only csv and xls and refused xlsx.
func TestExportQueryResultsWritesXLSX(t *testing.T) {
	rowsInfo, rows := sample()
	for _, f := range []ExportFormat{XLS, XLSX} {
		path := filepath.Join(t.TempDir(), "out."+string(f))
		if err := ExportQueryResults(rowsInfo, rows, ExportOptions{Format: f, FilePath: path, SheetName: "Data"}); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		wb, err := xlsxtest.Read(b)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(wb.Rows, [][]string{{"fullname"}, {"Budi"}}) {
			t.Errorf("%s: rows %q", f, wb.Rows)
		}
	}
}

// A refused workbook leaves no half-written file behind.
func TestExportQueryResultsRemovesFailedFile(t *testing.T) {
	rowsInfo, rows := sample()
	path := filepath.Join(t.TempDir(), "out.xlsx")
	if err := ExportQueryResults(rowsInfo, rows, ExportOptions{Format: XLSX, FilePath: path, SheetName: "a/b"}); err == nil {
		t.Fatal("sheet name a/b was accepted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("the failed file is still there: %v", err)
	}
}
