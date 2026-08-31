package export

import (
	"testing"

	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/utils"
)

// One column and one row, so each writer has something real to serialise.
func sample() (*db.DXDatabaseTableRowsInfo, []utils.JSON) {
	return &db.DXDatabaseTableRowsInfo{Columns: []string{"fullname"}},
		[]utils.JSON{{"fullname": "Budi"}}
}

// tables.DXTableExportFormatEnumSetAll offers xls, xlsx and csv, and an endpoint
// declaring that enum will let any of the three through. Every one of them has to
// reach a writer here: xlsx did not, so a request naming it was refused before a
// byte was written, and the only client asking for a spreadsheet asked for xlsx.
func TestExportToStreamAcceptsEveryFormatTheEnumOffers(t *testing.T) {
	rowsInfo, rows := sample()
	for _, f := range []ExportFormat{CSV, XLS, XLSX} {
		_, contentType, err := ExportToStream(rowsInfo, rows, ExportOptions{Format: f, SheetName: "Sheet1"})
		if err != nil {
			t.Errorf("format %q was refused: %v", f, err)
			continue
		}
		if contentType == "" {
			t.Errorf("format %q returned no content type", f)
		}
	}
}

// xls and xlsx are the same bytes -- excelize writes OOXML for both -- so they
// have to answer with the same content type, or a browser is told one thing about
// a file that is the other.
func TestSpreadsheetFormatsAgreeOnContentType(t *testing.T) {
	rowsInfo, rows := sample()
	_, xls, err := ExportToStream(rowsInfo, rows, ExportOptions{Format: XLS, SheetName: "Sheet1"})
	if err != nil {
		t.Fatal(err)
	}
	_, xlsx, err := ExportToStream(rowsInfo, rows, ExportOptions{Format: XLSX, SheetName: "Sheet1"})
	if err != nil {
		t.Fatal(err)
	}
	if xls != xlsx {
		t.Errorf("xls says %q, xlsx says %q", xls, xlsx)
	}
}
