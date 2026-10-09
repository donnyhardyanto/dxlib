package xlsx

import (
	"bytes"
	"testing"

	"github.com/xuri/excelize/v2"
)

// An independent reader agrees with ours on every cell. This goes away with the
// excelize dependency.
func TestExcelizeReadsWhatWeWrite(t *testing.T) {
	header := []string{"Name", "Kota"}
	data := write(t, Options{SheetName: "Data", ColumnWidths: []float64{15, 15}, HeaderStyle: &Style{Bold: true, Center: true, FillRGB: "E0EBF5"}},
		header, coverage...)
	f, err := excelize.OpenReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got, err := f.GetRows("Data", excelize.Options{RawCellValue: false})
	if err != nil {
		t.Fatal(err)
	}
	want := append([][]string{header}, coverage...)
	for r := range want {
		for c := range want[r] {
			cell := ""
			if r < len(got) && c < len(got[r]) {
				cell = got[r][c]
			}
			if exp := cleanText(want[r][c]); cell != exp {
				t.Errorf("row %d col %d: excelize reads %q, want %q", r+1, c+1, short(cell), short(exp))
			}
		}
	}
	if w, _ := f.GetColWidth("Data", "B"); w != 15 {
		t.Errorf("width of B is %v", w)
	}
}

func short(s string) string {
	if len(s) > 40 {
		return s[:20] + "…" + s[len(s)-20:]
	}
	return s
}
