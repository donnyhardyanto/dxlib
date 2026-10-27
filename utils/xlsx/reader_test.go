package xlsx

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// golden is what testdata/goldens/generate.go records from excelize: GetRows,
// and GetRows with RawCellValue.
type golden struct {
	Excelize string                `json:"excelize"`
	Sheets   []string              `json:"sheets"`
	Rows     map[string][][]string `json:"rows"`
	Raw      map[string][][]string `json:"raw"`
}

// knownDifferences are the rows where the reader and excelize disagree on
// purpose, by fixture, sheet and the row's first cell (DESIGN.md §11.4). The
// reader gives the value as stored there; TestUnsupportedFormats checks it.
// Raw values have none.
var knownDifferences = map[string]string{
	"excelize.xlsx/Formats/id 12": "fractions (# ?/?) are not supported",
}

func TestReaderMatchesExcelize(t *testing.T) {
	names, err := filepath.Glob("testdata/*.golden.json")
	if err != nil || len(names) == 0 {
		t.Fatalf("no golden files: %v", err)
	}
	for _, name := range names {
		fixture := strings.TrimSuffix(name, ".golden.json") + ".xlsx"
		t.Run(filepath.Base(fixture), func(t *testing.T) {
			var g golden
			b, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(b, &g); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(fixture)
			if err != nil {
				t.Fatal(err)
			}
			x, err := OpenBytes(data, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer x.Close()
			if got := x.GetSheetList(); strings.Join(got, "\x00") != strings.Join(g.Sheets, "\x00") {
				t.Fatalf("sheets %q, excelize %q", got, g.Sheets)
			}
			for _, sheet := range g.Sheets {
				rows, err := x.GetRows(sheet)
				if err != nil {
					t.Errorf("%s: %v", sheet, err)
					continue
				}
				compareRows(t, filepath.Base(fixture), sheet, rows, g.Rows[sheet])
				want, ok := g.Raw[sheet]
				if !ok {
					t.Errorf("%s: no raw rows in %s; rerun run.sh", sheet, name)
					continue
				}
				raw, err := x.GetRowsRaw(sheet)
				if err != nil {
					t.Errorf("%s, raw: %v", sheet, err)
					continue
				}
				compareRows(t, "", sheet+" (raw)", raw, want)
			}
		})
	}
}

func compareRows(t *testing.T, fixture, sheet string, got, want [][]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: %d rows, excelize %d", sheet, len(got), len(want))
	}
	for i := 0; i < len(got) && i < len(want); i++ {
		if len(got[i]) > 0 && knownDifferences[fixture+"/"+sheet+"/"+got[i][0]] != "" {
			continue
		}
		if len(got[i]) != len(want[i]) {
			t.Errorf("%s row %d: %d cells %q, excelize %d %q", sheet, i+1, len(got[i]), got[i], len(want[i]), want[i])
			continue
		}
		for j := range got[i] {
			if got[i][j] == want[i][j] {
				continue
			}
			t.Errorf("%s %s%d (%s): %q, excelize %q", sheet, columnLetters(j+1), i+1, got[i][0], got[i][j], want[i][j])
		}
	}
}

func columnLetters(n int) string {
	s, _ := columnName(n)
	return s
}
