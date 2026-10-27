//go:build libreoffice

package xlsx

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/xml"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func soffice(t *testing.T) string {
	if p, err := exec.LookPath("soffice"); err == nil {
		return p
	}
	const mac = "/Applications/LibreOffice.app/Contents/MacOS/soffice"
	if _, err := os.Stat(mac); err == nil {
		return mac
	}
	t.Skip("LibreOffice (soffice) is not installed")
	return ""
}

// LibreOffice opens the file and shows every cell as written. Run it with
//
//	go test -tags libreoffice ./utils/xlsx/
func TestLibreOfficeReadsWhatWeWrite(t *testing.T) {
	bin := soffice(t)
	dir := t.TempDir()
	header := []string{"Name", "Kota"}
	data := write(t, Options{SheetName: "Data", ColumnWidths: []float64{15, 15}, HeaderStyle: &Style{Bold: true, Center: true, FillRGB: "E0EBF5"}},
		header, coverage...)
	in := filepath.Join(dir, "book.xlsx")
	if err := os.WriteFile(in, data, 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// Its own profile keeps it clear of a LibreOffice the user has open.
	// Filter options: comma, double quote, UTF-8 (76), start at line 1.
	cmd := exec.CommandContext(ctx, bin, "-env:UserInstallation=file://"+filepath.Join(dir, "profile"), "--headless",
		"--convert-to", "csv:Text - txt - csv (StarCalc):44,34,76,1", "--outdir", dir, in)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("soffice: %v\n%s", err, out)
	}
	b, err := os.ReadFile(filepath.Join(dir, "book.csv"))
	if err != nil {
		t.Fatal(err)
	}
	// LibreOffice keeps a line break in a cell as LF, so "a\r\nb" shows as
	// "a\nb" there; excelize (excelize_test.go) and the test reader keep the CR,
	// which the file carries as &#xD;. encoding/csv would drop it anyway.
	r := csv.NewReader(bytes.NewReader(b))
	r.FieldsPerRecord = -1
	got, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	want := append([][]string{header}, coverage...)
	for i := range want {
		for j := range want[i] {
			cell := ""
			if i < len(got) && j < len(got[i]) {
				cell = got[i][j]
			}
			if exp := strings.ReplaceAll(cleanText(want[i][j]), "\r\n", "\n"); cell != exp {
				t.Errorf("row %d col %d: LibreOffice shows %q, want %q", i+1, j+1, short(cell), short(exp))
			}
		}
	}
}

// convert runs LibreOffice on the file with a CSV filter and returns the CSV
// of each sheet by name.
func convert(t *testing.T, bin, dir string, data []byte, filter string, sheets []string) map[string][][]string {
	t.Helper()
	out := filepath.Join(dir, "out")
	in := filepath.Join(dir, "book.xlsx")
	if err := os.WriteFile(in, data, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "-env:UserInstallation=file://"+filepath.Join(dir, "profile"), "--headless",
		"--convert-to", "csv:Text - txt - csv (StarCalc):"+filter, "--outdir", out, in)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("soffice: %v\n%s", err, b)
	}
	got := map[string][][]string{}
	for _, s := range sheets {
		b, err := os.ReadFile(filepath.Join(out, "book-"+s+".csv"))
		if err != nil {
			t.Fatal(err)
		}
		r := csv.NewReader(bytes.NewReader(b))
		r.FieldsPerRecord = -1
		if got[s], err = r.ReadAll(); err != nil {
			t.Fatal(err)
		}
	}
	return got
}

// LibreOffice opens a workbook of several sheets with numbers, styles and
// merges, and shows every value as written. The 12th filter token, -1, writes
// one CSV per sheet; the 9th says whether cells are written as shown.
func TestLibreOfficeReadsAWorkbook(t *testing.T) {
	bin := soffice(t)
	data := writeBOM(t)
	sheets := []string{"Ringkasan", "Item", "Kosong"}

	raw := convert(t, bin, t.TempDir(), data, "44,34,76,1,,1033,false,true,false,false,false,-1", sheets)
	want := map[string][][]string{
		"Ringkasan": {{"Daftar Bahan"}, {"Total", "1234567"}, {"Catatan panjang yang dibungkus"}},
		"Item": {
			{"No", "Uraian", "Jumlah", "Harga"},
			{"", "", "(unit)", "(Rp)"},
			{"1", "Kabel NYY 4x16", "12.5", "185000"},
			{"2", "-5", "", "-0.25"},
		},
	}
	for _, s := range sheets {
		if !reflect.DeepEqual(trimRows(raw[s]), want[s]) {
			t.Errorf("sheet %s: LibreOffice shows %q, want %q", s, raw[s], want[s])
		}
	}

	// As shown, in English (United States): the money format applies.
	shown := convert(t, bin, t.TempDir(), data, "44,34,76,1,,1033,false,true,true,false,false,-1", sheets)
	if got := shown["Ringkasan"][1][1]; got != "Rp1,234,567" {
		t.Errorf("1234567 in \"Rp\"#,##0 shows as %q", got)
	}
	if got := shown["Item"][2][3]; got != "Rp185,000" {
		t.Errorf("185000 in \"Rp\"#,##0 shows as %q", got)
	}
}

// trimRows drops trailing empty cells and empty rows, which LibreOffice pads
// to the used range.
func trimRows(rows [][]string) [][]string {
	var out [][]string
	for _, r := range rows {
		for len(r) > 0 && r[len(r)-1] == "" {
			r = r[:len(r)-1]
		}
		out = append(out, r)
	}
	for len(out) > 0 && len(out[len(out)-1]) == 0 {
		out = out[:len(out)-1]
	}
	if out == nil {
		return nil
	}
	return out
}

// LibreOffice keeps a sheet's default row height: a single-line row of a
// 15 pt sheet shows at 15 pt, and a row whose wrapped text needs more still
// grows. Without the height it sizes the row from the font it has.
func TestLibreOfficeKeepsTheDefaultRowHeight(t *testing.T) {
	bin := soffice(t)
	dir := t.TempDir()
	var buf bytes.Buffer
	b, err := NewWorkbook(&buf, WorkbookOptions{Sheets: []SheetOptions{{Name: "Set", DefaultRowHeight: 15}, {Name: "Unset"}}})
	if err != nil {
		t.Fatal(err)
	}
	wrap, _ := b.AddStyle(CellStyle{Wrap: true})
	rows := []Row{{Cells: []Cell{Text("one", 0)}}, {Cells: []Cell{Text("two\nlines", wrap)}}, {Cells: []Cell{Text("three", 0)}}}
	for i := 0; i < 2; i++ {
		for _, r := range rows {
			if err := b.WriteRow(r); err != nil {
				t.Fatal(err)
			}
		}
		if i == 0 {
			if err := b.NextSheet(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	in := filepath.Join(dir, "book.xlsx")
	if err := os.WriteFile(in, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "-env:UserInstallation=file://"+filepath.Join(dir, "profile"), "--headless",
		"--convert-to", "ods", "--outdir", dir, in)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("soffice: %v\n%s", err, out)
	}
	heights := odsRowHeights(t, filepath.Join(dir, "book.ods"))
	set, unset := heights["Set"], heights["Unset"]
	if len(set) < 3 || len(unset) < 3 {
		t.Fatalf("row heights %v", heights)
	}
	t.Logf("single-line rows: %.2f pt with the default height, %.2f pt without", set[0], unset[0])
	for _, i := range []int{0, 2} {
		if math.Abs(set[i]-15) > 0.1 {
			t.Errorf("row %d of Set is %.2f pt, want 15", i+1, set[i])
		}
	}
	if set[1] <= 15 {
		t.Errorf("the wrapped row of Set is %.2f pt, want it grown past 15", set[1])
	}
}

// odsRowHeights reads each table's row heights, in points, from an ODS file's
// content.xml.
func odsRowHeights(t *testing.T, path string) map[string][]float64 {
	t.Helper()
	z, err := zip.OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	f, err := z.Open("content.xml")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var doc struct {
		Styles []struct {
			Name string `xml:"name,attr"`
			Row  struct {
				Height string `xml:"row-height,attr"`
			} `xml:"table-row-properties"`
		} `xml:"automatic-styles>style"`
		Tables []struct {
			Name string `xml:"name,attr"`
			Rows []struct {
				Style  string `xml:"style-name,attr"`
				Repeat int    `xml:"number-rows-repeated,attr"`
			} `xml:"table-row"`
		} `xml:"body>spreadsheet>table"`
	}
	if err := xml.NewDecoder(f).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	style := map[string]float64{}
	for _, s := range doc.Styles {
		if s.Row.Height != "" {
			style[s.Name] = points(t, s.Row.Height)
		}
	}
	out := map[string][]float64{}
	for _, tb := range doc.Tables {
		for _, r := range tb.Rows {
			out[tb.Name] = append(out[tb.Name], style[r.Style])
		}
	}
	return out
}

func points(t *testing.T, length string) float64 {
	for unit, per := range map[string]float64{"in": 72, "cm": 72 / 2.54, "mm": 72 / 25.4, "pt": 1} {
		if v, ok := strings.CutSuffix(length, unit); ok {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Fatal(err)
			}
			return f * per
		}
	}
	t.Fatalf("length %q has no unit", length)
	return 0
}
