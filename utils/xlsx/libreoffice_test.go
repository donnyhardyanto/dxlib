//go:build libreoffice

package xlsx

import (
	"bytes"
	"context"
	"encoding/csv"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
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
