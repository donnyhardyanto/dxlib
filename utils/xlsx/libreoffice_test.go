//go:build libreoffice

package xlsx

import (
	"bytes"
	"context"
	"encoding/csv"
	"os"
	"os/exec"
	"path/filepath"
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
