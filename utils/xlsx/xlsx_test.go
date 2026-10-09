package xlsx

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/donnyhardyanto/dxlib/internal/xlsxtest"
)

func write(t *testing.T, opts Options, header []string, rows ...[]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := NewWriter(&buf, opts)
	if err != nil {
		t.Fatal(err)
	}
	if header != nil {
		if err := w.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range rows {
		if err := w.WriteRow(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func read(t *testing.T, data []byte) *xlsxtest.Workbook {
	t.Helper()
	wb, err := xlsxtest.Read(data)
	if err != nil {
		t.Fatal(err)
	}
	return wb
}

func TestColumnName(t *testing.T) {
	for n, want := range map[int]string{1: "A", 26: "Z", 27: "AA", 52: "AZ", 53: "BA", 702: "ZZ", 703: "AAA", 16384: "XFD"} {
		got, err := columnName(n)
		if err != nil || got != want {
			t.Errorf("columnName(%d) = %q, %v; want %q", n, got, err, want)
		}
	}
	for _, n := range []int{0, -1, 16385} {
		if _, err := columnName(n); err == nil {
			t.Errorf("columnName(%d) was accepted", n)
		}
	}
}

func TestEncodeCellText(t *testing.T) {
	for _, c := range []struct {
		in, want string
		preserve bool
	}{
		{"plain", "plain", false},
		{"<a & b>", "&lt;a &amp; b&gt;", false},
		{`say "hi" it's`, `say "hi" it's`, false},
		{" lead", " lead", true},
		{"trail ", "trail ", true},
		{"\tx", "\tx", true},
		{"x\n", "x\n", true},
		{"a\tb\nc", "a\tb\nc", false},
		{"a\rb", "a&#xD;b", false},
		{"\r", "&#xD;", true},
		{"a\x01b", "a_x0001_b", false},
		{"\x00\x08\x0b\x0c\x0e\x1f", "_x0000__x0008__x000B__x000C__x000E__x001F_", false},
		{"￾￿", "_xFFFE__xFFFF_", false},
		{"_x0041_", "_x005F_x0041_", false},
		{"_x0041_x0042_", "_x005F_x0041_x005F_x0042_", false},
		{"_xabcd_", "_x005F_xabcd_", false},
		{"_x004_", "_x004_", false},
		{"_xZZZZ_", "_xZZZZ_", false},
		{"_x0041\x01", "_x005F_x0041_x0001_", false},
		{"a\xffb", "a�b", false},
		{"\xff\xfe", "��", false},
		{"=HYPERLINK(\"x\")", "=HYPERLINK(\"x\")", false},
		{"", "", false},
	} {
		got, preserve := encodeCellText(c.in)
		if got != c.want || preserve != c.preserve {
			t.Errorf("encodeCellText(%q) = %q, %v; want %q, %v", c.in, got, preserve, c.want, c.preserve)
		}
	}
}

func TestCellTextIsCutAtExcelsLimit(t *testing.T) {
	long := strings.Repeat("a", MaxCellUnits+1)
	if got := cleanText(long); len(got) != MaxCellUnits {
		t.Errorf("a %d-unit string kept %d units", MaxCellUnits+1, len(got))
	}
	// An emoji is two UTF-16 units. With its first half at unit 32,767 it does
	// not fit, and it is dropped whole.
	split := strings.Repeat("a", MaxCellUnits-1) + "😀"
	got := cleanText(split)
	if got != strings.Repeat("a", MaxCellUnits-1) || !utf8.ValidString(got) {
		t.Errorf("an emoji across the limit left %d bytes", len(got))
	}
	fits := strings.Repeat("a", MaxCellUnits-2) + "😀"
	if cleanText(fits) != fits {
		t.Error("an emoji ending at the limit was cut")
	}
}

func TestSheetNames(t *testing.T) {
	bad := []string{
		"", strings.Repeat("a", 32), "'a", "a'", "History", "HISTORY", "a\xffb", "a_x0041_",
		"￾", "￿",
	}
	for _, c := range `[]:*?/\` {
		bad = append(bad, "a"+string(c)+"b")
	}
	for r := rune(0); r < 0x20; r++ {
		bad = append(bad, "a"+string(r)+"b")
	}
	for _, name := range bad {
		if _, err := NewWriter(io.Discard, Options{SheetName: name}); err == nil {
			t.Errorf("sheet name %q was accepted", name)
		}
	}
	for _, name := range []string{"Sheet1", strings.Repeat("a", 31), `a"b`, "it's", "a&b", "<x>", "Data 2026", "Penjualan—Q3", "日本"} {
		wb := read(t, write(t, Options{SheetName: name}, nil, []string{"x"}))
		if wb.SheetName != name {
			t.Errorf("sheet name %q read back as %q", name, wb.SheetName)
		}
	}
}

func TestPartOrder(t *testing.T) {
	wb := read(t, write(t, Options{SheetName: "Sheet1"}, []string{"h"}, []string{"v"}))
	want := []string{
		"[Content_Types].xml", "_rels/.rels", "xl/workbook.xml", "xl/_rels/workbook.xml.rels",
		"xl/styles.xml", "xl/worksheets/sheet1.xml", "xl/sharedStrings.xml",
	}
	if !reflect.DeepEqual(wb.Parts, want) {
		t.Errorf("parts in order %q, want %q", wb.Parts, want)
	}
}

// Every part is well-formed XML, every part named in [Content_Types].xml
// exists, and every relationship points at a part that exists.
func TestPartsAreConsistent(t *testing.T) {
	for _, style := range []*Style{nil, {Bold: true, Center: true, FillRGB: "E0EBF5"}, {Center: true}, {FillRGB: "ff0000"}} {
		wb := read(t, write(t, Options{SheetName: "S", ColumnWidths: []float64{15}, HeaderStyle: style}, []string{"h"}, []string{"v"}))
		for name, b := range wb.Raw {
			d := xml.NewDecoder(bytes.NewReader(b))
			for {
				_, err := d.Token()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
			}
		}
		var types struct {
			Overrides []struct {
				PartName string `xml:"PartName,attr"`
			} `xml:"Override"`
		}
		if err := xml.Unmarshal(wb.Raw["[Content_Types].xml"], &types); err != nil {
			t.Fatal(err)
		}
		for _, o := range types.Overrides {
			if _, ok := wb.Raw[strings.TrimPrefix(o.PartName, "/")]; !ok {
				t.Errorf("content type names %s, which is missing", o.PartName)
			}
		}
		for rels, dir := range map[string]string{"_rels/.rels": "", "xl/_rels/workbook.xml.rels": "xl/"} {
			var r struct {
				Rels []struct {
					Target string `xml:"Target,attr"`
				} `xml:"Relationship"`
			}
			if err := xml.Unmarshal(wb.Raw[rels], &r); err != nil {
				t.Fatal(err)
			}
			for _, rel := range r.Rels {
				if _, ok := wb.Raw[dir+rel.Target]; !ok {
					t.Errorf("%s points at %s, which is missing", rels, dir+rel.Target)
				}
			}
		}
	}
}

func TestRoundTrip(t *testing.T) {
	header := []string{"Name", "Kota", "Catatan"}
	rows := [][]string{
		{"Budi", "Bandung", ""},
		{"Siti", "Bandung", "日本語 😀 é"},
		{"", "", ""},
		{"_x0041_", "a\x01b", " spaced "},
		{"=1+1", "a\r\nb", "<&>"},
		{"one"},
	}
	data := write(t, Options{
		SheetName:    "Data",
		ColumnWidths: []float64{15, 15, 0, 20},
		HeaderStyle:  &Style{Bold: true, Center: true, FillRGB: "E0EBF5"},
	}, header, rows...)
	wb := read(t, data)
	want := append([][]string{header}, rows...)
	if !reflect.DeepEqual(wb.Rows, want) {
		t.Errorf("rows read back as %q, want %q", wb.Rows, want)
	}
	if wb.RowStyles[0] != 1 || wb.CellStyles[0][0] != 1 || wb.CellStyles[0][2] != 1 {
		t.Errorf("header styles %v %v, want 1", wb.RowStyles[0], wb.CellStyles[0])
	}
	if wb.RowStyles[1] != 0 || wb.CellStyles[1][0] != 0 {
		t.Error("a data row carries a style")
	}
	if !reflect.DeepEqual(wb.ColumnWidths, []float64{15, 15, 0, 20}) {
		t.Errorf("widths %v", wb.ColumnWidths)
	}
	// "Bandung" and "" appear more than once and are stored once each.
	if n := bytes.Count(wb.Raw["xl/sharedStrings.xml"], []byte("<t>Bandung</t>")); n != 1 {
		t.Errorf("Bandung is in the table %d times", n)
	}
	if bytes.Contains(wb.Raw["xl/worksheets/sheet1.xml"], []byte("<f>")) {
		t.Error("the sheet holds a formula")
	}
}

func TestNoHeaderStyleAndNoWidths(t *testing.T) {
	data := write(t, Options{SheetName: "S"}, []string{"h"}, []string{"v"})
	wb := read(t, data)
	if wb.RowStyles[0] != 0 || wb.CellStyles[0][0] != 0 {
		t.Error("a header with no style carries one")
	}
	if bytes.Contains(wb.Raw["xl/worksheets/sheet1.xml"], []byte("<cols")) {
		t.Error("<cols> written with no width set")
	}
	if data := write(t, Options{SheetName: "S", ColumnWidths: []float64{0, 0}}, nil); bytes.Contains(read(t, data).Raw["xl/worksheets/sheet1.xml"], []byte("<cols")) {
		t.Error("<cols> written with only zero widths")
	}
}

func TestEmptyWorkbook(t *testing.T) {
	wb := read(t, write(t, Options{SheetName: "S"}, nil))
	if len(wb.Rows) != 0 {
		t.Errorf("%d rows in an empty workbook", len(wb.Rows))
	}
}

func TestSameInputSameBytes(t *testing.T) {
	a := write(t, Options{SheetName: "S"}, []string{"h"}, []string{"v"})
	b := write(t, Options{SheetName: "S"}, []string{"h"}, []string{"v"})
	if !bytes.Equal(a, b) {
		t.Error("two writes of the same input differ")
	}
}

func TestOptionsAreChecked(t *testing.T) {
	for _, o := range []Options{
		{SheetName: "S", ColumnWidths: []float64{-1}},
		{SheetName: "S", ColumnWidths: []float64{255.5}},
		{SheetName: "S", ColumnWidths: make([]float64, MaxColumns+1)},
		{SheetName: "S", HeaderStyle: &Style{FillRGB: "E0EBF"}},
		{SheetName: "S", HeaderStyle: &Style{FillRGB: "#E0EBF5"}},
		{SheetName: "S", HeaderStyle: &Style{FillRGB: "GGGGGG"}},
	} {
		if _, err := NewWriter(io.Discard, o); err == nil {
			t.Errorf("options %+v were accepted", o)
		}
	}
	if _, err := NewWriter(io.Discard, Options{SheetName: "S", ColumnWidths: []float64{255, 0.5}}); err != nil {
		t.Error(err)
	}
}

func TestCellLimit(t *testing.T) {
	w, err := NewWriter(io.Discard, Options{SheetName: "S"})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow(make([]string, MaxColumns)); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow(make([]string, MaxColumns+1)); err == nil {
		t.Error("a row of 16,385 cells was accepted")
	}
}

func TestRowLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a million rows")
	}
	w, err := NewWriter(io.Discard, Options{SheetName: "S"})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteHeader(nil); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < MaxRows; i++ {
		if err := w.WriteRow(nil); err != nil {
			t.Fatalf("row %d: %v", i+1, err)
		}
	}
	if err := w.WriteRow(nil); err == nil {
		t.Error("row 1,048,577 was accepted")
	}
	if err := w.Close(); err == nil {
		t.Error("Close after the limit error succeeded")
	}
}

func TestCallOrder(t *testing.T) {
	w, _ := NewWriter(io.Discard, Options{SheetName: "S"})
	w.WriteHeader([]string{"a"})
	if err := w.WriteHeader([]string{"a"}); err == nil {
		t.Error("WriteHeader twice was accepted")
	}

	w, _ = NewWriter(io.Discard, Options{SheetName: "S"})
	w.WriteRow([]string{"a"})
	if err := w.WriteHeader([]string{"a"}); err == nil {
		t.Error("WriteHeader after WriteRow was accepted")
	}

	w, _ = NewWriter(io.Discard, Options{SheetName: "S"})
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if w.WriteRow([]string{"a"}) == nil || w.WriteHeader(nil) == nil || w.Close() == nil {
		t.Error("a call after Close was accepted")
	}
}

type failAfter struct{ n int }

var errWrite = errors.New("disk full")

func (f *failAfter) Write(p []byte) (int, error) {
	if f.n <= 0 {
		return 0, errWrite
	}
	if len(p) > f.n {
		n := f.n
		f.n = 0
		return n, errWrite
	}
	f.n -= len(p)
	return len(p), nil
}

// A writer that fails at any point gives its error back from some call, and
// nothing panics.
func TestFailingWriter(t *testing.T) {
	full := len(write(t, Options{SheetName: "S"}, []string{"h"}, []string{"v"}))
	for n := 0; n < full; n += 7 {
		w, err := NewWriter(&failAfter{n}, Options{SheetName: "S"})
		if err != nil {
			if !errors.Is(err, errWrite) {
				t.Errorf("at %d: %v", n, err)
			}
			continue
		}
		w.WriteHeader([]string{"h"})
		w.WriteRow([]string{"v"})
		if err := w.Close(); !errors.Is(err, errWrite) {
			t.Errorf("failing after %d bytes, Close returned %v", n, err)
		}
	}
}

// The zip is readable by archive/zip without any help from xlsxtest.
func TestPlainZip(t *testing.T) {
	data := write(t, Options{SheetName: "S"}, []string{"h"})
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if len(zr.File) != 7 {
		t.Errorf("%d entries", len(zr.File))
	}
}

// The value read back is the input after only the lossy steps: invalid UTF-8
// to U+FFFD and the cut at 32,767 units. Comparing against the encoder's own
// output would prove nothing, so the oracle is cleanText, which does no
// encoding, and the reader decodes _xHHHH_ as Excel does.
func FuzzCellText(f *testing.F) {
	for _, s := range []string{"", "a", "_x0041_", "_x0041_x0042_", "_x0041\x01", "\x00", "a\rb", " x ", "\xff", "￾", "<&>", "😀"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		var buf bytes.Buffer
		w, err := NewWriter(&buf, Options{SheetName: "S"})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteRow([]string{s}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		wb, err := xlsxtest.Read(buf.Bytes())
		if err != nil {
			t.Fatalf("%q: %v", s, err)
		}
		if got, want := wb.Rows[0][0], cleanText(s); got != want {
			t.Errorf("%q read back as %q, want %q", s, got, want)
		}
	})
}
