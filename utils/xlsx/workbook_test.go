package xlsx

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/internal/xlsxtest"
)

// bom builds the workbook the feature tests share: a styled summary sheet and
// an item sheet, the shape of a bill of materials.
type bom struct {
	title, head, money, cell, wrapped int
}

func writeBOM(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	b, err := NewWorkbook(&buf, WorkbookOptions{Sheets: []SheetOptions{
		{Name: "Ringkasan", ColumnWidths: []float64{30, 18}},
		{Name: "Item", ColumnWidths: []float64{6, 40, 10, 18}, FreezeRows: 2, DefaultRowHeight: 15},
		{Name: "Kosong", DefaultRowHeight: 20.25},
	}})
	if err != nil {
		t.Fatal(err)
	}
	thin := Border{Left: true, Right: true, Top: true, Bottom: true, ColorRGB: "808080"}
	var s bom
	add := func(c CellStyle) int {
		id, err := b.AddStyle(c)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	s.title = add(CellStyle{Font: Font{Name: "Arial", Size: 14, Bold: true, ColorRGB: "1F3864"}, Horizontal: "center", Vertical: "center"})
	s.head = add(CellStyle{Font: Font{Bold: true}, FillRGB: "D9E1F2", Border: thin, Horizontal: "center", Vertical: "center", Wrap: true})
	s.money = add(CellStyle{Border: thin, Horizontal: "right", NumberFormat: `"Rp"#,##0`})
	s.cell = add(CellStyle{Border: thin, Horizontal: "left", Vertical: "top"})
	s.wrapped = add(CellStyle{Font: Font{Italic: true}, Border: thin, Wrap: true})

	rows := []func() error{
		func() error { return b.WriteRow(Row{Cells: []Cell{Text("Daftar Bahan", s.title), Blank(s.title)}, Height: 24}) },
		func() error { return b.Merge(1, 1, 1, 2) },
		func() error { return b.WriteRow(Row{Cells: []Cell{Text("Total", s.cell), Number(1234567, s.money)}}) },
		func() error { return b.WriteRow(Row{Cells: []Cell{Text("Catatan panjang yang dibungkus", s.wrapped)}}) },
		b.NextSheet,
		func() error {
			return b.WriteRow(Row{Cells: []Cell{Text("No", s.head), Text("Uraian", s.head), Text("Jumlah", s.head), Text("Harga", s.head)}, Height: 30})
		},
		func() error {
			return b.WriteRow(Row{Cells: []Cell{Blank(s.head), Blank(s.head), Text("(unit)", s.head), Text("(Rp)", s.head)}})
		},
		func() error { return b.Merge(1, 1, 2, 1) },
		func() error { return b.Merge(1, 2, 2, 2) },
		func() error {
			return b.WriteRow(Row{Cells: []Cell{Number(1, s.cell), Text("Kabel NYY 4x16", s.cell), Number(12.5, s.cell), Number(185000, s.money)}})
		},
		func() error {
			return b.WriteRow(Row{Cells: []Cell{Number(2, s.cell), Text("-5", s.cell), {}, Number(-0.25, s.money)}})
		},
	}
	for i, f := range rows {
		if err := f(); err != nil {
			t.Fatalf("step %d: %v", i+1, err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestWorkbookSheets(t *testing.T) {
	wb := read(t, writeBOM(t))
	var names []string
	for _, s := range wb.Sheets {
		names = append(names, s.Name)
	}
	if !reflect.DeepEqual(names, []string{"Ringkasan", "Item", "Kosong"}) {
		t.Errorf("sheets %q", names)
	}
	want := []string{
		"[Content_Types].xml", "_rels/.rels", "xl/workbook.xml", "xl/_rels/workbook.xml.rels", "xl/styles.xml",
		"xl/worksheets/sheet1.xml", "xl/worksheets/sheet2.xml", "xl/worksheets/sheet3.xml", "xl/sharedStrings.xml",
	}
	if !reflect.DeepEqual(wb.Parts, want) {
		t.Errorf("parts %q", wb.Parts)
	}
	if wb.ActiveTab != 0 || !wb.Sheets[0].Selected || wb.Sheets[1].Selected || wb.Sheets[2].Selected {
		t.Errorf("active tab %d, selected %v %v %v", wb.ActiveTab, wb.Sheets[0].Selected, wb.Sheets[1].Selected, wb.Sheets[2].Selected)
	}
	if len(wb.Sheets[2].Rows) != 0 {
		t.Error("the sheet never reached holds rows")
	}
	if !reflect.DeepEqual(wb.Sheets[1].ColumnWidths, []float64{6, 40, 10, 18}) {
		t.Errorf("widths of Item %v", wb.Sheets[1].ColumnWidths)
	}
	checkParts(t, wb)
}

func TestWorkbookNumbers(t *testing.T) {
	values := []float64{0, math.Copysign(0, -1), 1, -1, 12.5, 1234567, 0.1, 1.0 / 3, 1e-5, 9.99e-6, 123456789012345, 1e15, -2.5e300, 5e-324, math.MaxFloat64}
	var buf bytes.Buffer
	b, _ := NewWorkbook(&buf, WorkbookOptions{Sheets: []SheetOptions{{Name: "N"}}})
	row := Row{}
	for _, v := range values {
		row.Cells = append(row.Cells, Number(v, 0))
	}
	row.Cells = append(row.Cells, Text("-5", 0), Text("1e3", 0))
	if err := b.WriteRow(row); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	wb := read(t, buf.Bytes())
	cells := wb.Sheets[0].Cells[0]
	for i, v := range values {
		if c := cells[i]; c.Kind != xlsxtest.Number || c.Number != v {
			t.Errorf("%v read back as %s %v (%q)", v, c.Kind, c.Number, c.Text)
		}
	}
	if cells[1].Text != "0" {
		t.Errorf("-0 written as %q", cells[1].Text)
	}
	if cells[5].Text != "1234567" || cells[10].Text != "123456789012345" {
		t.Errorf("integers written as %q, %q", cells[5].Text, cells[10].Text)
	}
	for _, i := range []int{len(values), len(values) + 1} {
		if cells[i].Kind != xlsxtest.Text {
			t.Errorf("text %q read back as %s", cells[i].Text, cells[i].Kind)
		}
	}

	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		b, _ := NewWorkbook(io.Discard, WorkbookOptions{Sheets: []SheetOptions{{Name: "N"}}})
		if err := b.WriteRow(Row{Cells: []Cell{Number(v, 0)}}); err == nil {
			t.Errorf("%v was accepted", v)
		}
	}
}

func TestWorkbookStyles(t *testing.T) {
	wb := read(t, writeBOM(t))
	sum, items := wb.Sheets[0], wb.Sheets[1]
	style := func(sh xlsxtest.Sheet, r, c int) xlsxtest.Style { return wb.Styles[sh.Cells[r][c].Style] }

	title := style(sum, 0, 0)
	if title.FontName != "Arial" || title.FontSize != 14 || !title.Bold || title.Italic || title.FontColor != "FF1F3864" ||
		title.Horizontal != "center" || title.Vertical != "center" || title.Wrap || title.Fill != "" || title.Border != [4]string{} {
		t.Errorf("title style %+v", title)
	}
	head := style(items, 0, 0)
	grey := [4]string{"FF808080", "FF808080", "FF808080", "FF808080"}
	if !head.Bold || head.FontName != "Calibri" || head.FontSize != 11 || head.Fill != "FFD9E1F2" || head.Border != grey || !head.Wrap {
		t.Errorf("head style %+v", head)
	}
	money := style(items, 2, 3)
	if money.NumberFormat != `"Rp"#,##0` || money.Horizontal != "right" || money.Border != grey {
		t.Errorf("money style %+v", money)
	}
	if !bytes.Contains(wb.Raw["xl/styles.xml"], []byte(`formatCode="&quot;Rp&quot;#,##0"`)) {
		t.Error("the format code is not attribute-escaped")
	}
	cell := style(items, 2, 1)
	if cell.Horizontal != "left" || cell.Vertical != "top" || cell.NumberFormat != "" {
		t.Errorf("cell style %+v", cell)
	}
	if w := style(sum, 2, 0); !w.Italic || !w.Wrap {
		t.Errorf("wrapped style %+v", w)
	}
	if c := items.Cells[3][2]; c.Kind != xlsxtest.Missing {
		t.Errorf("the zero Cell was written as %+v", c)
	}
	if c := items.Cells[1][0]; c.Kind != xlsxtest.Blank || wb.Styles[c.Style] != head {
		t.Errorf("a blank head cell read back as %+v", c)
	}

	// One grey border, one bold Calibri, one number format, however many
	// styles use them.
	st := string(wb.Raw["xl/styles.xml"])
	for frag, n := range map[string]int{`<border><left style="thin">`: 1, `<numFmt `: 1, `<b/><sz val="11"/>`: 1} {
		if got := strings.Count(st, frag); got != n {
			t.Errorf("%s appears %d times in styles.xml, want %d", frag, got, n)
		}
	}
}

func TestEqualStylesShareAnXF(t *testing.T) {
	b, _ := NewWorkbook(io.Discard, WorkbookOptions{Sheets: []SheetOptions{{Name: "S"}}})
	s := CellStyle{Font: Font{Bold: true}, NumberFormat: "0.00"}
	a, _ := b.AddStyle(s)
	c, _ := b.AddStyle(s)
	z, _ := b.AddStyle(CellStyle{})
	if a == c || b.styles.xf(a) != b.styles.xf(c) {
		t.Errorf("indices %d, %d; xfs %d, %d", a, c, b.styles.xf(a), b.styles.xf(c))
	}
	if b.styles.xf(z) != 0 {
		t.Errorf("the zero style is xf %d, want 0", b.styles.xf(z))
	}
}

func TestWorkbookMergesHeightsPanes(t *testing.T) {
	wb := read(t, writeBOM(t))
	sum, items := wb.Sheets[0], wb.Sheets[1]
	if !reflect.DeepEqual(sum.Merges, []string{"A1:B1"}) || !reflect.DeepEqual(items.Merges, []string{"A1:A2", "B1:B2"}) {
		t.Errorf("merges %q %q", sum.Merges, items.Merges)
	}
	if wb.Sheets[2].Merges != nil {
		t.Error("an empty sheet has merges")
	}
	if sum.RowHeights[0] != 24 || sum.RowHeights[1] != 0 || items.RowHeights[0] != 30 {
		t.Errorf("heights %v %v", sum.RowHeights, items.RowHeights)
	}
	if sum.FreezeRows != 0 || items.FreezeRows != 2 {
		t.Errorf("frozen rows %d %d", sum.FreezeRows, items.FreezeRows)
	}
	sheet := string(wb.Raw["xl/worksheets/sheet2.xml"])
	views, format, cols, data, merges := strings.Index(sheet, "<sheetViews>"), strings.Index(sheet, "<sheetFormatPr"),
		strings.Index(sheet, "<cols>"), strings.Index(sheet, "<sheetData>"), strings.Index(sheet, "<mergeCells")
	if !(0 < views && views < format && format < cols && cols < data && data < merges) {
		t.Errorf("worksheet children out of the schema's order: %d %d %d %d %d", views, format, cols, data, merges)
	}
	if !strings.Contains(sheet, `<pane ySplit="2" topLeftCell="A3" activePane="bottomLeft" state="frozen"/>`) {
		t.Error("no frozen pane under row 2")
	}

	var buf bytes.Buffer
	b, _ := NewWorkbook(&buf, WorkbookOptions{Sheets: []SheetOptions{{Name: "S"}}})
	style, _ := b.AddStyle(CellStyle{Font: Font{Bold: true}})
	b.WriteRow(Row{Cells: []Cell{Text("a", 0)}, Style: style})
	b.Close()
	if r := read(t, buf.Bytes()).Sheets[0]; r.RowStyles[0] == 0 || r.CellStyles[0][0] != 0 {
		t.Errorf("row style %d, cell style %d", r.RowStyles[0], r.CellStyles[0][0])
	}
}

func TestMergesAreChecked(t *testing.T) {
	for _, m := range [][4]int{
		{1, 1, 1, 1}, {2, 1, 1, 2}, {1, 2, 2, 1}, {0, 1, 1, 2}, {1, 0, 2, 1},
		{1, 1, MaxRows + 1, 1}, {1, 1, 1, MaxColumns + 1},
		{2, 2, 3, 3}, {1, 3, 2, 4}, {2, 3, 3, 4},
	} {
		b, _ := NewWorkbook(io.Discard, WorkbookOptions{Sheets: []SheetOptions{{Name: "S"}}})
		if err := b.Merge(1, 1, 2, 3); err != nil {
			t.Fatal(err)
		}
		if err := b.Merge(m[0], m[1], m[2], m[3]); err == nil {
			t.Errorf("merge %v was accepted beside A1:C2", m)
		}
	}
	// Ranges that only touch, and the same range on another sheet, are fine.
	b, _ := NewWorkbook(io.Discard, WorkbookOptions{Sheets: []SheetOptions{{Name: "S"}, {Name: "T"}}})
	for _, err := range []error{b.Merge(1, 1, 2, 3), b.Merge(3, 1, 3, 3), b.Merge(1, 4, 3, 4), b.NextSheet(), b.Merge(1, 1, 2, 3)} {
		if err != nil {
			t.Error(err)
		}
	}
}

// A default row height of 15 is written as excelize writes it; another needs
// customHeight for Excel to keep it; none writes no <sheetFormatPr> at all.
func TestWorkbookDefaultRowHeight(t *testing.T) {
	wb := read(t, writeBOM(t))
	for i, want := range []struct {
		height float64
		custom bool
		xml    string
	}{
		{0, false, ""},
		{15, false, `<sheetFormatPr defaultRowHeight="15"/>`},
		{20.25, true, `<sheetFormatPr defaultRowHeight="20.25" customHeight="1"/>`},
	} {
		sh := wb.Sheets[i]
		if sh.DefaultRowHeight != want.height || sh.CustomHeight != want.custom {
			t.Errorf("sheet %s: default row height %v, custom %v", sh.Name, sh.DefaultRowHeight, sh.CustomHeight)
		}
		raw := string(wb.Raw["xl/worksheets/sheet"+strconv.Itoa(i+1)+".xml"])
		if want.xml == "" && strings.Contains(raw, "<sheetFormatPr") || want.xml != "" && !strings.Contains(raw, want.xml) {
			t.Errorf("sheet %s: want %q in %s", sh.Name, want.xml, raw)
		}
	}
	// Writer's files stay as they were.
	if bytes.Contains(read(t, write(t, Options{SheetName: "S"}, []string{"h"}, []string{"v"})).Raw["xl/worksheets/sheet1.xml"], []byte("<sheetFormatPr")) {
		t.Error("Writer writes <sheetFormatPr>")
	}
}

func TestWorkbookOptionsAreChecked(t *testing.T) {
	for _, o := range []WorkbookOptions{
		{},
		{Sheets: []SheetOptions{{Name: "Data"}, {Name: "DATA"}}},
		{Sheets: []SheetOptions{{Name: "A"}, {Name: "History"}}},
		{Sheets: []SheetOptions{{Name: "A", ColumnWidths: []float64{256}}}},
		{Sheets: []SheetOptions{{Name: "A", FreezeRows: -1}}},
		{Sheets: []SheetOptions{{Name: "A", FreezeRows: MaxRows}}},
		{Sheets: []SheetOptions{{Name: "A", DefaultRowHeight: -1}}},
		{Sheets: []SheetOptions{{Name: "A", DefaultRowHeight: 409.5}}},
		{Sheets: []SheetOptions{{Name: "A", DefaultRowHeight: math.NaN()}}},
		{Sheets: []SheetOptions{{Name: "A", DefaultRowHeight: math.Inf(1)}}},
	} {
		if _, err := NewWorkbook(io.Discard, o); err == nil {
			t.Errorf("options %+v were accepted", o)
		}
	}
}

func TestCellStylesAreChecked(t *testing.T) {
	for _, s := range []CellStyle{
		{FillRGB: "12345"}, {Font: Font{ColorRGB: "#000000"}}, {Border: Border{Left: true, ColorRGB: "ZZZZZZ"}},
		{Font: Font{Name: strings.Repeat("a", 32)}}, {Font: Font{Name: "a\nb"}}, {Font: Font{Name: "a\xffb"}},
		{Font: Font{Size: 0.5}}, {Font: Font{Size: 410}}, {Font: Font{Size: -1}},
		{Horizontal: "justify"}, {Horizontal: "Center"}, {Vertical: "middle"},
		{NumberFormat: strings.Repeat("0", 256)}, {NumberFormat: "0\x01"},
	} {
		b, _ := NewWorkbook(io.Discard, WorkbookOptions{Sheets: []SheetOptions{{Name: "S"}}})
		if _, err := b.AddStyle(s); err == nil {
			t.Errorf("style %+v was accepted", s)
		}
	}
	b, _ := NewWorkbook(io.Discard, WorkbookOptions{Sheets: []SheetOptions{{Name: "S"}}})
	if _, err := b.AddStyle(CellStyle{Font: Font{Name: "Times New Roman", Size: 409, ColorRGB: "abcdef"}, NumberFormat: `#,##0.00;[Red]-#,##0.00`}); err != nil {
		t.Error(err)
	}
}

func TestWorkbookCallOrder(t *testing.T) {
	one := WorkbookOptions{Sheets: []SheetOptions{{Name: "S"}}}

	b, _ := NewWorkbook(io.Discard, one)
	b.WriteRow(Row{})
	if _, err := b.AddStyle(CellStyle{}); err == nil {
		t.Error("AddStyle after WriteRow was accepted")
	}

	b, _ = NewWorkbook(io.Discard, one)
	if err := b.WriteRow(Row{Cells: []Cell{Text("a", 1)}}); err == nil {
		t.Error("style 1 was accepted with an empty table")
	}
	b, _ = NewWorkbook(io.Discard, one)
	if err := b.WriteRow(Row{Style: -1}); err == nil {
		t.Error("row style -1 was accepted")
	}
	for _, h := range []float64{-1, 409.5, math.NaN()} {
		b, _ = NewWorkbook(io.Discard, one)
		if err := b.WriteRow(Row{Height: h}); err == nil {
			t.Errorf("row height %v was accepted", h)
		}
	}

	b, _ = NewWorkbook(io.Discard, one)
	if err := b.NextSheet(); err == nil {
		t.Error("NextSheet on the last sheet was accepted")
	}
	if err := b.Close(); err == nil {
		t.Error("an error did not stay")
	}

	b, _ = NewWorkbook(io.Discard, one)
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if b.WriteRow(Row{}) == nil || b.Merge(1, 1, 1, 2) == nil || b.NextSheet() == nil || b.Close() == nil {
		t.Error("a call after Close was accepted")
	}
	if _, err := b.AddStyle(CellStyle{}); err == nil {
		t.Error("AddStyle after Close was accepted")
	}
}

func TestWorkbookFailingWriter(t *testing.T) {
	write := func(w io.Writer) error {
		b, err := NewWorkbook(w, WorkbookOptions{Sheets: []SheetOptions{{Name: "A"}, {Name: "B"}}})
		if err != nil {
			t.Fatal(err)
		}
		b.WriteRow(Row{Cells: []Cell{Text("h", 0), Number(1, 0)}})
		b.Merge(2, 1, 2, 2)
		b.NextSheet()
		b.WriteRow(Row{Cells: []Cell{Text("v", 0)}})
		return b.Close()
	}
	var full bytes.Buffer
	if err := write(&full); err != nil {
		t.Fatal(err)
	}
	for n := 0; n < full.Len(); n += 7 {
		if err := write(&failAfter{n}); !errors.Is(err, errWrite) {
			t.Errorf("failing after %d bytes, Close returned %v", n, err)
		}
	}
}

func TestWorkbookSameBytes(t *testing.T) {
	if !bytes.Equal(writeBOM(t), writeBOM(t)) {
		t.Error("two writes of the same input differ")
	}
}

func wellFormed(b []byte) error {
	d := xml.NewDecoder(bytes.NewReader(b))
	for {
		_, err := d.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// partNames gives every part [Content_Types].xml and the relationships name.
func partNames(t *testing.T, wb *xlsxtest.Workbook) []string {
	t.Helper()
	var types struct {
		Overrides []struct {
			PartName string `xml:"PartName,attr"`
		} `xml:"Override"`
	}
	if err := xml.Unmarshal(wb.Raw["[Content_Types].xml"], &types); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, o := range types.Overrides {
		names = append(names, strings.TrimPrefix(o.PartName, "/"))
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
			names = append(names, dir+rel.Target)
		}
	}
	return names
}

// checkParts: every part is well-formed, and every content type and
// relationship names a part that exists.
func checkParts(t *testing.T, wb *xlsxtest.Workbook) {
	t.Helper()
	for name, b := range wb.Raw {
		if err := wellFormed(b); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for _, p := range partNames(t, wb) {
		if _, ok := wb.Raw[p]; !ok {
			t.Errorf("%s is named but missing", p)
		}
	}
}

// TestWriteSample writes the feature workbook to the path in XLSX_SAMPLE, for
// opening by hand in Excel and Numbers:
//
//	XLSX_SAMPLE=/tmp/sample.xlsx go test -run TestWriteSample ./utils/xlsx/
func TestWriteSample(t *testing.T) {
	path := os.Getenv("XLSX_SAMPLE")
	if path == "" {
		t.Skip("XLSX_SAMPLE is not set")
	}
	if err := os.WriteFile(path, writeBOM(t), 0o644); err != nil {
		t.Fatal(err)
	}
}
