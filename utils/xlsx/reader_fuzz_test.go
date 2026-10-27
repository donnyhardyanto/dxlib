package xlsx

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fuzzLimits keep each run small, so the fuzzer explores instead of
// inflating, and so a limit is reached often enough to be exercised.
var fuzzLimits = ReaderLimits{
	MaxFileSize:      1 << 20,
	MaxEntries:       64,
	MaxPartSize:      1 << 20,
	MaxTotalSize:     4 << 20,
	MaxXMLDepth:      32,
	MaxXMLTokens:     100_000,
	MaxSheets:        16,
	MaxRows:          2000,
	MaxColumns:       200,
	MaxCells:         20_000,
	MaxSheetBytes:    256 << 10,
	MaxSharedStrings: 1000,
	MaxStringUnits:   1000,
	MaxStyles:        200,
}

// readAll opens data and reads every sheet, formatted and raw. It must never
// panic; an error is an answer. What it returns must respect the limits.
func readAll(t *testing.T, data []byte) {
	x, err := OpenBytes(data, &fuzzLimits)
	if err != nil {
		return
	}
	defer x.Close()
	for _, s := range x.GetSheetList() {
		rows, err := x.GetRows(s)
		if err == nil {
			checkBounds(t, rows)
		}
		if rows, err = x.GetRowsRaw(s); err == nil {
			checkBounds(t, rows)
		}
	}
}

// checkBounds fails when rows go past fuzzLimits.
func checkBounds(t *testing.T, rows [][]string) {
	if len(rows) > fuzzLimits.MaxRows {
		t.Fatalf("%d rows past MaxRows", len(rows))
	}
	cells, n := int64(0), int64(0)
	for _, r := range rows {
		if len(r) > fuzzLimits.MaxColumns {
			t.Fatalf("%d cells in a row past MaxColumns", len(r))
		}
		cells += int64(max(len(r), 1))
		for _, c := range r {
			if u := utf16Len(c); u > fuzzLimits.MaxStringUnits {
				t.Fatalf("a cell of %d UTF-16 units past MaxStringUnits", u)
			}
			n += int64(len(c))
		}
	}
	if cells > fuzzLimits.MaxCells {
		t.Fatalf("%d cells past MaxCells", cells)
	}
	if n > fuzzLimits.MaxSheetBytes {
		t.Fatalf("%d bytes past MaxSheetBytes", n)
	}
}

// FuzzOpen feeds whole files: the zip boundary and everything behind it.
func FuzzOpen(f *testing.F) {
	names, _ := filepath.Glob("testdata/*.xlsx")
	for _, n := range names {
		if b, err := os.ReadFile(n); err == nil && len(b) <= int(fuzzLimits.MaxFileSize) {
			f.Add(b)
		}
	}
	f.Add(pkg(f, `<row><c t="s"><v>0</v></c></row>`, nil))
	f.Fuzz(func(t *testing.T, data []byte) {
		readAll(t, data)
	})
}

// FuzzSheetXML feeds a worksheet part inside an otherwise valid package: the
// XML boundary, rows, cells and value rendering.
func FuzzSheetXML(f *testing.F) {
	for _, s := range []string{
		`<worksheet><sheetData><row r="1"><c r="A1" t="s"><v>1</v></c><c r="C1" s="1"><v>2.5</v></c></row></sheetData></worksheet>`,
		`<worksheet><sheetData><row><c t="inlineStr"><is><r><t>a</t></r><rPh><t>b</t></rPh></is></c><c t="b"><v>1</v></c><c t="d"><v>2026-10-27T10:30:00Z</v></c><c t="e"><v>#N/A</v></c><c t="str"><f>x</f><v>y</v></c></row></sheetData></worksheet>`,
		`<!DOCTYPE w [<!ENTITY a "x">]><worksheet/>`,
		`<worksheet><sheetData><row r="1048576"><c r="XFD1048576"><v>1</v></c></row></sheetData></worksheet>`,
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, sheet string) {
		readAll(t, pkg(t, "", map[string]string{"xl/worksheets/sheet1.xml": sheet}))
	})
}

// FuzzSharedStrings feeds the shared string table and the styles, read by a
// sheet that uses the first strings and styles.
func FuzzSharedStrings(f *testing.F) {
	f.Add(`<sst><si><t>a_x0041_</t></si><si><r><t>b</t></r></si></sst>`, `<styleSheet><numFmts><numFmt numFmtId="164" formatCode="0.00%"/></numFmts><cellXfs><xf/><xf numFmtId="164"/><xf numFmtId="14"/></cellXfs></styleSheet>`)
	f.Add(`<sst><si><t xml:space="preserve"> x </t><rPh><t>y</t></rPh></si></sst>`, `<styleSheet><cellXfs><xf numFmtId="49"/><xf numFmtId="22"/></cellXfs></styleSheet>`)
	body := `<row><c t="s"><v>0</v></c><c t="s" s="1"><v>1</v></c><c s="1"><v>45000.5</v></c><c s="2"><v>-3.25</v></c><c s="3"><v>1e300</v></c></row>`
	f.Fuzz(func(t *testing.T, sst, styles string) {
		readAll(t, pkg(t, body, map[string]string{"xl/sharedStrings.xml": sst, "xl/styles.xml": styles}))
	})
}

// FuzzNumFmt renders arbitrary values with arbitrary format codes.
func FuzzNumFmt(f *testing.F) {
	for code := range builtinCodes {
		f.Add(builtinCodes[code], "1234.5678", false)
	}
	f.Add(`"Rp"#,##0.00;[Red]\-"Rp"#,##0.00`, "-1500000", false)
	f.Add("[$-421]dddd, d mmmm yyyy h:mm AM/PM", "45000.75", true)
	f.Add("[h]:mm:ss.000", "1e15", false)
	f.Add(strings.Repeat("@", 1024), strings.Repeat("x", 100), false)
	f.Fuzz(func(t *testing.T, code, value string, date1904 bool) {
		const maxUnits = 1000
		nf := parseNumFmt(code)
		for _, k := range []valueKind{kindNumber, kindDate, kindText} {
			s, over := nf.renderMax(value, k, date1904, maxUnits)
			if over > 0 && (over <= maxUnits || s != "") {
				t.Fatalf("over %d with %d bytes rendered", over, len(s))
			}
			// Past the cap only by what a format writes once: the value,
			// at most maxNumFmtRunes characters, and what a date or a
			// number expands them to.
			if u := utf16Len(s); u > maxUnits && u > 4*(utf16Len(value)+maxNumFmtRunes)+400 {
				t.Fatalf("%d UTF-16 units from a %d-unit value", u, utf16Len(value))
			}
		}
	})
}
