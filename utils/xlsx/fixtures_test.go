package xlsx

import (
	"archive/zip"
	"bytes"
	"io"
	"os"
	"testing"
)

// TestWriteFixtures writes the reader's fixtures made here into testdata.
// The golden files are then made from them by testdata/goldens/run.sh:
//
//	XLSX_WRITE_FIXTURES=1 go test -run TestWriteFixtures ./utils/xlsx/
//	utils/xlsx/testdata/goldens/run.sh
func TestWriteFixtures(t *testing.T) {
	if os.Getenv("XLSX_WRITE_FIXTURES") == "" {
		t.Skip("XLSX_WRITE_FIXTURES is not set")
	}
	files := map[string][]byte{
		"testdata/utilsxlsx-workbook.xlsx": writeBOM(t),
		"testdata/utilsxlsx-writer.xlsx":   write(t, Options{SheetName: "Coverage"}, coverage[0], coverage[1:]...),
		"testdata/handmade.xlsx":           zipParts(t, handmadeParts(false)),
		"testdata/handmade-strict.xlsx":    zipParts(t, handmadeParts(true)),
	}
	for name, b := range files {
		if err := os.WriteFile(name, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// zipParts zips parts in the order given: name, content, name, content...
func zipParts(t testing.TB, parts []string) []byte {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for i := 0; i+1 < len(parts); i += 2 {
		w, err := zw.CreateHeader(&zip.FileHeader{Name: parts[i], Method: zip.Deflate, Modified: entryTime})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(parts[i+1])); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// handmadeParts is a package with what spreadsheet applications write and
// utils/xlsx and excelize do not: phonetic runs, rich text, inline strings,
// t="str", t="e", t="d", rows and cells without r, a lower-case reference, a
// built-in format redefined, an unknown built-in id, a sheet at an absolute
// target, a hidden sheet, a chart sheet and an external relationship that must
// not be followed. strict uses the namespaces of ISO 29500 Strict.
func handmadeParts(strict bool) []string {
	main := "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
	rel := "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
	if strict {
		main = "http://purl.oclc.org/ooxml/spreadsheetml/main"
		rel = "http://purl.oclc.org/ooxml/officeDocument/relationships"
	}
	decl := `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` + "\n"
	ws := func(body string) string {
		return decl + `<worksheet xmlns="` + main + `" xmlns:r="` + rel + `"><sheetData>` + body + `</sheetData></worksheet>`
	}
	return []string{
		"[Content_Types].xml", decl + `<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">` +
			`<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>` +
			`<Default Extension="xml" ContentType="application/xml"/>` +
			`<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>` +
			`<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>` +
			`<Override PartName="/xl/other/abs.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>` +
			`<Override PartName="/xl/worksheets/sheet3.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>` +
			`<Override PartName="/xl/chartsheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.chartsheet+xml"/>` +
			`<Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>` +
			`<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>` +
			`</Types>`,
		"_rels/.rels", decl + `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">` +
			`<Relationship Id="rId1" Type="` + rel + `/officeDocument" Target="xl/workbook.xml"/></Relationships>`,
		"xl/workbook.xml", decl + `<workbook xmlns="` + main + `" xmlns:r="` + rel + `"><workbookPr date1904="false"/><sheets>` +
			`<sheet name="Cells" sheetId="1" r:id="rId1"/>` +
			`<sheet name="Absolute" sheetId="2" r:id="rId2"/>` +
			`<sheet name="Hidden" sheetId="3" state="hidden" r:id="rId3"/>` +
			`<sheet name="Chart" sheetId="4" r:id="rId4"/>` +
			`</sheets></workbook>`,
		"xl/_rels/workbook.xml.rels", decl + `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">` +
			`<Relationship Id="rId1" Type="` + rel + `/worksheet" Target="worksheets/sheet1.xml"/>` +
			`<Relationship Id="rId2" Type="` + rel + `/worksheet" Target="/xl/other/abs.xml"/>` +
			`<Relationship Id="rId3" Type="` + rel + `/worksheet" Target="./worksheets/../worksheets/sheet3.xml"/>` +
			`<Relationship Id="rId4" Type="` + rel + `/chartsheet" Target="chartsheets/sheet1.xml"/>` +
			`<Relationship Id="rId5" Type="` + rel + `/sharedStrings" Target="sharedStrings.xml"/>` +
			`<Relationship Id="rId6" Type="` + rel + `/styles" Target="styles.xml"/>` +
			`<Relationship Id="rId7" Type="` + rel + `/externalLink" Target="http://example.invalid/other.xlsx" TargetMode="External"/>` +
			`</Relationships>`,
		"xl/sharedStrings.xml", decl + `<sst xmlns="` + main + `" count="9" uniqueCount="9">` +
			`<si><t>plain</t></si>` +
			`<si><r><t>rich </t></r><r><rPr><b/></rPr><t>text</t></r></si>` +
			`<si><t>漢字</t><rPh sb="0" eb="2"><t>かんじ</t></rPh><phoneticPr fontId="1"/></si>` +
			`<si><t>head </t><r><t>run</t></r></si>` +
			`<si><t>cr_x000D_lf _x005F_x0041_ _x0041_x0042_ _xD83D_</t></si>` +
			`<si><t/></si>` +
			`<si><t xml:space="preserve">  spaced  </t></si>` +
			`<si><t>first</t><t>second</t></si>` +
			`<si><t>a &amp; b &lt;c&gt; &#x263A;</t></si>` +
			`</sst>`,
		"xl/styles.xml", decl + `<styleSheet xmlns="` + main + `">` +
			`<numFmts count="2"><numFmt numFmtId="164" formatCode="0.0"/><numFmt numFmtId="14" formatCode="yyyy/mm/dd"/></numFmts>` +
			`<cellStyleXfs count="1"><xf numFmtId="0"/></cellStyleXfs>` +
			`<cellXfs count="7"><xf numFmtId="0"/><xf numFmtId="164" applyNumberFormat="1"/><xf numFmtId="14"/><xf numFmtId="49"/><xf numFmtId="5"/><xf numFmtId="22"/><xf/></cellXfs>` +
			`</styleSheet>`,
		"xl/worksheets/sheet1.xml", ws(
			`<row><c t="s"><v>0</v></c><c t="s"><v>1</v></c><c t="s"><v>2</v></c><c t="s"><v>3</v></c><c t="s"><v>4</v></c><c t="s"><v>5</v></c><c t="s"><v>6</v></c><c t="s"><v>7</v></c><c t="s"><v>8</v></c></row>` +
				`<row><c t="str"><f>"a"&amp;"b"</f><v>ab</v></c><c t="e"><f>1/0</f><v>#DIV/0!</v></c><c t="d"><v>2026-10-27T10:30:00Z</v></c><c t="d" s="5"><v>2026-10-27T10:30:00Z</v></c><c t="d"><v>not a date</v></c></row>` +
				`<row r="4"><c r="A4" t="inlineStr"><is><t>inline</t></is></c><c r="B4" t="inlineStr"><is><r><t>in</t></r><r><t>line runs</t></r></is></c><c r="C4" t="b"><v>1</v></c><c r="D4" t="b"><v>0</v></c><c r="E4" t="b"><v>2</v></c></row>` +
				`<row r="5"><c r="A5" t="n"><v>3.25</v></c><c r="B5" s="1"><v>3.25</v></c><c r="C5" s="2"><v>46322</v></c><c r="D5" s="3" t="s"><v>0</v></c><c r="E5" s="4"><v>1234.5</v></c><c r="F5" s="1"/><c r="G5" s="6"><v>7</v></c><c r="H5"><f>SUM(A5:B5)</f></c></row>` +
				`<row r="7"><c r="b7"><v>  12  </v></c><c r="d7" t="s"><v> 0 </v></c></row>` +
				`<row r="9"><c r="A9" s="1"/></row>`,
		),
		"xl/other/abs.xml", ws(`<row r="1"><c r="A1" t="inlineStr"><is><t>absolute target</t></is></c></row>`),
		"xl/worksheets/sheet3.xml", ws(`<row r="2"><c r="C2"><v>-0</v></c></row>`),
		"xl/chartsheets/sheet1.xml", decl + `<chartsheet xmlns="` + main + `" xmlns:r="` + rel + `"><sheetViews><sheetView workbookViewId="0"/></sheetViews><drawing r:id="rId1"/></chartsheet>`,
	}
}

// The committed hand-made fixtures hold exactly handmadeParts, compared part
// by part so that a change in the deflate output alone does not fail it.
func TestHandmadeFixturesAreCurrent(t *testing.T) {
	for name, strict := range map[string]bool{"testdata/handmade.xlsx": false, "testdata/handmade-strict.xlsx": true} {
		have, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		zr, err := zip.NewReader(bytes.NewReader(have), int64(len(have)))
		if err != nil {
			t.Fatal(err)
		}
		want := handmadeParts(strict)
		if len(zr.File)*2 != len(want) {
			t.Fatalf("%s holds %d parts, handmadeParts %d; rerun TestWriteFixtures and run.sh", name, len(zr.File), len(want)/2)
		}
		for i, f := range zr.File {
			rc, err := f.Open()
			if err != nil {
				t.Fatal(err)
			}
			b, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				t.Fatal(err)
			}
			if f.Name != want[2*i] || string(b) != want[2*i+1] {
				t.Errorf("%s: part %d (%s) differs from handmadeParts; rerun TestWriteFixtures and run.sh", name, i, f.Name)
			}
		}
	}
}
