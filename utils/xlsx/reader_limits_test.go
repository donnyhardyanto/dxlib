package xlsx

import (
	"archive/zip"
	"bytes"
	"compress/flate"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const (
	relNS  = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
	pkgRel = "http://schemas.openxmlformats.org/package/2006/relationships"
)

// pkgParts is a package of one sheet, S, holding body inside <sheetData>.
// over replaces or adds parts; an empty content leaves a part out.
func pkgParts(body string, over map[string]string) []string {
	parts := map[string]string{
		"_rels/.rels":     `<Relationships xmlns="` + pkgRel + `"><Relationship Id="rId1" Type="` + relNS + `/officeDocument" Target="xl/workbook.xml"/></Relationships>`,
		"xl/workbook.xml": `<workbook xmlns="` + nsMain + `" xmlns:r="` + relNS + `"><sheets><sheet name="S" sheetId="1" r:id="rId1"/></sheets></workbook>`,
		"xl/_rels/workbook.xml.rels": `<Relationships xmlns="` + pkgRel + `">` +
			`<Relationship Id="rId1" Type="` + relNS + `/worksheet" Target="worksheets/sheet1.xml"/>` +
			`<Relationship Id="rId2" Type="` + relNS + `/sharedStrings" Target="sharedStrings.xml"/>` +
			`<Relationship Id="rId3" Type="` + relNS + `/styles" Target="styles.xml"/></Relationships>`,
		"xl/worksheets/sheet1.xml": `<worksheet xmlns="` + nsMain + `"><sheetData>` + body + `</sheetData></worksheet>`,
		"xl/sharedStrings.xml":     `<sst xmlns="` + nsMain + `"><si><t>a</t></si><si><t>b</t></si></sst>`,
		"xl/styles.xml":            `<styleSheet xmlns="` + nsMain + `"><cellXfs><xf numFmtId="0"/><xf numFmtId="2"/></cellXfs></styleSheet>`,
	}
	for k, v := range over {
		parts[k] = v
	}
	var out []string
	for _, name := range []string{"_rels/.rels", "xl/workbook.xml", "xl/_rels/workbook.xml.rels", "xl/worksheets/sheet1.xml", "xl/sharedStrings.xml", "xl/styles.xml"} {
		if parts[name] != "" {
			out = append(out, name, parts[name])
		}
		delete(parts, name)
	}
	for k, v := range parts {
		if v != "" {
			out = append(out, k, v)
		}
	}
	return out
}

func pkg(t testing.TB, body string, over map[string]string) []byte {
	return zipParts(t, pkgParts(body, over))
}

// rowsOf opens data and reads sheet S.
func rowsOf(data []byte, lim *ReaderLimits) ([][]string, error) {
	x, err := OpenBytes(data, lim)
	if err != nil {
		return nil, err
	}
	defer x.Close()
	return x.GetRows("S")
}

// rawRowsOf opens data and reads sheet S as stored.
func rawRowsOf(data []byte, lim *ReaderLimits) ([][]string, error) {
	x, err := OpenBytes(data, lim)
	if err != nil {
		return nil, err
	}
	defer x.Close()
	return x.GetRowsRaw("S")
}

func TestReaderBasics(t *testing.T) {
	data := pkg(t, `<row r="1"><c r="A1" t="s"><v>1</v></c><c r="C1" s="1"><v>2.5</v></c></row><row r="3"><c r="B3"><v>7</v></c></row>`, nil)
	x, err := OpenBytes(data, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := x.GetSheetList(); len(got) != 1 || got[0] != "S" {
		t.Fatalf("sheets %q", got)
	}
	rows, err := x.GetRows("s") // any case, as excelize
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"b", "", "2.50"}, nil, {"", "7"}}
	if !sameRows(rows, want) {
		t.Fatalf("rows %q, want %q", rows, want)
	}
	if rows, err := x.GetRowsRaw("s"); err != nil || !sameRows(rows, [][]string{{"b", "", "2.5"}, nil, {"", "7"}}) {
		t.Fatalf("raw rows %q, %v", rows, err)
	}
	for _, get := range []func(string) ([][]string, error){x.GetRows, x.GetRowsRaw} {
		if _, err := get("T"); err == nil || !strings.Contains(err.Error(), `sheet "T" does not exist`) {
			t.Fatalf("unknown sheet: %v", err)
		}
	}
	if err := x.Close(); err != nil {
		t.Fatal(err)
	}
	for _, get := range []func(string) ([][]string, error){x.GetRows, x.GetRowsRaw} {
		if _, err := get("S"); !errors.Is(err, errReaderClosed) {
			t.Fatalf("after Close: %v", err)
		}
	}

	// From a stream and from a file, as OpenReader and Open take them.
	y, err := OpenReader(bytes.NewReader(data), nil)
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := y.GetRows("S"); err != nil || !sameRows(rows, want) {
		t.Fatalf("OpenReader: %q %v", rows, err)
	}
	path := filepath.Join(t.TempDir(), "a.xlsx")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	z, err := Open(f, int64(len(data)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := z.GetRows("S"); err != nil || !sameRows(rows, want) {
		t.Fatalf("Open: %q %v", rows, err)
	}
}

func sameRows(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if strings.Join(a[i], "\x00") != strings.Join(b[i], "\x00") || len(a[i]) != len(b[i]) {
			return false
		}
	}
	return true
}

func TestReaderConcurrentGetRows(t *testing.T) {
	data, err := os.ReadFile("testdata/excelize.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	x, err := OpenBytes(data, nil)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, s := range x.GetSheetList() {
				if _, err := x.GetRows(s); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
}

// zeros is a part that deflates about a thousand to one.
func zeros(n int) string {
	return `<worksheet xmlns="` + nsMain + `">` + strings.Repeat(" ", n) + `<sheetData/></worksheet>`
}

func TestReaderLimits(t *testing.T) {
	tests := []struct {
		name  string
		data  func(t *testing.T) []byte
		lim   ReaderLimits
		limit string // LimitError.Limit
		seen  int64
	}{
		{"file size", func(t *testing.T) []byte { return pkg(t, "", nil) }, ReaderLimits{MaxFileSize: 100}, "file size in bytes", -1},
		{"entries", func(t *testing.T) []byte { return pkg(t, "", nil) }, ReaderLimits{MaxEntries: 5}, "zip entries", 6},
		{"zip bomb by ratio", func(t *testing.T) []byte {
			return pkg(t, "", map[string]string{"xl/worksheets/sheet1.xml": zeros(4 << 20)})
		}, ReaderLimits{}, "compression ratio", -1},
		{"part size", func(t *testing.T) []byte {
			return pkg(t, "", map[string]string{"xl/worksheets/sheet1.xml": zeros(64 << 10)})
		}, ReaderLimits{MaxPartSize: 32 << 10}, "decompressed bytes of a part", -1},
		{"total size", func(t *testing.T) []byte {
			return pkg(t, "", map[string]string{"xl/worksheets/sheet1.xml": zeros(64 << 10)})
		}, ReaderLimits{MaxTotalSize: 32 << 10}, "decompressed bytes in all", -1},
		{"depth", func(t *testing.T) []byte {
			return pkg(t, `<row>`+strings.Repeat("<x>", 70)+strings.Repeat("</x>", 70)+`</row>`, nil)
		}, ReaderLimits{}, "XML element depth", 65},
		{"tokens", func(t *testing.T) []byte {
			return pkg(t, strings.Repeat(`<row/>`, 100), nil)
		}, ReaderLimits{MaxXMLTokens: 50}, "XML tokens in a part", 51},
		{"sheets", func(t *testing.T) []byte {
			return pkg(t, "", map[string]string{"xl/workbook.xml": `<workbook xmlns:r="` + relNS + `"><sheets><sheet name="S" r:id="rId1"/><sheet name="T" r:id="rId1"/></sheets></workbook>`})
		}, ReaderLimits{MaxSheets: 1}, "sheets", 2},
		{"rows", func(t *testing.T) []byte { return pkg(t, `<row r="11"><c><v>1</v></c></row>`, nil) }, ReaderLimits{MaxRows: 10}, "row number", 11},
		{"Excel's last row and one", func(t *testing.T) []byte { return pkg(t, `<row r="1048577"/>`, nil) }, ReaderLimits{}, "row number", 1048577},
		{"columns", func(t *testing.T) []byte { return pkg(t, `<row><c r="K1"><v>1</v></c></row>`, nil) }, ReaderLimits{MaxColumns: 10}, "column number", 11},
		{"a run of letters", func(t *testing.T) []byte { return pkg(t, `<row><c r="AAAAAAAAAAAAAAAAAAAA1"><v>1</v></c></row>`, nil) }, ReaderLimits{}, "column number", -1},
		{"column XFE", func(t *testing.T) []byte { return pkg(t, `<row><c r="XFE1"><v>1</v></c></row>`, nil) }, ReaderLimits{}, "column number", 16385},
		{"cells padded", func(t *testing.T) []byte { return pkg(t, `<row><c r="F1"><v>1</v></c></row>`, nil) }, ReaderLimits{MaxCells: 5}, "cells returned for a sheet", 6},
		{"empty rows count", func(t *testing.T) []byte { return pkg(t, `<row r="10"><c><v>1</v></c></row>`, nil) }, ReaderLimits{MaxCells: 5}, "cells returned for a sheet", 10},
		{"shared strings", func(t *testing.T) []byte { return pkg(t, `<row><c t="s"><v>0</v></c></row>`, nil) }, ReaderLimits{MaxSharedStrings: 1}, "shared strings", 2},
		{"shared string length", func(t *testing.T) []byte {
			return pkg(t, `<row><c t="s"><v>0</v></c></row>`, map[string]string{"xl/sharedStrings.xml": `<sst><si><t>abc😀def</t></si></sst>`})
		}, ReaderLimits{MaxStringUnits: 5}, "shared string length in UTF-16 units", 8},
		{"inline string bytes", func(t *testing.T) []byte {
			return pkg(t, `<row><c t="inlineStr"><is><t>`+strings.Repeat("x", 30)+`</t></is></c></row>`, nil)
		}, ReaderLimits{MaxStringUnits: 5}, "inline string bytes", -1},
		{"output bomb through a text format", func(t *testing.T) []byte {
			// A 32,000-unit string shown through 1,024 @s: 50 such cells
			// once made 1.6 GB of GetRows output from a 1.8 KB file.
			return pkg(t, strings.Repeat(`<row><c t="s" s="1"><v>0</v></c></row>`, 50), map[string]string{
				"xl/sharedStrings.xml": `<sst><si><t>` + strings.Repeat("x", 32000) + `</t></si></sst>`,
				"xl/styles.xml":        `<styleSheet><numFmts><numFmt numFmtId="164" formatCode="` + strings.Repeat("@", 1024) + `"/></numFmts><cellXfs><xf/><xf numFmtId="164"/></cellXfs></styleSheet>`,
			})
		}, ReaderLimits{}, "rendered cell length in UTF-16 units", 32000 * 1024},
		{"rendered cell", func(t *testing.T) []byte {
			return pkg(t, `<row><c t="s" s="1"><v>0</v></c></row>`, map[string]string{
				"xl/sharedStrings.xml": `<sst><si><t>abcdefghij</t></si></sst>`,
				"xl/styles.xml":        `<styleSheet><numFmts><numFmt numFmtId="164" formatCode="@&quot;-&quot;@"/></numFmts><cellXfs><xf/><xf numFmtId="164"/></cellXfs></styleSheet>`,
			})
		}, ReaderLimits{MaxStringUnits: 20}, "rendered cell length in UTF-16 units", 21},
		{"rendered number", func(t *testing.T) []byte {
			return pkg(t, `<row><c s="1"><v>1</v></c></row>`, map[string]string{
				"xl/styles.xml": `<styleSheet><numFmts><numFmt numFmtId="164" formatCode="` + strings.Repeat("0", 30) + `"/></numFmts><cellXfs><xf/><xf numFmtId="164"/></cellXfs></styleSheet>`,
			})
		}, ReaderLimits{MaxStringUnits: 20}, "rendered cell length in UTF-16 units", 30},
		{"t=str value", func(t *testing.T) []byte {
			return pkg(t, `<row><c t="str"><v>`+strings.Repeat("y", 30)+`</v></c></row>`, nil)
		}, ReaderLimits{MaxStringUnits: 10}, "rendered cell length in UTF-16 units", 30},
		{"sheet bytes", func(t *testing.T) []byte {
			return pkg(t, `<row>`+strings.Repeat(`<c t="s"><v>0</v></c>`, 10)+`</row>`, map[string]string{"xl/sharedStrings.xml": `<sst><si><t>0123456789</t></si></sst>`})
		}, ReaderLimits{MaxSheetBytes: 95}, "bytes of text returned for a sheet", 100},
		{"zip bomb whose header claims more compressed bytes", func(t *testing.T) []byte {
			return lyingZip(t, pkgParts("", map[string]string{"xl/worksheets/sheet1.xml": zeros(4 << 20)}), "xl/worksheets/sheet1.xml", 1<<20)
		}, ReaderLimits{}, "compression ratio", -1},
		{"cell formats", func(t *testing.T) []byte {
			return pkg(t, `<row><c><v>1</v></c></row>`, map[string]string{"xl/styles.xml": `<styleSheet><cellXfs><xf/><xf/><xf/></cellXfs></styleSheet>`})
		}, ReaderLimits{MaxStyles: 2}, "cell formats", 3},
		{"number formats", func(t *testing.T) []byte {
			return pkg(t, `<row><c><v>1</v></c></row>`, map[string]string{"xl/styles.xml": `<styleSheet><numFmts><numFmt numFmtId="164" formatCode="0"/><numFmt numFmtId="165" formatCode="0"/></numFmts></styleSheet>`})
		}, ReaderLimits{MaxStyles: 1}, "number formats", 2},
	}
	// formattedOnly are the cases whose cell is long only once its number
	// format is applied; GetRowsRaw gives the stored value and passes them.
	formattedOnly := map[string]bool{"output bomb through a text format": true, "rendered cell": true, "rendered number": true}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := tt.data(t)
			for _, read := range []struct {
				name string
				rows func([]byte, *ReaderLimits) ([][]string, error)
			}{{"GetRows", rowsOf}, {"GetRowsRaw", rawRowsOf}} {
				lim := tt.lim
				_, err := read.rows(data, &lim)
				if read.name == "GetRowsRaw" && formattedOnly[tt.name] {
					if err != nil {
						t.Fatalf("%s: %v, want the stored value", read.name, err)
					}
					continue
				}
				checkLimitError(t, read.name, err, tt.limit, tt.seen)
			}
		})
	}
}

func checkLimitError(t *testing.T, read string, err error, limit string, seen int64) {
	t.Helper()
	var le *LimitError
	if !errors.As(err, &le) {
		t.Fatalf("%s: error %v, want a *LimitError", read, err)
	}
	if le.Limit != limit || seen >= 0 && le.Seen != seen || le.Seen <= le.Max {
		t.Fatalf("%s: %+v, want limit %q seen %d", read, *le, limit, seen)
	}
	if !strings.Contains(err.Error(), limit) || !strings.Contains(err.Error(), "ReaderLimits") {
		t.Fatalf("%s: message %q does not name the limit", read, err)
	}
}

// A number is written with 15 significant digits once formatted, but as
// stored when raw, so a long <v> passes GetRows and is too long for
// GetRowsRaw: the raw value goes through the same cell length check.
func TestReaderRawCellLength(t *testing.T) {
	data := pkg(t, `<row><c><v>`+strings.Repeat("1", 30)+`</v></c></row>`, nil)
	lim := ReaderLimits{MaxStringUnits: 25}
	rows, err := rowsOf(data, &lim)
	if err != nil || !sameRows(rows, [][]string{{"1.11111111111111E+29"}}) {
		t.Fatalf("GetRows: %q, %v", rows, err)
	}
	lim = ReaderLimits{MaxStringUnits: 25}
	_, err = rawRowsOf(data, &lim)
	checkLimitError(t, "GetRowsRaw", err, "rendered cell length in UTF-16 units", 30)
}

// lyingZip zips parts as zipParts does, but writes part's directory entry
// with a compressed size of claim: a ratio taken from the header would be
// about four, while what actually inflates is over a thousand to one.
func lyingZip(t testing.TB, parts []string, part string, claim uint64) []byte {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for i := 0; i < len(parts); i += 2 {
		name, body := parts[i], parts[i+1]
		if name != part {
			w, err := zw.Create(name)
			if err != nil {
				t.Fatal(err)
			}
			io.WriteString(w, body)
			continue
		}
		var comp bytes.Buffer
		fw, _ := flate.NewWriter(&comp, flate.BestCompression)
		io.WriteString(fw, body)
		fw.Close()
		// Pad the stream's bytes up to the claim, as a forged entry would:
		// flate stops at the stream's end and never reads the padding.
		raw := append(comp.Bytes(), make([]byte, int(claim)-comp.Len())...)
		w, err := zw.CreateRaw(&zip.FileHeader{Name: name, Method: zip.Deflate, CRC32: crc32.ChecksumIEEE([]byte(body)),
			CompressedSize64: claim, UncompressedSize64: uint64(len(body))})
		if err != nil {
			t.Fatal(err)
		}
		w.Write(raw)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestReaderChecksDirectoryCRC(t *testing.T) {
	parts := pkgParts(`<row><c><v>1</v></c></row>`, nil)
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for i := 0; i < len(parts); i += 2 {
		name, body := parts[i], parts[i+1]
		crc := crc32.ChecksumIEEE([]byte(body))
		if name == "xl/workbook.xml" { // read to its end; a sheet is read only to </sheetData>
			crc++
		}
		w, err := zw.CreateRaw(&zip.FileHeader{Name: name, Method: zip.Store, CRC32: crc,
			CompressedSize64: uint64(len(body)), UncompressedSize64: uint64(len(body))})
		if err != nil {
			t.Fatal(err)
		}
		io.WriteString(w, body)
	}
	zw.Close()
	if _, err := rowsOf(buf.Bytes(), nil); !errors.Is(err, zip.ErrChecksum) {
		t.Fatalf("error %v, want zip.ErrChecksum", err)
	}
}

func TestReaderStreamOverFileSize(t *testing.T) {
	_, err := OpenReader(bytes.NewReader(make([]byte, 101)), &ReaderLimits{MaxFileSize: 100})
	var le *LimitError
	if !errors.As(err, &le) || le.Limit != "file size in bytes" {
		t.Fatalf("%v", err)
	}
}

func TestReaderRefusesMalformed(t *testing.T) {
	sheet := func(s string) map[string]string { return map[string]string{"xl/worksheets/sheet1.xml": s} }
	dupe := func(t *testing.T) []byte {
		parts := pkgParts(`<row><c><v>1</v></c></row>`, nil)
		return zipParts(t, append(parts, "xl/worksheets/sheet1.xml", "<worksheet/>"))
	}
	tests := []struct {
		name string
		data func(t *testing.T) []byte
		want string
	}{
		{"not a zip", func(t *testing.T) []byte { return []byte("hello, world") }, "not a readable zip package"},
		{"compound file", func(t *testing.T) []byte {
			return append([]byte{0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1}, make([]byte, 512)...)
		}, "Compound File"},
		{"truncated zip", func(t *testing.T) []byte { b := pkg(t, "", nil); return b[:len(b)-30] }, "not a readable zip package"},
		{"no package rels", func(t *testing.T) []byte { return pkg(t, "", map[string]string{"_rels/.rels": ""}) }, "no _rels/.rels"},
		{"no workbook", func(t *testing.T) []byte {
			return pkg(t, "", map[string]string{"_rels/.rels": `<Relationships/>`})
		}, "names no workbook"},
		{"DTD", func(t *testing.T) []byte {
			return pkg(t, "", sheet(`<!DOCTYPE w [<!ENTITY a "aaaaaaaaaa"><!ENTITY b "&a;&a;&a;&a;">]><worksheet><sheetData><row><c t="inlineStr"><is><t>&b;</t></is></c></row></sheetData></worksheet>`))
		}, "DTDs are refused"},
		{"undefined entity", func(t *testing.T) []byte {
			return pkg(t, `<row><c t="inlineStr"><is><t>&ext;</t></is></c></row>`, nil)
		}, "malformed XML"},
		{"UTF-16 declared", func(t *testing.T) []byte {
			return pkg(t, "", sheet(`<?xml version="1.0" encoding="UTF-16"?><worksheet/>`))
		}, "malformed XML"},
		{"mismatched tags", func(t *testing.T) []byte { return pkg(t, `<row><c></row>`, nil) }, "malformed XML"},
		{"truncated XML", func(t *testing.T) []byte { return pkg(t, "", sheet(`<worksheet><sheetData><row><c>`)) }, "unexpected EOF"},
		{"shared string index past the table", func(t *testing.T) []byte { return pkg(t, `<row><c t="s"><v>2</v></c></row>`, nil) }, "outside the table of 2 strings"},
		{"shared string index not a number", func(t *testing.T) []byte { return pkg(t, `<row><c t="s"><v>x</v></c></row>`, nil) }, "outside the table"},
		{"negative shared string index", func(t *testing.T) []byte { return pkg(t, `<row><c t="s"><v>-1</v></c></row>`, nil) }, "outside the table"},
		{"rows going back", func(t *testing.T) []byte { return pkg(t, `<row r="2"/><row r="1"/>`, nil) }, "row 1 comes after row 2"},
		{"row twice", func(t *testing.T) []byte { return pkg(t, `<row r="2"/><row r="2"/>`, nil) }, "row 2 comes after row 2"},
		{"row zero", func(t *testing.T) []byte { return pkg(t, `<row r="0"/>`, nil) }, "not a positive integer"},
		{"cells going back", func(t *testing.T) []byte {
			return pkg(t, `<row><c r="C1"><v>1</v></c><c r="B1"><v>1</v></c></row>`, nil)
		}, "column 2 comes after column 3"},
		{"bad cell reference", func(t *testing.T) []byte { return pkg(t, `<row><c r="1A"><v>1</v></c></row>`, nil) }, "not a column and a row"},
		{"bad style index", func(t *testing.T) []byte { return pkg(t, `<row><c s="x"><v>1</v></c></row>`, nil) }, "style index"},
		{"sheet part missing", func(t *testing.T) []byte { return pkg(t, "", sheet("")) }, "missing from the zip"},
		{"part twice in the zip", dupe, "more than once"},
		{"target leaving the package", func(t *testing.T) []byte {
			return pkg(t, "", map[string]string{"xl/_rels/workbook.xml.rels": `<Relationships><Relationship Id="rId1" Type="x/worksheet" Target="../../../etc/passwd"/></Relationships>`})
		}, "leaves the package"},
		{"target with a scheme", func(t *testing.T) []byte {
			return pkg(t, "", map[string]string{"xl/_rels/workbook.xml.rels": `<Relationships><Relationship Id="rId1" Type="x/worksheet" Target="file:///etc/passwd"/></Relationships>`})
		}, "not a part name"},
		{"external sheet not followed", func(t *testing.T) []byte {
			return pkg(t, "", map[string]string{"xl/_rels/workbook.xml.rels": `<Relationships><Relationship Id="rId1" Type="x/worksheet" Target="http://example.invalid/s.xml" TargetMode="External"/></Relationships>`})
		}, "no target for it"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := rowsOf(tt.data(t), nil)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %v, want one with %q", err, tt.want)
			}
		})
	}
}

func TestResolveTarget(t *testing.T) {
	for _, tt := range []struct{ base, target, want string }{
		{"xl", "worksheets/sheet1.xml", "xl/worksheets/sheet1.xml"},
		{"xl", "/xl/worksheets/sheet1.xml", "xl/worksheets/sheet1.xml"},
		{"xl", "./worksheets/../sheet%201.xml", "xl/sheet 1.xml"},
		{"", "xl/workbook.xml", "xl/workbook.xml"},
		{"xl", "../xl/a.xml", "xl/a.xml"},
		{"xl", "../../a.xml", ""},
		{"xl", `worksheets\sheet1.xml`, ""},
		{"xl", "http://x/y.xml", ""},
		{"xl", "/", ""},
		{"xl", "%zz", ""},
	} {
		got, err := resolveTarget(tt.base, tt.target)
		if tt.want == "" && err == nil || tt.want != "" && got != tt.want {
			t.Errorf("resolveTarget(%q, %q) = %q, %v; want %q", tt.base, tt.target, got, err, tt.want)
		}
	}
}

func TestUnsupportedFormats(t *testing.T) {
	for _, code := range []string{"# ?/?", "# ??/??", "[<100]0;0", "[DBNum1]0", "[$-411]mmmm", "aaa", "ggg e", "0;0;0;0;0", "[Red"} {
		for _, v := range []string{"1.5", "45000.25", "-3"} {
			if got := parseNumFmt(code).render(v, kindNumber, false); got != v {
				t.Errorf("%q on %s gives %q, want the value as stored", code, v, got)
			}
		}
	}
	if got := parseNumFmt(strings.Repeat("0", maxNumFmtRunes+1)).render("2", kindNumber, false); got != "2" {
		t.Errorf("a format past %d characters gives %q", maxNumFmtRunes, got)
	}
}

func TestDecodeEscapes(t *testing.T) {
	for in, want := range map[string]string{
		"_x0041_":       "A",
		"_x005F_x0041_": "_x0041_",
		"_x0041_x0042_": "Ax0042_",
		"a_xD83D_b":     "ab",
		"_x00e9_":       "é",
		"_x004":         "_x004",
		"plain":         "plain",
		// The edges of the four-digit range, and the surrogate range dropped.
		"_x0000_": "\x00",
		"_xFFFF_": "\uFFFF",
		"_xD800_": "",
		"_xDFFF_": "",
		"_xE000_": "\uE000",
	} {
		if got := decodeEscapes(in); got != want {
			t.Errorf("decodeEscapes(%q) = %q, want %q", in, got, want)
		}
	}
}

// The writer's output read back by the reader: every encoding case of §5.2
// as Excel would show it.
func TestReaderReadsTheWriter(t *testing.T) {
	data := write(t, Options{SheetName: "Coverage"}, coverage[0], coverage[1:]...)
	x, err := OpenBytes(data, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := x.GetRows("Coverage")
	if err != nil {
		t.Fatal(err)
	}
	want := read(t, data).Rows
	for i := range want {
		for len(want[i]) > 0 && want[i][len(want[i])-1] == "" {
			want[i] = want[i][:len(want[i])-1]
		}
	}
	if !sameRows(rows, want) {
		for i := range rows {
			for j := range rows[i] {
				if i < len(want) && j < len(want[i]) && rows[i][j] != want[i][j] {
					t.Errorf("%s%d: %q, want %q", columnLetters(j+1), i+1, short(rows[i][j]), short(want[i][j]))
				}
			}
		}
		t.Fatalf("rows differ in shape: %d rows, want %d", len(rows), len(want))
	}
}

// Only <numFmts> and <cellXfs> count: a conditional format's <numFmt> and a
// cell style's <xf> do not change a cell's format.
func TestReaderStylesScope(t *testing.T) {
	styles := `<styleSheet><numFmts><numFmt numFmtId="164" formatCode="0.0"/></numFmts>` +
		`<cellStyleXfs><xf numFmtId="164"/></cellStyleXfs>` +
		`<cellXfs><xf/><xf numFmtId="164"/><xf numFmtId="165"/></cellXfs>` +
		`<dxfs><dxf><numFmt numFmtId="165" formatCode="0.000"/></dxf></dxfs></styleSheet>`
	rows, err := rowsOf(pkg(t, `<row><c s="1"><v>2</v></c><c s="2"><v>2</v></c><c s="0"><v>2</v></c></row>`, map[string]string{"xl/styles.xml": styles}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := [][]string{{"2.0", "2", "2"}}; !sameRows(rows, want) {
		t.Fatalf("rows %q, want %q", rows, want)
	}
}

// A large export of repetitive values, as the writer makes it, is read with
// the default limits: its sheet part compresses about 13 to 1, far under
// MaxCompressionRatio, and passes the 1 MiB floor where the ratio counts.
func TestReaderDefaultsReadALargeExport(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a 60,000-row workbook")
	}
	var buf bytes.Buffer
	wb, err := NewWorkbook(&buf, WorkbookOptions{Sheets: []SheetOptions{{Name: "S"}}})
	if err != nil {
		t.Fatal(err)
	}
	style, err := wb.AddStyle(CellStyle{NumberFormat: "#,##0.00"})
	if err != nil {
		t.Fatal(err)
	}
	const n = 60000
	for i := 0; i < n; i++ {
		if err := wb.WriteRow(Row{Cells: []Cell{Text("same text", 0), Number(1234.5, style), Number(1, 0), Blank(style), Text("x", 0)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := wb.Close(); err != nil {
		t.Fatal(err)
	}
	rows, err := rowsOf2(buf.Bytes(), "S")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != n || !sameRows(rows[n-1:], [][]string{{"same text", "1,234.50", "1", "", "x"}}) {
		t.Fatalf("%d rows, last %q", len(rows), rows[len(rows)-1])
	}
}

func rowsOf2(data []byte, sheet string) ([][]string, error) {
	x, err := OpenBytes(data, nil)
	if err != nil {
		return nil, err
	}
	return x.GetRows(sheet)
}
