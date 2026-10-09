# Replacing excelize with `utils/xlsx`

## Summary

dxlib requires `github.com/xuri/excelize/v2` v2.11.0 for one job: turning a page
of database rows into a one-sheet `.xlsx` download. That release carries some
fifteen advisories with no fixed version (GHSA-2j4c, -5h23, -8jjq, -8mcq, -c85p,
-fq3v, -fw94, -g27h, -jrfj, -jw42, -mx22, -r7x5, -rxcj, -wp2g, -x2q3; GitHub
lists -q5j5, -fx5j and -h69g as well). All of them are denial of service on
*reading* a malformed workbook: panics, hangs, unbounded allocation. dxlib never
reads one, so none is reachable, and they are accepted in
`.dependency-allowlist.json` and `.trivyignore` only until 2026-10-23.

The decision: drop excelize and write a small writer of our own, `utils/xlsx`,
that produces what dxlib produces today and nothing else (two deliberate
encoding fixes aside, §8). It is our own
code, built on `archive/zip` and hand-written XML, not a copy of excelize.

The public surface of `databases/export` (`ExportFormat`, `ExportOptions`,
`ExportToStream`, `ExportQueryResults`) does not change. No module outside dxlib
calls excelize or `databases/export` directly, so the change reaches them only
as a smaller dependency graph after the pin moves.

## 1. Where excelize is used today

### 1.1 In dxlib

One production file, write-only: `databases/export/export.go`.

| Call | What it does here |
|---|---|
| `excelize.NewFile()` | a new workbook holding one sheet, `Sheet1` |
| `CoordinatesToCellName(col, row)` | `A1`-style names for each cell |
| `SetCellValue(sheet, cell, string)` | every value; `formatValue` has already turned it into a string |
| `NewStyle(Font{Bold}, Alignment{Horizontal: "center"}, Fill{pattern 1, #E0EBF5})` | the header style |
| `SetRowStyle(sheet, 1, 1, style)` | that style on row 1 |
| `ColumnNumberToName(i)` + `SetColWidth(sheet, col, col, 15)` | width 15 on every column |
| `SaveAs(path)` / `WriteToBuffer()` | the file, or the bytes for a download |
| `Close()` | releases excelize's temp files |

Features in use: one sheet, string cells only, one cell style, fixed column
widths. Not in use: numbers, dates or booleans as typed cells, number formats,
formulas, merged cells, more than one sheet, images, charts, streaming writer,
encryption, and any reading or calculation.

How excelize stores a string (v2.11.0, `cell.go`), which is what the files
downloaded today contain:

- in the shared string table (`t="s"`), deduplicated;
- truncated to 32,767 UTF-16 code units (`TotalCellChars`) without an error;
- `xml:space="preserve"` when the value starts or ends with a tab, LF, CR or
  space;
- a literal `_xHHHH_` left as it is: `setSharedString` builds the `<si>` from
  the raw value and uses the `bstrMarshal` form only as the dedup key, so
  Excel shows `_x0041_` as `A`;
- a character XML 1.0 forbids (U+0001 and the like) replaced with U+FFFD by
  `encoding/xml`, so it does not survive;
- CR written as `&#xD;`;
- an empty value stored as a shared string `""`.

A string is never written as a formula, which is why the XLSX path needs no
CSV-style formula neutralising (`TestXLSXKeepsRawValue`).

The one test that reads a workbook back is `TestXLSXKeepsRawValue` in
`export_csv_injection_test.go`, through `excelize.OpenReader`.

### 1.2 Callers inside dxlib

`tables.DXRawTable.DoRequestSearchPagingDownload` (and `DXTable`'s override of
`RequestSearchPagingDownload`, which calls it) is the only caller of
`databases/export`. It passes `SheetName: "Sheet1"`, a date format, a timezone
and a language, calls `ExportToStream`, and sets its own `Content-Type` per
format.

### 1.3 Callers outside dxlib

Searched on 2026-10-09 (Go sources and every other file):

- `dxlib_module`: no reference to excelize, `databases/export` or the download
  handler. It sets `DownloadableOrderByFieldNames` on several tables, which only
  matters once something registers the handler. excelize is in its `go.mod` as
  `// indirect`.
- `dx-erp` backends (outside its nested `libraries/` copies): no reference.
  excelize is `// indirect` in `src/backends/go.mod` and
  `src/tools/tool-system0-reset/go.mod`, and in its nested `dxlib_module`
  copy; its nested `dxlib` copy requires it directly.
- `dx-service-push-notification`: no reference; `// indirect` in `go.mod`.

So no consumer changes code. Each one loses excelize from its `go.mod` and
`go.sum` once it moves to a dxlib tag without it.

### 1.4 Two defects found while mapping

Both are in `databases/export`, both fixed by the switch-over (step 2 in §7):

1. **`SheetName` other than `Sheet1` fails.** `excelize.NewFile()` makes only
   `Sheet1`; `SetCellValue` on any other name returns
   `sheet Data does not exist`, so `ExportToStream` with `SheetName: "Data"`
   errors. Every caller passes `Sheet1` today, so nobody has hit it. The new
   writer names the sheet whatever the option says.
2. **`ExportQueryResults` refuses `xlsx`.** `ExportToStream` was taught `XLSX`;
   the file-path variant still only knows `CSV` and `XLS` and answers
   `unsupported export format: xlsx`.

Both were checked by running the current code on 2026-10-09.

## 2. Scope: a writer only

dxlib does not read `.xlsx` files anywhere in production code. So `utils/xlsx`
is a writer and has no reader. The advisories are all about parsing hostile
files; with no parser there is nothing for them to reach, and nothing for
malformed-file tests to exercise.

The tests still have to read what the writer wrote. They do that with a small
test-only parser (§6.2) that reads only files this writer produced. It is not
exported and not built into any binary.

If dxlib ever has to read `.xlsx` (an import feature), that is a new design,
and it starts from the limits listed in §5.3, which are written down now so
that nobody grows the test parser into a production reader without them.

## 3. The package

### 3.1 API

```go
package xlsx

// Style is the only cell formatting the writer knows.
type Style struct {
	Bold       bool
	Center     bool   // horizontal alignment
	FillRGB    string // "E0EBF5"; empty means no fill
}

type Options struct {
	SheetName    string    // required; validated (§5.1)
	ColumnWidths []float64 // one per column, in character units; 0 leaves the default
	HeaderStyle  *Style    // applied to the first row written with WriteHeader; nil for none
}

// NewWriter starts a workbook with one sheet. It writes to w as rows arrive.
func NewWriter(w io.Writer, opts Options) (*Writer, error)

// WriteHeader writes the first row, in HeaderStyle. At most once, before any WriteRow.
func (w *Writer) WriteHeader(cells []string) error

// WriteRow writes the next row. Every cell is text.
func (w *Writer) WriteRow(cells []string) error

// Close writes the remaining parts and the zip directory. It does not close w.
func (w *Writer) Close() error
```

Everything is text, as it is today. Typed cells (numbers, dates) are a possible
later addition (§8) and not part of this design.

Errors are values from `dxlib/errors`; the writer never panics on any input.
After an error, every later call returns the same error.

### 3.2 How `databases/export` uses it

`writeXLSContent` becomes:

```go
xw, err := xlsx.NewWriter(out, xlsx.Options{
	SheetName:    sheetName,                       // "Sheet1" when empty, as now
	ColumnWidths: widths,                          // 15 for each column
	HeaderStyle:  &xlsx.Style{Bold: true, Center: true, FillRGB: "E0EBF5"},
})
// WriteHeader(translated headers), WriteRow(formatValue(...)) per row, Close.
```

`exportToXLSStream` writes into a `bytes.Buffer`; `exportToXLS` writes into the
`os.File` it creates. `ExportQueryResults` accepts `XLSX` beside `XLS`.

### 3.3 Streaming and memory

The zip entries are written in this order:

1. `xl/worksheets/sheet1.xml`, row by row as `WriteRow` is called;
2. `xl/sharedStrings.xml`, after `Close`, from the table built while the rows
   went by;
3. the fixed parts (§4).

Zip readers locate parts through the central directory, so the order inside
the archive does not matter to them. Memory is the shared string table (one
copy of each distinct string plus an index map) and the deflate window, not
the whole sheet. Today's caller already holds every row in memory, so this is
no worse than excelize and needs no temp files.

Column widths go in `<cols>`, which the schema puts before `<sheetData>`;
that is why they are given in `Options` and not set later. `<cols>` is written
only when at least one width is non-zero, because the schema requires at least
one `<col>` in it and Excel offers to repair a file with an empty one; adjacent
equal widths share one `<col min max>`.

`<dimension>` is left out. The schema also puts it before `<sheetData>`, and the
row count is not known until `Close`. It is optional and Excel does not need
it; a placeholder would be wrong for every sheet.

### 3.4 Shared strings, not inline strings

Inline strings (`t="inlineStr"`) would make the writer simpler: no table, no
second entry. They are valid OOXML, but some lightweight readers and import
tools only look in the shared string table, and every file downloaded today
uses it. The table costs one map and is kept, so a file from the new writer
reads the same everywhere the old one did.

## 4. The parts written

| Part | Content |
|---|---|
| `[Content_Types].xml` | defaults for `rels` and `xml`; overrides for workbook, worksheet, styles, sharedStrings |
| `_rels/.rels` | officeDocument → `xl/workbook.xml` |
| `xl/workbook.xml` | one `<sheet name="…" sheetId="1" r:id="rId1"/>`, name XML-escaped |
| `xl/_rels/workbook.xml.rels` | rId1 → worksheet, rId2 → styles, rId3 → sharedStrings |
| `xl/worksheets/sheet1.xml` | `<cols>` (only when a width is set), `<sheetData>` |
| `xl/styles.xml` | see below |
| `xl/sharedStrings.xml` | `<sst count uniqueCount>` with one `<si><t>` per string |

`styles.xml` holds the minimum the schema and Excel need: two fonts (default
Calibri 11, and the same bold, each with an explicit `<color rgb="FF000000"/>`
and no `<scheme>`, since there is no theme part to point `theme="1"` or
`minor` at), two fills (`none` and `gray125`, which Excel
expects at indices 0 and 1) plus one solid fill for the header colour, one
empty border, the default `cellStyleXfs`/`cellStyles` entry, and two `cellXfs`
(0 default, 1 header: bold font, solid fill, `<alignment horizontal="center"/>`).

Rows: `<row r="N">` with `<c r="A1" t="s"><v>i</v></c>` per cell. An empty
cell is written as a shared string `""`, as excelize does, so every row has
one `<c>` per column. The header
row also carries `s="1" customFormat="1"`, and each of its cells `s="1"`, which
is what `SetRowStyle` produced. Cell references are written out (`r=`) because
some readers require them.

Left out: `docProps/core.xml`, `docProps/app.xml` and `xl/theme/theme1.xml`.
None is required by the package rules. Leaving them out keeps the output
deterministic (no creation time) and smaller. Excel accepts a package without them. That is
checked, not assumed: LibreOffice (§6.3) is more lenient than Excel, so it is
not the proof; one manual open in Excel itself, with no repair prompt, is
required before step 2 lands. If Excel complains, `docProps` goes back in with
a fixed timestamp.

Zip entries are deflated and carry a fixed modification time, so the same
input gives the same entries.

## 5. Limits and text encoding

### 5.1 Enforced by the writer, as errors

| Limit | Value | Where it comes from |
|---|---|---|
| rows per sheet | 1,048,576 (header included) | Excel's sheet size |
| cells per row | 16,384 (column `XFD`) | Excel's sheet size |
| sheet name | 1 to 31 UTF-16 units; none of `[ ] : * ? / \`; not starting or ending with `'`; not `History` in any letter case | Excel's rules for sheet names |
| `ColumnWidths` | at most 16,384 entries, each 0 or in (0, 255] | Excel's column width range |
| `FillRGB` | exactly six hex digits | |
| `WriteHeader` | once, before the first `WriteRow` | |

An export that hits the row limit fails with an error, as it does today
(`CoordinatesToCellName` refuses row 1,048,577), instead of producing a file
Excel cannot open.

### 5.2 Text, applied to every cell and to the sheet name

In this order:

1. **Invalid UTF-8** becomes U+FFFD, rune by rune (what `encoding/xml` does).
2. **Length**: a value longer than 32,767 UTF-16 units is cut to 32,767,
   never between the two halves of a surrogate pair. Silent, as today, so an
   export that works now keeps working.
3. **`_xHHHH_`**: every `_` at which `_x[0-9A-Fa-f]{4}_` starts, overlapping
   matches included, is written as `_x005F_`, so Excel shows the text as
   typed. Overlaps matter: in `_x0041_x0042_` the middle `_` starts the second
   sequence, and escaping only non-overlapping matches leaves Excel decoding
   it. This is a change from today (§1.1), where such text is not escaped.
4. **Characters XML 1.0 forbids** (U+0000–U+0008, U+000B, U+000C,
   U+000E–U+001F, U+FFFE, U+FFFF) are written as `_xHHHH_` (ECMA-376 Part 1,
   ST_Xstring), so they are never raw in the XML and Excel restores them.
   Today they become U+FFFD (§1.1); this is a change, and a better one.
5. **XML escaping**: `&`, `<`, `>` as entities; CR as `&#xD;` so a parser does
   not fold it into LF. Tab and LF stay literal.
6. **`xml:space="preserve"`** on `<t>` when the value starts or ends with a
   space, tab, LF or CR.

In the sheet name, step 4's characters and the forbidden punctuation are
refused (§5.1) rather than encoded.

Nothing is ever written as a formula or as a bare `<v>` number: a value such as
`=HYPERLINK("http://x","y")` stays text, which `TestXLSXKeepsRawValue` checks.

### 5.3 If a reader is ever needed

Not built now. Any future reader starts from these, each one returning an
error, never panicking:

- the input size capped before opening (default 50 MiB), read through
  `io.LimitReader`;
- a cap on entries in the zip (default 1,000) and on the total and per-entry
  *decompressed* size (default 200 MiB total), counted while inflating, not
  taken from the headers; Zip64 sizes and offsets not trusted;
- a fixed list of part names to open, resolved without `..` or absolute paths;
  anything else is ignored, never followed;
- `encoding/xml` token by token with a depth cap (default 64) and a token-size
  cap, no recursion on element nesting;
- row and column caps as in §5.1, and a shared-string index checked against the
  table length;
- encrypted (CFB/OLE) files, macros, external links and formulas refused or
  ignored, never evaluated.

The malformed-input tests for such a reader would be modelled on the advisories
in the Summary: a zip bomb, deep nesting, a huge declared dimension, an
out-of-range shared-string index, a style index past the table, a truncated
entry.

## 6. Tests

### 6.1 Unit tests in `utils/xlsx`

- Column names: 1 → `A`, 26 → `Z`, 27 → `AA`, 16,384 → `XFD`; 0 and 16,385
  refused.
- Text encoding, table-driven, one case per rule in §5.2: `<>&`, leading and
  trailing whitespace, each forbidden control character, a literal `_x0041_`,
  `_x0041_x0042_` (overlapping), invalid UTF-8, CR, a 32,768-unit string, a string whose 32,767th unit is the
  first half of an emoji.
- Sheet names: each forbidden character, 31 and 32 units, `'a`, `History` and
  `HISTORY`, empty.
- Limits: the 16,385th cell and the 1,048,577th row are refused (the row test
  writes empty rows into `io.Discard` so it stays fast).
- State: `WriteHeader` twice, `WriteHeader` after `WriteRow`, any call after
  `Close`, a failing `io.Writer` (the error comes back, no panic).
- Parts: unzip the output; every part parses with `encoding/xml`; every part
  named in `[Content_Types].xml` and every relationship target exists.
- Fuzz: `FuzzCellText` writes arbitrary strings and reads them back through
  the test parser, which decodes `_xHHHH_` (including `_x005F_`) the way Excel
  does. The file must parse, and each decoded value must equal the input after
  only the lossy steps: invalid UTF-8 to U+FFFD and truncation to 32,767 units.
  Comparing against the encoder's own output would prove nothing.

### 6.2 Round trip

A test-only parser (`archive/zip` + `encoding/xml`) in `internal/xlsxtest` at
the module root, so the tests of both `utils/xlsx` and `databases/export` can
import it and nothing outside dxlib can, reads `sheet1.xml` and `sharedStrings.xml` and returns
`[][]string`, plus the header style index and the column widths. It handles
only what the writer emits and errors on anything else.

Round-trip tests: headers, rows, unicode, empty cells, a duplicated string
(one table entry), the header style and widths.

While excelize is still in `go.mod` (step 1 in §7), one extra test opens the
writer's output with `excelize.OpenReader` and compares every cell: an
independent reader agreeing with ours. It goes away with the dependency.

### 6.3 A real spreadsheet application

Behind a build tag, `//go:build libreoffice`, so `go test ./...` stays fast and
needs nothing installed: write a workbook covering every case in §6.1, then

    soffice -env:UserInstallation=file://<tmp>/profile --headless \
        --convert-to csv --outdir <tmp> <file>.xlsx

and compare the CSV with the expected values. The test looks for `soffice` on
`PATH` and at `/Applications/LibreOffice.app/Contents/MacOS/soffice`, and skips
when neither exists. Its own profile directory keeps it from failing or hanging
when another LibreOffice instance is running. Run it as

    go test -tags libreoffice ./utils/xlsx/

LibreOffice passing does not mean Excel will open the file without a repair
prompt. One manual open in Excel is required in step 2 (§4).

### 6.4 `databases/export`

- `TestXLSXKeepsRawValue` moves from `excelize.OpenReader` to the test parser
  and also checks that the cell is a shared string, not a formula.
- New: `SheetName: "Data"` works; `ExportQueryResults` with `XLSX` writes a
  file the test parser reads.
- The CSV tests do not change.

## 7. Implementation steps

Each is one queue item for dxlib unless it says otherwise. All of 1 and 2 have
to land before 2026-10-23, when the advisory acceptance runs out.

1. **Write `utils/xlsx`** with the tests in §6.1–6.3, and `internal/xlsxtest`.
   excelize stays for now and is used only as the cross-check in §6.2. Run the
   LibreOffice test. No dependency change.
2. **Switch `databases/export` and drop excelize.** Rewrite `writeXLSContent`,
   `exportToXLS` and `exportToXLSStream`; accept `XLSX` in
   `ExportQueryResults`; honour `SheetName`; move the tests (§6.4); remove the
   excelize cross-check test; `go mod tidy`. That drops excelize,
   `richardlehane/mscfb`, `richardlehane/msoleps`, `xuri/efp`, `xuri/nfp` and
   `tiendc/go-deepcopy` (each needed only by excelize, per `go mod why`);
   `golang.org/x/image` stays for `captcha`. Remove the fifteen excelize entries
   from `.dependency-allowlist.json` and the excelize block from `.trivyignore`.
   SBOM scan and licence check as in `CLAUDE.md`. Update `LIBRARY.md`
   (`tables` and `databases` sections; add `utils/xlsx`). Open one file by hand
   in Excel and check there is no repair prompt.
3. **Tag a dxlib release.** The owner's call (version and when).
4. **Move the pins** in `dxlib_module` and `dx-service-push-notification` to
   that tag (an item for each of those repos), `go mod tidy`, SBOM scan; their
   indirect excelize goes away.
5. **dx-erp's nested copies** (`src/backends/libraries/dxlib` and
   `dxlib_module`): move them to the tag, in an item for dx-erp.

Steps 1 and 2 clear dxlib itself by 2026-10-23. The other repos carry excelize
as an indirect dependency until their pins move (steps 4 and 5), and none of
them records an acceptance of the advisories. So either steps 3 and 4 also land
before 2026-10-23, or each of those repos records a temporary acceptance until
its pin moves. dx-erp's step has no date yet and needs one of the two.

## 8. Decisions worth flagging

- **Writer only.** No reader, so the advisory class does not exist in our code.
  A reader later is a separate design starting from §5.3.
- **Strings only.** Same as today. Numbers stay text in the sheet, so a user who
  sums a column in Excel still has to convert it, as now. Typed numeric and
  date cells would be a later, separate change (a `Cell` type beside
  `[]string`), with its own tests.
- **Silent truncation at 32,767** units, matching excelize, so the switch does
  not turn a working export into an error.
- **Two encoding changes from today's files**: a literal `_xHHHH_` is escaped
  and shows as typed (today Excel decodes it), and forbidden control characters
  survive as `_xHHHH_` (today they become U+FFFD). Cells holding either will
  read differently after the switch, and more faithfully.
- **No `docProps` or theme**, pending the manual open in Excel.
- **Two defects fixed on the way** (§1.4): `SheetName` is honoured, and
  `ExportQueryResults` writes `xlsx`.

## 9. Out of scope

Multiple sheets, merged cells, formulas, number formats, images, encryption,
reading, and the legacy binary `.xls` format (the `xls` export format already
writes OOXML, as it does today).
