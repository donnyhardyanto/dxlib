package xlsx

import (
	"archive/zip"
	"bufio"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/donnyhardyanto/dxlib/errors"
)

// SheetOptions describes one sheet of a Workbook.
type SheetOptions struct {
	Name         string    // Excel's rules for sheet names; unique in the workbook, ignoring letter case
	ColumnWidths []float64 // one per column, in character units; 0 leaves the default
	FreezeRows   int       // rows kept in view above a frozen pane; 0 for none
}

type WorkbookOptions struct {
	Sheets []SheetOptions // at least one; the first is the active sheet
}

type cellKind uint8

const (
	blankCell cellKind = iota
	textCell
	numberCell
)

// Cell is one cell of a row: Text, Number, or Blank. The zero Cell is blank
// with the default style, and is not written at all.
type Cell struct {
	kind   cellKind
	text   string
	number float64
	style  int
}

// Text is a text cell. It is never read as a number or a formula.
func Text(s string, style int) Cell { return Cell{kind: textCell, text: s, style: style} }

// Number is a numeric cell. NaN and ±Inf are refused when the row is written.
func Number(v float64, style int) Cell { return Cell{kind: numberCell, number: v, style: style} }

// Blank is a cell with a style and no value, such as the rest of a bordered
// merged range.
func Blank(style int) Cell { return Cell{style: style} }

type Row struct {
	Cells  []Cell  // from column A, one per column
	Height float64 // points; 0 for the default
	Style  int     // the row's own style; 0 for none
}

// Workbook writes a workbook of one or more sheets, streaming the rows into an
// io.Writer. It is not safe for concurrent use. DESIGN.md §10 has the rules.
type Workbook struct {
	w      io.Writer
	sheets []SheetOptions
	styles styleTable

	started    bool
	zw         *zip.Writer
	sheet      *bufio.Writer
	current    int // index of the current sheet
	rows       int // rows written to the current sheet
	merges     []mergeRange
	strings    map[string]int
	stringList []string
	refs       int
	err        error
}

type mergeRange struct{ r1, c1, r2, c2 int }

var (
	errStylesClosed = errors.New("xlsx: AddStyle called after the first row, merge, NextSheet or Close")
	errLastSheet    = errors.New("xlsx: NextSheet called on the last sheet")
)

// NewWorkbook checks the options. It writes nothing yet: an error from w comes
// back from the first call that writes.
func NewWorkbook(w io.Writer, opts WorkbookOptions) (*Workbook, error) {
	if len(opts.Sheets) == 0 {
		return nil, errors.New("xlsx: a workbook needs at least one sheet")
	}
	seen := map[string]string{}
	for _, s := range opts.Sheets {
		if err := checkSheetName(s.Name); err != nil {
			return nil, err
		}
		key := strings.ToLower(s.Name)
		if prev, ok := seen[key]; ok {
			return nil, errors.Errorf("xlsx: sheet names %q and %q are the same to Excel", prev, s.Name)
		}
		seen[key] = s.Name
		if err := checkWidths(s.ColumnWidths); err != nil {
			return nil, errors.Wrapf(err, "xlsx: sheet %q", s.Name)
		}
		if s.FreezeRows < 0 || s.FreezeRows >= MaxRows {
			return nil, errors.Errorf("xlsx: sheet %q freezes %d rows, outside 0..%d", s.Name, s.FreezeRows, MaxRows-1)
		}
	}
	return &Workbook{w: w, sheets: append([]SheetOptions(nil), opts.Sheets...), strings: map[string]int{}}, nil
}

func checkWidths(widths []float64) error {
	if len(widths) > MaxColumns {
		return errors.Errorf("xlsx: %d column widths, at most %d", len(widths), MaxColumns)
	}
	for i, cw := range widths {
		if !(cw == 0 || cw > 0 && cw <= 255) {
			return errors.Errorf("xlsx: width %v of column %d is outside (0, 255]", cw, i+1)
		}
	}
	return nil
}

// AddStyle adds a style to the table and returns its index for cells and rows.
// Style 0 is the default. Only before the first WriteRow, Merge, NextSheet or
// Close, since the table is written before the first sheet.
func (b *Workbook) AddStyle(s CellStyle) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if b.started {
		b.err = errStylesClosed
		return 0, b.err
	}
	if b.err = s.check(); b.err != nil {
		return 0, b.err
	}
	return b.styles.add(s), nil
}

// start writes the parts before the first sheet and starts it.
func (b *Workbook) start() error {
	if b.started {
		return b.err
	}
	b.started = true
	b.zw = zip.NewWriter(b.w)
	parts := []struct{ name, body string }{
		{"[Content_Types].xml", b.contentTypes()},
		{"_rels/.rels", rootRels},
		{"xl/workbook.xml", b.workbookXML()},
		{"xl/_rels/workbook.xml.rels", b.workbookRels()},
		{"xl/styles.xml", b.styles.xml()},
	}
	for _, p := range parts {
		e, err := b.create(p.name)
		if err != nil {
			b.err = err
			return err
		}
		if _, err := io.WriteString(e, p.body); err != nil {
			b.err = errors.Wrapf(err, "xlsx: writing %s", p.name)
			return b.err
		}
	}
	b.err = b.startSheet()
	return b.err
}

func (b *Workbook) create(name string) (io.Writer, error) {
	e, err := b.zw.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Deflate, Modified: entryTime})
	if err != nil {
		return nil, errors.Wrapf(err, "xlsx: starting %s", name)
	}
	return e, nil
}

func (b *Workbook) startSheet() error {
	s := b.sheets[b.current]
	e, err := b.create("xl/worksheets/sheet" + strconv.Itoa(b.current+1) + ".xml")
	if err != nil {
		return err
	}
	b.rows = 0
	b.merges = nil
	b.sheet = bufio.NewWriterSize(e, 64*1024)
	b.sheet.WriteString(xmlDecl + `<worksheet xmlns="` + nsMain + `" xmlns:r="` + nsRel + `">`)
	b.sheet.WriteString(sheetViewsXML(b.current == 0, s.FreezeRows))
	b.sheet.WriteString(colsXML(s.ColumnWidths))
	b.sheet.WriteString(`<sheetData>`)
	if err := b.sheet.Flush(); err != nil {
		return errors.Wrapf(err, "xlsx: writing sheet %q", s.Name)
	}
	if err := b.zw.Flush(); err != nil {
		return errors.Wrap(err, "xlsx: writing the workbook")
	}
	return nil
}

func (b *Workbook) endSheet() error {
	b.sheet.WriteString(`</sheetData>`)
	if len(b.merges) > 0 {
		b.sheet.WriteString(`<mergeCells count="` + strconv.Itoa(len(b.merges)) + `">`)
		for _, m := range b.merges {
			from, _ := columnName(m.c1)
			to, _ := columnName(m.c2)
			b.sheet.WriteString(`<mergeCell ref="` + from + strconv.Itoa(m.r1) + `:` + to + strconv.Itoa(m.r2) + `"/>`)
		}
		b.sheet.WriteString(`</mergeCells>`)
	}
	b.sheet.WriteString(`</worksheet>`)
	if err := b.sheet.Flush(); err != nil {
		return errors.Wrapf(err, "xlsx: writing sheet %q", b.sheets[b.current].Name)
	}
	return nil
}

// WriteRow writes the next row of the current sheet.
func (b *Workbook) WriteRow(r Row) error {
	if b.start() != nil {
		return b.err
	}
	b.err = b.writeRow(r)
	return b.err
}

func (b *Workbook) checkStyle(i int) error {
	if i < 0 || i > len(b.styles.styles) {
		return errors.Errorf("xlsx: style %d is not in the table (0..%d)", i, len(b.styles.styles))
	}
	return nil
}

func (b *Workbook) writeRow(r Row) error {
	if b.rows >= MaxRows {
		return errTooManyRows
	}
	if len(r.Cells) > MaxColumns {
		return errTooManyCells
	}
	if !(r.Height == 0 || r.Height > 0 && r.Height <= 409) {
		return errors.Errorf("xlsx: row height %v is outside (0, 409]", r.Height)
	}
	if err := b.checkStyle(r.Style); err != nil {
		return err
	}
	// Every cell is checked before anything is written, so a refused row
	// leaves no half row behind.
	for i, c := range r.Cells {
		if err := b.checkStyle(c.style); err != nil {
			return err
		}
		if c.kind == numberCell && (math.IsNaN(c.number) || math.IsInf(c.number, 0)) {
			return errors.Errorf("xlsx: cell %d of row %d is %v, which Excel cannot hold", i+1, b.rows+1, c.number)
		}
	}
	b.rows++
	row := strconv.Itoa(b.rows)
	w := b.sheet
	w.WriteString(`<row r="`)
	w.WriteString(row)
	if r.Style != 0 {
		w.WriteString(`" s="` + strconv.Itoa(b.styles.xf(r.Style)) + `" customFormat="1`)
	}
	if r.Height != 0 {
		w.WriteString(`" ht="` + strconv.FormatFloat(r.Height, 'f', -1, 64) + `" customHeight="1`)
	}
	w.WriteString(`">`)
	for i, c := range r.Cells {
		if c.kind == blankCell && c.style == 0 {
			continue
		}
		col, _ := columnName(i + 1)
		w.WriteString(`<c r="`)
		w.WriteString(col)
		w.WriteString(row)
		if c.style != 0 {
			w.WriteString(`" s="` + strconv.Itoa(b.styles.xf(c.style)))
		}
		switch c.kind {
		case blankCell:
			w.WriteString(`"/>`)
		case textCell:
			w.WriteString(`" t="s"><v>`)
			w.WriteString(strconv.Itoa(b.sharedIndex(c.text)))
			w.WriteString(`</v></c>`)
		case numberCell:
			w.WriteString(`"><v>`)
			w.WriteString(formatNumber(c.number))
			w.WriteString(`</v></c>`)
		}
	}
	if _, err := w.WriteString(`</row>`); err != nil {
		return errors.Wrapf(err, "xlsx: writing sheet %q", b.sheets[b.current].Name)
	}
	return nil
}

// formatNumber writes v in the shortest form that reads back as the same
// float64, with an exponent only for very large or very small values.
func formatNumber(v float64) string {
	if v == 0 {
		return "0"
	}
	if a := math.Abs(v); a >= 1e-5 && a < 1e15 {
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
	return strconv.FormatFloat(v, 'E', -1, 64)
}

// sharedIndex returns the shared-string index of a cell value, adding it to the
// table the first time it is seen. The table holds the encoded <si>, so two
// values that encode the same share one entry.
func (b *Workbook) sharedIndex(value string) int {
	text, preserve := encodeCellText(value)
	si := "<si><t>" + text + "</t></si>"
	if preserve {
		si = `<si><t xml:space="preserve">` + text + "</t></si>"
	}
	b.refs++
	if i, ok := b.strings[si]; ok {
		return i
	}
	i := len(b.stringList)
	b.strings[si] = i
	b.stringList = append(b.stringList, si)
	return i
}

// Merge merges a range of the current sheet. Rows and columns are 1-based and
// inclusive. It may be called any time while that sheet is current.
func (b *Workbook) Merge(firstRow, firstCol, lastRow, lastCol int) error {
	if b.start() != nil {
		return b.err
	}
	b.err = b.merge(firstRow, firstCol, lastRow, lastCol)
	return b.err
}

func (b *Workbook) merge(firstRow, firstCol, lastRow, lastCol int) error {
	m := mergeRange{firstRow, firstCol, lastRow, lastCol}
	switch {
	case firstRow < 1 || lastRow > MaxRows || firstCol < 1 || lastCol > MaxColumns || firstRow > lastRow || firstCol > lastCol:
		return errors.Errorf("xlsx: merge rows %d..%d, columns %d..%d is not a range inside the sheet", firstRow, lastRow, firstCol, lastCol)
	case firstRow == lastRow && firstCol == lastCol:
		return errors.Errorf("xlsx: merge of the single cell at row %d, column %d", firstRow, firstCol)
	}
	for _, o := range b.merges {
		if m.r1 <= o.r2 && o.r1 <= m.r2 && m.c1 <= o.c2 && o.c1 <= m.c2 {
			return errors.Errorf("xlsx: merge rows %d..%d, columns %d..%d overlaps rows %d..%d, columns %d..%d",
				m.r1, m.r2, m.c1, m.c2, o.r1, o.r2, o.c1, o.c2)
		}
	}
	b.merges = append(b.merges, m)
	return nil
}

// NextSheet ends the current sheet and makes the next one in Sheets current.
func (b *Workbook) NextSheet() error {
	if b.start() != nil {
		return b.err
	}
	if b.current+1 >= len(b.sheets) {
		b.err = errLastSheet
		return b.err
	}
	if b.err = b.endSheet(); b.err != nil {
		return b.err
	}
	b.current++
	b.err = b.startSheet()
	return b.err
}

// Close ends the current sheet, writes the sheets never reached as empty
// sheets, then the shared strings and the zip directory. It does not close the
// io.Writer given to NewWorkbook.
func (b *Workbook) Close() error {
	if b.start() != nil {
		return b.err
	}
	b.err = b.close()
	if b.err == nil {
		b.err = errClosed
		return nil
	}
	return b.err
}

func (b *Workbook) close() error {
	for {
		if err := b.endSheet(); err != nil {
			return err
		}
		if b.current+1 >= len(b.sheets) {
			break
		}
		b.current++
		if err := b.startSheet(); err != nil {
			return err
		}
	}
	e, err := b.create("xl/sharedStrings.xml")
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(e, 64*1024)
	w.WriteString(xmlDecl + `<sst xmlns="` + nsMain + `" count="`)
	w.WriteString(strconv.Itoa(b.refs))
	w.WriteString(`" uniqueCount="`)
	w.WriteString(strconv.Itoa(len(b.stringList)))
	w.WriteString(`">`)
	for _, si := range b.stringList {
		w.WriteString(si)
	}
	w.WriteString(`</sst>`)
	if err := w.Flush(); err != nil {
		return errors.Wrap(err, "xlsx: writing the shared strings")
	}
	if err := b.zw.Close(); err != nil {
		return errors.Wrap(err, "xlsx: finishing the zip")
	}
	return nil
}

// sheetViewsXML gives the first sheet its selected tab, and a sheet with frozen
// rows its pane; other sheets get no <sheetViews>.
func sheetViewsXML(first bool, freeze int) string {
	if !first && freeze == 0 {
		return ""
	}
	v := `<sheetViews><sheetView`
	if first {
		v += ` tabSelected="1"`
	}
	v += ` workbookViewId="0"`
	if freeze == 0 {
		return v + `/></sheetViews>`
	}
	cell := "A" + strconv.Itoa(freeze+1)
	return v + `><pane ySplit="` + strconv.Itoa(freeze) + `" topLeftCell="` + cell + `" activePane="bottomLeft" state="frozen"/>` +
		`<selection pane="bottomLeft" activeCell="` + cell + `" sqref="` + cell + `"/></sheetView></sheetViews>`
}

func (b *Workbook) contentTypes() string {
	var s strings.Builder
	s.WriteString(xmlDecl +
		`<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">` +
		`<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>` +
		`<Default Extension="xml" ContentType="application/xml"/>` +
		`<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>`)
	for i := range b.sheets {
		s.WriteString(`<Override PartName="/xl/worksheets/sheet` + strconv.Itoa(i+1) +
			`.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>`)
	}
	s.WriteString(`<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>` +
		`<Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>` +
		`</Types>`)
	return s.String()
}

func (b *Workbook) workbookXML() string {
	var s strings.Builder
	s.WriteString(xmlDecl + `<workbook xmlns="` + nsMain + `" xmlns:r="` + nsRel + `">` +
		`<bookViews><workbookView activeTab="0"/></bookViews><sheets>`)
	for i, sh := range b.sheets {
		n := strconv.Itoa(i + 1)
		s.WriteString(`<sheet name="` + escapeAttr(sh.Name) + `" sheetId="` + n + `" r:id="rId` + n + `"/>`)
	}
	s.WriteString(`</sheets></workbook>`)
	return s.String()
}

// workbookRels gives rId1..rIdN to the sheets, then styles and shared strings.
func (b *Workbook) workbookRels() string {
	var s strings.Builder
	s.WriteString(xmlDecl + `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">`)
	for i := range b.sheets {
		n := strconv.Itoa(i + 1)
		s.WriteString(`<Relationship Id="rId` + n + `" Type="` + nsRel + `/worksheet" Target="worksheets/sheet` + n + `.xml"/>`)
	}
	n := len(b.sheets)
	s.WriteString(`<Relationship Id="rId` + strconv.Itoa(n+1) + `" Type="` + nsRel + `/styles" Target="styles.xml"/>`)
	s.WriteString(`<Relationship Id="rId` + strconv.Itoa(n+2) + `" Type="` + nsRel + `/sharedStrings" Target="sharedStrings.xml"/>`)
	s.WriteString(`</Relationships>`)
	return s.String()
}
