// Package xlsx writes a one-sheet .xlsx workbook of text cells, streaming the
// rows into an io.Writer. It is a writer only: dxlib never reads a workbook,
// so there is no parser here for a malformed file to reach. DESIGN.md has the
// reasons and the rules.
package xlsx

import (
	"archive/zip"
	"bufio"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/errors"
)

// Style is the only cell formatting the writer knows.
type Style struct {
	Bold    bool
	Center  bool   // horizontal alignment
	FillRGB string // "E0EBF5"; empty means no fill
}

type Options struct {
	SheetName    string    // required; Excel's rules for sheet names apply
	ColumnWidths []float64 // one per column, in character units; 0 leaves the default
	HeaderStyle  *Style    // applied to the row written with WriteHeader; nil for none
}

// Writer writes one workbook. It is not safe for concurrent use.
type Writer struct {
	zw         *zip.Writer
	sheet      *bufio.Writer
	styled     bool
	rows       int
	header     bool
	strings    map[string]int
	stringList []string
	refs       int
	err        error
}

var (
	errClosed       = errors.New("xlsx: writer is closed")
	errHeaderTwice  = errors.New("xlsx: WriteHeader called twice")
	errHeaderLate   = errors.New("xlsx: WriteHeader called after WriteRow")
	errTooManyRows  = errors.Errorf("xlsx: more than %d rows", MaxRows)
	errTooManyCells = errors.Errorf("xlsx: more than %d cells in a row", MaxColumns)
)

// Zip entries carry a fixed time, so the same input gives the same entries.
var entryTime = time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)

const (
	nsMain  = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
	nsRel   = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
	xmlDecl = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` + "\n"
)

// NewWriter starts a workbook with one sheet. It writes the fixed parts at once,
// so an error from w can come back here, and then the rows as they arrive.
func NewWriter(w io.Writer, opts Options) (*Writer, error) {
	if err := checkSheetName(opts.SheetName); err != nil {
		return nil, err
	}
	if len(opts.ColumnWidths) > MaxColumns {
		return nil, errors.Errorf("xlsx: %d column widths, at most %d", len(opts.ColumnWidths), MaxColumns)
	}
	for i, cw := range opts.ColumnWidths {
		if !(cw == 0 || cw > 0 && cw <= 255) {
			return nil, errors.Errorf("xlsx: width %v of column %d is outside (0, 255]", cw, i+1)
		}
	}
	if opts.HeaderStyle != nil && opts.HeaderStyle.FillRGB != "" {
		if err := checkRGB(opts.HeaderStyle.FillRGB); err != nil {
			return nil, err
		}
	}

	x := &Writer{zw: zip.NewWriter(w), styled: opts.HeaderStyle != nil, strings: map[string]int{}}
	parts := []struct{ name, body string }{
		{"[Content_Types].xml", contentTypes},
		{"_rels/.rels", rootRels},
		{"xl/workbook.xml", xmlDecl + `<workbook xmlns="` + nsMain + `" xmlns:r="` + nsRel + `"><sheets><sheet name="` +
			escapeAttr(opts.SheetName) + `" sheetId="1" r:id="rId1"/></sheets></workbook>`},
		{"xl/_rels/workbook.xml.rels", workbookRels},
		{"xl/styles.xml", stylesXML(opts.HeaderStyle)},
	}
	for _, p := range parts {
		e, err := x.create(p.name)
		if err != nil {
			return nil, err
		}
		if _, err := io.WriteString(e, p.body); err != nil {
			return nil, errors.Wrapf(err, "xlsx: writing %s", p.name)
		}
	}

	e, err := x.create("xl/worksheets/sheet1.xml")
	if err != nil {
		return nil, err
	}
	x.sheet = bufio.NewWriterSize(e, 64*1024)
	x.sheet.WriteString(xmlDecl + `<worksheet xmlns="` + nsMain + `" xmlns:r="` + nsRel + `">`)
	x.sheet.WriteString(colsXML(opts.ColumnWidths))
	x.sheet.WriteString(`<sheetData>`)
	if err := x.sheet.Flush(); err != nil {
		return nil, errors.Wrap(err, "xlsx: writing the sheet")
	}
	if err := x.zw.Flush(); err != nil {
		return nil, errors.Wrap(err, "xlsx: writing the workbook")
	}
	return x, nil
}

func (x *Writer) create(name string) (io.Writer, error) {
	e, err := x.zw.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Deflate, Modified: entryTime})
	if err != nil {
		return nil, errors.Wrapf(err, "xlsx: starting %s", name)
	}
	return e, nil
}

// WriteHeader writes the first row, in the header style. At most once, before any WriteRow.
func (x *Writer) WriteHeader(cells []string) error {
	if x.err != nil {
		return x.err
	}
	switch {
	case x.header:
		x.err = errHeaderTwice
	case x.rows > 0:
		x.err = errHeaderLate
	default:
		x.header = true
		x.writeRow(cells, x.styled)
	}
	return x.err
}

// WriteRow writes the next row. Every cell is text.
func (x *Writer) WriteRow(cells []string) error {
	if x.err != nil {
		return x.err
	}
	x.writeRow(cells, false)
	return x.err
}

func (x *Writer) writeRow(cells []string, styled bool) {
	if x.rows >= MaxRows {
		x.err = errTooManyRows
		return
	}
	if len(cells) > MaxColumns {
		x.err = errTooManyCells
		return
	}
	x.rows++
	r := strconv.Itoa(x.rows)
	b := x.sheet
	b.WriteString(`<row r="`)
	b.WriteString(r)
	if styled {
		b.WriteString(`" s="1" customFormat="1`)
	}
	b.WriteString(`">`)
	for i, c := range cells {
		col, _ := columnName(i + 1)
		b.WriteString(`<c r="`)
		b.WriteString(col)
		b.WriteString(r)
		if styled {
			b.WriteString(`" s="1`)
		}
		b.WriteString(`" t="s"><v>`)
		b.WriteString(strconv.Itoa(x.sharedIndex(c)))
		b.WriteString(`</v></c>`)
	}
	if _, err := b.WriteString(`</row>`); err != nil {
		x.err = errors.Wrap(err, "xlsx: writing the sheet")
	}
}

// sharedIndex returns the shared-string index of a cell value, adding it to the
// table the first time it is seen. The table holds the encoded <si>, so two
// values that encode the same share one entry.
func (x *Writer) sharedIndex(value string) int {
	text, preserve := encodeCellText(value)
	si := "<si><t>" + text + "</t></si>"
	if preserve {
		si = `<si><t xml:space="preserve">` + text + "</t></si>"
	}
	x.refs++
	if i, ok := x.strings[si]; ok {
		return i
	}
	i := len(x.stringList)
	x.strings[si] = i
	x.stringList = append(x.stringList, si)
	return i
}

// Close writes the remaining parts and the zip directory. It does not close the
// io.Writer given to NewWriter.
func (x *Writer) Close() error {
	if x.err != nil {
		return x.err
	}
	x.err = x.close()
	if x.err == nil {
		x.err = errClosed
		return nil
	}
	return x.err
}

func (x *Writer) close() error {
	x.sheet.WriteString(`</sheetData></worksheet>`)
	if err := x.sheet.Flush(); err != nil {
		return errors.Wrap(err, "xlsx: writing the sheet")
	}
	e, err := x.create("xl/sharedStrings.xml")
	if err != nil {
		return err
	}
	b := bufio.NewWriterSize(e, 64*1024)
	b.WriteString(xmlDecl + `<sst xmlns="` + nsMain + `" count="`)
	b.WriteString(strconv.Itoa(x.refs))
	b.WriteString(`" uniqueCount="`)
	b.WriteString(strconv.Itoa(len(x.stringList)))
	b.WriteString(`">`)
	for _, si := range x.stringList {
		b.WriteString(si)
	}
	b.WriteString(`</sst>`)
	if err := b.Flush(); err != nil {
		return errors.Wrap(err, "xlsx: writing the shared strings")
	}
	if err := x.zw.Close(); err != nil {
		return errors.Wrap(err, "xlsx: finishing the zip")
	}
	return nil
}

// colsXML gives the <cols> element for the widths, adjacent equal widths
// sharing one <col>, or nothing when no width is set: the schema wants at least
// one <col> inside <cols>.
func colsXML(widths []float64) string {
	var b strings.Builder
	for i := 0; i < len(widths); {
		j := i
		for j+1 < len(widths) && widths[j+1] == widths[i] {
			j++
		}
		if widths[i] != 0 {
			b.WriteString(`<col min="`)
			b.WriteString(strconv.Itoa(i + 1))
			b.WriteString(`" max="`)
			b.WriteString(strconv.Itoa(j + 1))
			b.WriteString(`" width="`)
			b.WriteString(strconv.FormatFloat(widths[i], 'f', -1, 64))
			b.WriteString(`" customWidth="1"/>`)
		}
		i = j + 1
	}
	if b.Len() == 0 {
		return ""
	}
	return "<cols>" + b.String() + "</cols>"
}

const contentTypes = xmlDecl +
	`<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">` +
	`<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>` +
	`<Default Extension="xml" ContentType="application/xml"/>` +
	`<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>` +
	`<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>` +
	`<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>` +
	`<Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>` +
	`</Types>`

const rootRels = xmlDecl +
	`<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">` +
	`<Relationship Id="rId1" Type="` + nsRel + `/officeDocument" Target="xl/workbook.xml"/>` +
	`</Relationships>`

const workbookRels = xmlDecl +
	`<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">` +
	`<Relationship Id="rId1" Type="` + nsRel + `/worksheet" Target="worksheets/sheet1.xml"/>` +
	`<Relationship Id="rId2" Type="` + nsRel + `/styles" Target="styles.xml"/>` +
	`<Relationship Id="rId3" Type="` + nsRel + `/sharedStrings" Target="sharedStrings.xml"/>` +
	`</Relationships>`

// stylesXML gives the minimum style sheet Excel needs: a default font, the two
// fills Excel expects at 0 and 1, one empty border, the Normal style, and cell
// format 1 for the header when there is a header style.
func stylesXML(h *Style) string {
	const font = `<sz val="11"/><color rgb="FF000000"/><name val="Calibri"/><family val="2"/></font>`
	fonts := []string{"<font>" + font}
	fills := []string{`<fill><patternFill patternType="none"/></fill>`, `<fill><patternFill patternType="gray125"/></fill>`}
	xfs := []string{`<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>`}
	if h != nil {
		fontID, fillID := 0, 0
		attrs := ""
		if h.Bold {
			fonts = append(fonts, "<font><b/>"+font)
			fontID = 1
			attrs += ` applyFont="1"`
		}
		if h.FillRGB != "" {
			fills = append(fills, `<fill><patternFill patternType="solid"><fgColor rgb="FF`+strings.ToUpper(h.FillRGB)+
				`"/><bgColor indexed="64"/></patternFill></fill>`)
			fillID = 2
			attrs += ` applyFill="1"`
		}
		xf := `<xf numFmtId="0" fontId="` + strconv.Itoa(fontID) + `" fillId="` + strconv.Itoa(fillID) + `" borderId="0" xfId="0"` + attrs
		if h.Center {
			xf += ` applyAlignment="1"><alignment horizontal="center"/></xf>`
		} else {
			xf += `/>`
		}
		xfs = append(xfs, xf)
	}
	var b strings.Builder
	b.WriteString(xmlDecl + `<styleSheet xmlns="` + nsMain + `">`)
	b.WriteString(`<fonts count="` + strconv.Itoa(len(fonts)) + `">` + strings.Join(fonts, "") + `</fonts>`)
	b.WriteString(`<fills count="` + strconv.Itoa(len(fills)) + `">` + strings.Join(fills, "") + `</fills>`)
	b.WriteString(`<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>`)
	b.WriteString(`<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>`)
	b.WriteString(`<cellXfs count="` + strconv.Itoa(len(xfs)) + `">` + strings.Join(xfs, "") + `</cellXfs>`)
	b.WriteString(`<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>`)
	b.WriteString(`</styleSheet>`)
	return b.String()
}
