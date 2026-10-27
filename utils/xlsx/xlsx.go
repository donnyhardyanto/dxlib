// Package xlsx writes and reads .xlsx workbooks. Writer writes one sheet of
// text cells, streaming the rows into an io.Writer; Workbook writes several
// sheets with numbers, a style table, merged ranges, row heights and frozen
// panes. Reader reads the values of a workbook it cannot trust, the sheet list
// and each sheet's rows as text the way excelize's GetRows gives them, number
// formats applied (GetRows) or values as stored (GetRowsRaw), within limits on
// the zip, the XML and the sheet (ReaderLimits). DESIGN.md has the
// reasons and the rules; §11 is the reader.
package xlsx

import (
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/errors"
)

// Style is the header formatting Writer knows. Workbook takes a CellStyle.
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

// Writer writes a workbook of one sheet of text cells: a Workbook with the
// header style as its only style. It is not safe for concurrent use.
type Writer struct {
	wb     *Workbook
	header bool
	rows   int
	style  int
	err    error
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
	wb, err := NewWorkbook(w, WorkbookOptions{Sheets: []SheetOptions{{Name: opts.SheetName, ColumnWidths: opts.ColumnWidths}}})
	if err != nil {
		return nil, err
	}
	x := &Writer{wb: wb}
	if h := opts.HeaderStyle; h != nil {
		s := CellStyle{Font: Font{Bold: h.Bold}, FillRGB: h.FillRGB}
		if h.Center {
			s.Horizontal = "center"
		}
		if x.style, err = wb.AddStyle(s); err != nil {
			return nil, err
		}
	}
	if err := wb.start(); err != nil {
		return nil, err
	}
	return x, nil
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
		x.err = x.wb.WriteRow(textRow(cells, x.style))
	}
	return x.err
}

// WriteRow writes the next row. Every cell is text.
func (x *Writer) WriteRow(cells []string) error {
	if x.err != nil {
		return x.err
	}
	x.rows++
	x.err = x.wb.WriteRow(textRow(cells, 0))
	return x.err
}

func textRow(cells []string, style int) Row {
	r := Row{Cells: make([]Cell, len(cells)), Style: style}
	for i, c := range cells {
		r.Cells[i] = Text(c, style)
	}
	return r
}

// Close writes the remaining parts and the zip directory. It does not close the
// io.Writer given to NewWriter.
func (x *Writer) Close() error {
	if x.err != nil {
		return x.err
	}
	x.err = x.wb.Close()
	if x.err == nil {
		x.err = errClosed
		return nil
	}
	return x.err
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

const rootRels = xmlDecl +
	`<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">` +
	`<Relationship Id="rId1" Type="` + nsRel + `/officeDocument" Target="xl/workbook.xml"/>` +
	`</Relationships>`
