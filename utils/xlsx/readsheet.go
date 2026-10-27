package xlsx

import (
	"encoding/xml"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/donnyhardyanto/dxlib/errors"
)

// readStyles is what rendering a value needs from styles.xml: the number
// format of each cell format.
type readStyles struct {
	xfNumFmt []int           // numFmtId of each <xf> in <cellXfs>
	custom   map[int]*numFmt // <numFmts>, by id
}

func (x *Reader) readStyles() (*readStyles, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.styleDone {
		return x.styles, x.styleErr
	}
	x.styleDone = true
	part := x.stylePart
	if part == "" {
		return nil, nil
	}
	st := &readStyles{custom: map[int]*numFmt{}}
	// Only <numFmts> and <cellXfs> count: a <numFmt> inside a <dxf> (a
	// conditional format) or an <xf> inside <cellStyleXfs> is not a cell's.
	err := x.scanPart(part, func(s *xmlScanner, e xml.StartElement) error {
		switch e.Name.Local {
		case "numFmts":
			return eachChild(s, "numFmt", func(e xml.StartElement) error {
				if len(st.custom) >= x.lim.MaxStyles {
					return limitError("number formats", int64(x.lim.MaxStyles), int64(len(st.custom)+1), part)
				}
				id, ok := attrInt(e, "numFmtId")
				if !ok {
					return nil
				}
				code, _ := attr(e, "formatCode")
				if c16, ok := attr(e, "formatCode16"); ok && c16 != "" {
					code = c16
				}
				if _, seen := st.custom[id]; !seen {
					st.custom[id] = parseNumFmt(code)
				}
				return nil
			})
		case "cellXfs":
			return eachChild(s, "xf", func(e xml.StartElement) error {
				if len(st.xfNumFmt) >= x.lim.MaxStyles {
					return limitError("cell formats", int64(x.lim.MaxStyles), int64(len(st.xfNumFmt)+1), part)
				}
				id, _ := attrInt(e, "numFmtId")
				st.xfNumFmt = append(st.xfNumFmt, id)
				return nil
			})
		case "dxfs", "cellStyleXfs":
			return s.skip()
		}
		return nil
	})
	if err != nil {
		x.styleErr = err
		return nil, err
	}
	x.styles = st
	return st, nil
}

// eachChild calls fn for each child element named local of the element whose
// start was just returned, and reads to its end.
func eachChild(s *xmlScanner, local string, fn func(e xml.StartElement) error) error {
	for {
		t, err := s.next()
		if err != nil {
			return s.eof(err)
		}
		switch t := t.(type) {
		case xml.EndElement:
			return nil
		case xml.StartElement:
			if t.Name.Local == local {
				if err := fn(t); err != nil {
					return err
				}
			}
			if err := s.skip(); err != nil {
				return err
			}
		}
	}
}

// format of cell format s, or nil for the value as stored.
func (st *readStyles) format(s int) *numFmt {
	if st == nil || s <= 0 || s >= len(st.xfNumFmt) {
		return nil
	}
	id := st.xfNumFmt[s]
	if f, ok := st.custom[id]; ok {
		return f
	}
	return builtinNumFmt(id)
}

// GetRows gives the rows of a sheet as text, as excelize's GetRows does: a
// row ends at its last cell with a value, a missing or empty cell before it
// is "", an empty row before the last row with a value is an empty slice,
// and nothing follows that last row. Values are rendered with their number
// format (DESIGN.md §11 has which formats, and what happens outside them);
// a formula gives its cached value, and is never evaluated. sheet is matched
// without regard to case, as excelize does.
func (x *Reader) GetRows(sheet string) ([][]string, error) {
	if x.closed.Load() {
		return nil, errReaderClosed
	}
	var ref *sheetRef
	for i := range x.sheets {
		if strings.EqualFold(x.sheets[i].name, sheet) {
			ref = &x.sheets[i]
			break
		}
	}
	if ref == nil {
		return nil, errors.Errorf("xlsx: sheet %q does not exist; the workbook has %d sheets", sheet, len(x.sheets))
	}
	if ref.part == "" {
		return nil, errors.Errorf("xlsx: sheet %q has no part: the workbook's relationships give no target for it", ref.name)
	}
	sst, err := x.sharedStrings()
	if err != nil {
		return nil, err
	}
	st, err := x.readStyles()
	if err != nil {
		return nil, err
	}
	rc, err := x.openPart(ref.part)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	sr := sheetReader{x: x, s: newXMLScanner(rc, ref.part, &x.lim), sst: sst, st: st, rows: [][]string{}}
	if err := sr.read(); err != nil {
		return nil, errors.Wrapf(err, "xlsx: sheet %q", ref.name)
	}
	return sr.rows, nil
}

type sheetReader struct {
	x     *Reader
	s     *xmlScanner
	sst   []string
	st    *readStyles
	rows  [][]string
	cells int64 // counted against MaxCells
}

func (r *sheetReader) read() error {
	for {
		t, err := r.s.next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if e, ok := t.(xml.StartElement); ok && e.Name.Local == "sheetData" {
			return r.sheetData()
		}
	}
}

func (r *sheetReader) sheetData() error {
	prev := 0
	for {
		t, err := r.s.next()
		if err != nil {
			return r.s.eof(err)
		}
		switch t := t.(type) {
		case xml.EndElement:
			return nil
		case xml.StartElement:
			if t.Name.Local != "row" {
				if err := r.s.skip(); err != nil {
					return err
				}
				continue
			}
			n := prev + 1
			if v, ok := attr(t, "r"); ok {
				if n, err = strconv.Atoi(strings.TrimSpace(v)); err != nil || n < 1 {
					return errors.Errorf("xlsx: %s: row number %q is not a positive integer", r.s.part, clip(v, 20))
				}
			}
			if n <= prev {
				return errors.Errorf("xlsx: %s: row %d comes after row %d; rows must be in increasing order", r.s.part, n, prev)
			}
			if n > r.x.lim.MaxRows {
				return limitError("row number", int64(r.x.lim.MaxRows), int64(n), r.s.part)
			}
			prev = n
			cells, err := r.row(n)
			if err != nil {
				return err
			}
			if len(cells) > 0 {
				if gap := n - 1 - len(r.rows); gap > 0 {
					if err := r.count(int64(gap)); err != nil {
						return err
					}
					r.rows = append(r.rows, make([][]string, gap)...)
				}
				r.rows = append(r.rows, cells)
			}
		}
	}
}

func (r *sheetReader) count(n int64) error {
	r.cells += n
	if r.cells > r.x.lim.MaxCells {
		return limitError("cells returned for a sheet", r.x.lim.MaxCells, r.cells, r.s.part)
	}
	return nil
}

// row reads the cells of row n, whose start was just returned.
func (r *sheetReader) row(n int) ([]string, error) {
	var cells []string
	col := 0
	for {
		t, err := r.s.next()
		if err != nil {
			return nil, r.s.eof(err)
		}
		switch t := t.(type) {
		case xml.EndElement:
			return cells, nil
		case xml.StartElement:
			if t.Name.Local != "c" {
				if err := r.s.skip(); err != nil {
					return nil, err
				}
				continue
			}
			c := col + 1
			if ref, ok := attr(t, "r"); ok {
				if c, err = cellColumn(ref); err != nil {
					return nil, errors.Errorf("xlsx: %s: row %d: %v", r.s.part, n, err)
				}
			}
			if c <= col {
				return nil, errors.Errorf("xlsx: %s: row %d: a cell in column %d comes after column %d; cells must be in increasing order", r.s.part, n, c, col)
			}
			if c > r.x.lim.MaxColumns {
				return nil, limitError("column number", int64(r.x.lim.MaxColumns), int64(c), r.s.part)
			}
			col = c
			v, keep, err := r.cell(t)
			if err != nil {
				return nil, errors.Wrapf(err, "row %d, column %d", n, c)
			}
			if !keep {
				continue
			}
			if err := r.count(int64(c - len(cells))); err != nil {
				return nil, err
			}
			for len(cells) < c-1 {
				cells = append(cells, "")
			}
			cells = append(cells, v)
		}
	}
}

// cellColumn gives the column of a reference such as "B7" or "b7".
func cellColumn(ref string) (int, error) {
	col, i := 0, 0
	for ; i < len(ref); i++ {
		ch := ref[i]
		if 'a' <= ch && ch <= 'z' {
			ch -= 'a' - 'A'
		}
		if ch < 'A' || ch > 'Z' {
			break
		}
		if col <= MaxColumns { // past it already: stop counting before it overflows
			col = col*26 + int(ch-'A'+1)
		}
	}
	if i == 0 || i == len(ref) {
		return 0, errors.Errorf("cell reference %q is not a column and a row", clip(ref, 20))
	}
	for _, ch := range ref[i:] {
		if ch < '0' || ch > '9' {
			return 0, errors.Errorf("cell reference %q is not a column and a row", clip(ref, 20))
		}
	}
	return col, nil
}

// cell reads a <c> whose start was just returned and renders its value. keep
// is false for a cell excelize's GetRows leaves out: one whose value renders
// as "" and that has no formula.
func (r *sheetReader) cell(e xml.StartElement) (string, bool, error) {
	typ, _ := attr(e, "t")
	style := 0
	if v, ok := attr(e, "s"); ok {
		var err error
		if style, err = strconv.Atoi(strings.TrimSpace(v)); err != nil {
			return "", false, errors.Errorf("style index %q is not an integer", clip(v, 20))
		}
	}
	var v, inline string
	hasInline, hasFormula := false, false
	maxBytes := int64(r.x.lim.MaxStringUnits) * 4
	for {
		t, err := r.s.next()
		if err != nil {
			return "", false, r.s.eof(err)
		}
		if _, ok := t.(xml.EndElement); ok {
			break
		}
		se, ok := t.(xml.StartElement)
		if !ok {
			continue
		}
		switch se.Name.Local {
		case "v":
			if v, err = r.s.text(maxBytes, "cell value bytes"); err != nil {
				return "", false, err
			}
		case "f":
			hasFormula = true
			err = r.s.skip()
		case "is":
			hasInline = true
			inline, err = r.x.readStringItem(r.s, "inline string")
		default:
			err = r.s.skip()
		}
		if err != nil {
			return "", false, err
		}
	}
	out, err := r.value(typ, style, v, inline, hasInline)
	if err != nil {
		return "", false, err
	}
	return out, out != "" || hasFormula, nil
}

// value renders a cell as excelize's getValueFrom does with raw off.
func (r *sheetReader) value(typ string, style int, v, inline string, hasInline bool) (string, error) {
	f := r.st.format(style)
	switch typ {
	case "b":
		switch v {
		case "1":
			return "TRUE", nil
		case "0":
			return "FALSE", nil
		}
		return f.render(v, kindText, r.x.date1904), nil
	case "d":
		return f.render(isoDateToSerial(v), kindDate, r.x.date1904), nil
	case "s":
		if v == "" {
			return f.render(v, kindText, r.x.date1904), nil
		}
		i, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil || i < 0 || i >= len(r.sst) {
			return "", errors.Errorf("shared string index %q is outside the table of %d strings", clip(v, 20), len(r.sst))
		}
		return f.render(r.sst[i], kindText, r.x.date1904), nil
	case "str":
		return v, nil
	case "inlineStr":
		if hasInline {
			return f.render(inline, kindText, r.x.date1904), nil
		}
		return f.render(v, kindText, r.x.date1904), nil
	}
	if isNum, precision, d := isNumeric(v); isNum {
		if precision > 15 {
			v = strconv.FormatFloat(d, 'G', 15, 64)
		} else {
			v = strconv.FormatFloat(d, 'f', -1, 64)
		}
	}
	return f.render(v, kindNumber, r.x.date1904), nil
}

// isoDateToSerial turns the ISO 8601 value of a t="d" cell into a date serial
// number, as excelize does before formatting it; a value it cannot read stays.
func isoDateToSerial(v string) string {
	layout := "20060102T150405.999"
	if strings.HasSuffix(v, "Z") {
		layout = "20060102T150405Z"
		if strings.Contains(v, "-") {
			layout = "2006-01-02T15:04:05Z"
		}
	} else if strings.Contains(v, "-") {
		layout = "2006-01-02 15:04:05Z"
	}
	t, err := time.Parse(layout, strings.ReplaceAll(v, ",", "."))
	if err != nil {
		return v
	}
	serial := excelSerial(t)
	return strconv.FormatFloat(serial, 'G', 15, 64)
}

// isNumeric is excelize's: a float without "_", and its count of significant
// characters written out without an exponent.
func isNumeric(s string) (bool, int, float64) {
	if strings.Contains(s, "_") {
		return false, 0, 0
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return false, 0, 0
	}
	plain := strconv.FormatFloat(f, 'f', -1, 64)
	return true, len(plain) - strings.Count(plain, "."), f
}
