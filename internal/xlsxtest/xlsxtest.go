// Package xlsxtest reads back the workbooks utils/xlsx writes, for tests only.
// It understands exactly what that writer emits and errors on anything else.
// It is not a reader for workbooks from elsewhere: it has none of the limits a
// reader of untrusted files needs (utils/xlsx/DESIGN.md §5.3).
package xlsxtest

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Workbook is what a test needs to know about a written file. The fields
// before Sheets describe the first sheet.
type Workbook struct {
	SheetName    string
	Rows         [][]string // cell values as Excel shows them, _xHHHH_ decoded; a number as written in <v>
	RowStyles    []int      // the s= of each row, 0 when absent
	CellStyles   [][]int    // the s= of each cell, 0 when absent
	ColumnWidths []float64  // one per column up to the last <col>, 0 where none is set
	Parts        []string   // zip entries, in the order their local headers appear
	Raw          map[string][]byte

	Sheets    []Sheet
	ActiveTab int
	Styles    []Style // cellXfs, resolved
}

// Sheet is one worksheet.
type Sheet struct {
	Name         string
	Rows         [][]string // as Workbook.Rows; "" for a blank or missing cell
	Cells        [][]Cell
	RowStyles    []int
	RowHeights   []float64 // 0 when absent
	CellStyles   [][]int
	ColumnWidths []float64
	Merges       []string // the ref of each <mergeCell>, in order
	FreezeRows   int
	Selected     bool
}

// Kind of a cell as written.
const (
	Missing = "" // no <c> at that column
	Blank   = "blank"
	Text    = "text"
	Number  = "number"
)

type Cell struct {
	Kind   string
	Text   string
	Number float64
	Style  int
}

// Style is one cellXfs entry with its parts looked up.
type Style struct {
	NumberFormat string // the format code; "" for id 0
	FontName     string
	FontSize     float64
	Bold, Italic bool
	FontColor    string // ARGB as written
	Fill         string // ARGB of a solid fill; "" for none
	Border       [4]string
	Horizontal   string
	Vertical     string
	Wrap         bool
}

// Border sides in Style.Border: the colour of a thin line, "" for none.
const (
	Left = iota
	Right
	Top
	Bottom
)

// Read parses a workbook written by utils/xlsx.
func Read(data []byte) (*Workbook, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	wb := &Workbook{Raw: map[string][]byte{}}
	type part struct {
		name   string
		offset int64
	}
	var order []part
	for _, f := range zr.File {
		off, err := f.DataOffset()
		if err != nil {
			return nil, err
		}
		order = append(order, part{f.Name, off})
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		b, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f.Name, err)
		}
		wb.Raw[f.Name] = b
	}
	for i := 1; i < len(order); i++ {
		for j := i; j > 0 && order[j].offset < order[j-1].offset; j-- {
			order[j], order[j-1] = order[j-1], order[j]
		}
	}
	for _, p := range order {
		wb.Parts = append(wb.Parts, p.name)
	}

	var workbook struct {
		Views []struct {
			ActiveTab int `xml:"activeTab,attr"`
		} `xml:"bookViews>workbookView"`
		Sheets []struct {
			Name    string `xml:"name,attr"`
			SheetID int    `xml:"sheetId,attr"`
			RID     string `xml:"http://schemas.openxmlformats.org/officeDocument/2006/relationships id,attr"`
		} `xml:"sheets>sheet"`
	}
	if err := xml.Unmarshal(wb.Raw["xl/workbook.xml"], &workbook); err != nil {
		return nil, fmt.Errorf("workbook.xml: %w", err)
	}
	if len(workbook.Sheets) == 0 {
		return nil, fmt.Errorf("workbook.xml: no sheets")
	}
	if len(workbook.Views) == 1 {
		wb.ActiveTab = workbook.Views[0].ActiveTab
	}
	var rels struct {
		Rels []struct {
			ID     string `xml:"Id,attr"`
			Target string `xml:"Target,attr"`
		} `xml:"Relationship"`
	}
	if err := xml.Unmarshal(wb.Raw["xl/_rels/workbook.xml.rels"], &rels); err != nil {
		return nil, fmt.Errorf("workbook.xml.rels: %w", err)
	}
	target := map[string]string{}
	for _, r := range rels.Rels {
		target[r.ID] = r.Target
	}

	if err := wb.readStyles(); err != nil {
		return nil, err
	}

	var sst struct {
		Count       int `xml:"count,attr"`
		UniqueCount int `xml:"uniqueCount,attr"`
		SI          []struct {
			T struct {
				Space string `xml:"space,attr"`
				Text  string `xml:",chardata"`
			} `xml:"t"`
		} `xml:"si"`
	}
	if err := xml.Unmarshal(wb.Raw["xl/sharedStrings.xml"], &sst); err != nil {
		return nil, fmt.Errorf("sharedStrings.xml: %w", err)
	}
	if sst.UniqueCount != len(sst.SI) {
		return nil, fmt.Errorf("sharedStrings.xml: uniqueCount %d, %d entries", sst.UniqueCount, len(sst.SI))
	}
	strs := make([]string, len(sst.SI))
	for i, si := range sst.SI {
		strs[i] = DecodeEscapes(si.T.Text)
	}

	refs := 0
	for i, s := range workbook.Sheets {
		if s.SheetID != i+1 {
			return nil, fmt.Errorf("workbook.xml: sheet %d has sheetId %d", i+1, s.SheetID)
		}
		name := "xl/" + target[s.RID]
		if want := fmt.Sprintf("xl/worksheets/sheet%d.xml", i+1); name != want {
			return nil, fmt.Errorf("workbook.xml: sheet %q is %s, want %s", s.Name, name, want)
		}
		sh, n, err := wb.readSheet(name, strs)
		if err != nil {
			return nil, err
		}
		sh.Name = s.Name
		refs += n
		wb.Sheets = append(wb.Sheets, sh)
	}
	if refs != sst.Count {
		return nil, fmt.Errorf("sharedStrings.xml: count %d, %d cells", sst.Count, refs)
	}
	first := wb.Sheets[0]
	wb.SheetName, wb.Rows, wb.RowStyles, wb.CellStyles, wb.ColumnWidths =
		first.Name, first.Rows, first.RowStyles, first.CellStyles, first.ColumnWidths
	return wb, nil
}

func (wb *Workbook) readSheet(name string, strs []string) (Sheet, int, error) {
	var sh Sheet
	var sheet struct {
		Views []struct {
			TabSelected bool `xml:"tabSelected,attr"`
			Pane        *struct {
				YSplit      int    `xml:"ySplit,attr"`
				TopLeftCell string `xml:"topLeftCell,attr"`
				State       string `xml:"state,attr"`
			} `xml:"pane"`
		} `xml:"sheetViews>sheetView"`
		Cols []struct {
			Min   int     `xml:"min,attr"`
			Max   int     `xml:"max,attr"`
			Width float64 `xml:"width,attr"`
		} `xml:"cols>col"`
		Rows []struct {
			R     int     `xml:"r,attr"`
			S     int     `xml:"s,attr"`
			Ht    float64 `xml:"ht,attr"`
			Cells []struct {
				R string    `xml:"r,attr"`
				S int       `xml:"s,attr"`
				T string    `xml:"t,attr"`
				F *struct{} `xml:"f"`
				V *string   `xml:"v"`
			} `xml:"c"`
		} `xml:"sheetData>row"`
		Merges struct {
			Count int `xml:"count,attr"`
			Cells []struct {
				Ref string `xml:"ref,attr"`
			} `xml:"mergeCell"`
		} `xml:"mergeCells"`
	}
	raw, ok := wb.Raw[name]
	if !ok {
		return sh, 0, fmt.Errorf("%s is missing", name)
	}
	if err := xml.Unmarshal(raw, &sheet); err != nil {
		return sh, 0, fmt.Errorf("%s: %w", name, err)
	}
	if len(sheet.Views) > 1 {
		return sh, 0, fmt.Errorf("%s: %d sheet views", name, len(sheet.Views))
	}
	if len(sheet.Views) == 1 {
		v := sheet.Views[0]
		sh.Selected = v.TabSelected
		if p := v.Pane; p != nil {
			if p.State != "frozen" || p.TopLeftCell != "A"+strconv.Itoa(p.YSplit+1) {
				return sh, 0, fmt.Errorf("%s: pane %+v", name, *p)
			}
			sh.FreezeRows = p.YSplit
		}
	}
	for _, c := range sheet.Cols {
		for len(sh.ColumnWidths) < c.Max {
			sh.ColumnWidths = append(sh.ColumnWidths, 0)
		}
		for i := c.Min; i <= c.Max; i++ {
			sh.ColumnWidths[i-1] = c.Width
		}
	}
	refs := 0
	for i, row := range sheet.Rows {
		if row.R != i+1 {
			return sh, 0, fmt.Errorf("%s: row %d has r=%d", name, i+1, row.R)
		}
		if err := wb.checkStyle(row.S); err != nil {
			return sh, 0, fmt.Errorf("%s: row %d: %w", name, row.R, err)
		}
		var cells []Cell
		last := 0
		for _, c := range row.Cells {
			col, r, err := splitRef(c.R)
			if err != nil || r != row.R || col <= last {
				return sh, 0, fmt.Errorf("%s: cell %q in row %d after column %d", name, c.R, row.R, last)
			}
			if err := wb.checkStyle(c.S); err != nil {
				return sh, 0, fmt.Errorf("%s: cell %s: %w", name, c.R, err)
			}
			if c.F != nil {
				return sh, 0, fmt.Errorf("%s: cell %s holds a formula", name, c.R)
			}
			for len(cells) < col-1 {
				cells = append(cells, Cell{})
			}
			cell := Cell{Style: c.S}
			switch {
			case c.T == "s" && c.V != nil:
				k, err := strconv.Atoi(*c.V)
				if err != nil || k < 0 || k >= len(strs) {
					return sh, 0, fmt.Errorf("%s: cell %s points at shared string %q", name, c.R, *c.V)
				}
				cell.Kind, cell.Text = Text, strs[k]
				refs++
			case c.T == "" && c.V != nil:
				v, err := strconv.ParseFloat(*c.V, 64)
				if err != nil {
					return sh, 0, fmt.Errorf("%s: cell %s holds number %q", name, c.R, *c.V)
				}
				cell.Kind, cell.Text, cell.Number = Number, *c.V, v
			case c.T == "" && c.V == nil:
				if c.S == 0 {
					return sh, 0, fmt.Errorf("%s: cell %s is blank with no style", name, c.R)
				}
				cell.Kind = Blank
			default:
				return sh, 0, fmt.Errorf("%s: cell %s is t=%q", name, c.R, c.T)
			}
			cells = append(cells, cell)
			last = col
		}
		values := make([]string, len(cells))
		styles := make([]int, len(cells))
		for j, c := range cells {
			values[j], styles[j] = c.Text, c.Style
		}
		sh.Rows = append(sh.Rows, values)
		sh.Cells = append(sh.Cells, cells)
		sh.RowStyles = append(sh.RowStyles, row.S)
		sh.RowHeights = append(sh.RowHeights, row.Ht)
		sh.CellStyles = append(sh.CellStyles, styles)
	}
	if sheet.Merges.Count != len(sheet.Merges.Cells) {
		return sh, 0, fmt.Errorf("%s: mergeCells count %d, %d entries", name, sheet.Merges.Count, len(sheet.Merges.Cells))
	}
	for _, m := range sheet.Merges.Cells {
		sh.Merges = append(sh.Merges, m.Ref)
	}
	return sh, refs, nil
}

func (wb *Workbook) checkStyle(s int) error {
	if s < 0 || s >= len(wb.Styles) {
		return fmt.Errorf("style %d outside the %d cellXfs", s, len(wb.Styles))
	}
	return nil
}

func (wb *Workbook) readStyles() error {
	type color struct {
		RGB string `xml:"rgb,attr"`
	}
	type side struct {
		Style string `xml:"style,attr"`
		Color *color `xml:"color"`
	}
	var st struct {
		NumFmts struct {
			Count int `xml:"count,attr"`
			List  []struct {
				ID   int    `xml:"numFmtId,attr"`
				Code string `xml:"formatCode,attr"`
			} `xml:"numFmt"`
		} `xml:"numFmts"`
		Fonts struct {
			Count int `xml:"count,attr"`
			List  []struct {
				B  *struct{} `xml:"b"`
				I  *struct{} `xml:"i"`
				Sz struct {
					Val float64 `xml:"val,attr"`
				} `xml:"sz"`
				Col  color `xml:"color"`
				Name struct {
					Val string `xml:"val,attr"`
				} `xml:"name"`
			} `xml:"font"`
		} `xml:"fonts"`
		Fills struct {
			Count int `xml:"count,attr"`
			List  []struct {
				Pattern struct {
					Type string `xml:"patternType,attr"`
					Fg   color  `xml:"fgColor"`
				} `xml:"patternFill"`
			} `xml:"fill"`
		} `xml:"fills"`
		Borders struct {
			Count int `xml:"count,attr"`
			List  []struct {
				Left   side `xml:"left"`
				Right  side `xml:"right"`
				Top    side `xml:"top"`
				Bottom side `xml:"bottom"`
			} `xml:"border"`
		} `xml:"borders"`
		Xfs struct {
			Count int `xml:"count,attr"`
			List  []struct {
				NumFmt int `xml:"numFmtId,attr"`
				Font   int `xml:"fontId,attr"`
				Fill   int `xml:"fillId,attr"`
				Border int `xml:"borderId,attr"`
				Align  *struct {
					H    string `xml:"horizontal,attr"`
					V    string `xml:"vertical,attr"`
					Wrap bool   `xml:"wrapText,attr"`
				} `xml:"alignment"`
			} `xml:"xf"`
		} `xml:"cellXfs"`
	}
	if err := xml.Unmarshal(wb.Raw["xl/styles.xml"], &st); err != nil {
		return fmt.Errorf("styles.xml: %w", err)
	}
	for what, c := range map[string][2]int{
		"numFmts": {st.NumFmts.Count, len(st.NumFmts.List)}, "fonts": {st.Fonts.Count, len(st.Fonts.List)},
		"fills": {st.Fills.Count, len(st.Fills.List)}, "borders": {st.Borders.Count, len(st.Borders.List)},
		"cellXfs": {st.Xfs.Count, len(st.Xfs.List)},
	} {
		if c[0] != c[1] {
			return fmt.Errorf("styles.xml: %s count %d, %d entries", what, c[0], c[1])
		}
	}
	codes := map[int]string{}
	for _, f := range st.NumFmts.List {
		codes[f.ID] = f.Code
	}
	sideColor := func(s side) (string, error) {
		switch {
		case s.Style == "" && s.Color == nil:
			return "", nil
		case s.Style == "thin" && s.Color != nil:
			return s.Color.RGB, nil
		}
		return "", fmt.Errorf("styles.xml: border side %+v", s)
	}
	for i, x := range st.Xfs.List {
		if x.Font >= len(st.Fonts.List) || x.Fill >= len(st.Fills.List) || x.Border >= len(st.Borders.List) {
			return fmt.Errorf("styles.xml: xf %d points past a table", i)
		}
		code, ok := codes[x.NumFmt]
		if x.NumFmt != 0 && !ok {
			return fmt.Errorf("styles.xml: xf %d has numFmtId %d, not defined", i, x.NumFmt)
		}
		f := st.Fonts.List[x.Font]
		s := Style{NumberFormat: code, FontName: f.Name.Val, FontSize: f.Sz.Val, Bold: f.B != nil, Italic: f.I != nil, FontColor: f.Col.RGB}
		if p := st.Fills.List[x.Fill].Pattern; p.Type == "solid" {
			s.Fill = p.Fg.RGB
		}
		b := st.Borders.List[x.Border]
		for k, sd := range []side{b.Left, b.Right, b.Top, b.Bottom} {
			c, err := sideColor(sd)
			if err != nil {
				return err
			}
			s.Border[k] = c
		}
		if a := x.Align; a != nil {
			s.Horizontal, s.Vertical, s.Wrap = a.H, a.V, a.Wrap
		}
		wb.Styles = append(wb.Styles, s)
	}
	if len(wb.Styles) == 0 {
		return fmt.Errorf("styles.xml: no cellXfs")
	}
	return nil
}

// splitRef turns "AB12" into column 28, row 12.
func splitRef(ref string) (col, row int, err error) {
	i := 0
	for i < len(ref) && 'A' <= ref[i] && ref[i] <= 'Z' {
		col = col*26 + int(ref[i]-'A'+1)
		i++
	}
	if i == 0 || i == len(ref) {
		return 0, 0, fmt.Errorf("bad reference %q", ref)
	}
	row, err = strconv.Atoi(ref[i:])
	if err != nil || row < 1 || strings.HasPrefix(ref[i:], "0") {
		return 0, 0, fmt.Errorf("bad reference %q", ref)
	}
	return col, row, nil
}

func isHex(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F'
}

// DecodeEscapes turns each _xHHHH_ into its character, left to right, the way
// Excel reads ST_Xstring text.
func DecodeEscapes(s string) string {
	if !strings.Contains(s, "_x") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); {
		if i+7 <= len(s) && s[i] == '_' && s[i+1] == 'x' && isHex(s[i+2]) && isHex(s[i+3]) && isHex(s[i+4]) && isHex(s[i+5]) && s[i+6] == '_' {
			v, _ := strconv.ParseUint(s[i+2:i+6], 16, 32)
			b.WriteRune(rune(v))
			i += 7
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}
