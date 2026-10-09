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

// Workbook is what a test needs to know about a written file.
type Workbook struct {
	SheetName    string
	Rows         [][]string // cell values as Excel shows them, _xHHHH_ decoded
	RowStyles    []int      // the s= of each row, 0 when absent
	CellStyles   [][]int    // the s= of each cell, 0 when absent
	ColumnWidths []float64  // one per column up to the last <col>, 0 where none is set
	Parts        []string   // zip entries, in the order their local headers appear
	Raw          map[string][]byte
}

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
		Sheets []struct {
			Name string `xml:"name,attr"`
		} `xml:"sheets>sheet"`
	}
	if err := xml.Unmarshal(wb.Raw["xl/workbook.xml"], &workbook); err != nil {
		return nil, fmt.Errorf("workbook.xml: %w", err)
	}
	if len(workbook.Sheets) != 1 {
		return nil, fmt.Errorf("workbook.xml: %d sheets, want 1", len(workbook.Sheets))
	}
	wb.SheetName = workbook.Sheets[0].Name

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

	var sheet struct {
		Cols []struct {
			Min   int     `xml:"min,attr"`
			Max   int     `xml:"max,attr"`
			Width float64 `xml:"width,attr"`
		} `xml:"cols>col"`
		Rows []struct {
			R     int `xml:"r,attr"`
			S     int `xml:"s,attr"`
			Cells []struct {
				R string    `xml:"r,attr"`
				S int       `xml:"s,attr"`
				T string    `xml:"t,attr"`
				F *struct{} `xml:"f"`
				V string    `xml:"v"`
			} `xml:"c"`
		} `xml:"sheetData>row"`
	}
	if err := xml.Unmarshal(wb.Raw["xl/worksheets/sheet1.xml"], &sheet); err != nil {
		return nil, fmt.Errorf("sheet1.xml: %w", err)
	}
	for _, c := range sheet.Cols {
		for len(wb.ColumnWidths) < c.Max {
			wb.ColumnWidths = append(wb.ColumnWidths, 0)
		}
		for i := c.Min; i <= c.Max; i++ {
			wb.ColumnWidths[i-1] = c.Width
		}
	}
	refs := 0
	for i, row := range sheet.Rows {
		if row.R != i+1 {
			return nil, fmt.Errorf("sheet1.xml: row %d has r=%d", i+1, row.R)
		}
		values := make([]string, len(row.Cells))
		styles := make([]int, len(row.Cells))
		for j, c := range row.Cells {
			if want := columnName(j+1) + strconv.Itoa(row.R); c.R != want {
				return nil, fmt.Errorf("sheet1.xml: cell %s, want %s", c.R, want)
			}
			if c.T != "s" || c.F != nil {
				return nil, fmt.Errorf("sheet1.xml: cell %s is t=%q, formula %v; want a shared string", c.R, c.T, c.F != nil)
			}
			k, err := strconv.Atoi(c.V)
			if err != nil || k < 0 || k >= len(strs) {
				return nil, fmt.Errorf("sheet1.xml: cell %s points at shared string %q", c.R, c.V)
			}
			values[j] = strs[k]
			styles[j] = c.S
			refs++
		}
		wb.Rows = append(wb.Rows, values)
		wb.RowStyles = append(wb.RowStyles, row.S)
		wb.CellStyles = append(wb.CellStyles, styles)
	}
	if refs != sst.Count {
		return nil, fmt.Errorf("sharedStrings.xml: count %d, %d cells", sst.Count, refs)
	}
	return wb, nil
}

func columnName(n int) string {
	s := ""
	for n > 0 {
		n--
		s = string(rune('A'+n%26)) + s
		n /= 26
	}
	return s
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
