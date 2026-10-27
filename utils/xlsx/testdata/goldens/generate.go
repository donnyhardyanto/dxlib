//go:build ignore

// generate writes the excelize fixtures and, for every workbook in testdata,
// the golden file holding excelize's GetSheetList and GetRows, formatted and
// with RawCellValue. It needs
// excelize, which dxlib does not require, so run.sh builds it in a module of
// its own outside the repository. The reader's tests compare against the
// golden files.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

// formats is one row of the Formats sheet each: the format, then the same
// values in every row.
var formats = []struct {
	id   int
	code string
}{
	{1, ""}, {2, ""}, {3, ""}, {4, ""}, {9, ""}, {10, ""}, {11, ""}, {12, ""},
	{14, ""}, {15, ""}, {16, ""}, {17, ""}, {18, ""}, {19, ""}, {20, ""},
	{21, ""}, {22, ""}, {37, ""}, {38, ""}, {39, ""}, {40, ""}, {41, ""},
	{43, ""}, {44, ""}, {45, ""}, {46, ""}, {47, ""}, {49, ""},
	{0, "0.000"},
	{0, "#,##0.00"},
	{0, `"Rp"#,##0`},
	{0, `"Rp"#,##0.00;[Red]\-"Rp"#,##0.00`},
	{0, "#,##0;(#,##0)"},
	{0, "0.0%"},
	{0, "$#,##0.00"},
	{0, `#,##0.00 "kg"`},
	{0, "#.##"},
	{0, "000000"},
	{0, "yyyy-mm-dd"},
	{0, "dd/mm/yyyy hh:mm:ss"},
	{0, "d mmm yyyy"},
	{0, "mmmm d, yyyy"},
	{0, "dddd, mmmmm"},
	{0, "h:mm AM/PM"},
	{0, "h:mm:ss A/P"},
	{0, "[h]:mm:ss"},
	{0, "hh:mm:ss.000"},
	{0, "yy-m-d h:m:s"},
	{0, `"x"@`},
	{0, `0;-0;"zero";"text: "@`},
	{0, "[Blue]0.00"},
	{0, "[$-409]mmmm d, yyyy"},
	{0, "[$-421]dd mmmm yyyy"},
	{0, "[$€-407]#,##0.00"},
	{0, "#,##0,"},
	{0, "0.00E+00"},
	{0, "General"},
	{0, `0" units"`},
}

var values = []any{1234.5678, -1234.5678, 0, 0.5, 45000.75, 1.25, 45658.999999, 0.000123, 12345678901234567, "abc", true}

func fixture(path string) error {
	f := excelize.NewFile()
	defer f.Close()
	if err := f.SetSheetName("Sheet1", "Values"); err != nil {
		return err
	}
	set := func(sheet, cell string, v any) {
		if err := f.SetCellValue(sheet, cell, v); err != nil {
			panic(err)
		}
	}
	set("Values", "A1", "text")
	set("Values", "B1", "  lead and trail  ")
	set("Values", "C1", "_x0041_ stays")
	set("Values", "D1", "日本語 😀 é")
	set("Values", "F1", "after a gap")
	set("Values", "A2", 42)
	set("Values", "B2", -7)
	set("Values", "C2", 0.1+0.2)
	set("Values", "D2", 12345678901234567890.0)
	set("Values", "E2", 1e-7)
	set("Values", "F2", 3.0)
	set("Values", "A3", true)
	set("Values", "B3", false)
	set("Values", "C3", time.Date(2026, 10, 27, 13, 45, 30, 0, time.UTC))
	set("Values", "D3", time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC))
	set("Values", "E3", 90*time.Minute)
	if err := f.SetCellDefault("Values", "A4", "inline string"); err != nil {
		return err
	}
	if err := f.SetCellDefault("Values", "B4", "12.50"); err != nil {
		return err
	}
	if err := f.SetCellFormula("Values", "C4", "SUM(A2:B2)"); err != nil {
		return err
	}
	if err := f.SetCellRichText("Values", "D4", []excelize.RichTextRun{{Text: "rich "}, {Text: "text", Font: &excelize.Font{Bold: true}}}); err != nil {
		return err
	}
	// Row 5 is left out: GetRows gives an empty row between 4 and 6.
	set("Values", "B6", "line one\nline two")
	set("Values", "C6", "tab\there")
	set("Values", "A8", "")
	set("Values", "H9", "far")

	if _, err := f.NewSheet("Formats"); err != nil {
		return err
	}
	for i, fm := range formats {
		row := i + 1
		st := &excelize.Style{NumFmt: fm.id}
		label := fmt.Sprintf("id %d", fm.id)
		if fm.code != "" {
			code := fm.code
			st = &excelize.Style{CustomNumFmt: &code}
			label = fm.code
		}
		style, err := f.NewStyle(st)
		if err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		set("Formats", fmt.Sprintf("A%d", row), label)
		for j, v := range values {
			cell, _ := excelize.CoordinatesToCellName(j+2, row)
			set("Formats", cell, v)
			if err := f.SetCellStyle("Formats", cell, cell, style); err != nil {
				return err
			}
		}
	}
	if _, err := f.NewSheet("Empty"); err != nil {
		return err
	}
	if _, err := f.NewSheet("Données 日本"); err != nil {
		return err
	}
	set("Données 日本", "B2", "x")
	return f.SaveAs(path)
}

func fixture1904(path string) error {
	f := excelize.NewFile()
	defer f.Close()
	yes := true
	if err := f.SetWorkbookProps(&excelize.WorkbookPropsOptions{Date1904: &yes}); err != nil {
		return err
	}
	style, err := f.NewStyle(&excelize.Style{NumFmt: 22})
	if err != nil {
		return err
	}
	for i, v := range []float64{0, 1, 366.5, 43000.25} {
		cell := fmt.Sprintf("A%d", i+1)
		if err := f.SetCellValue("Sheet1", cell, v); err != nil {
			return err
		}
		if err := f.SetCellStyle("Sheet1", cell, cell, style); err != nil {
			return err
		}
	}
	return f.SaveAs(path)
}

// fixtureDates holds date serials under the date formats a caller reads raw,
// custom and built-in, and cells whose format hides the value, so raw and
// formatted rows differ in what they trim.
func fixtureDates(path string) error {
	f := excelize.NewFile()
	defer f.Close()
	if err := f.SetSheetName("Sheet1", "Dates"); err != nil {
		return err
	}
	type col struct {
		head string
		id   int
		code string
	}
	cols := []col{
		{"serial", 0, ""},
		{"mmm-yy", 0, "mmm-yy"},
		{"mm-dd-yy", 0, "mm-dd-yy"},
		{"yyyy-mm-dd", 0, "yyyy-mm-dd"},
		{"id 14", 14, ""},
		{"id 17", 17, ""},
		{"id 22", 22, ""},
		{";;;", 0, ";;;"},
	}
	dates := []any{
		45717,
		45717.0,
		45658.75,
		46322.5,
		1.0,
		time.Date(2026, 10, 27, 13, 45, 30, 0, time.UTC),
		time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC),
		"2025-03-01",
		true,
	}
	for j, c := range cols {
		head, _ := excelize.CoordinatesToCellName(j+1, 1)
		if err := f.SetCellValue("Dates", head, c.head); err != nil {
			return err
		}
		st := &excelize.Style{NumFmt: c.id}
		if c.code != "" {
			code := c.code
			st = &excelize.Style{CustomNumFmt: &code}
		}
		style, err := f.NewStyle(st)
		if err != nil {
			return fmt.Errorf("%s: %w", c.head, err)
		}
		for i, v := range dates {
			cell, _ := excelize.CoordinatesToCellName(j+1, i+2)
			if err := f.SetCellValue("Dates", cell, v); err != nil {
				return err
			}
			if c.id != 0 || c.code != "" {
				if err := f.SetCellStyle("Dates", cell, cell, style); err != nil {
					return err
				}
			}
		}
	}
	return f.SaveAs(path)
}

type golden struct {
	Excelize string                `json:"excelize"`
	Sheets   []string              `json:"sheets"`
	Rows     map[string][][]string `json:"rows"`
	Raw      map[string][][]string `json:"raw"`
}

func orEmpty(rows [][]string) [][]string {
	if rows == nil {
		return [][]string{}
	}
	return rows
}

func dump(path string) error {
	f, err := excelize.OpenFile(path)
	if err != nil {
		return err
	}
	defer f.Close()
	g := golden{Excelize: "v2.11.0", Sheets: f.GetSheetList(), Rows: map[string][][]string{}, Raw: map[string][][]string{}}
	for _, s := range g.Sheets {
		rows, err := f.GetRows(s)
		if err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
		raw, err := f.GetRows(s, excelize.Options{RawCellValue: true})
		if err != nil {
			return fmt.Errorf("%s, raw: %w", s, err)
		}
		g.Rows[s], g.Raw[s] = orEmpty(rows), orEmpty(raw)
	}
	b, err := json.MarshalIndent(g, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(strings.TrimSuffix(path, ".xlsx")+".golden.json", append(b, '\n'), 0o644)
}

func main() {
	dir := os.Args[1]
	if err := fixture(filepath.Join(dir, "excelize.xlsx")); err != nil {
		panic(err)
	}
	if err := fixture1904(filepath.Join(dir, "excelize-1904.xlsx")); err != nil {
		panic(err)
	}
	if err := fixtureDates(filepath.Join(dir, "excelize-dates.xlsx")); err != nil {
		panic(err)
	}
	names, _ := filepath.Glob(filepath.Join(dir, "*.xlsx"))
	sort.Strings(names)
	for _, n := range names {
		if err := dump(n); err != nil {
			panic(fmt.Errorf("%s: %w", n, err))
		}
		fmt.Println("golden:", filepath.Base(n))
	}
}
