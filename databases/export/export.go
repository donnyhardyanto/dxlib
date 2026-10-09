package export

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/donnyhardyanto/dxlib/databases/db"
	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/language"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/donnyhardyanto/dxlib/utils/xlsx"
)

type ExportFormat string

const (
	CSV ExportFormat = "csv"
	XLS ExportFormat = "xls"
	// XLSX is what a caller asking for a spreadsheet almost always names, and
	// tables.DXTableExportFormatEnumSetAll has offered it as a valid choice all
	// along. It was missing here, so every request for it was answered
	// "unsupported export format: xlsx" before a byte was written. It writes the
	// same bytes as XLS: utils/xlsx produces OOXML either way, which is why the
	// content type for XLS is already the OOXML one.
	XLSX ExportFormat = "xlsx"
)

type ExportOptions struct {
	Format            ExportFormat
	FilePath          string
	SheetName         string
	DateFormat        string
	Timezone          string                           // Target timezone for timestamp display (e.g. "Asia/Jakarta"); empty = use original
	Language          language.DXLanguage              // Language for header translation
	TranslateFallback language.DXTranslateFallbackMode // Fallback mode for missing translations
}

func resolveTimezone(tz string) *time.Location {
	if tz == "" {
		return nil
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil
	}
	return loc
}

func ExportQueryResults(rowsInfo *db.DXDatabaseTableRowsInfo, rows []utils.JSON, opts ExportOptions) error {
	if opts.DateFormat == "" {
		opts.DateFormat = "2006-01-02 15:04:05"
	}

	dir := filepath.Dir(opts.FilePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return errors.Errorf("failed to create directory: %+v", err)
	}

	switch opts.Format {
	case CSV:
		return exportToCSV(rowsInfo, rows, opts)
	case XLS, XLSX:
		return exportToXLS(rowsInfo, rows, opts)
	default:
		return errors.Errorf("unsupported export format: %s", opts.Format)
	}
}

func ExportToStream(rowsInfo *db.DXDatabaseTableRowsInfo, rows []utils.JSON, opts ExportOptions) ([]byte, string, error) {
	if opts.DateFormat == "" {
		opts.DateFormat = "2006-01-02 15:04:05"
	}

	var contentType string
	switch opts.Format {
	case CSV:
		contentType = "text/csv"
		data, err := exportToCSVStream(rowsInfo, rows, opts)
		return data, contentType, err
	case XLS, XLSX:
		contentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
		data, err := exportToXLSStream(rowsInfo, rows, opts)
		return data, contentType, err
	default:
		return nil, "", errors.Errorf("unsupported export format: %s", opts.Format)
	}
}

func exportToCSV(rowsInfo *db.DXDatabaseTableRowsInfo, rows []utils.JSON, opts ExportOptions) error {
	file, err := os.Create(opts.FilePath)
	if err != nil {
		return errors.Errorf("failed to create CSV file: %+v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	loc := resolveTimezone(opts.Timezone)

	// Translate headers
	headers := make([]string, len(rowsInfo.Columns))
	for i, col := range rowsInfo.Columns {
		headers[i] = language.Translate(col, opts.Language, opts.TranslateFallback)
	}
	if err := writer.Write(headers); err != nil {
		return errors.Errorf("failed to write CSV headers: %+v", err)
	}

	for _, row := range rows {
		record := make([]string, len(rowsInfo.Columns))
		for i, col := range rowsInfo.Columns {
			record[i] = neutraliseCSVFormula(formatValue(row[col], opts.DateFormat, loc))
		}
		if err := writer.Write(record); err != nil {
			return errors.Errorf("failed to write CSV record: %+v", err)
		}
	}

	return nil
}

func exportToCSVStream(rowsInfo *db.DXDatabaseTableRowsInfo, rows []utils.JSON, opts ExportOptions) ([]byte, error) {
	buf := new(bytes.Buffer)
	writer := csv.NewWriter(buf)

	loc := resolveTimezone(opts.Timezone)

	// Translate headers
	headers := make([]string, len(rowsInfo.Columns))
	for i, col := range rowsInfo.Columns {
		headers[i] = language.Translate(col, opts.Language, opts.TranslateFallback)
	}
	if err := writer.Write(headers); err != nil {
		return nil, errors.Errorf("failed to write CSV headers: %+v", err)
	}

	for _, row := range rows {
		record := make([]string, len(rowsInfo.Columns))
		for i, col := range rowsInfo.Columns {
			record[i] = neutraliseCSVFormula(formatValue(row[col], opts.DateFormat, loc))
		}
		if err := writer.Write(record); err != nil {
			return nil, errors.Errorf("failed to write CSV record: %+v", err)
		}
	}
	writer.Flush()

	return buf.Bytes(), nil
}

func exportToXLS(rowsInfo *db.DXDatabaseTableRowsInfo, rows []utils.JSON, opts ExportOptions) error {
	file, err := os.Create(opts.FilePath)
	if err != nil {
		return errors.Errorf("failed to create Excel file: %+v", err)
	}
	// A half-written workbook is removed rather than left for someone to open.
	err = writeXLSContent(file, rowsInfo, rows, opts)
	if cerr := file.Close(); err == nil && cerr != nil {
		err = errors.Errorf("failed to write Excel file: %+v", cerr)
	}
	if err != nil {
		os.Remove(opts.FilePath)
	}
	return err
}

func exportToXLSStream(rowsInfo *db.DXDatabaseTableRowsInfo, rows []utils.JSON, opts ExportOptions) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := writeXLSContent(buf, rowsInfo, rows, opts); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeXLSContent(out io.Writer, rowsInfo *db.DXDatabaseTableRowsInfo, rows []utils.JSON, opts ExportOptions) error {
	sheetName := opts.SheetName
	if sheetName == "" {
		sheetName = "Sheet1"
	}

	widths := make([]float64, len(rowsInfo.Columns))
	for i := range widths {
		widths[i] = 15
	}
	xw, err := xlsx.NewWriter(out, xlsx.Options{
		SheetName:    sheetName,
		ColumnWidths: widths,
		HeaderStyle:  &xlsx.Style{Bold: true, Center: true, FillRGB: "E0EBF5"},
	})
	if err != nil {
		return errors.Errorf("failed to start Excel file: %+v", err)
	}

	loc := resolveTimezone(opts.Timezone)

	headers := make([]string, len(rowsInfo.Columns))
	for i, col := range rowsInfo.Columns {
		headers[i] = language.Translate(col, opts.Language, opts.TranslateFallback)
	}
	if err := xw.WriteHeader(headers); err != nil {
		return errors.Errorf("failed to write header: %+v", err)
	}

	record := make([]string, len(rowsInfo.Columns))
	for _, row := range rows {
		for i, col := range rowsInfo.Columns {
			record[i] = formatValue(row[col], opts.DateFormat, loc)
		}
		if err := xw.WriteRow(record); err != nil {
			return errors.Errorf("failed to write row: %+v", err)
		}
	}

	if err := xw.Close(); err != nil {
		return errors.Errorf("failed to finish Excel file: %+v", err)
	}
	return nil
}

func formatValue(v interface{}, dateFormat string, loc *time.Location) string {
	if v == nil {
		return ""
	}

	switch val := v.(type) {
	case time.Time:
		if loc != nil {
			val = val.In(loc)
		}
		return val.Format(dateFormat)
	case []byte:
		return string(val)
	case []any:
		if len(val) == 0 {
			return "All"
		}
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = fmt.Sprintf("%v", item)
		}
		return strings.Join(parts, ", ")
	case []string:
		if len(val) == 0 {
			return "All"
		}
		return strings.Join(val, ", ")
	default:
		return fmt.Sprintf("%v", val)
	}
}

// csvPlainNumber matches a plain decimal number such as -5, +1.5 or -2e3.
var csvPlainNumber = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$`)

// neutraliseCSVFormula prefixes a single quote to a CSV cell that a spreadsheet
// would read as a formula: one starting with = + - @, a tab or a carriage return
// (CSV formula injection, CWE-1236; OWASP CSV Injection). Rows such as security
// events carry text the client chose, its User-Agent for one. A plain number
// keeps its sign. The XLSX writer does not need this: utils/xlsx stores a string
// as text, never as a formula.
func neutraliseCSVFormula(s string) string {
	if s == "" {
		return s
	}
	switch s[0] {
	case '=', '@', '\t', '\r':
		return "'" + s
	case '+', '-':
		if csvPlainNumber.MatchString(s) {
			return s
		}
		return "'" + s
	}
	return s
}
