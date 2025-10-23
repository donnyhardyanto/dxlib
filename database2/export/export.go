package export

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"time"
	_ "time/tzdata"

	"github.com/donnyhardyanto/dxlib/database2/db"
	"github.com/donnyhardyanto/dxlib/utils"
	"github.com/pkg/errors"
	"github.com/xuri/excelize/v2"
)

type ExportFormat string

const (
	CSV ExportFormat = "csv"
	XLS ExportFormat = "xls"
)

type ExportOptions struct {
	Format     ExportFormat
	FilePath   string
	SheetName  string
	DateFormat string
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
	case XLS:
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
	case XLS:
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

	if err := writer.Write(rowsInfo.Columns); err != nil {
		return errors.Errorf("failed to write CSV headers: %+v", err)
	}

	for _, row := range rows {
		record := make([]string, len(rowsInfo.Columns))
		for i, col := range rowsInfo.Columns {
			record[i] = formatValue(row[col], opts.DateFormat)
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

	if err := writer.Write(rowsInfo.Columns); err != nil {
		return nil, errors.Errorf("failed to write CSV headers: %+v", err)
	}

	for _, row := range rows {
		record := make([]string, len(rowsInfo.Columns))
		for i, col := range rowsInfo.Columns {
			record[i] = formatValue(row[col], opts.DateFormat)
		}
		if err := writer.Write(record); err != nil {
			return nil, errors.Errorf("failed to write CSV record: %+v", err)
		}
	}
	writer.Flush()

	return buf.Bytes(), nil
}

func exportToXLS(rowsInfo *db.DXDatabaseTableRowsInfo, rows []utils.JSON, opts ExportOptions) error {
	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Printf("Error closing Excel file: %v\n", err)
		}
	}()

	if err := writeXLSContent(f, rowsInfo, rows, opts); err != nil {
		return errors.Wrap(err, "error occured")
	}

	return f.SaveAs(opts.FilePath)
}

func exportToXLSStream(rowsInfo *db.DXDatabaseTableRowsInfo, rows []utils.JSON, opts ExportOptions) ([]byte, error) {
	f := excelize.NewFile()
	defer f.Close()

	if err := writeXLSContent(f, rowsInfo, rows, opts); err != nil {
		return nil, err
	}

	buf, err := f.WriteToBuffer()
	if err != nil {
		return nil, errors.Errorf("failed to write Excel to buffer: %+v", err)
	}
	return buf.Bytes(), nil
}

func writeXLSContent(f *excelize.File, rowsInfo *db.DXDatabaseTableRowsInfo, rows []utils.JSON, opts ExportOptions) error {
	sheetName := opts.SheetName
	if sheetName == "" {
		sheetName = "Sheet1"
	}

	// Write headers
	for i, col := range rowsInfo.Columns {
		cellName, err := excelize.CoordinatesToCellName(i+1, 1)
		if err != nil {
			return errors.Errorf("invalid cell coordinates: %+v", err)
		}
		if err := f.SetCellValue(sheetName, cellName, col); err != nil {
			return errors.Errorf("failed to write header: %+v", err)
		}
	}

	// Write data rows
	for rowIdx, row := range rows {
		for colIdx, col := range rowsInfo.Columns {
			cellName, err := excelize.CoordinatesToCellName(colIdx+1, rowIdx+2)
			if err != nil {
				return errors.Errorf("invalid cell coordinates: %+v", err)
			}
			if err := f.SetCellValue(sheetName, cellName, formatValue(row[col], opts.DateFormat)); err != nil {
				return errors.Errorf("failed to write cell value: %+v", err)
			}
		}
	}

	// Apply styling
	style, err := f.NewStyle(&excelize.Style{
		Font:      &excelize.Font{Bold: true},
		Alignment: &excelize.Alignment{Horizontal: "center"},
		Fill:      excelize.Fill{Type: "pattern", Pattern: 1, Color: []string{"#E0EBF5"}},
	})
	if err == nil {
		if err := f.SetRowStyle(sheetName, 1, 1, style); err != nil {
			return errors.Errorf("failed to apply header style: %+v", err)
		}
	}

	// Auto-fit columns
	for i := range rowsInfo.Columns {
		col, err := excelize.ColumnNumberToName(i + 1)
		if err != nil {
			return errors.Errorf("invalid column number: %+v", err)
		}
		if err := f.SetColWidth(sheetName, col, col, 15); err != nil {
			return errors.Errorf("failed to set column width: %+v", err)
		}
	}

	return nil
}

func formatValue(v interface{}, dateFormat string) string {
	if v == nil {
		return ""
	}

	switch val := v.(type) {
	case time.Time:
		return val.Format(dateFormat)
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}
