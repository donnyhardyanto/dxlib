package sqlfile

import (
	"database/sql"
	"fmt"
	"github.com/donnyhardyanto/dxlib/database/protected/sqlfile/sqlparser"
	"os"
	"strings"
)

// SQLFile represents a SQL file handler
type SQLFile struct {
	files   []string
	queries []string
	parser  *sqlparser.Parser
}

// NewSQLFile creates a new SQLFile instance
func NewSQLFile() *SQLFile {
	return &SQLFile{
		files:   make([]string, 0),
		queries: make([]string, 0),
		parser:  sqlparser.NewParser(),
	}
}

// File loads and parses a single SQL file
func (s *SQLFile) File(filename string) error {
	// Read file content
	content, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", filename, err)
	}

	// Parse statements
	statements, err := s.parser.Parse(content)
	if err != nil {
		return fmt.Errorf("failed to parse SQL from file %s: %w", filename, err)
	}

	// Add file to processed files list
	s.files = append(s.files, filename)

	// Add statements to queries list
	for _, stmt := range statements {
		if query := strings.TrimSpace(string(stmt.Query)); query != "" {
			s.queries = append(s.queries, query)
		}
	}

	return nil
}

// Files loads and parses multiple SQL files
func (s *SQLFile) Files(files ...string) error {
	for _, file := range files {
		if err := s.File(file); err != nil {
			return err
		}
	}
	return nil
}

// Execute executes all loaded queries in a transaction
func (s *SQLFile) Execute(db *sql.DB) error {
	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Use defer to ensure rollback in case of error
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Execute each query
	for _, query := range s.queries {
		if _, err := tx.Exec(query); err != nil {
			return fmt.Errorf("failed to execute query: %w\nQuery: %s", err, query)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// ExecuteSQL executes a SQL string directly
func ExecuteSQL(db *sql.DB, sqlContent string) error {
	parser := sqlparser.NewParser()

	// Parse SQL statements
	statements, err := parser.Parse([]byte(sqlContent))
	if err != nil {
		return fmt.Errorf("failed to parse SQL: %w", err)
	}

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Execute each statement
	for _, stmt := range statements {
		if query := strings.TrimSpace(string(stmt.Query)); query != "" {
			if _, err := tx.Exec(query); err != nil {
				return fmt.Errorf("failed to execute query: %w\nQuery: %s", err, query)
			}
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetQueries returns the list of loaded queries
func (s *SQLFile) GetQueries() []string {
	result := make([]string, len(s.queries))
	copy(result, s.queries)
	return result
}

// GetFiles returns the list of processed files
func (s *SQLFile) GetFiles() []string {
	result := make([]string, len(s.files))
	copy(result, s.files)
	return result
}

// Clear removes all loaded queries and files
func (s *SQLFile) Clear() {
	s.queries = s.queries[:0]
	s.files = s.files[:0]
}
