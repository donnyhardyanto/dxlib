// Package sqlparser provides a fast, memory-efficient SQL statement parser
package sqlparser

import (
	"database/sql"
	"fmt"
	"github.com/donnyhardyanto/dxlib/database/protected/sqlfile/sqlparser"
	"os"
	"path/filepath"
	"strings"
)

// SQLFile represents a queries holder
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

// File adds and loads queries from input file
func (s *SQLFile) File(file string) error {
	content, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", file, err)
	}

	// Parse SQL statements
	statements, err := s.parser.Parse(content)
	if err != nil {
		return fmt.Errorf("failed to parse SQL from file %s: %w", file, err)
	}

	// Add file to processed files list
	s.files = append(s.files, file)

	// Add parsed statements to queries list
	for _, stmt := range statements {
		if query := strings.TrimSpace(string(stmt.Query)); query != "" {
			s.queries = append(s.queries, query)
		}
	}

	return nil
}

// Files adds and loads queries from multiple input files
func (s *SQLFile) Files(files ...string) error {
	for _, file := range files {
		if err := s.File(file); err != nil {
			return err
		}
	}
	return nil
}

// Directory adds and loads queries from *.sql files in specified directory
func (s *SQLFile) Directory(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read directory %s: %w", dir, err)
	}

	foundSQL := false
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if filepath.Ext(entry.Name()) != ".sql" {
			continue
		}

		foundSQL = true
		fullPath := filepath.Join(dir, entry.Name())
		if err := s.File(fullPath); err != nil {
			return err
		}
	}

	if !foundSQL {
		return fmt.Errorf("no SQL files found in directory %s", dir)
	}

	return nil
}

// Execute executes the SQL statements
func (s *SQLFile) Execute(db *sql.DB) error {
	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
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
	statements, err := parser.ParseString(sqlContent)
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
			tx.Rollback()
		}
	}()

	// Execute each statement
	for _, stmt := range statements {
		query := strings.TrimSpace(string(stmt.Query))
		if query == "" {
			continue
		}

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
