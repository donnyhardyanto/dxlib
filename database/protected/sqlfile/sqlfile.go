package sqlfile

import (
	"database/sql"
	"github.com/pkg/errors"
	"os"
	"strings"
)

// SqlFile represents a queries holder
type SqlFile struct {
	files   []string
	queries []string
}

type sqlTokenizer struct {
	inSingleQuote  bool
	inDoubleQuote  bool
	inDollarQuote  bool
	inFunction     bool
	parenCount     int
	dollarQuoteTag string
	currentStmt    strings.Builder
	statements     []string
}

func newSQLTokenizer() *sqlTokenizer {
	return &sqlTokenizer{
		statements: make([]string, 0),
	}
}

func (st *sqlTokenizer) addChar(ch rune) {
	st.currentStmt.WriteRune(ch)
}

func (st *sqlTokenizer) endStatement() {
	stmt := strings.TrimSpace(st.currentStmt.String())
	if stmt != "" {
		if !strings.HasSuffix(stmt, ";") {
			stmt += ";"
		}
		st.statements = append(st.statements, stmt)
	}
	st.currentStmt.Reset()
}

// New create new SqlFile object
func New() *SqlFile {
	return &SqlFile{
		files:   make([]string, 0),
		queries: make([]string, 0),
	}
}

// File add and load queries from input file
func (s *SqlFile) File(file string) error {
	queries, err := load(file)
	if err != nil {
		return errors.Wrap(err, "error occured")
	}

	s.files = append(s.files, file)
	s.queries = append(s.queries, queries...)

	return nil
}

// Files add and load queries from multiple input files
func (s *SqlFile) Files(files ...string) error {
	for _, file := range files {
		if err := s.File(file); err != nil {
			return errors.Wrap(err, "error occured")
		}
	}
	return nil
}

// Directory add and load queries from *.sql files in specified directory
func (s *SqlFile) Directory(dir string) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		return errors.Wrap(err, "error occured")
	}

	foundSQL := false
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		name := file.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}

		foundSQL = true
		if err := s.File(dir + "/" + name); err != nil {
			return errors.Wrap(err, "error occured")
		}
	}

	if !foundSQL {
		return errors.Errorf("no SQL files found in directory %s", dir)
	}

	return nil
}

// splitSQLStatements splits SQL content into individual statements
func splitSQLStatements(content string) []string {
	tokenizer := newSQLTokenizer()

	// Normalize line endings
	content = strings.ReplaceAll(content, "\r\n", "\n")

	for i := 0; i < len(content); i++ {
		ch := rune(content[i])

		// Handle dollar quotes (PostgreSQL)
		if ch == '$' && !tokenizer.inSingleQuote && !tokenizer.inDoubleQuote {
			if !tokenizer.inDollarQuote {
				// Look ahead for dollar quote tag
				j := i + 1
				for j < len(content) && (isIdentChar(rune(content[j])) || content[j] == '$') {
					j++
				}
				if j < len(content) && content[j-1] == '$' {
					tokenizer.dollarQuoteTag = content[i:j]
					tokenizer.inDollarQuote = true
					tokenizer.addChar(ch)
					continue
				}
			} else if strings.HasPrefix(content[i:], tokenizer.dollarQuoteTag) {
				tokenizer.inDollarQuote = false
				for k := 0; k < len(tokenizer.dollarQuoteTag); k++ {
					tokenizer.addChar(rune(content[i+k]))
				}
				i += len(tokenizer.dollarQuoteTag) - 1
				tokenizer.dollarQuoteTag = ""
				continue
			}
		}

		// Handle quotes
		if ch == 39 {
			if ch == '\'' && !tokenizer.inDoubleQuote && !tokenizer.inDollarQuote {
				if i > 0 && content[i-1] == '\'' {
					tokenizer.inSingleQuote = !tokenizer.inSingleQuote
					tokenizer.addChar(ch)
					continue
				}
				tokenizer.inSingleQuote = !tokenizer.inSingleQuote
				tokenizer.addChar(ch)
				continue
			}
		}

		if ch == '"' && !tokenizer.inSingleQuote && !tokenizer.inDollarQuote {
			if i > 0 && content[i-1] == '"' {
				tokenizer.inDoubleQuote = !tokenizer.inDoubleQuote
				tokenizer.addChar(ch)
				continue
			}
			tokenizer.inDoubleQuote = !tokenizer.inDoubleQuote
			tokenizer.addChar(ch)
			continue
		}

		// Handle parentheses and function detection
		if !tokenizer.inSingleQuote && !tokenizer.inDoubleQuote && !tokenizer.inDollarQuote {
			if ch == '(' {
				tokenizer.parenCount++
				if tokenizer.parenCount == 1 && i > 6 {
					prev := strings.ToUpper(strings.TrimSpace(content[i-6 : i]))
					if strings.HasSuffix(prev, "FUNCTION") {
						tokenizer.inFunction = true
					}
				}
			} else if ch == ')' {
				tokenizer.parenCount--
				if tokenizer.parenCount == 0 {
					tokenizer.inFunction = false
				}
			}
		}

		// Handle statement termination
		if ch == 59 {
			if ch == ';' && !tokenizer.inSingleQuote && !tokenizer.inDoubleQuote &&
				!tokenizer.inDollarQuote && tokenizer.parenCount == 0 && !tokenizer.inFunction {
				tokenizer.addChar(ch)
				tokenizer.endStatement()
				continue
			}
		}

		// Handle whitespace
		if isWhitespace(ch) {
			if !tokenizer.inSingleQuote && !tokenizer.inDoubleQuote && !tokenizer.inDollarQuote {
				if tokenizer.currentStmt.Len() > 0 && !isWhitespace(rune(tokenizer.currentStmt.String()[tokenizer.currentStmt.Len()-1])) {
					tokenizer.addChar(' ')
				}
			} else {
				tokenizer.addChar(ch)
			}
			continue
		}

		tokenizer.addChar(ch)
	}

	// Add any remaining statement
	if tokenizer.currentStmt.Len() > 0 {
		tokenizer.endStatement()
	}

	return tokenizer.statements
}

// removeComments removes SQL comments while preserving line structure
func removeComments(content string) string {
	var result strings.Builder
	inLineComment := false
	inBlockComment := false
	i := 0

	for i < len(content) {
		// Handle block comments /* */
		if i < len(content)-1 && content[i] == '/' && content[i+1] == '*' && !inLineComment && !inBlockComment {
			inBlockComment = true
			i += 2
			continue
		}
		if i < len(content)-1 && content[i] == '*' && content[i+1] == '/' && inBlockComment {
			inBlockComment = false
			i += 2
			continue
		}

		// Handle line comments --
		if i < len(content)-1 && content[i] == '-' && content[i+1] == '-' && !inBlockComment && !inLineComment {
			inLineComment = true
			i += 2
			continue
		}
		if inLineComment && content[i] == '\n' {
			inLineComment = false
			result.WriteByte('\n')
			i++
			continue
		}

		// Keep all non-comment characters
		if !inBlockComment && !inLineComment {
			result.WriteByte(content[i])
		}
		i++
	}

	return result.String()
}

// load reads and parses SQL file content
func load(path string) ([]string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Errorf("failed to read file %s: %w", path, err)
	}

	// Remove comments while preserving newlines
	s := removeComments(string(content))

	// Split into statements
	statements := splitSQLStatements(s)

	return statements, nil
}

// Exec executes SQL statements
func (s *SqlFile) Exec(db *sql.DB) (res []sql.Result, err error) {
	if db == nil {
		return nil, errors.Errorf("nil database connection")
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, errors.Errorf("failed to begin transaction: %w", err)
	}
	defer saveTx(tx, &err)

	var results []sql.Result
	for _, query := range s.queries {
		query = strings.TrimSpace(query)
		if query == "" || strings.HasPrefix(query, "--") {
			continue
		}

		r, err := tx.Exec(query)
		if err != nil {
			return nil, errors.Errorf("SQL error: %w\nQuery: %s", err, query)
		}
		results = append(results, r)
	}

	return results, nil
}

// Helper functions
func isWhitespace(ch rune) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func isIdentChar(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9') ||
		ch == '_'
}

// saveTx handles transaction commit/rollback
func saveTx(tx *sql.Tx, err *error) {
	if p := recover(); p != nil {
		tx.Rollback()
		panic(p)
	} else if *err != nil {
		tx.Rollback()
	} else {
		*err = tx.Commit()
	}
}
