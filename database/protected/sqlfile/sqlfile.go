// Package sqlfile provides a way to execute sql file easily
//
// For more usage see https://github.com/tanimutomo/sqlfile
package sqlfile

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// SqlFile represents a queries holder
type SqlFile struct {
	files   []string
	queries []string
}

// New create new SqlFile object
func New() *SqlFile {
	return &SqlFile{}
}

// File add and load queries from input file
func (s *SqlFile) File(file string) error {
	queries, err := load(file)
	if err != nil {
		return err
	}

	s.files = append(s.files, file)
	s.queries = append(s.queries, queries...)

	return nil
}

// Files add and load queries from multiple input files
func (s *SqlFile) Files(files ...string) error {
	for _, file := range files {
		if err := s.File(file); err != nil {
			return err
		}
	}
	return nil
}

// Directory add and load queries from *.sql files in specified directory
func (s *SqlFile) Directory(dir string) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		name := file.Name()
		if name[len(name)-3:] != "sql" {
			continue
		}

		if err := s.File(dir + "/" + name); err != nil {
			return err
		}
	}

	return nil
}

// Exec execute SQL statements written int the specified sql file
func (s *SqlFile) Exec(db *sql.DB) (res []sql.Result, err error) {
	tx, err := db.Begin()
	if err != nil {
		return res, err
	}
	defer saveTx(tx, &err)

	var rs []sql.Result
	for _, q := range s.queries {
		r, err := tx.Exec(q)
		if err != nil {
			return res, fmt.Errorf(err.Error() + " : when executing > " + q)
		}
		rs = append(rs, r)
	}

	return rs, err
}

func load(path string) ([]string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Remove comments
	commentRegex := regexp.MustCompile(`--.*$|/\*[\s\S]*?\*/`)
	cleanContent := commentRegex.ReplaceAllString(string(content), "")

	// Split the content into statements
	statements := splitSQLStatements2(cleanContent)

	// Trim and filter out empty statements
	var queries []string
	for _, stmt := range statements {
		if trimmedStmt := strings.TrimSpace(stmt); trimmedStmt != "" {
			queries = append(queries, trimmedStmt)
		}
	}

	return queries, nil
}

func splitSQLStatements(content string) []string {
	var statements []string
	var currentStmt strings.Builder
	var inQuote bool
	var inDollarQuote bool
	var dollarQuoteTag string

	for i := 0; i < len(content); i++ {
		ch := content[i]
		currentStmt.WriteByte(ch)

		switch ch {
		case '\'':
			if !inDollarQuote {
				inQuote = !inQuote
			}
		case '$':
			if !inQuote && !inDollarQuote {
				if tag, ok := extractDollarQuoteTag(content[i:]); ok {
					inDollarQuote = true
					dollarQuoteTag = tag
					currentStmt.WriteString(tag[1:])
					i += len(tag) - 1
				}
			} else if inDollarQuote {
				if strings.HasPrefix(content[i:], dollarQuoteTag) {
					inDollarQuote = false
					currentStmt.WriteString(dollarQuoteTag[1:])
					i += len(dollarQuoteTag) - 1
				}
			}
		case ';':
			if !inQuote && !inDollarQuote {
				statements = append(statements, currentStmt.String())
				currentStmt.Reset()
			}
		}
	}

	// Add the last statement if there's any content
	if currentStmt.Len() > 0 {
		statements = append(statements, currentStmt.String())
	}

	return statements
}

func splitSQLStatements2(script string) []string {
	var statements []string
	var currentStatement strings.Builder
	var inQuote bool
	var quoteChar rune
	var inDollarQuote bool
	var dollarQuoteTag string

	for _, char := range script {
		currentStatement.WriteRune(char)

		switch {
		case inDollarQuote:
			if strings.HasSuffix(currentStatement.String(), dollarQuoteTag) {
				inDollarQuote = false
				dollarQuoteTag = ""
			}
		case inQuote:
			if char == quoteChar {
				inQuote = false
			}
		case char == '\'':
			inQuote = true
			quoteChar = '\''
		case char == '"':
			inQuote = true
			quoteChar = '"'
		case char == '$' && !inQuote:
			if dollarQuoteTag == "" {
				dollarQuoteTag = "$"
			} else {
				inDollarQuote = true
			}
		case char == ';' && !inQuote && !inDollarQuote:
			statements = append(statements, strings.TrimSpace(currentStatement.String()))
			currentStatement.Reset()
		}
	}

	// Add the last statement if there's any content
	if currentStatement.Len() > 0 {
		statements = append(statements, strings.TrimSpace(currentStatement.String()))
	}

	return statements
}

func extractDollarQuoteTag(s string) (string, bool) {
	if !strings.HasPrefix(s, "$$") {
		endIndex := strings.Index(s, "$")
		if endIndex > 0 {
			return s[:endIndex+1], true
		}
	}
	return "", false
}

// Old Load load sql file from path, and return SqlFile pointer
func load2(path string) (qs []string, err error) {
	ls, err := readFileByLine(path)
	if err != nil {
		return qs, err
	}

	var ncls []string
	for _, l := range ls {
		ncl := excludeComment(l)
		ncls = append(ncls, ncl)
	}

	l := strings.Join(ncls, "\n")
	qs = strings.Split(l, ";")
	qs = qs[:len(qs)-1]

	return qs, nil
}

func readFileByLine(path string) (ls []string, err error) {
	f, err := os.ReadFile(path)
	if err != nil {
		return ls, err
	}

	ls = strings.Split(string(f), "\n")
	return ls, nil
}

func excludeComment(line string) string {
	d := "\""
	s := "'"
	c := "--"

	var nc string
	ck := line
	mx := len(line) + 1

	for {
		if len(ck) == 0 {
			return nc
		}

		di := strings.Index(ck, d)
		si := strings.Index(ck, s)
		ci := strings.Index(ck, c)

		if di < 0 {
			di = mx
		}
		if si < 0 {
			si = mx
		}
		if ci < 0 {
			ci = mx
		}

		var ei int

		if di < si && di < ci {
			nc += ck[:di+1]
			ck = ck[di+1:]
			ei = strings.Index(ck, d)
		} else if si < di && si < ci {
			nc += ck[:si+1]
			ck = ck[si+1:]
			ei = strings.Index(ck, s)
		} else if ci < di && ci < si {
			return nc + ck[:ci]
		} else {
			return nc + ck
		}

		nc += ck[:ei+1]
		ck = ck[ei+1:]
	}
}

func saveTx(tx *sql.Tx, err *error) {
	if p := recover(); p != nil {
		tx.Rollback()
		panic(p)
	} else if *err != nil {
		tx.Rollback()
	} else {
		e := tx.Commit()
		err = &e
	}
}
