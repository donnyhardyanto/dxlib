package sqlparser

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"strings"
)

// Constants for parser states
const (
	stateNormal = iota
	stateString
	stateIdent
	stateLineComment
	stateBlockComment
	stateDollarQuote
	stateFunctionBody
)

// Position tracks location in input
type Position struct {
	Offset int
	Line   int
	Column int
}

// Statement represents a SQL statement
type Statement struct {
	Query    []byte
	Position Position
	Comments [][]byte
}

// SQLFile represents a SQL file handler
type SQLFile struct {
	files   []string
	queries []string
	parser  *Parser
}

// Parser holds parsing state
type Parser struct {
	input         []byte
	pos           Position
	state         int
	statements    []*Statement
	currentStmt   *Statement
	buffer        bytes.Buffer
	dollarTag     []byte
	inFunction    bool
	functionDepth int
	maxSize       int
}

// NewSQLFile creates a new SQL file handler
func NewSQLFile() *SQLFile {
	return &SQLFile{
		files:   make([]string, 0),
		queries: make([]string, 0),
		parser:  NewParser(),
	}
}

// NewParser creates a new SQL parser
func NewParser() *Parser {
	return &Parser{
		maxSize:    1 << 20, // 1MB
		statements: make([]*Statement, 0),
		pos:        Position{Line: 1, Column: 1},
	}
}

// File loads and parses a single SQL file
func (s *SQLFile) File(filename string) error {
	content, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", filename, err)
	}

	statements, err := s.parser.Parse(content)
	if err != nil {
		return fmt.Errorf("failed to parse SQL from file %s: %w", filename, err)
	}

	s.files = append(s.files, filename)

	for _, stmt := range statements {
		if query := strings.TrimSpace(string(stmt.Query)); query != "" {
			s.queries = append(s.queries, query)
		}
	}

	return nil
}

// Execute executes all loaded queries
func (s *SQLFile) Execute(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("nil database connection")
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	for _, query := range s.queries {
		if _, err := tx.Exec(query); err != nil {
			return fmt.Errorf("failed to execute query: %w\nQuery: %s", err, query)
		}
	}

	return tx.Commit()
}

// Parse parses SQL from a byte slice
func (p *Parser) Parse(input []byte) ([]*Statement, error) {
	if input == nil {
		return nil, fmt.Errorf("nil input")
	}

	if len(input) > p.maxSize {
		return nil, fmt.Errorf("input exceeds maximum size of %d bytes", p.maxSize)
	}

	p.reset(input)

	for !p.isEOF() {
		if err := p.parseStatement(); err != nil {
			return nil, fmt.Errorf("parse error at %v: %w", p.pos, err)
		}
	}

	if p.buffer.Len() > 0 {
		p.finalizeStatement()
	}

	return p.statements, nil
}

// ParseString parses SQL from a string
func (p *Parser) ParseString(input string) ([]*Statement, error) {
	return p.Parse([]byte(input))
}

func (p *Parser) reset(input []byte) {
	p.input = input
	p.pos = Position{Line: 1, Column: 1}
	p.state = stateNormal
	p.statements = make([]*Statement, 0)
	p.currentStmt = nil
	p.buffer.Reset()
	p.dollarTag = nil
	p.inFunction = false
	p.functionDepth = 0
}

func (p *Parser) parseStatement() error {
	if p.currentStmt == nil {
		p.currentStmt = &Statement{
			Position: p.pos,
			Comments: make([][]byte, 0),
		}
	}

	for !p.isEOF() {
		if err := p.parseToken(); err != nil {
			return err
		}

		if p.canRead(1) && p.input[p.pos.Offset] == ';' && !p.inFunction {
			p.buffer.WriteByte(';')
			p.advance(1)
			p.finalizeStatement()
			return nil
		}
	}

	return nil
}

func (p *Parser) parseToken() error {
	if p.isEOF() {
		return nil
	}

	ch := p.input[p.pos.Offset]

	switch p.state {
	case stateNormal:
		return p.parseNormal(ch)
	case stateString:
		return p.parseString()
	case stateIdent:
		return p.parseIdentifier()
	case stateLineComment:
		return p.parseLineComment()
	case stateBlockComment:
		return p.parseBlockComment()
	case stateDollarQuote:
		return p.parseDollarQuote()
	case stateFunctionBody:
		return p.parseFunctionBody()
	}

	return nil
}

func (p *Parser) parseNormal(ch byte) error {
	if p.matchKeyword("CREATE") || p.matchKeyword("CREATE OR REPLACE FUNCTION") {
		p.inFunction = true
	}

	switch ch {
	case '\'':
		if p.inFunction && !p.isInString() {
			p.state = stateFunctionBody
			p.buffer.WriteByte(ch)
			p.advance(1)
			return nil
		}
		p.state = stateString
		p.buffer.WriteByte(ch)
		p.advance(1)
		return nil

	case '"':
		p.state = stateIdent
		p.buffer.WriteByte(ch)
		p.advance(1)
		return nil

	case '-':
		if p.canRead(2) && p.input[p.pos.Offset+1] == '-' {
			p.state = stateLineComment
			p.advance(2)
			return nil
		}

	case '/':
		if p.canRead(2) && p.input[p.pos.Offset+1] == '*' {
			p.state = stateBlockComment
			p.advance(2)
			return nil
		}

	case '$':
		if tag := p.scanDollarTag(); tag != nil {
			p.dollarTag = tag
			p.state = stateDollarQuote
			p.buffer.Write(tag)
			return nil
		}
	}

	p.buffer.WriteByte(ch)
	p.advance(1)
	return nil
}

func (p *Parser) parseFunctionBody() error {
	ch := p.input[p.pos.Offset]

	if p.matchKeyword("BEGIN") {
		p.functionDepth++
	} else if p.matchKeyword("END") {
		p.functionDepth--
	}

	if ch == '\'' && p.functionDepth <= 0 {
		if p.canRead(2) && p.input[p.pos.Offset+1] == '\'' {
			p.buffer.WriteByte('\'')
			p.buffer.WriteByte('\'')
			p.advance(2)
			return nil
		}

		p.buffer.WriteByte('\'')
		p.advance(1)
		p.state = stateNormal
		p.inFunction = false
		return nil
	}

	if ch == '\'' && p.canRead(2) && p.input[p.pos.Offset+1] == '\'' {
		p.buffer.WriteByte('\'')
		p.buffer.WriteByte('\'')
		p.advance(2)
		return nil
	}

	p.buffer.WriteByte(ch)
	p.advance(1)
	return nil
}

func (p *Parser) parseString() error {
	ch := p.input[p.pos.Offset]
	p.buffer.WriteByte(ch)
	p.advance(1)

	if ch == '\'' {
		if p.canRead(1) && p.input[p.pos.Offset] == '\'' {
			p.buffer.WriteByte('\'')
			p.advance(1)
		} else {
			p.state = stateNormal
		}
	}

	return nil
}

func (p *Parser) parseIdentifier() error {
	ch := p.input[p.pos.Offset]
	p.buffer.WriteByte(ch)
	p.advance(1)

	if ch == '"' {
		if p.canRead(1) && p.input[p.pos.Offset] == '"' {
			p.buffer.WriteByte('"')
			p.advance(1)
		} else {
			p.state = stateNormal
		}
	}

	return nil
}

func (p *Parser) parseLineComment() error {
	for !p.isEOF() {
		ch := p.input[p.pos.Offset]
		if ch == '\n' {
			p.state = stateNormal
			p.advance(1)
			return nil
		}
		p.advance(1)
	}
	p.state = stateNormal
	return nil
}

func (p *Parser) parseBlockComment() error {
	for !p.isEOF() {
		if p.canRead(2) && p.input[p.pos.Offset] == '*' && p.input[p.pos.Offset+1] == '/' {
			p.state = stateNormal
			p.advance(2)
			return nil
		}
		p.advance(1)
	}
	return fmt.Errorf("unclosed block comment")
}

func (p *Parser) parseDollarQuote() error {
	if !p.canRead(len(p.dollarTag)) {
		return fmt.Errorf("unclosed dollar quote")
	}

	if bytes.Equal(p.input[p.pos.Offset:p.pos.Offset+len(p.dollarTag)], p.dollarTag) {
		p.buffer.Write(p.dollarTag)
		p.advance(len(p.dollarTag))
		p.state = stateNormal
		p.dollarTag = nil
		return nil
	}

	p.buffer.WriteByte(p.input[p.pos.Offset])
	p.advance(1)
	return nil
}

func (p *Parser) scanDollarTag() []byte {
	if !p.canRead(2) {
		return nil
	}

	if p.input[p.pos.Offset+1] == '$' {
		p.advance(2)
		return []byte("$$")
	}

	start := p.pos.Offset
	end := start + 1
	for end < len(p.input) {
		if p.input[end] == '$' {
			tag := p.input[start : end+1]
			if p.isValidTag(tag) {
				p.advance(len(tag))
				return tag
			}
			return nil
		}
		end++
	}

	return nil
}

func (p *Parser) isValidTag(tag []byte) bool {
	if len(tag) < 2 || tag[0] != '$' || tag[len(tag)-1] != '$' {
		return false
	}

	if len(tag) == 2 {
		return true
	}

	for i := 1; i < len(tag)-1; i++ {
		ch := tag[i]
		if !((ch >= 'a' && ch <= 'z') ||
			(ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') ||
			ch == '_') {
			return false
		}
	}

	return true
}

func (p *Parser) matchKeyword(keyword string) bool {
	if !p.canRead(len(keyword)) {
		return false
	}

	for i := 0; i < len(keyword); i++ {
		ch1 := p.input[p.pos.Offset+i]
		ch2 := keyword[i]

		if ch1 >= 'A' && ch1 <= 'Z' {
			ch1 += 'a' - 'A'
		}
		if ch2 >= 'A' && ch2 <= 'Z' {
			ch2 += 'a' - 'A'
		}

		if ch1 != ch2 {
			return false
		}
	}

	if p.canRead(len(keyword) + 1) {
		ch := p.input[p.pos.Offset+len(keyword)]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' {
			return false
		}
	}

	return true
}

func (p *Parser) isInString() bool {
	return p.state == stateString
}

func (p *Parser) isEOF() bool {
	return p.pos.Offset >= len(p.input)
}

func (p *Parser) canRead(n int) bool {
	return p.pos.Offset+n <= len(p.input)
}

func (p *Parser) advance(n int) {
	for i := 0; i < n && p.pos.Offset < len(p.input); i++ {
		if p.input[p.pos.Offset] == '\n' {
			p.pos.Line++
			p.pos.Column = 1
		} else {
			p.pos.Column++
		}
		p.pos.Offset++
	}
}

func (p *Parser) finalizeStatement() {
	if p.buffer.Len() > 0 {
		query := make([]byte, p.buffer.Len())
		copy(query, p.buffer.Bytes())
		p.currentStmt.Query = query
		p.statements = append(p.statements, p.currentStmt)
		p.currentStmt = nil
		p.buffer.Reset()
	}
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
