package sqlparser

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

// Constants for parser configuration
const (
	DefaultBufferSize = 4096
	DefaultMaxSize    = 1 << 20 // 1MB
	DefaultMaxStmts   = 1000
	DefaultMaxTagLen  = 64
	DefaultMaxDepth   = 100
	MinBufferSize     = 512
)

// Error definitions
var (
	ErrInvalidSyntax   = errors.New("invalid syntax")
	ErrTooLong         = errors.New("input too long")
	ErrUnclosed        = errors.New("unclosed element")
	ErrInvalidTag      = errors.New("invalid dollar quote tag")
	ErrNestedTooDeep   = errors.New("nested comments too deep")
	ErrInvalidEncoding = errors.New("invalid UTF-8 encoding")
)

// Parser states
const (
	stateNormal = iota
	stateString
	stateIdent
	stateLineComment
	stateBlockComment
	stateDollar
)

// Buffer pool for reusing byte slices
var bufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, DefaultBufferSize)
		return &buf
	},
}

// Position tracks the current parsing position
type Position struct {
	Line   int
	Column int
	Offset int
}

// String returns a string representation of the position
func (p Position) String() string {
	return fmt.Sprintf("line %d, column %d", p.Line, p.Column)
}

// ParseError contains detailed error information
type ParseError struct {
	Msg string
	Pos Position
	Err error
}

func (e *ParseError) Error() string {
	if e.Pos.Line > 0 {
		return fmt.Sprintf("%s at %s", e.Msg, e.Pos)
	}
	return e.Msg
}

func (e *ParseError) Unwrap() error {
	return e.Err
}

// Options configures the SQL parser
type Options struct {
	MaxSize       int  // Maximum input size in bytes
	MaxStmts      int  // Maximum number of statements
	MaxTagLen     int  // Maximum dollar quote tag length
	MaxDepth      int  // Maximum nested comment depth
	StripComments bool // Remove comments from output
}

// Parser is the main SQL parser struct
type Parser struct {
	opts      Options
	tokenizer *Tokenizer
}

// Tokenizer handles the actual parsing
type Tokenizer struct {
	buf        []byte
	pos        Position
	width      int
	state      uint8
	depth      int
	dollarTags []string
	output     []byte
	opts       *Options
}

// Statement represents a parsed SQL statement
type Statement struct {
	Query    []byte
	Comments [][]byte
	Pos      Position
}

// NewParser creates a new SQL parser with default options
func NewParser() *Parser {
	return NewParserWithOptions(Options{
		MaxSize:   DefaultMaxSize,
		MaxStmts:  DefaultMaxStmts,
		MaxTagLen: DefaultMaxTagLen,
		MaxDepth:  DefaultMaxDepth,
	})
}

// NewParserWithOptions creates a new SQL parser with custom options
func NewParserWithOptions(opts Options) *Parser {
	if opts.MaxSize <= 0 {
		opts.MaxSize = DefaultMaxSize
	}
	if opts.MaxStmts <= 0 {
		opts.MaxStmts = DefaultMaxStmts
	}
	if opts.MaxTagLen <= 0 {
		opts.MaxTagLen = DefaultMaxTagLen
	}
	if opts.MaxDepth <= 0 {
		opts.MaxDepth = DefaultMaxDepth
	}

	return &Parser{
		opts: opts,
		tokenizer: &Tokenizer{
			dollarTags: make([]string, 0, 4),
			pos:        Position{Line: 1, Column: 1},
		},
	}
}

// ParseString parses SQL statements from a string
func (p *Parser) ParseString(input string) ([]Statement, error) {
	return p.Parse([]byte(input))
}

// Parse parses SQL statements from a byte slice
func (p *Parser) Parse(input []byte) ([]Statement, error) {
	if len(input) > p.opts.MaxSize {
		return nil, &ParseError{
			Msg: fmt.Sprintf("input exceeds maximum size of %d bytes", p.opts.MaxSize),
			Err: ErrTooLong,
		}
	}

	statements := make([]Statement, 0, 16)
	buf := *bufPool.Get().(*[]byte)
	defer func() {
		buf = buf[:0]
		bufPool.Put(&buf)
	}()

	p.tokenizer.reset(input, &p.opts)

	var currentStmt Statement
	for {
		token, err := p.tokenizer.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, &ParseError{
				Msg: err.Error(),
				Pos: p.tokenizer.pos,
				Err: err,
			}
		}

		// Handle statement termination
		if token[len(token)-1] == ';' && p.tokenizer.state == stateNormal {
			if len(buf) > 0 {
				currentStmt.Query = make([]byte, len(buf))
				copy(currentStmt.Query, buf)
				if len(statements) >= p.opts.MaxStmts {
					return nil, &ParseError{
						Msg: fmt.Sprintf("exceeded maximum of %d statements", p.opts.MaxStmts),
						Pos: p.tokenizer.pos,
						Err: ErrTooLong,
					}
				}
				statements = append(statements, currentStmt)
				currentStmt = Statement{Pos: p.tokenizer.pos}
				buf = buf[:0]
			}
		} else {
			// Handle comments
			if p.tokenizer.state == stateLineComment || p.tokenizer.state == stateBlockComment {
				if !p.opts.StripComments {
					comment := make([]byte, len(token))
					copy(comment, token)
					currentStmt.Comments = append(currentStmt.Comments, comment)
				}
			} else {
				buf = append(buf, token...)
			}
		}
	}

	// Handle final statement
	if len(buf) > 0 {
		if p.tokenizer.state != stateNormal {
			return nil, &ParseError{
				Msg: "unclosed " + p.tokenizer.unclosedElement(),
				Pos: p.tokenizer.pos,
				Err: ErrUnclosed,
			}
		}
		currentStmt.Query = make([]byte, len(buf))
		copy(currentStmt.Query, buf)
		statements = append(statements, currentStmt)
	}

	return statements, nil
}

// Reset prepares the tokenizer for reuse
func (t *Tokenizer) reset(input []byte, opts *Options) {
	t.buf = input
	t.pos = Position{Line: 1, Column: 1}
	t.width = 0
	t.state = stateNormal
	t.depth = 0
	t.dollarTags = t.dollarTags[:0]
	t.output = t.output[:0]
	t.opts = opts
}

// next returns the next token
func (t *Tokenizer) next() ([]byte, error) {
	for t.pos.Offset < len(t.buf) {
		switch t.state {
		case stateNormal:
			return t.handleNormal()
		case stateString:
			return t.handleString()
		case stateIdent:
			return t.handleIdentifier()
		case stateLineComment:
			return t.handleLineComment()
		case stateBlockComment:
			return t.handleBlockComment()
		case stateDollar:
			return t.handleDollarQuote()
		}
	}
	return nil, io.EOF
}

func (t *Tokenizer) handleNormal() ([]byte, error) {
	start := t.pos.Offset
	r, size := utf8.DecodeRune(t.buf[t.pos.Offset:])
	if r == utf8.RuneError {
		return nil, ErrInvalidEncoding
	}
	t.advance(size)

	switch {
	case r == '\'':
		t.state = stateString
		return t.buf[start:t.pos.Offset], nil

	case r == '"':
		t.state = stateIdent
		return t.buf[start:t.pos.Offset], nil

	case r == '-' && t.peekByte() == '-':
		t.state = stateLineComment
		t.advance(1)
		return t.buf[start:t.pos.Offset], nil

	case r == '/' && t.peekByte() == '*':
		t.state = stateBlockComment
		t.depth++
		if t.depth > t.opts.MaxDepth {
			return nil, ErrNestedTooDeep
		}
		t.advance(1)
		return t.buf[start:t.pos.Offset], nil

	case r == '$':
		if tag := t.scanDollarTag(); tag != nil {
			t.state = stateDollar
			t.dollarTags = append(t.dollarTags, string(tag))
			return t.buf[start:t.pos.Offset], nil
		}
	}

	// Handle normal SQL tokens
	if unicode.IsSpace(r) {
		return t.scanWhitespace(start), nil
	}

	if isWordStart(r) {
		return t.scanWord(start), nil
	}

	// Single character token
	return t.buf[start:t.pos.Offset], nil
}

// Rest of the implementation methods follow...
// (handleString, handleIdentifier, handleLineComment, handleBlockComment, handleDollarQuote)
// (scanWhitespace, scanWord, scanDollarTag, etc.)

// Helper methods for character classification
func isWordStart(r rune) bool {
	return unicode.IsLetter(r) || r == '_'
}

func isWordContinue(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}

func isValidTagChar(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}

// Position tracking helpers
func (t *Tokenizer) advance(n int) {
	for i := 0; i < n; i++ {
		if t.pos.Offset < len(t.buf) {
			if t.buf[t.pos.Offset] == '\n' {
				t.pos.Line++
				t.pos.Column = 1
			} else {
				t.pos.Column++
			}
			t.pos.Offset++
		}
	}
}

func (t *Tokenizer) peekByte() byte {
	if t.pos.Offset >= len(t.buf) {
		return 0
	}
	return t.buf[t.pos.Offset]
}

func (t *Tokenizer) unclosedElement() string {
	switch t.state {
	case stateString:
		return "string literal"
	case stateIdent:
		return "identifier"
	case stateBlockComment:
		return "block comment"
	case stateDollar:
		if len(t.dollarTags) > 0 {
			return fmt.Sprintf("dollar quote %s", t.dollarTags[len(t.dollarTags)-1])
		}
	}
	return "element"
}

// Example usage:
func Example() {
	input := `
        -- Create users table
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            /* Multi-line
               comment */
            created_at TIMESTAMP DEFAULT NOW()
        );

        -- Insert data
        INSERT INTO users (name) VALUES 
            ($tag$John's "nickname"$tag$),
            ($$Mary's /* comment */ name$$);
    `

	parser := NewParser()
	statements, err := parser.ParseString(input)
	if err != nil {
		panic(err)
	}

	for i, stmt := range statements {
		fmt.Printf("Statement %d:\n", i+1)
		fmt.Printf("Query: %s\n", string(stmt.Query))
		fmt.Printf("Position: %s\n", stmt.Pos)
		for _, comment := range stmt.Comments {
			fmt.Printf("Comment: %s\n", string(comment))
		}
		fmt.Println()
	}
}

// Benchmark helper if needed
func Benchmark(input []byte) {
	parser := NewParser()
	for i := 0; i < 1000; i++ {
		_, _ = parser.Parse(input)
	}
}

// String handling methods
func (t *Tokenizer) handleString() ([]byte, error) {
	start := t.pos.Offset
	for {
		if t.pos.Offset >= len(t.buf) {
			return nil, ErrUnclosed
		}

		ch := t.buf[t.pos.Offset]
		t.advance(1)

		if ch == '\'' {
			if t.peekByte() == '\'' {
				// Handle escaped quote
				t.advance(1)
				continue
			}
			t.state = stateNormal
			return t.buf[start:t.pos.Offset], nil
		}
	}
}

func (t *Tokenizer) handleIdentifier() ([]byte, error) {
	start := t.pos.Offset
	for {
		if t.pos.Offset >= len(t.buf) {
			return nil, ErrUnclosed
		}

		ch := t.buf[t.pos.Offset]
		t.advance(1)

		if ch == '"' {
			if t.peekByte() == '"' {
				// Handle escaped quote
				t.advance(1)
				continue
			}
			t.state = stateNormal
			return t.buf[start:t.pos.Offset], nil
		}
	}
}

// Comment handling methods
func (t *Tokenizer) handleLineComment() ([]byte, error) {
	start := t.pos.Offset
	for t.pos.Offset < len(t.buf) {
		ch := t.buf[t.pos.Offset]
		t.advance(1)

		if ch == '\n' {
			t.state = stateNormal
			return t.buf[start:t.pos.Offset], nil
		}
	}
	// Line comment can end with EOF
	t.state = stateNormal
	return t.buf[start:t.pos.Offset], nil
}

func (t *Tokenizer) handleBlockComment() ([]byte, error) {
	start := t.pos.Offset
	for t.pos.Offset < len(t.buf) {
		if t.pos.Offset+1 >= len(t.buf) {
			return nil, ErrUnclosed
		}

		ch := t.buf[t.pos.Offset]
		nextCh := t.buf[t.pos.Offset+1]

		if ch == '/' && nextCh == '*' {
			t.advance(2)
			t.depth++
			if t.depth > t.opts.MaxDepth {
				return nil, ErrNestedTooDeep
			}
			continue
		}

		if ch == '*' && nextCh == '/' {
			t.advance(2)
			t.depth--
			if t.depth == 0 {
				t.state = stateNormal
				return t.buf[start:t.pos.Offset], nil
			}
			continue
		}

		t.advance(1)
	}
	return nil, ErrUnclosed
}

func (t *Tokenizer) handleDollarQuote() ([]byte, error) {
	start := t.pos.Offset
	currentTag := t.dollarTags[len(t.dollarTags)-1]
	tagLen := len(currentTag)

	for t.pos.Offset < len(t.buf) {
		// Look for closing tag
		if t.buf[t.pos.Offset] == '$' &&
			t.pos.Offset+tagLen <= len(t.buf) &&
			bytes.Equal(t.buf[t.pos.Offset:t.pos.Offset+tagLen], []byte(currentTag)) {
			t.advance(tagLen)
			t.dollarTags = t.dollarTags[:len(t.dollarTags)-1]
			if len(t.dollarTags) == 0 {
				t.state = stateNormal
			}
			return t.buf[start:t.pos.Offset], nil
		}
		t.advance(1)
	}
	return nil, ErrUnclosed
}

// Scanner methods
func (t *Tokenizer) scanWhitespace(start int) []byte {
	for t.pos.Offset < len(t.buf) && unicode.IsSpace(rune(t.buf[t.pos.Offset])) {
		t.advance(1)
	}
	return t.buf[start:t.pos.Offset]
}

func (t *Tokenizer) scanWord(start int) []byte {
	// First character already validated
	t.advance(1)

	// Scan remaining characters
	for t.pos.Offset < len(t.buf) {
		r, size := utf8.DecodeRune(t.buf[t.pos.Offset:])
		if !isWordContinue(r) {
			break
		}
		t.advance(size)
	}

	return t.buf[start:t.pos.Offset]
}

func (t *Tokenizer) scanDollarTag() []byte {
	if t.pos.Offset >= len(t.buf) {
		return nil
	}

	// Handle $$ case
	if t.buf[t.pos.Offset] == '$' {
		t.advance(1)
		return []byte("$$")
	}

	// Scan custom tag
	start := t.pos.Offset - 1 // Include the first $
	count := 0

	for t.pos.Offset < len(t.buf) && count < t.opts.MaxTagLen {
		ch := t.buf[t.pos.Offset]
		if ch == '$' {
			t.advance(1)
			tag := t.buf[start:t.pos.Offset]

			// Validate tag
			if isValidTag(tag) {
				return tag
			}
			return nil
		}

		if !isValidTagChar(rune(ch)) {
			return nil
		}

		t.advance(1)
		count++
	}

	return nil
}

// Tag validation helpers
func isValidTag(tag []byte) bool {
	if len(tag) < 2 || tag[0] != '$' || tag[len(tag)-1] != '$' {
		return false
	}

	// Empty tag ($$) is valid
	if len(tag) == 2 {
		return true
	}

	// Check middle characters
	for i := 1; i < len(tag)-1; i++ {
		ch := tag[i]
		if !isValidTagByte(ch) {
			return false
		}
	}

	return true
}

func isValidTagByte(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9') ||
		ch == '_'
}

// Utility methods for better error reporting
func (t *Tokenizer) currentLine() string {
	start := t.pos.Offset
	for start > 0 && t.buf[start-1] != '\n' {
		start--
	}

	end := t.pos.Offset
	for end < len(t.buf) && t.buf[end] != '\n' {
		end++
	}

	return string(t.buf[start:end])
}

func (t *Tokenizer) errorContext() string {
	line := t.currentLine()
	pointer := strings.Repeat(" ", t.pos.Column-1) + "^"
	return fmt.Sprintf("%s\n%s", line, pointer)
}

// Debug helper method
func (t *Tokenizer) dumpState() string {
	return fmt.Sprintf(
		"State: %d, Pos: %d, Line: %d, Col: %d, Depth: %d, Tags: %v",
		t.state,
		t.pos.Offset,
		t.pos.Line,
		t.pos.Column,
		t.depth,
		t.dollarTags,
	)
}
