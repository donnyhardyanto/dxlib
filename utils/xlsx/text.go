package xlsx

import (
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/donnyhardyanto/dxlib/errors"
)

const (
	// MaxRows is Excel's row count per sheet, the header row included.
	MaxRows = 1048576
	// MaxColumns is Excel's column count per sheet (column XFD).
	MaxColumns = 16384
	// MaxCellUnits is the most UTF-16 code units Excel keeps in one cell.
	MaxCellUnits = 32767
	// MaxSheetNameUnits is the longest sheet name Excel accepts, in UTF-16 code units.
	MaxSheetNameUnits = 31
)

// columnName turns a 1-based column number into its letters: 1 is A, 27 is AA.
func columnName(n int) (string, error) {
	if n < 1 || n > MaxColumns {
		return "", errors.Errorf("xlsx: column %d is outside 1..%d", n, MaxColumns)
	}
	var b [3]byte
	i := len(b)
	for n > 0 {
		n--
		i--
		b[i] = byte('A' + n%26)
		n /= 26
	}
	return string(b[i:]), nil
}

// forbiddenInXML reports a character XML 1.0 does not allow in a document.
// Tab, LF and CR are allowed.
func forbiddenInXML(r rune) bool {
	switch {
	case r == '\t' || r == '\n' || r == '\r':
		return false
	case r < 0x20:
		return true
	case r == 0xFFFE || r == 0xFFFF:
		return true
	}
	return false
}

func isHex(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F'
}

// hasEscapeSequence reports whether s holds _xHHHH_, which Excel decodes.
func hasEscapeSequence(s string) bool {
	for i := 0; i+7 <= len(s); i++ {
		if s[i] == '_' && s[i+1] == 'x' && isHex(s[i+2]) && isHex(s[i+3]) && isHex(s[i+4]) && isHex(s[i+5]) && s[i+6] == '_' {
			return true
		}
	}
	return false
}

// cleanText applies the lossy steps of the cell encoding (DESIGN.md §5.2, steps 1
// and 2): invalid UTF-8 becomes U+FFFD byte by byte, and the value is cut to
// MaxCellUnits UTF-16 units, never between the halves of a surrogate pair. What
// it returns is exactly what a reader shows for the cell.
func cleanText(s string) string {
	units := 0
	var b strings.Builder
	valid := utf8.ValidString(s)
	if valid && len(s) <= MaxCellUnits {
		// A rune never takes more UTF-16 units than UTF-8 bytes, so a string of
		// this many bytes cannot pass the limit.
		return s
	}
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		n := utf16.RuneLen(r)
		if n < 0 {
			n = 1
		}
		if units+n > MaxCellUnits {
			break
		}
		units += n
		b.WriteRune(r)
		i += size
	}
	return b.String()
}

// encodeCellText turns a cell value into the content of a <t> element, and says
// whether that element needs xml:space="preserve". It follows DESIGN.md §5.2.
func encodeCellText(s string) (string, bool) {
	s = cleanText(s)
	preserve := false
	if s != "" {
		first, last := s[0], s[len(s)-1]
		preserve = first == ' ' || first == '\t' || first == '\n' || first == '\r' ||
			last == ' ' || last == '\t' || last == '\n' || last == '\r'
	}

	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == '_' && startsEscape(s[i:]):
			// Excel would decode the text after this underscore, so the underscore
			// itself is written as _x005F_ and the text shows as typed.
			b.WriteString("_x005F_")
		case forbiddenInXML(r):
			b.WriteString("_x")
			h := strconv.FormatInt(int64(r), 16)
			b.WriteString(strings.Repeat("0", 4-len(h)))
			b.WriteString(strings.ToUpper(h))
			b.WriteByte('_')
		case r == '&':
			b.WriteString("&amp;")
		case r == '<':
			b.WriteString("&lt;")
		case r == '>':
			b.WriteString("&gt;")
		case r == '\r':
			b.WriteString("&#xD;")
		default:
			b.WriteString(s[i : i+size])
		}
		i += size
	}
	return b.String(), preserve
}

// startsEscape reports whether s, which starts with '_', would be read by Excel
// as the start of _xHHHH_ once encoded: the closing underscore is either in the
// text itself or is the first byte of the _xHHHH_ a forbidden character
// right after the four digits turns into.
func startsEscape(s string) bool {
	if len(s) < 7 || s[1] != 'x' || !isHex(s[2]) || !isHex(s[3]) || !isHex(s[4]) || !isHex(s[5]) {
		return false
	}
	if s[6] == '_' {
		return true
	}
	r, _ := utf8.DecodeRuneInString(s[6:])
	return forbiddenInXML(r)
}

// checkSheetName applies Excel's rules for a sheet name, plus what an XML
// attribute cannot carry (DESIGN.md §5.1).
func checkSheetName(name string) error {
	if !utf8.ValidString(name) {
		return errors.New("xlsx: sheet name is not valid UTF-8")
	}
	units := 0
	for _, r := range name {
		units += utf16.RuneLen(r)
		switch {
		case r < 0x20 || r == 0xFFFE || r == 0xFFFF:
			return errors.Errorf("xlsx: sheet name %q holds control character U+%04X", name, r)
		case strings.ContainsRune(`[]:*?/\`, r):
			return errors.Errorf("xlsx: sheet name %q holds %q, which Excel does not allow", name, r)
		}
	}
	switch {
	case units == 0:
		return errors.New("xlsx: sheet name is empty")
	case units > MaxSheetNameUnits:
		return errors.Errorf("xlsx: sheet name %q is longer than %d characters", name, MaxSheetNameUnits)
	case name[0] == '\'' || name[len(name)-1] == '\'':
		return errors.Errorf("xlsx: sheet name %q starts or ends with an apostrophe", name)
	case strings.EqualFold(name, "History"):
		return errors.New("xlsx: History is reserved by Excel as a sheet name")
	case hasEscapeSequence(name):
		return errors.Errorf("xlsx: sheet name %q holds an _xHHHH_ sequence", name)
	}
	return nil
}

var attrEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", "'", "&apos;")

// escapeAttr escapes a value checked by checkSheetName for an XML attribute.
func escapeAttr(s string) string {
	return attrEscaper.Replace(s)
}

func checkRGB(s string) error {
	if len(s) != 6 {
		return errors.Errorf("xlsx: fill colour %q is not six hex digits", s)
	}
	for i := 0; i < 6; i++ {
		if !isHex(s[i]) {
			return errors.Errorf("xlsx: fill colour %q is not six hex digits", s)
		}
	}
	return nil
}
