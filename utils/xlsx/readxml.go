package xlsx

import (
	"bytes"
	"encoding/xml"
	"io"
	"strconv"
	"strings"

	"github.com/donnyhardyanto/dxlib/errors"
)

// xmlScanner reads one part token by token. It refuses a DTD (so there is no
// entity to expand: encoding/xml knows only the five predefined ones and
// character references) and holds the part to the depth and token limits.
type xmlScanner struct {
	d      *xml.Decoder
	part   string
	lim    *ReaderLimits
	depth  int
	tokens int64
}

func newXMLScanner(r io.Reader, part string, lim *ReaderLimits) *xmlScanner {
	d := xml.NewDecoder(r)
	d.Strict = true
	// No CharsetReader: a part declaring anything but UTF-8 is an error.
	return &xmlScanner{d: d, part: part, lim: lim}
}

// next returns the next token that matters: a start or end element, or text.
// Comments and processing instructions are skipped; a directive is an error.
func (s *xmlScanner) next() (xml.Token, error) {
	for {
		t, err := s.d.Token()
		if err != nil {
			if err == io.EOF {
				if s.depth != 0 {
					return nil, errors.Errorf("xlsx: %s: unexpected end of XML inside an element", s.part)
				}
				return nil, io.EOF
			}
			return nil, s.wrap(err)
		}
		s.tokens++
		if s.tokens > s.lim.MaxXMLTokens {
			return nil, limitError("XML tokens in a part", s.lim.MaxXMLTokens, s.tokens, s.part)
		}
		switch t := t.(type) {
		case xml.StartElement:
			s.depth++
			if s.depth > s.lim.MaxXMLDepth {
				return nil, limitError("XML element depth", int64(s.lim.MaxXMLDepth), int64(s.depth), s.part)
			}
			return t, nil
		case xml.EndElement:
			s.depth--
			if s.depth < 0 {
				return nil, errors.Errorf("xlsx: %s: end element </%s> without a start", s.part, t.Name.Local)
			}
			return t, nil
		case xml.CharData:
			return t, nil
		case xml.Directive:
			return nil, errors.Errorf("xlsx: %s: XML holds a directive (<!%s>); DTDs are refused", s.part, clip(string(t), 40))
		}
	}
}

// wrap names the part and offset of a syntax error. An error from reading
// the part (a limit, a broken deflate stream) comes back as it is.
func (s *xmlScanner) wrap(err error) error {
	var le *LimitError
	var pe *partReadError
	if errors.As(err, &le) || errors.As(err, &pe) {
		return err
	}
	return errors.Errorf("xlsx: %s: malformed XML at byte %d: %v", s.part, s.d.InputOffset(), err)
}

// skip consumes the rest of the element whose start was just returned.
func (s *xmlScanner) skip() error {
	for depth := 1; depth > 0; {
		t, err := s.next()
		if err != nil {
			return s.eof(err)
		}
		switch t.(type) {
		case xml.StartElement:
			depth++
		case xml.EndElement:
			depth--
		}
	}
	return nil
}

// text gathers the character data of the element whose start was just
// returned, child elements' text included, up to its end. maxBytes bounds it.
func (s *xmlScanner) text(maxBytes int64, what string) (string, error) {
	var b bytes.Buffer
	for depth := 1; depth > 0; {
		t, err := s.next()
		if err != nil {
			return "", s.eof(err)
		}
		switch t := t.(type) {
		case xml.StartElement:
			depth++
		case xml.EndElement:
			depth--
		case xml.CharData:
			b.Write(t)
			if int64(b.Len()) > maxBytes {
				return "", limitError(what, maxBytes, int64(b.Len()), s.part)
			}
		}
	}
	return b.String(), nil
}

func (s *xmlScanner) eof(err error) error {
	if err == io.EOF {
		return errors.Errorf("xlsx: %s: unexpected end of XML", s.part)
	}
	return err
}

func attr(e xml.StartElement, local string) (string, bool) {
	for _, a := range e.Attr {
		if a.Name.Local == local && a.Name.Space != "xmlns" && !(a.Name.Space == "" && a.Name.Local == "xmlns") {
			return a.Value, true
		}
	}
	return "", false
}

// attrRelID is r:id: an attribute id in a relationships namespace, the
// transitional or the strict one, whatever its prefix.
func attrRelID(e xml.StartElement) string {
	for _, a := range e.Attr {
		if a.Name.Local == "id" && strings.HasSuffix(a.Name.Space, "relationships") {
			return a.Value
		}
	}
	return ""
}

// attrInt is strconv.Atoi on an attribute; absent or malformed gives ok false.
func attrInt(e xml.StartElement, local string) (int, bool) {
	v, ok := attr(e, local)
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, false
	}
	return n, true
}

// clip shortens s for an error message.
func clip(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n]) + "…"
	}
	return s
}
