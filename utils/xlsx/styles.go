package xlsx

import (
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/donnyhardyanto/dxlib/errors"
)

// CellStyle is one entry of a Workbook's style table. The zero value is the
// default style.
type CellStyle struct {
	Font         Font
	FillRGB      string // solid fill, "E0EBF5"; empty for none
	Border       Border
	Horizontal   string // "", "left", "center", "right"
	Vertical     string // "", "top", "center", "bottom"
	Wrap         bool
	NumberFormat string // a format code such as `"Rp"#,##0`; empty for General
}

type Font struct {
	Name     string  // empty for Calibri
	Size     float64 // points; 0 for 11
	Bold     bool
	Italic   bool
	ColorRGB string // empty for black
}

// Border draws thin lines on the sides set, in ColorRGB (empty for black).
type Border struct {
	Left, Right, Top, Bottom bool
	ColorRGB                 string
}

const (
	defaultFont     = "Calibri"
	defaultFontSize = 11
	// MaxFontNameUnits is the longest font name Excel keeps, in UTF-16 code units.
	MaxFontNameUnits = 31
	// MaxNumberFormatUnits is the longest number format code Excel accepts.
	MaxNumberFormatUnits = 255
	// firstCustomFormat is the first numFmtId free for custom format codes.
	firstCustomFormat = 164
)

func (s CellStyle) check() error {
	for what, c := range map[string]string{"fill": s.FillRGB, "font": s.Font.ColorRGB, "border": s.Border.ColorRGB} {
		if c != "" {
			if err := checkRGB(c); err != nil {
				return errors.Wrapf(err, "xlsx: %s colour", what)
			}
		}
	}
	if s.Font.Name != "" {
		if err := checkPlain(s.Font.Name, MaxFontNameUnits); err != nil {
			return errors.Wrap(err, "xlsx: font name")
		}
	}
	if !(s.Font.Size == 0 || s.Font.Size >= 1 && s.Font.Size <= 409) {
		return errors.Errorf("xlsx: font size %v is outside 1..409", s.Font.Size)
	}
	switch s.Horizontal {
	case "", "left", "center", "right":
	default:
		return errors.Errorf("xlsx: horizontal alignment %q is not left, center or right", s.Horizontal)
	}
	switch s.Vertical {
	case "", "top", "center", "bottom":
	default:
		return errors.Errorf("xlsx: vertical alignment %q is not top, center or bottom", s.Vertical)
	}
	if s.NumberFormat != "" {
		if err := checkPlain(s.NumberFormat, MaxNumberFormatUnits); err != nil {
			return errors.Wrap(err, "xlsx: number format")
		}
	}
	return nil
}

// checkPlain refuses what an attribute value cannot carry as it is: invalid
// UTF-8, control characters and U+FFFE/U+FFFF, and a value longer than max
// UTF-16 units.
func checkPlain(s string, max int) error {
	if !utf8.ValidString(s) {
		return errors.Errorf("%q is not valid UTF-8", s)
	}
	units := 0
	for _, r := range s {
		if r < 0x20 || r == 0xFFFE || r == 0xFFFF {
			return errors.Errorf("%q holds control character U+%04X", s, r)
		}
		units += utf16.RuneLen(r)
	}
	if units > max {
		return errors.Errorf("%q is longer than %d characters", s, max)
	}
	return nil
}

// styleTable holds the styles in the order AddStyle gave them, each mapped to
// a cellXfs entry; fonts, fills, borders, number formats and the xfs
// themselves are stored once each.
type styleTable struct {
	styles  []CellStyle
	xfOf    []int // xf index of style i+1
	fonts   index
	fills   index
	borders index
	formats index
	xfs     index
}

// index keeps distinct XML fragments in the order they were first seen.
type index struct {
	list []string
	pos  map[string]int
}

func (x *index) put(s string) int {
	if i, ok := x.pos[s]; ok {
		return i
	}
	if x.pos == nil {
		x.pos = map[string]int{}
	}
	x.pos[s] = len(x.list)
	x.list = append(x.list, s)
	return len(x.list) - 1
}

func (t *styleTable) init() {
	if t.xfs.pos != nil {
		return
	}
	t.fonts.put(fontXML(Font{}))
	t.fills.put(`<fill><patternFill patternType="none"/></fill>`)
	t.fills.put(`<fill><patternFill patternType="gray125"/></fill>`)
	t.borders.put(borderXML(Border{}))
	t.xfs.put(`<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>`)
}

// add stores a checked style and returns its index, from 1.
func (t *styleTable) add(s CellStyle) int {
	t.init()
	font := t.fonts.put(fontXML(s.Font))
	fill := 0
	if s.FillRGB != "" {
		fill = t.fills.put(`<fill><patternFill patternType="solid"><fgColor rgb="FF` + strings.ToUpper(s.FillRGB) +
			`"/><bgColor indexed="64"/></patternFill></fill>`)
	}
	border := t.borders.put(borderXML(s.Border))
	format := 0
	if s.NumberFormat != "" && s.NumberFormat != "General" {
		format = firstCustomFormat + t.formats.put(s.NumberFormat)
	}

	xf := `<xf numFmtId="` + strconv.Itoa(format) + `" fontId="` + strconv.Itoa(font) + `" fillId="` + strconv.Itoa(fill) +
		`" borderId="` + strconv.Itoa(border) + `" xfId="0"`
	if format != 0 {
		xf += ` applyNumberFormat="1"`
	}
	if font != 0 {
		xf += ` applyFont="1"`
	}
	if fill != 0 {
		xf += ` applyFill="1"`
	}
	if border != 0 {
		xf += ` applyBorder="1"`
	}
	if s.Horizontal != "" || s.Vertical != "" || s.Wrap {
		xf += ` applyAlignment="1"><alignment`
		if s.Horizontal != "" {
			xf += ` horizontal="` + s.Horizontal + `"`
		}
		if s.Vertical != "" {
			xf += ` vertical="` + s.Vertical + `"`
		}
		if s.Wrap {
			xf += ` wrapText="1"`
		}
		xf += `/></xf>`
	} else {
		xf += `/>`
	}
	t.styles = append(t.styles, s)
	t.xfOf = append(t.xfOf, t.xfs.put(xf))
	return len(t.styles)
}

// xf gives the cellXfs index of a style index already checked.
func (t *styleTable) xf(style int) int {
	if style == 0 {
		return 0
	}
	return t.xfOf[style-1]
}

func fontXML(f Font) string {
	var b strings.Builder
	b.WriteString("<font>")
	if f.Bold {
		b.WriteString("<b/>")
	}
	if f.Italic {
		b.WriteString("<i/>")
	}
	size := f.Size
	if size == 0 {
		size = defaultFontSize
	}
	b.WriteString(`<sz val="` + strconv.FormatFloat(size, 'f', -1, 64) + `"/>`)
	color := "000000"
	if f.ColorRGB != "" {
		color = strings.ToUpper(f.ColorRGB)
	}
	b.WriteString(`<color rgb="FF` + color + `"/>`)
	name := f.Name
	if name == "" {
		name = defaultFont
	}
	b.WriteString(`<name val="` + escapeAttr(name) + `"/>`)
	if name == defaultFont {
		b.WriteString(`<family val="2"/>`)
	}
	b.WriteString("</font>")
	return b.String()
}

func borderXML(br Border) string {
	color := "000000"
	if br.ColorRGB != "" {
		color = strings.ToUpper(br.ColorRGB)
	}
	side := func(name string, on bool) string {
		if !on {
			return "<" + name + "/>"
		}
		return "<" + name + ` style="thin"><color rgb="FF` + color + `"/></` + name + ">"
	}
	return "<border>" + side("left", br.Left) + side("right", br.Right) + side("top", br.Top) + side("bottom", br.Bottom) +
		"<diagonal/></border>"
}

// xml gives styles.xml for the table.
func (t *styleTable) xml() string {
	t.init()
	var b strings.Builder
	b.WriteString(xmlDecl + `<styleSheet xmlns="` + nsMain + `">`)
	if len(t.formats.list) > 0 {
		b.WriteString(`<numFmts count="` + strconv.Itoa(len(t.formats.list)) + `">`)
		for i, code := range t.formats.list {
			b.WriteString(`<numFmt numFmtId="` + strconv.Itoa(firstCustomFormat+i) + `" formatCode="` + escapeAttr(code) + `"/>`)
		}
		b.WriteString(`</numFmts>`)
	}
	section := func(name string, x index) {
		b.WriteString(`<` + name + ` count="` + strconv.Itoa(len(x.list)) + `">` + strings.Join(x.list, "") + `</` + name + `>`)
	}
	section("fonts", t.fonts)
	section("fills", t.fills)
	section("borders", t.borders)
	b.WriteString(`<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>`)
	section("cellXfs", t.xfs)
	b.WriteString(`<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>`)
	b.WriteString(`</styleSheet>`)
	return b.String()
}
