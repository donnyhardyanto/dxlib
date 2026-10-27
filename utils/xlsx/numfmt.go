package xlsx

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// The reader renders a value with its cell's number format the way
// excelize's GetRows does (v2.11.0, the version the golden files record), for
// the formats DESIGN.md §11.4 lists: General, digits, grouping, percent,
// scientific, literals, currency, colours, alignment, sections, text, and
// dates and times with English or Indonesian names. Anything else (fractions,
// conditions, other calendars and languages, switch arguments) gives the
// value as stored, which is also what excelize gives for a format it does not
// support. The arithmetic follows excelize's so the text matches it, quirks
// included; NOTICE credits it.

type valueKind int

const (
	kindNumber valueKind = iota
	kindDate
	kindText
)

type tokKind int

const (
	tLiteral tokKind = iota
	tGeneral
	tZero      // 0
	tHash      // #
	tDecimal   // .
	tThousands // , after a digit placeholder
	tPercent   // %
	tExponent  // E+
	tText      // @
	tDateTime  // y m d h s, AM/PM
	tElapsed   // [h] [m] [s]
	tAlignment // _x
	tRepeat    // *x
	tColor     // [Red]
	tCurrency  // [$...-lcid]
)

type nfToken struct {
	kind tokKind
	val  string
	// for tCurrency
	currency string
	lang     string
	hasLang  bool
}

type nfSection struct {
	text        bool // holds @: used for text, never for numbers
	unsupported bool // a value this section would render is given as stored
	items       []nfToken
}

// numFmt is a parsed format code. unsupported means the value is given as
// stored.
type numFmt struct {
	sections    []nfSection
	unsupported bool
}

// maxNumFmtRunes bounds a format code; Excel's own limit is 255 characters.
const maxNumFmtRunes = 1024

// builtinCodes are the built-in formats of ECMA-376 Part 1 §18.8.30 that
// excelize knows without a culture set; the other ids give the value as
// stored, as they do in excelize.
var builtinCodes = map[int]string{
	0: "General", 1: "0", 2: "0.00", 3: "#,##0", 4: "#,##0.00",
	9: "0%", 10: "0.00%", 11: "0.00E+00", 12: "# ?/?", 13: "# ??/??",
	14: "mm-dd-yy", 15: "d-mmm-yy", 16: "d-mmm", 17: "mmm-yy",
	18: "h:mm AM/PM", 19: "h:mm:ss AM/PM", 20: "hh:mm", 21: "hh:mm:ss",
	22: "m/d/yy hh:mm",
	37: "#,##0 ;(#,##0)", 38: "#,##0 ;[red](#,##0)",
	39: "#,##0.00 ;(#,##0.00)", 40: "#,##0.00 ;[red](#,##0.00)",
	41: `_(* #,##0_);_(* \(#,##0\);_(* "-"_);_(@_)`,
	42: `_("$"* #,##0_);_("$"* \(#,##0\);_("$"* "-"_);_(@_)`,
	43: `_(* #,##0.00_);_(* \(#,##0.00\);_(* "-"??_);_(@_)`,
	44: `_("$"* #,##0.00_);_("$"* \(#,##0.00\);_("$"* "-"??_);_(@_)`,
	45: "mm:ss", 46: "[h]:mm:ss", 47: "mm:ss.0", 48: "##0.0E+0", 49: "@",
}

var builtinParsed = func() map[int]*numFmt {
	m := make(map[int]*numFmt, len(builtinCodes))
	for id, code := range builtinCodes {
		m[id] = parseNumFmt(code)
	}
	return m
}()

func builtinNumFmt(id int) *numFmt {
	return builtinParsed[id] // nil for an unknown id: the value as stored
}

var colorNames = []string{"black", "blue", "cyan", "green", "magenta", "red", "white", "yellow"}

// parseNumFmt splits a format code into sections of tokens.
func parseNumFmt(code string) *numFmt {
	code = strings.TrimSpace(code)
	if utf8.RuneCountInString(code) > maxNumFmtRunes || !utf8.ValidString(code) {
		return &numFmt{unsupported: true}
	}
	f := &numFmt{sections: []nfSection{{}}}
	sec := &f.sections[0]
	add := func(k tokKind, v string) {
		// A run of the same placeholder or date letter is one token.
		if n := len(sec.items); n > 0 {
			last := &sec.items[n-1]
			switch {
			case k == tZero && last.kind == tZero, k == tHash && last.kind == tHash, k == tPercent && last.kind == tPercent:
				last.val += v
				return
			case k == tDateTime && last.kind == tDateTime && last.val[0] == v[0] && !isAmPm(last.val):
				last.val += v
				return
			}
		}
		sec.items = append(sec.items, nfToken{kind: k, val: v})
	}
	r := []rune(code)
	for i := 0; i < len(r); i++ {
		c := r[i]
		switch {
		case c == '"':
			j := i + 1
			for j < len(r) && r[j] != '"' {
				j++
			}
			sec.items = append(sec.items, nfToken{kind: tLiteral, val: string(r[i+1 : j])})
			i = j
		case c == '\\':
			if i+1 < len(r) {
				sec.items = append(sec.items, nfToken{kind: tLiteral, val: string(r[i+1])})
				i++
			}
		case c == '_':
			sec.items = append(sec.items, nfToken{kind: tAlignment, val: " "})
			i++
		case c == '*':
			sec.items = append(sec.items, nfToken{kind: tRepeat})
			i++
		case c == ';':
			if len(f.sections) == 4 {
				return &numFmt{unsupported: true}
			}
			f.sections = append(f.sections, nfSection{})
			sec = &f.sections[len(f.sections)-1]
		case c == '[':
			j := i + 1
			for j < len(r) && r[j] != ']' {
				j++
			}
			if j == len(r) {
				return &numFmt{unsupported: true}
			}
			tok, ok := bracketToken(string(r[i+1 : j]))
			if !ok {
				// A condition changes which section applies; a switch
				// argument changes the digits.
				return &numFmt{unsupported: true}
			}
			sec.items = append(sec.items, tok)
			i = j
		case c == '0':
			add(tZero, "0")
		case c == '#':
			add(tHash, "#")
		case c == '?':
			sec.unsupported = true // fractions and digit-or-space placeholders
		case c == '.':
			sec.items = append(sec.items, nfToken{kind: tDecimal, val: "."})
		case c == ',':
			if n := len(sec.items); n > 0 && (sec.items[n-1].kind == tZero || sec.items[n-1].kind == tHash || sec.items[n-1].kind == tThousands) {
				sec.items = append(sec.items, nfToken{kind: tThousands, val: ","})
			} else {
				sec.items = append(sec.items, nfToken{kind: tLiteral, val: ","})
			}
		case c == '%':
			add(tPercent, "%")
		case c == '@':
			sec.text = true
			sec.items = append(sec.items, nfToken{kind: tText, val: "@"})
		case (c == 'E' || c == 'e') && i+1 < len(r) && r[i+1] == '+':
			sec.items = append(sec.items, nfToken{kind: tExponent, val: "E+"})
			i++
		case hasPrefixFold(r[i:], "General"):
			sec.items = append(sec.items, nfToken{kind: tGeneral, val: string(r[i : i+7])})
			i += 6
		case hasPrefixFold(r[i:], "AM/PM"):
			sec.items = append(sec.items, nfToken{kind: tDateTime, val: string(r[i : i+5])})
			i += 4
		case hasPrefixFold(r[i:], "A/P"):
			sec.items = append(sec.items, nfToken{kind: tDateTime, val: string(r[i : i+3])})
			i += 2
		case strings.ContainsRune("yYmMdDhHsS", c):
			add(tDateTime, string(c))
		case strings.ContainsRune("eE", c):
			add(tDateTime, string(c)) // the year, outside the Japanese and Taiwanese calendars
		case strings.ContainsRune("aAbBgGrR", c):
			sec.unsupported = true // weekday by 'a', other calendars and eras
		case '1' <= c && c <= '9':
			sec.unsupported = true // digits other than 0 only make sense in fractions
		default:
			sec.items = append(sec.items, nfToken{kind: tLiteral, val: string(c)})
		}
	}
	return f
}

func hasPrefixFold(r []rune, p string) bool {
	return len(r) >= len(p) && strings.EqualFold(string(r[:len(p)]), p)
}

func isAmPm(v string) bool {
	return strings.EqualFold(v, "AM/PM") || strings.EqualFold(v, "A/P")
}

// bracketToken reads what is inside [ ]: a colour, an elapsed time, or a
// currency and language. Anything else (a condition, a switch argument) is
// not supported.
func bracketToken(in string) (nfToken, bool) {
	for _, c := range colorNames {
		if strings.EqualFold(in, c) {
			return nfToken{kind: tColor, val: in}, true
		}
	}
	if in != "" && strings.Trim(strings.ToLower(in), string(in[0]|0x20)) == "" && strings.ContainsRune("hms", rune(in[0]|0x20)) {
		return nfToken{kind: tElapsed, val: in}, true
	}
	if strings.HasPrefix(in, "$") {
		t := nfToken{kind: tCurrency, val: in}
		body := in[1:]
		if i := strings.LastIndexByte(body, '-'); i >= 0 {
			t.currency, t.lang, t.hasLang = body[:i], body[i+1:], true
		} else {
			t.currency = body
		}
		return t, true
	}
	return nfToken{}, false
}

// language is what a date needs from a [$-lcid]: month, weekday and AM/PM.
type language struct {
	months, monthsAbbr     []string
	weekdays, weekdaysAbbr []string
}

var (
	english = &language{
		months:       []string{"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"},
		weekdays:     []string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"},
		weekdaysAbbr: []string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"},
	}
	indonesian = &language{
		months:       []string{"Januari", "Februari", "Maret", "April", "Mei", "Juni", "Juli", "Agustus", "September", "Oktober", "November", "Desember"},
		monthsAbbr:   []string{"Jan", "Feb", "Mar", "Apr", "Mei", "Jun", "Jul", "Agu", "Sep", "Okt", "Nov", "Des"},
		weekdays:     []string{"Minggu", "Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu"},
		weekdaysAbbr: []string{"Mgg", "Sen", "Sel", "Rab", "Kam", "Jum", "Sab"},
	}
)

// languageOf gives the language of an LCID (hex, as in [$-421]) or tag, or nil
// for one the reader does not know. The system date and time codes stand for
// US English, as in excelize when no pattern is set.
func languageOf(code string) *language {
	switch strings.ToUpper(code) {
	case "409", "0409", "EN-US", "F800", "X-SYSDATE", "1010000", "F400", "X-SYSTIME":
		return english
	case "421", "0421", "21", "ID", "ID-ID":
		return indonesian
	}
	return nil
}

func (l *language) month(m time.Month, abbr int) string {
	switch abbr {
	case 3:
		if l.monthsAbbr != nil {
			return l.monthsAbbr[m-1]
		}
		return l.months[m-1][:3]
	case 4:
		return l.months[m-1]
	}
	if l.monthsAbbr != nil {
		r, _ := utf8.DecodeRuneInString(l.monthsAbbr[m-1])
		return string(r)
	}
	return l.months[m-1][:1]
}

// render gives v as the format shows it; a nil format gives v.
func (f *numFmt) render(v string, kind valueKind, date1904 bool) string {
	if f == nil {
		return v
	}
	if f.unsupported {
		return v
	}
	nf := numberFormat{f: f, value: v, kind: kind, date1904: date1904}
	return nf.run()
}

// numberFormat is one rendering, holding excelize's state for it.
type numberFormat struct {
	f        *numFmt
	sec      *nfSection
	value    string
	kind     valueKind
	date1904 bool
	number   float64
	numeric  bool

	usePositive, usePointer, useCommaSep, useSci, useMillisecond bool
	intHolder, intPadding, fracHolder, fracPadding, expBaseLen   int
	percent, scalingFactor, scalingStart                         int

	lang    *language
	langSet bool // a [$-lcid] chose the language: AM/PM is then always AM/PM
	t       time.Time
	ap      string
	result  string
}

func (nf *numberFormat) run() string {
	want := "text"
	if nf.kind != kindText {
		if ok, _, n := isNumeric(nf.value); ok {
			nf.numeric, nf.number = true, n
			want = "positive"
			if n < 0 {
				want = "positive"
				nf.usePositive = true
				if len(nf.f.sections) > 1 && !nf.f.sections[1].text {
					want, nf.usePositive = "negative", false
				}
			}
		}
	}
	for i := range nf.f.sections {
		sec := &nf.f.sections[i]
		typ := [...]string{"positive", "negative", "zero", "text"}[i]
		if sec.text {
			typ = "text"
		}
		if typ != want {
			continue
		}
		if sec.unsupported {
			return nf.value
		}
		nf.sec = sec
		var out string
		switch {
		case !nf.numeric:
			out = nf.textHandler()
		case want == "positive":
			out = nf.positiveHandler()
		default:
			out = nf.negativeHandler()
		}
		return nf.alignment(out)
	}
	return nf.value
}

func (nf *numberFormat) alignment(s string) string {
	items := nf.sec.items
	if len(items) == 0 {
		return s
	}
	if items[0].kind == tAlignment {
		s = " " + s
	}
	if items[len(items)-1].kind == tAlignment {
		s += " "
	}
	return s
}

func (nf *numberFormat) textHandler() string {
	var b strings.Builder
	for _, t := range nf.sec.items {
		switch t.kind {
		case tLiteral:
			b.WriteString(t.val)
		case tText, tZero:
			b.WriteString(nf.value)
		}
	}
	return b.String()
}

func isNumberToken(k tokKind) bool {
	return k == tExponent || k == tHash || k == tPercent || k == tZero
}

func isDateToken(k tokKind) bool { return k == tDateTime || k == tElapsed }

func (nf *numberFormat) positiveHandler() string {
	fmtNum := false
	for _, t := range nf.sec.items {
		if t.kind == tGeneral {
			if _, precision, _ := isNumeric(nf.value); precision > 11 {
				return strconv.FormatFloat(nf.number, 'G', 10, 64)
			}
			return nf.value
		}
		if isNumberToken(t.kind) {
			fmtNum = true
		}
		if isDateToken(t.kind) {
			if fmtNum || nf.number < 0 {
				return nf.value
			}
			dates := false
			for _, t2 := range nf.sec.items {
				if isDateToken(t2.kind) {
					if dates && nf.useMillisecond {
						return nf.value
					}
					dates = true
				}
				if isNumberToken(t2.kind) {
					if t2.kind == tZero {
						nf.useMillisecond = true
						continue
					}
					return nf.value
				}
			}
			return nf.dateTime()
		}
	}
	return nf.numberHandler()
}

func (nf *numberFormat) negativeHandler() string {
	for _, t := range nf.sec.items {
		if t.kind == tGeneral || isDateToken(t.kind) {
			return nf.value
		}
	}
	return nf.numberHandler()
}

func (nf *numberFormat) lastDigit() int {
	last := -1
	for i, t := range nf.sec.items {
		if t.kind == tZero || t.kind == tHash {
			last = i
		}
	}
	return last
}

func (nf *numberFormat) numberConf() {
	last := nf.lastDigit()
	for i, t := range nf.sec.items {
		switch t.kind {
		case tHash:
			if nf.usePointer {
				nf.fracHolder += len(t.val)
			} else {
				nf.intHolder += len(t.val)
			}
		case tExponent:
			nf.useSci = true
		case tThousands:
			if i > last {
				if nf.scalingFactor == 0 {
					nf.scalingStart = i
				}
				nf.scalingFactor++
			} else {
				nf.useCommaSep = true
			}
		case tLiteral:
			if t.val == "," && i > last && nf.scalingFactor > 0 {
				nf.scalingFactor++
			}
		case tPercent:
			nf.percent += len(t.val)
		case tDecimal:
			nf.usePointer = true
		case tZero:
			nf.intHolder = 0
			switch {
			case nf.usePointer && nf.useSci:
				nf.expBaseLen += len(t.val)
			case nf.usePointer:
				nf.fracPadding += len(t.val)
			default:
				nf.intPadding += len(t.val)
			}
		}
	}
}

func (nf *numberFormat) partLens() (int, int) {
	parts := strings.Split(strconv.FormatFloat(math.Abs(nf.number), 'f', -1, 64), ".")
	intPart, fracPart := len(parts[0]), 0
	if len(parts) == 2 {
		fracPart = len(parts[1])
	}
	if nf.intHolder > intPart {
		nf.intHolder = intPart
	}
	intLen := intPart
	if nf.intPadding+nf.intHolder > intPart {
		intLen = nf.intPadding + nf.intHolder
	}
	fracLen := fracPart
	if fracPart > nf.fracHolder+nf.fracPadding {
		fracLen = nf.fracHolder + nf.fracPadding
	}
	if nf.fracPadding > fracPart {
		fracLen = nf.fracPadding
	}
	return intLen, fracLen
}

func (nf *numberFormat) numberHandler() string {
	nf.numberConf()
	if nf.scalingFactor > 0 {
		nf.number /= math.Pow(1000, float64(nf.scalingFactor))
	}
	num := nf.number
	intLen, fracLen := nf.partLens()
	if intLen+fracLen > 15 && !nf.useSci {
		if isNum, precision, d := isNumeric(nf.value); isNum && precision > 15 {
			return nf.literals(nf.bigNumber(d, fracLen))
		}
	}
	pad := intLen + fracLen
	if fracLen > 0 {
		pad++
	}
	code := fmt.Sprintf("%%0%d.%df%s", pad, fracLen, strings.Repeat("%%", nf.percent))
	if nf.useSci {
		if nf.expBaseLen != 2 {
			return nf.value
		}
		code = fmt.Sprintf("%%.%dE%s", fracLen, strings.Repeat("%%", nf.percent))
	}
	if nf.percent > 0 {
		num *= math.Pow(100, float64(nf.percent))
	}
	if !nf.useSci {
		ratio := math.Pow(10, float64(fracLen))
		num = math.Round(num*ratio) / ratio
	}
	s := fmt.Sprintf(code, math.Abs(num))
	if nf.useCommaSep {
		s = commaSep(s)
	}
	return nf.literals(s)
}

func commaSep(s string) string {
	parts := strings.Split(s, ".")
	var b strings.Builder
	n := len(parts[0])
	for i := 0; i < n; i++ {
		if i > 0 && (n-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteByte(s[i])
	}
	if len(parts) == 2 {
		b.WriteByte('.')
		b.WriteString(parts[1])
	}
	return b.String()
}

func (nf *numberFormat) bigNumber(d float64, fracLen int) string {
	exp := 0.0
	if nf.percent > 0 {
		exp = 1
	}
	s := strings.TrimLeft(strconv.FormatFloat(d*math.Pow(100, exp), 'f', -1, 64), "-")
	if nf.useCommaSep {
		s = commaSep(s)
	}
	if fracLen > 0 {
		if parts := strings.Split(s, "."); len(parts) == 2 {
			if n := len(parts[1]); n < fracLen {
				s += strings.Repeat("0", fracLen-n)
			} else if n > fracLen {
				s = parts[0] + "." + parts[1][:fracLen]
			}
		} else {
			s += "." + strings.Repeat("0", fracLen)
		}
	}
	if nf.percent > 0 {
		return s + "%"
	}
	return s
}

// literals lays the digits of text over the placeholders, with the literals
// between them, as excelize's printNumberLiteral does.
func (nf *numberFormat) literals(text string) string {
	var b strings.Builder
	if nf.usePositive {
		b.WriteByte('-')
	}
	digits := 0
	for _, t := range nf.sec.items {
		if t.kind == tHash || t.kind == tZero {
			digits += len(t.val)
		}
	}
	runes := []rune(text)
	used := 0
	for i, t := range nf.sec.items {
		switch t.kind {
		case tCurrency:
			b.WriteString(t.currency)
		case tLiteral:
			if nf.scalingFactor > 0 && i >= nf.scalingStart && t.val == "," {
				continue
			}
			b.WriteString(t.val)
		case tHash, tZero:
			n := len(t.val)
			if used == 0 && len(runes) > digits {
				n = len(runes) + len(t.val) - digits
			}
			start := used
			if len(runes) < digits {
				start += len(runes) - digits
			}
			for k := 0; k < n; k++ {
				if j := k + start; 0 <= j && j < len(runes) {
					b.WriteRune(runes[j])
				}
			}
			used += n
		}
	}
	return b.String()
}

func (nf *numberFormat) dateTime() string {
	nf.lang = english
	nf.t = timeFromSerial(nf.number, nf.date1904)
	if !nf.useMillisecond {
		nf.t = nf.t.Add(time.Duration(math.Round(float64(nf.t.Nanosecond())/1e9)) * time.Second)
	}
	for i, t := range nf.sec.items {
		switch t.kind {
		case tCurrency:
			if t.hasLang {
				l := languageOf(t.lang)
				if l == nil {
					return nf.value
				}
				nf.lang, nf.langSet = l, true
			}
			nf.result += t.currency
		case tDateTime:
			nf.dateToken(i, t)
		case tElapsed:
			nf.elapsed(t)
		case tLiteral:
			nf.result += t.val
		case tDecimal:
			nf.result += "."
		case tZero:
			n := min(len(t.val), 3)
			nf.result += fmt.Sprintf("%03d", nf.t.Nanosecond()/1e6)[:n]
		}
	}
	return nf.result
}

func (nf *numberFormat) dateToken(i int, t nfToken) {
	up := strings.ToUpper(t.val)
	if isAmPm(t.val) {
		if nf.ap == "" {
			aps := strings.Split(nf.amPm(t.val), "/")
			nf.ap = aps[0]
			if nf.hoursNext(i) >= 12 {
				nf.ap = aps[1]
			}
		}
		nf.result += nf.ap
		return
	}
	n := len(t.val)
	switch up[0] {
	case 'M':
		m := int(nf.t.Month())
		if nf.number < 2 {
			m = 1
		}
		if 60 <= nf.number && nf.number < 61 {
			m = 2
		}
		switch {
		case n == 1 && nf.isMonth(i):
			nf.result += strconv.Itoa(m)
		case n == 2 && nf.isMonth(i):
			nf.result += fmt.Sprintf("%02d", m)
		case n == 3:
			nf.result += nf.monthName(3)
		case n == 4 || n > 5:
			nf.result += nf.monthName(4)
		case n == 5:
			nf.result += nf.monthName(5)
		default:
			if n == 1 {
				nf.result += strconv.Itoa(nf.t.Minute())
			} else {
				nf.result += fmt.Sprintf("%02d", nf.t.Minute())
			}
		}
	case 'Y':
		year := nf.t.Year()
		if nf.number < 2 {
			year = 1900
		}
		if y := strconv.Itoa(year); n <= 2 && len(y) > 2 {
			nf.result += y[2:]
		} else if n <= 2 {
			nf.result += y // a year of one or two characters, reached only by a serial far out of range
		} else {
			nf.result += strconv.Itoa(year)
		}
	case 'E':
		year := nf.t.Year()
		if nf.number < 2 {
			year = 1900
		}
		nf.result += strconv.Itoa(year)
	case 'D':
		d := nf.t.Day()
		if nf.number < 1 {
			d = 0
		}
		if 1 <= nf.number && nf.number < 60 {
			d = timeFromSerial(nf.number+1, nf.date1904).Day()
		}
		if 60 <= nf.number && nf.number < 61 {
			d = 29
		}
		switch n {
		case 1:
			nf.result += strconv.Itoa(d)
		case 2:
			nf.result += fmt.Sprintf("%02d", d)
		case 3:
			nf.result += nf.lang.weekdaysAbbr[nf.t.Weekday()]
		default:
			nf.result += nf.lang.weekdays[nf.t.Weekday()]
		}
	case 'H':
		h := nf.t.Hour()
		if ap, ok := nf.apNext(i); ok {
			nf.ap = ap[0]
			if h >= 12 {
				nf.ap = ap[1]
			}
			if h > 12 {
				h -= 12
			}
		}
		if nf.ap != "" {
			if nf.hoursNext(i) == -1 && h > 12 {
				h -= 12
			}
			if h == 0 {
				h = 12
			}
		}
		if n == 1 {
			nf.result += strconv.Itoa(h)
		} else {
			nf.result += fmt.Sprintf("%02d", h)
		}
	case 'S':
		if n == 1 {
			nf.result += strconv.Itoa(nf.t.Second())
		} else {
			nf.result += fmt.Sprintf("%02d", nf.t.Second())
		}
	}
}

func (nf *numberFormat) monthName(abbr int) string {
	t := nf.t
	if nf.number < 1 {
		t = timeFromSerial(nf.number+2, nf.date1904)
	}
	if 1 <= nf.number && nf.number < 60 {
		t = timeFromSerial(nf.number+1, nf.date1904)
	}
	return nf.lang.month(t.Month(), abbr)
}

var excel1900Epoch = time.Date(1899, time.December, 30, 0, 0, 0, 0, time.UTC)

func (nf *numberFormat) elapsed(t nfToken) {
	d := nf.t.Sub(excel1900Epoch)
	switch t.val[0] | 0x20 {
	case 'h':
		nf.result += fmt.Sprintf("%.f", math.Floor(d.Hours()))
	case 'm':
		nf.result += fmt.Sprintf("%.f", math.Floor(d.Minutes()))
	case 's':
		nf.result += fmt.Sprintf("%.f", math.Floor(d.Seconds()))
	}
}

// hoursNext gives the hour when an hours token follows token i, else -1.
func (nf *numberFormat) hoursNext(i int) int {
	items := nf.sec.items
	for j := i + 1; j < len(items); j++ {
		if items[j].kind == tDateTime && strings.ContainsAny(items[j].val, "hH") {
			return timeFromSerial(nf.number, false).Hour()
		}
	}
	return -1
}

// apNext gives the AM/PM pair when one follows token i before any hours token.
func (nf *numberFormat) apNext(i int) ([]string, bool) {
	items := nf.sec.items
	for j := i + 1; j < len(items); j++ {
		if items[j].kind != tDateTime {
			continue
		}
		if strings.ContainsAny(items[j].val, "hH") {
			return nil, false
		}
		if isAmPm(items[j].val) {
			return strings.Split(nf.amPm(items[j].val), "/"), true
		}
	}
	return nil, false
}

func (nf *numberFormat) amPm(v string) string {
	if nf.langSet {
		return "AM/PM"
	}
	return v
}

// isMonth tells m and mm as month from minutes: minutes when an hours or
// seconds token comes just before, or a seconds token just after.
func (nf *numberFormat) isMonth(i int) bool {
	items := nf.sec.items
	timeBefore, secondsAfter := false, false
	for j := i - 1; j >= 0; j-- {
		if items[j].kind == tDateTime {
			timeBefore = strings.ContainsAny(strings.ToUpper(items[j].val), "HS")
			break
		}
		if items[j].kind == tElapsed {
			timeBefore = true
			break
		}
	}
	for j := i + 1; j < len(items); j++ {
		if items[j].kind == tDateTime {
			secondsAfter = strings.ContainsAny(strings.ToUpper(items[j].val), "S")
			break
		}
	}
	return !timeBefore && !secondsAfter
}

// timeFromSerial turns a date serial number into a time as excelize does:
// the Julian calendar up to day 61 and the 1900 leap-year bug kept.
func timeFromSerial(serial float64, date1904 bool) time.Time {
	whole := int(serial)
	if whole <= 61 {
		offset := 15018.0
		if date1904 {
			offset = 16480.0
		}
		return julianToTime(2400000.5, serial+offset)
	}
	frac := serial - float64(whole) + 1e-9
	date := excel1900Epoch
	if date1904 {
		date = time.Date(1904, time.January, 1, 0, 0, 0, 0, time.UTC)
	}
	date = date.AddDate(0, 0, whole).Add(time.Duration(float64(24*time.Hour) * frac))
	if date.Nanosecond()/1e6 > 500 {
		return date.Round(time.Second)
	}
	return date.Truncate(time.Second)
}

func julianToTime(part1, part2 float64) time.Time {
	i1, f1 := math.Modf(part1)
	i2, f2 := math.Modf(part2)
	days, frac := i1+i2, f1+f2
	switch {
	case -0.5 < frac && frac < 0.5:
		frac += 0.5
	case frac >= 0.5:
		days++
		frac -= 0.5
	case frac <= -0.5:
		days--
		frac += 1.5
	}
	// Fliegel and Van Flandern.
	l := int(days) + 68569
	n := (4 * l) / 146097
	l = l - (146097*n+3)/4
	y4 := (4000 * (l + 1)) / 1461001
	l = l - (1461*y4)/4 + 31
	j := (80 * l) / 2447
	day := l - (2447*j)/80
	l = j / 11
	month := j + 2 - (12 * l)
	year := 100*(n-49) + y4 + l
	ns := int64(24*60*60*1e9*frac + 1e3/2)
	nanos := int((ns%1e9)/1e3) * 1e3
	ns /= 1e9
	sec := int(ns % 60)
	ns /= 60
	return time.Date(year, time.Month(month), day, int(ns/60), int(ns%60), sec, nanos, time.UTC)
}

// excelSerial turns a time into a 1900-system serial number, as excelize's
// timeToExcelTime does for a t="d" cell.
func excelSerial(t time.Time) float64 {
	min1900 := time.Date(1899, time.December, 31, 0, 0, 0, 0, time.UTC)
	if t.Before(min1900) {
		return 0
	}
	const maxDuration = 290 * 364 * 24 * time.Hour
	tt, diff, result := t, t.Sub(min1900), 0.0
	for diff >= maxDuration {
		result += float64(maxDuration / (24 * time.Hour))
		tt = tt.Add(-maxDuration)
		diff = tt.Sub(min1900)
	}
	rem := diff % (24 * time.Hour)
	result += float64(diff-rem)/float64(24*time.Hour) + float64(rem)/float64(24*time.Hour)
	if t.After(time.Date(1900, time.March, 1, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond)) {
		result++
	}
	return result
}
