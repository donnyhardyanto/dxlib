package xlsx

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/donnyhardyanto/dxlib/errors"
)

// ReaderLimits bounds what a Reader accepts from a workbook it cannot trust.
// A zero field takes its default (DefaultReaderLimits). Going past any of them
// is a *LimitError naming the field, the limit and the value seen.
type ReaderLimits struct {
	MaxFileSize         int64 // bytes of the .xlsx file; 50 MiB
	MaxEntries          int   // entries in the zip; 1,000
	MaxPartSize         int64 // decompressed bytes of one part; 100 MiB
	MaxTotalSize        int64 // decompressed bytes over the Reader's life, every part read counted each time; 200 MiB
	MaxCompressionRatio int64 // decompressed to compressed bytes of a part, checked once it passes 1 MiB; 100
	MaxXMLDepth         int   // element nesting in a part; 64
	MaxXMLTokens        int64 // tokens in a part; 50,000,000
	MaxSheets           int   // sheets in the workbook; 1,000
	MaxRows             int   // highest row number in a sheet; 1,048,576 (Excel's)
	MaxColumns          int   // highest column in a sheet; 16,384 (Excel's, column XFD)
	MaxCells            int64 // cells GetRows returns for one sheet, padding included, plus one per empty row; 5,000,000
	MaxSharedStrings    int   // entries in the shared string table; 1,048,576
	MaxStringUnits      int   // UTF-16 units in one string (shared or inline); 32,767 (Excel's)
	MaxStyles           int   // cell formats (cellXfs) and number formats, each; 65,536
}

// DefaultReaderLimits are the limits a zero ReaderLimits field stands for.
func DefaultReaderLimits() ReaderLimits {
	return ReaderLimits{
		MaxFileSize:         50 << 20,
		MaxEntries:          1000,
		MaxPartSize:         100 << 20,
		MaxTotalSize:        200 << 20,
		MaxCompressionRatio: 100,
		MaxXMLDepth:         64,
		MaxXMLTokens:        50_000_000,
		MaxSheets:           1000,
		MaxRows:             MaxRows,
		MaxColumns:          MaxColumns,
		MaxCells:            5_000_000,
		MaxSharedStrings:    1 << 20,
		MaxStringUnits:      MaxCellUnits,
		MaxStyles:           1 << 16,
	}
}

// ratioFloor is the size a part reaches before its compression ratio counts:
// small XML parts compress far better than 100:1 and are harmless.
const ratioFloor = 1 << 20

func (l *ReaderLimits) withDefaults() ReaderLimits {
	d := DefaultReaderLimits()
	if l == nil {
		return d
	}
	o := *l
	setInt64 := func(v *int64, def int64) {
		if *v <= 0 {
			*v = def
		}
	}
	setInt := func(v *int, def int) {
		if *v <= 0 {
			*v = def
		}
	}
	setInt64(&o.MaxFileSize, d.MaxFileSize)
	setInt(&o.MaxEntries, d.MaxEntries)
	setInt64(&o.MaxPartSize, d.MaxPartSize)
	setInt64(&o.MaxTotalSize, d.MaxTotalSize)
	setInt64(&o.MaxCompressionRatio, d.MaxCompressionRatio)
	setInt(&o.MaxXMLDepth, d.MaxXMLDepth)
	setInt64(&o.MaxXMLTokens, d.MaxXMLTokens)
	setInt(&o.MaxSheets, d.MaxSheets)
	setInt(&o.MaxRows, d.MaxRows)
	setInt(&o.MaxColumns, d.MaxColumns)
	setInt64(&o.MaxCells, d.MaxCells)
	setInt(&o.MaxSharedStrings, d.MaxSharedStrings)
	setInt(&o.MaxStringUnits, d.MaxStringUnits)
	setInt(&o.MaxStyles, d.MaxStyles)
	return o
}

// LimitError reports a workbook that went past one of the ReaderLimits.
type LimitError struct {
	Limit string // what was counted, such as "zip entries"
	Max   int64  // the limit in force
	Seen  int64  // the value that went past it; for a count kept while reading, the first count over Max
	Part  string // the part being read, empty for the file as a whole
}

func (e *LimitError) Error() string {
	where := ""
	if e.Part != "" {
		where = e.Part + ": "
	}
	return "xlsx: " + where + e.Limit + " reached " + strconv.FormatInt(e.Seen, 10) +
		", over the limit of " + strconv.FormatInt(e.Max, 10) + " (ReaderLimits)"
}

func limitError(limit string, max, seen int64, part string) error {
	return errors.WithStack(&LimitError{Limit: limit, Max: max, Seen: seen, Part: part})
}

// Reader reads the values of a workbook: its sheet list and each sheet's rows
// as text, the way excelize's GetRows renders them. It holds no file handle of
// its own; Close only stops it. Its methods are safe for concurrent use.
type Reader struct {
	lim       ReaderLimits
	files     map[string]*zip.File // by part name, lower case
	dups      map[string]bool      // part names present more than once
	sheets    []sheetRef
	date1904  bool
	sstPart   string
	stylePart string

	total  atomic.Int64
	closed atomic.Bool

	mu        sync.Mutex
	sstDone   bool
	sst       []string
	sstErr    error
	styleDone bool
	styles    *readStyles
	styleErr  error
}

type sheetRef struct {
	name string
	part string // empty when the workbook names no target for it
}

var errReaderClosed = errors.New("xlsx: reader is closed")

// OpenReader reads the whole workbook from r, at most MaxFileSize bytes, and
// opens it. It is the counterpart of excelize.OpenReader.
func OpenReader(r io.Reader, limits *ReaderLimits) (*Reader, error) {
	lim := limits.withDefaults()
	b, err := io.ReadAll(io.LimitReader(r, lim.MaxFileSize+1))
	if err != nil {
		return nil, errors.Wrap(err, "xlsx: reading the workbook")
	}
	return open(bytes.NewReader(b), int64(len(b)), lim)
}

// OpenBytes opens a workbook held in memory.
func OpenBytes(b []byte, limits *ReaderLimits) (*Reader, error) {
	return open(bytes.NewReader(b), int64(len(b)), limits.withDefaults())
}

// Open opens a workbook of size bytes read through r, such as an *os.File.
// r must stay readable until the Reader is no longer used.
func Open(r io.ReaderAt, size int64, limits *ReaderLimits) (*Reader, error) {
	return open(r, size, limits.withDefaults())
}

func open(r io.ReaderAt, size int64, lim ReaderLimits) (*Reader, error) {
	if size > lim.MaxFileSize {
		return nil, limitError("file size in bytes", lim.MaxFileSize, size, "")
	}
	var magic [8]byte
	n, _ := r.ReadAt(magic[:], 0)
	if n == 8 && bytes.Equal(magic[:], []byte{0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1}) {
		return nil, errors.New("xlsx: the file is a Compound File (an encrypted workbook or a legacy .xls), not an .xlsx zip package; neither is read")
	}
	zr, err := zip.NewReader(r, size)
	if err != nil && !errors.Is(err, zip.ErrInsecurePath) {
		// An insecure name is only refused when a relationship leads to it,
		// and relationship targets are resolved without "..".
		return nil, errors.Errorf("xlsx: not a readable zip package (%d bytes): %v", size, err)
	}
	if len(zr.File) > lim.MaxEntries {
		return nil, limitError("zip entries", int64(lim.MaxEntries), int64(len(zr.File)), "")
	}
	x := &Reader{lim: lim, files: make(map[string]*zip.File, len(zr.File)), dups: map[string]bool{}}
	for _, f := range zr.File {
		name := strings.ToLower(strings.TrimPrefix(f.Name, "/"))
		if _, ok := x.files[name]; ok {
			x.dups[name] = true
		}
		x.files[name] = f
	}
	if err := x.readPackage(); err != nil {
		return nil, err
	}
	return x, nil
}

// GetSheetList gives the sheet names in workbook order, hidden sheets and
// chart sheets included, as excelize's GetSheetList does.
func (x *Reader) GetSheetList() []string {
	names := make([]string, len(x.sheets))
	for i, s := range x.sheets {
		names[i] = s.name
	}
	return names
}

// Close stops the Reader; later calls return an error. It closes nothing given
// to Open.
func (x *Reader) Close() error {
	x.closed.Store(true)
	return nil
}

// relationship is one <Relationship> of a .rels part, its target resolved.
type relationship struct {
	id, typ, target string
	external        bool
}

func (x *Reader) readPackage() error {
	rels, err := x.readRels("_rels/.rels", "")
	if err != nil {
		return err
	}
	workbook := ""
	for _, r := range rels {
		if !r.external && strings.HasSuffix(r.typ, "/officeDocument") {
			workbook = r.target
			break
		}
	}
	if workbook == "" {
		return errors.New("xlsx: _rels/.rels names no workbook (no officeDocument relationship); the file is not a spreadsheet package")
	}
	type sheetEntry struct{ name, rid string }
	var entries []sheetEntry
	err = x.scanPart(workbook, func(s *xmlScanner, e xml.StartElement) error {
		switch e.Name.Local {
		case "workbookPr":
			if v, ok := attr(e, "date1904"); ok {
				x.date1904, _ = strconv.ParseBool(v)
			}
		case "sheet":
			name, _ := attr(e, "name")
			if len(entries) >= x.lim.MaxSheets {
				return limitError("sheets", int64(x.lim.MaxSheets), int64(len(entries)+1), workbook)
			}
			entries = append(entries, sheetEntry{name: name, rid: attrRelID(e)})
		}
		return nil
	})
	if err != nil {
		return err
	}
	dir, file := splitPart(workbook)
	wbRels, err := x.readRels(joinPart(dir, "_rels/"+file+".rels"), dir)
	if err != nil {
		return err
	}
	byID := map[string]relationship{}
	for _, r := range wbRels {
		byID[r.id] = r
		switch {
		case r.external:
		case strings.HasSuffix(r.typ, "/sharedStrings") && x.sstPart == "":
			x.sstPart = r.target
		case strings.HasSuffix(r.typ, "/styles") && x.stylePart == "":
			x.stylePart = r.target
		}
	}
	for _, e := range entries {
		ref := sheetRef{name: e.name}
		if r, ok := byID[e.rid]; ok && !r.external {
			ref.part = r.target
		}
		x.sheets = append(x.sheets, ref)
	}
	return nil
}

// readRels reads a .rels part; base is the folder its targets are relative
// to. A package without the part has no relationships.
func (x *Reader) readRels(part, base string) ([]relationship, error) {
	if _, ok := x.files[strings.ToLower(part)]; !ok {
		if part == "_rels/.rels" {
			return nil, errors.New("xlsx: the zip has no _rels/.rels; it is not an Office Open XML package")
		}
		return nil, nil
	}
	var rels []relationship
	err := x.scanPart(part, func(s *xmlScanner, e xml.StartElement) error {
		if e.Name.Local != "Relationship" {
			return nil
		}
		r := relationship{}
		r.id, _ = attr(e, "Id")
		r.typ, _ = attr(e, "Type")
		target, _ := attr(e, "Target")
		if mode, _ := attr(e, "TargetMode"); mode == "External" {
			r.external = true // never followed
		} else {
			t, err := resolveTarget(base, target)
			if err != nil {
				return errors.Errorf("xlsx: %s: relationship %q: %v", part, r.id, err)
			}
			r.target = t
		}
		if len(rels) >= x.lim.MaxEntries {
			return limitError("relationships", int64(x.lim.MaxEntries), int64(len(rels)+1), part)
		}
		rels = append(rels, r)
		return nil
	})
	return rels, err
}

// resolveTarget turns a relationship target into a part name: relative to
// base, or to the package root when it starts with "/". It never leaves the
// package: ".." past the root, a scheme or a backslash is an error.
func resolveTarget(base, target string) (string, error) {
	if strings.ContainsAny(target, "\\") || strings.Contains(target, "://") {
		return "", errors.Errorf("target %q is not a part name inside the package", clip(target, 80))
	}
	t, err := url.PathUnescape(target)
	if err != nil {
		return "", errors.Errorf("target %q is not a valid URI path: %v", clip(target, 80), err)
	}
	p := t
	if !strings.HasPrefix(t, "/") && base != "" {
		p = base + "/" + t
	}
	var segs []string
	for _, s := range strings.Split(p, "/") {
		switch s {
		case "", ".":
		case "..":
			if len(segs) == 0 {
				return "", errors.Errorf("target %q leaves the package", clip(target, 80))
			}
			segs = segs[:len(segs)-1]
		default:
			segs = append(segs, s)
		}
	}
	if len(segs) == 0 {
		return "", errors.Errorf("target %q names no part", clip(target, 80))
	}
	return strings.Join(segs, "/"), nil
}

func splitPart(p string) (dir, file string) {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[:i], p[i+1:]
	}
	return "", p
}

func joinPart(dir, p string) string {
	if dir == "" {
		return p
	}
	return dir + "/" + p
}

// openPart opens a part for reading, counted against the size limits.
func (x *Reader) openPart(part string) (io.ReadCloser, error) {
	if x.closed.Load() {
		return nil, errReaderClosed
	}
	key := strings.ToLower(part)
	if x.dups[key] {
		return nil, errors.Errorf("xlsx: the zip holds %s more than once; which one is meant cannot be told", part)
	}
	f, ok := x.files[key]
	if !ok {
		return nil, errors.Errorf("xlsx: part %s is named by the workbook but missing from the zip", part)
	}
	rc, err := f.Open()
	if err != nil {
		return nil, errors.Errorf("xlsx: opening part %s: %v", part, err)
	}
	compressed := max(int64(f.CompressedSize64), 1)
	return &partReader{rc: rc, x: x, part: part, compressed: compressed}, nil
}

// partReader counts the decompressed bytes of a part as they come, so no size
// in a zip header is trusted.
type partReader struct {
	rc         io.ReadCloser
	x          *Reader
	part       string
	n          int64
	compressed int64
}

func (p *partReader) Read(b []byte) (int, error) {
	n, err := p.rc.Read(b)
	p.n += int64(n)
	lim := &p.x.lim
	if p.n > lim.MaxPartSize {
		return 0, limitError("decompressed bytes of a part", lim.MaxPartSize, p.n, p.part)
	}
	if t := p.x.total.Add(int64(n)); t > lim.MaxTotalSize {
		return 0, limitError("decompressed bytes in all", lim.MaxTotalSize, t, p.part)
	}
	if p.n > ratioFloor && p.n/p.compressed > lim.MaxCompressionRatio {
		return 0, limitError("compression ratio", lim.MaxCompressionRatio, p.n/p.compressed, p.part)
	}
	if err != nil && err != io.EOF {
		return n, &partReadError{part: p.part, err: err}
	}
	return n, err
}

// partReadError is a part that could not be read: a broken deflate stream or
// a checksum that does not match.
type partReadError struct {
	part string
	err  error
}

func (e *partReadError) Error() string { return "xlsx: reading part " + e.part + ": " + e.err.Error() }
func (e *partReadError) Unwrap() error { return e.err }

func (p *partReader) Close() error { return p.rc.Close() }

// scanPart calls fn for each start element of a part. fn may consume the
// element's content through s; what it leaves is read by the scan.
func (x *Reader) scanPart(part string, fn func(s *xmlScanner, e xml.StartElement) error) error {
	rc, err := x.openPart(part)
	if err != nil {
		return err
	}
	defer rc.Close()
	s := newXMLScanner(rc, part, &x.lim)
	for {
		t, err := s.next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if e, ok := t.(xml.StartElement); ok {
			if err := fn(s, e); err != nil {
				return err
			}
		}
	}
}

// sharedStrings loads the shared string table once.
func (x *Reader) sharedStrings() ([]string, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.sstDone {
		return x.sst, x.sstErr
	}
	x.sstDone = true
	part := x.sstPart
	if part == "" {
		return nil, nil
	}
	err := x.scanPart(part, func(s *xmlScanner, e xml.StartElement) error {
		if e.Name.Local != "si" {
			return nil
		}
		if len(x.sst) >= x.lim.MaxSharedStrings {
			return limitError("shared strings", int64(x.lim.MaxSharedStrings), int64(len(x.sst)+1), part)
		}
		v, err := x.readStringItem(s, "shared string")
		if err != nil {
			return err
		}
		x.sst = append(x.sst, v)
		return nil
	})
	if err != nil {
		x.sst, x.sstErr = nil, err
	}
	return x.sst, x.sstErr
}

// readStringItem reads an <si> or <is> whose start was just returned: the
// text of its own <t> and of each run's <t>, phonetic runs (<rPh>) left out,
// with _xHHHH_ decoded, as excelize's xlsxSI.String gives it.
func (x *Reader) readStringItem(s *xmlScanner, what string) (string, error) {
	maxBytes := int64(x.lim.MaxStringUnits) * 4
	var plain, runs strings.Builder
	for {
		t, err := s.next()
		if err != nil {
			return "", s.eof(err)
		}
		switch t := t.(type) {
		case xml.EndElement:
			v := plain.String() + runs.String()
			if v == "" {
				return "", nil
			}
			v = decodeEscapes(v)
			if u := utf16Len(v); u > x.lim.MaxStringUnits {
				return "", limitError(what+" length in UTF-16 units", int64(x.lim.MaxStringUnits), int64(u), s.part)
			}
			return v, nil
		case xml.StartElement:
			switch t.Name.Local {
			case "t":
				v, err := s.text(maxBytes, what+" bytes")
				if err != nil {
					return "", err
				}
				// One <t> per item: a second replaces the first, as
				// decoding into excelize's struct does.
				plain.Reset()
				plain.WriteString(v)
			case "r":
				v, err := runText(s, maxBytes, what)
				if err != nil {
					return "", err
				}
				runs.WriteString(v)
			default:
				if err := s.skip(); err != nil {
					return "", err
				}
			}
			if int64(plain.Len()+runs.Len()) > maxBytes {
				return "", limitError(what+" bytes", maxBytes, int64(plain.Len()+runs.Len()), s.part)
			}
		}
	}
}

// runText reads an <r> whose start was just returned and gives its <t>.
func runText(s *xmlScanner, maxBytes int64, what string) (string, error) {
	v := ""
	for {
		t, err := s.next()
		if err != nil {
			return "", s.eof(err)
		}
		switch t := t.(type) {
		case xml.EndElement:
			return v, nil
		case xml.StartElement:
			if t.Name.Local == "t" {
				if v, err = s.text(maxBytes, what+" bytes"); err != nil {
					return "", err
				}
				continue
			}
			if err := s.skip(); err != nil {
				return "", err
			}
		}
	}
}

// decodeEscapes turns each _xHHHH_ into its character, left to right without
// overlapping, _x005F_ into "_", and drops a surrogate half, as excelize's
// bstrUnmarshal does.
func decodeEscapes(s string) string {
	if !strings.Contains(s, "_x") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); {
		if i+7 <= len(s) && s[i] == '_' && s[i+1] == 'x' && isHex(s[i+2]) && isHex(s[i+3]) && isHex(s[i+4]) && isHex(s[i+5]) && s[i+6] == '_' {
			code, _ := strconv.ParseUint(s[i+2:i+6], 16, 32)
			if code < 0xD800 || code > 0xDFFF {
				b.WriteRune(rune(code))
			}
			i += 7
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

func utf16Len(s string) int {
	n := 0
	for _, r := range s {
		n++
		if r >= 0x10000 {
			n++
		}
	}
	return n
}
