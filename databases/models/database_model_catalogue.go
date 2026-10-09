package models

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/donnyhardyanto/dxlib/base"
)

// ============================================================================
// Catalogue dump - the declared model in the shape of a PostgreSQL catalogue
// ============================================================================
//
// CatalogueAsJSON writes what the model declares as the catalogue dump of a
// PostgreSQL database: the JSON document a catalogue query over pg_class,
// pg_attribute, pg_constraint and pg_index prints after the model's DDL is
// applied. The query is SpecArch's tools/catalogue/catalogue.sql; its layout
// is jsonb_pretty's. Putting the declared model in the same shape as a live
// database's catalogue lets the two be compared with a plain diff, and lets a
// reader of catalogue dumps read the model with no database at all.
//
// The document is rendered from the PostgreSQL DDL that CreateDDL writes, so
// it holds what that DDL would make:
//
//   - types as format_type prints them: VARCHAR(255) is "character varying(255)",
//     INT "integer", TIME "time without time zone", SERIAL "integer" with a
//     nextval default; a type outside that table is written lower-cased;
//   - constraint, index and sequence names as PostgreSQL chooses them for an
//     inline PRIMARY KEY, UNIQUE or REFERENCES: <table>_pkey, <table>_<col>_key,
//     <table>_<col>_fkey, <table>_<col>_seq, cut to 63 bytes the way PostgreSQL
//     cuts them; each key has its backing index;
//   - a table with no schema in "public", and the views' names folded to lower
//     case, since their DDL leaves them unquoted.
//
// Expressions are written as declared, not as PostgreSQL rewrites them: a
// default other than a literal, a generated column's expression and a partial
// index's condition may differ in casts and parentheses from a live dump. A
// literal default gets the cast PostgreSQL adds ('open'::character varying).
// Every foreign key's onDelete is "no action", since the model declares none;
// comments are null and the enums list is empty for the same reason.
//
// Lists are in a fixed order (tables and views by schema and name, columns by
// Order then name, constraints and indexes by name), so the same model gives
// the same bytes.

// CatalogueFormat is the catalogue document's "catalogue" member: the
// database the model is rendered for.
const CatalogueFormat = "postgresql"

// postgresNameDataLen is NAMEDATALEN - 1: the longest identifier PostgreSQL keeps.
const postgresNameDataLen = 63

// CatalogueAsJSON returns the model's catalogue document. path and commit are
// written as the document's "path" and "commit" members, which name the
// source the catalogue was made from and the commit that last changed it; the
// model cannot know them, so the caller passes them, or "" for none.
func (d *ModelDB) CatalogueAsJSON(path, commit string) ([]byte, error) {
	tables := []jsonValue{}
	var all []*ModelDBTable
	for _, s := range d.Schemas {
		all = append(all, s.Tables...)
	}
	sort.SliceStable(all, func(i, j int) bool {
		si, sj := catalogueSchemaName(all[i].Schema), catalogueSchemaName(all[j].Schema)
		if si != sj {
			return si < sj
		}
		return all[i].TableName() < all[j].TableName()
	})
	seen := map[string]bool{}
	for _, t := range all {
		qualified := catalogueSchemaName(t.Schema) + "." + t.TableName()
		if seen[qualified] {
			return nil, fmt.Errorf("CATALOGUE_DUPLICATE_TABLE:%s", qualified)
		}
		seen[qualified] = true
		table, err := t.catalogueTable()
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	type viewName struct {
		schema, name string
		materialized bool
	}
	var viewNames []viewName
	for _, s := range d.Schemas {
		for _, v := range s.Views {
			viewNames = append(viewNames, viewName{catalogueFoldedSchemaName(v.Schema), strings.ToLower(v.Name), false})
		}
		for _, mv := range s.MaterializedViews {
			viewNames = append(viewNames, viewName{catalogueFoldedSchemaName(mv.Schema), strings.ToLower(mv.Name), true})
		}
	}
	sort.SliceStable(viewNames, func(i, j int) bool {
		if viewNames[i].schema != viewNames[j].schema {
			return viewNames[i].schema < viewNames[j].schema
		}
		return viewNames[i].name < viewNames[j].name
	})
	views := []jsonValue{}
	for _, v := range viewNames {
		views = append(views, jsonObject{
			{"schema", v.schema},
			{"name", v.name},
			{"materialized", v.materialized},
		})
	}

	doc := jsonObject{
		{"catalogue", CatalogueFormat},
		{"path", path},
		{"commit", commit},
		{"tables", tables},
		{"views", views},
		{"enums", []jsonValue{}},
	}
	var sb strings.Builder
	writeJSONBPretty(&sb, doc, 0)
	sb.WriteString("\n")
	return []byte(sb.String()), nil
}

func catalogueSchemaName(s *ModelDBSchema) string {
	if s == nil || s.Name == "" {
		return "public"
	}
	return s.Name
}

// catalogueFoldedSchemaName is the schema of an object whose DDL names it
// unquoted, as PostgreSQL folds it.
func catalogueFoldedSchemaName(s *ModelDBSchema) string {
	return strings.ToLower(catalogueSchemaName(s))
}

// catalogueOrderedFields is getOrderedFields with ties in Order broken by
// name, so that the order does not depend on the map's.
func (t *ModelDBTable) catalogueOrderedFields() []string {
	names := make([]string, 0, len(t.Fields))
	for name := range t.Fields {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		oi, oj := t.Fields[names[i]].Order, t.Fields[names[j]].Order
		if oi != oj {
			return oi < oj
		}
		return names[i] < names[j]
	})
	return names
}

func (t *ModelDBTable) catalogueTable() (jsonValue, error) {
	schema := catalogueSchemaName(t.Schema)
	table := t.TableName()
	qualifiedForIndex := postgresQuoteIdent(schema) + "." + postgresQuoteIdent(table)

	columns := []jsonValue{}
	var constraints []jsonObject
	var indexes []jsonObject
	var primary []string
	for _, name := range t.catalogueOrderedFields() {
		f := t.Fields[name]
		if f.IsGenerated {
			// The generated column's DDL leaves its name unquoted, and does
			// not declare it NOT NULL or a key.
			expr := f.GeneratedExpression
			if e, ok := f.GeneratedExpressionByDBType[base.DXDatabaseTypePostgreSQL]; ok && e != "" {
				expr = e
			}
			columns = append(columns, catalogueColumn(strings.ToLower(name), postgresFormatType(t.postgresDeclaredType(f)), false, &expr, "s"))
			continue
		}
		declared := t.postgresDeclaredType(f)
		typ := postgresFormatType(declared)
		var def *string
		if serial, ok := postgresSerialTypes[strings.ToUpper(strings.TrimSpace(declared))]; ok {
			typ = serial
			seq := postgresObjectName(table, name, "seq")
			if schema != "public" {
				seq = postgresQuoteIdent(schema) + "." + postgresQuoteIdent(seq)
			} else {
				seq = postgresQuoteIdent(seq)
			}
			s := "nextval('" + strings.ReplaceAll(seq, "'", "''") + "'::regclass)"
			def = &s
		}
		if s := t.getDefaultValueForDBType(*f, base.DXDatabaseTypePostgreSQL); s != "" {
			s = postgresCastLiteral(s, typ)
			def = &s
		}
		notNull := f.IsNotNull || f.IsPrimaryKey || def != nil && strings.HasPrefix(*def, "nextval(")
		columns = append(columns, catalogueColumn(name, typ, notNull, def, ""))

		quoted := postgresQuoteIdent(name)
		switch {
		case f.IsPrimaryKey:
			primary = append(primary, name)
		case f.IsUnique:
			key := postgresObjectName(table, name, "key")
			constraints = append(constraints, catalogueConstraint(key, "unique", []string{name}, "UNIQUE ("+quoted+")", nil, nil, nil, nil))
			indexes = append(indexes, catalogueIndex(key, true, false, true,
				"CREATE UNIQUE INDEX "+postgresQuoteIdent(key)+" ON "+qualifiedForIndex+" USING btree ("+quoted+")"))
		}
		if f.References != "" {
			// As fieldToDDL writes it: schema.table.field, used as written.
			parts := strings.Split(f.References, ".")
			if len(parts) != 3 {
				continue
			}
			refSchema, refTable, refColumn := parts[0], parts[1], parts[2]
			target := postgresQuoteIdent(refTable)
			if refSchema != "public" {
				target = postgresQuoteIdent(refSchema) + "." + target
			}
			key := postgresObjectName(table, name, "fkey")
			noAction := "no action"
			constraints = append(constraints, catalogueConstraint(key, "foreign", []string{name},
				"FOREIGN KEY ("+quoted+") REFERENCES "+target+"("+postgresQuoteIdent(refColumn)+")",
				&refSchema, &refTable, []string{refColumn}, &noAction))
		}
	}
	if len(primary) > 1 {
		// Each field writes its own inline PRIMARY KEY, which PostgreSQL refuses.
		return nil, fmt.Errorf("CATALOGUE_MULTIPLE_PRIMARY_KEYS:%s.%s:%s", schema, table, strings.Join(primary, ","))
	}
	if len(primary) == 1 {
		key := postgresObjectName(table, "", "pkey")
		quoted := postgresQuoteIdent(primary[0])
		constraints = append(constraints, catalogueConstraint(key, "primary", primary, "PRIMARY KEY ("+quoted+")", nil, nil, nil, nil))
		indexes = append(indexes, catalogueIndex(key, true, true, true,
			"CREATE UNIQUE INDEX "+postgresQuoteIdent(key)+" ON "+qualifiedForIndex+" USING btree ("+quoted+")"))
	}
	for _, idx := range t.Indexes {
		indexes = append(indexes, catalogueIndex(idx.Name, idx.IsUnique, false, false, idx.postgresIndexDef(qualifiedForIndex)))
	}

	sort.SliceStable(constraints, func(i, j int) bool { return constraints[i][0].value.(string) < constraints[j][0].value.(string) })
	sort.SliceStable(indexes, func(i, j int) bool { return indexes[i][0].value.(string) < indexes[j][0].value.(string) })
	constraintList := []jsonValue{}
	for _, c := range constraints {
		constraintList = append(constraintList, c)
	}
	indexList := []jsonValue{}
	for _, i := range indexes {
		indexList = append(indexList, i)
	}
	return jsonObject{
		{"schema", schema},
		{"name", table},
		{"comment", nil},
		{"columns", columns},
		{"constraints", constraintList},
		{"indexes", indexList},
	}, nil
}

func (t *ModelDBTable) postgresDeclaredType(f *ModelDBField) string {
	if f.Type.TypeByDatabaseType != nil {
		if s := f.Type.TypeByDatabaseType[base.DXDatabaseTypePostgreSQL]; s != "" {
			return s
		}
	}
	return "TEXT"
}

func catalogueColumn(name, typ string, notNull bool, def *string, generated string) jsonObject {
	var gen jsonValue
	if generated != "" {
		gen = generated
	}
	var defValue jsonValue
	if def != nil {
		defValue = *def
	}
	return jsonObject{
		{"name", name},
		{"type", typ},
		{"notNull", notNull},
		{"default", defValue},
		{"identity", nil},
		{"generated", gen},
		{"comment", nil},
	}
}

func catalogueConstraint(name, kind string, columns []string, definition string, refSchema, refTable *string, refColumns []string, onDelete *string) jsonObject {
	return jsonObject{
		{"name", name},
		{"kind", kind},
		{"columns", jsonStrings(columns)},
		{"definition", definition},
		{"referencesSchema", jsonStringOrNull(refSchema)},
		{"referencesTable", jsonStringOrNull(refTable)},
		{"referencesColumns", jsonStrings(refColumns)},
		{"onDelete", jsonStringOrNull(onDelete)},
	}
}

func catalogueIndex(name string, unique, primary, backsConstraint bool, definition string) jsonObject {
	return jsonObject{
		{"name", name},
		{"unique", unique},
		{"primary", primary},
		{"backsConstraint", backsConstraint},
		{"definition", definition},
	}
}

// postgresIndexDef is the index as pg_get_indexdef prints it: the owner
// schema-qualified, the method named, no tablespace.
func (i *ModelDBIndex) postgresIndexDef(qualifiedOwner string) string {
	var sb strings.Builder
	sb.WriteString("CREATE ")
	if i.IsUnique {
		sb.WriteString("UNIQUE ")
	}
	sb.WriteString("INDEX " + postgresQuoteIdent(i.Name) + " ON " + qualifiedOwner + " USING ")
	method := strings.ToLower(string(i.Method))
	if method == "" {
		method = "btree"
	}
	sb.WriteString(method + " (")
	for n, c := range i.Columns {
		if n > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(postgresQuoteIdent(c.Name))
		if strings.EqualFold(c.Order, "DESC") {
			sb.WriteString(" DESC")
		}
		if nulls := postgresNullsOrder(c.Order, c.NullsOrder); nulls != "" {
			sb.WriteString(" " + nulls)
		}
	}
	sb.WriteString(")")
	if len(i.Include) > 0 {
		quoted := make([]string, len(i.Include))
		for n, c := range i.Include {
			quoted[n] = postgresQuoteIdent(strings.TrimSpace(c))
		}
		sb.WriteString(" INCLUDE (" + strings.Join(quoted, ", ") + ")")
	}
	if i.Where != "" {
		sb.WriteString(" WHERE (" + i.Where + ")")
	}
	return sb.String()
}

// postgresNullsOrder is the nulls order pg_get_indexdef writes after a
// column: none when it is the default for the direction (NULLS LAST after
// ASC, NULLS FIRST after DESC).
func postgresNullsOrder(order, nulls string) string {
	nulls = strings.ToUpper(strings.Join(strings.Fields(nulls), " "))
	defaultNulls := "NULLS LAST"
	if strings.EqualFold(order, "DESC") {
		defaultNulls = "NULLS FIRST"
	}
	if nulls == defaultNulls {
		return ""
	}
	return nulls
}

// postgresSerialTypes are the pseudo-types that make an integer column with a
// sequence default.
var postgresSerialTypes = map[string]string{
	"SMALLSERIAL": "smallint",
	"SERIAL2":     "smallint",
	"SERIAL":      "integer",
	"SERIAL4":     "integer",
	"BIGSERIAL":   "bigint",
	"SERIAL8":     "bigint",
}

// postgresTypeNames maps a type name as DDL may write it to format_type's
// name for it.
var postgresTypeNames = map[string]string{
	"INT":                         "integer",
	"INTEGER":                     "integer",
	"INT4":                        "integer",
	"SMALLINT":                    "smallint",
	"INT2":                        "smallint",
	"BIGINT":                      "bigint",
	"INT8":                        "bigint",
	"SMALLSERIAL":                 "smallint",
	"SERIAL2":                     "smallint",
	"SERIAL":                      "integer",
	"SERIAL4":                     "integer",
	"BIGSERIAL":                   "bigint",
	"SERIAL8":                     "bigint",
	"REAL":                        "real",
	"FLOAT4":                      "real",
	"DOUBLE PRECISION":            "double precision",
	"FLOAT8":                      "double precision",
	"FLOAT":                       "double precision",
	"BOOLEAN":                     "boolean",
	"BOOL":                        "boolean",
	"TEXT":                        "text",
	"VARCHAR":                     "character varying",
	"CHARACTER VARYING":           "character varying",
	"CHAR":                        "character",
	"CHARACTER":                   "character",
	"BPCHAR":                      "character",
	"NUMERIC":                     "numeric",
	"DECIMAL":                     "numeric",
	"DATE":                        "date",
	"TIME":                        "time without time zone",
	"TIME WITHOUT TIME ZONE":      "time without time zone",
	"TIMETZ":                      "time with time zone",
	"TIME WITH TIME ZONE":         "time with time zone",
	"TIMESTAMP":                   "timestamp without time zone",
	"TIMESTAMP WITHOUT TIME ZONE": "timestamp without time zone",
	"TIMESTAMPTZ":                 "timestamp with time zone",
	"TIMESTAMP WITH TIME ZONE":    "timestamp with time zone",
	"INTERVAL":                    "interval",
	"BYTEA":                       "bytea",
	"JSON":                        "json",
	"JSONB":                       "jsonb",
	"UUID":                        "uuid",
}

var postgresTypeWithModifiers = regexp.MustCompile(`^([A-Za-z][A-Za-z0-9 ]*?)\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)\s*(.*)$`)

// postgresFormatType writes a declared PostgreSQL type as format_type prints
// it. A type the table does not know is written lower-cased, its spacing
// collapsed.
func postgresFormatType(declared string) string {
	s := strings.Join(strings.Fields(declared), " ")
	array := ""
	for strings.HasSuffix(s, "[]") {
		array += "[]"
		s = strings.TrimSpace(strings.TrimSuffix(s, "[]"))
	}
	upper := strings.ToUpper(s)
	if name, ok := postgresTypeNames[upper]; ok {
		if name == "character" {
			return "character(1)" + array
		}
		return name + array
	}
	if t, ok := postgisFormatType(s); ok {
		return t + array
	}
	if m := postgresTypeWithModifiers.FindStringSubmatch(s); m != nil {
		base := strings.ToUpper(m[1])
		rest := strings.ToUpper(strings.TrimSpace(m[4]))
		width, scale := m[2], m[3]
		switch base {
		case "VARCHAR", "CHARACTER VARYING", "CHAR", "CHARACTER", "BPCHAR":
			if rest == "" {
				return postgresTypeNames[base] + "(" + width + ")" + array
			}
		case "NUMERIC", "DECIMAL":
			if rest == "" {
				if scale == "" {
					scale = "0"
				}
				return "numeric(" + width + "," + scale + ")" + array
			}
		case "TIMESTAMP", "TIME":
			zone := "without time zone"
			switch rest {
			case "", "WITHOUT TIME ZONE":
			case "WITH TIME ZONE":
				zone = "with time zone"
			default:
				return strings.ToLower(s) + array
			}
			name := "timestamp"
			if base == "TIME" {
				name = "time"
			}
			return name + "(" + width + ") " + zone + array
		case "TIMESTAMPTZ", "TIMETZ":
			if rest == "" {
				return strings.ToLower(strings.TrimSuffix(base, "TZ")) + "(" + width + ") with time zone" + array
			}
		}
	}
	return strings.ToLower(s) + array
}

// postgisGeometryTypes are PostGIS's geometry type names as its typmod
// output writes them, keyed by their upper-case spelling.
var postgisGeometryTypes = func() map[string]string {
	m := map[string]string{}
	for _, name := range []string{"Geometry", "Point", "LineString", "Polygon", "MultiPoint", "MultiLineString",
		"MultiPolygon", "GeometryCollection", "CircularString", "CompoundCurve", "CurvePolygon", "MultiCurve",
		"MultiSurface", "PolyhedralSurface", "Triangle", "Tin"} {
		m[strings.ToUpper(name)] = name
	}
	return m
}()

var postgisTypeWithModifiers = regexp.MustCompile(`^(?i)(geometry|geography)\s*\(\s*([A-Za-z]+)\s*(?:,\s*(\d+)\s*)?\)$`)

// postgisFormatType writes a PostGIS geometry or geography type with a
// typmod as format_type prints it: geometry(Point, 4326) is
// geometry(Point,4326). The subtype takes PostGIS's own case, a Z, M or ZM
// suffix is upper-cased, an SRID of 0 is left out, and geography without an
// SRID gets 4326. ok is false for anything else.
func postgisFormatType(s string) (string, bool) {
	m := postgisTypeWithModifiers.FindStringSubmatch(s)
	if m == nil {
		return "", false
	}
	base := strings.ToLower(m[1])
	subtype := strings.ToUpper(m[2])
	dims := ""
	for _, suffix := range []string{"ZM", "Z", "M"} {
		if trimmed := strings.TrimSuffix(subtype, suffix); trimmed != subtype {
			if _, ok := postgisGeometryTypes[trimmed]; ok {
				subtype, dims = trimmed, suffix
				break
			}
		}
	}
	name, ok := postgisGeometryTypes[subtype]
	if !ok {
		return "", false
	}
	srid := strings.TrimLeft(m[3], "0")
	if base == "geography" && srid == "" {
		srid = "4326"
	}
	if name == "Geometry" && dims == "" && srid == "" {
		return base, true
	}
	if srid != "" {
		return base + "(" + name + dims + "," + srid + ")", true
	}
	return base + "(" + name + dims + ")", true
}

var postgresQuotedLiteral = regexp.MustCompile(`^'(?:[^']|'')*'$`)
var postgresTypeModifiers = regexp.MustCompile(`\(\d+(,\d+)?\)`)

// postgresCastLiteral adds the cast PostgreSQL writes after a quoted literal
// default: 'open' on a character varying column is 'open'::character varying.
// Anything that is not a single quoted literal is returned as it is.
func postgresCastLiteral(def, typ string) string {
	if !postgresQuotedLiteral.MatchString(def) {
		return def
	}
	cast := postgresTypeModifiers.ReplaceAllString(typ, "")
	if strings.HasPrefix(cast, "character") && !strings.HasPrefix(cast, "character varying") {
		cast = "bpchar" + strings.TrimPrefix(cast, "character")
	}
	return def + "::" + cast
}

// postgresObjectName is PostgreSQL's makeObjectName: name1_name2_label, with
// the longer of name1 and name2 shortened, one byte at a time, until the
// whole fits in 63 bytes. name2 may be empty.
func postgresObjectName(name1, name2, label string) string {
	overhead := len(label) + 1
	if name2 != "" {
		overhead++
	}
	avail := postgresNameDataLen - overhead
	n1, n2 := len(name1), len(name2)
	for n1+n2 > avail {
		if n1 > n2 {
			n1--
		} else {
			n2--
		}
	}
	name := postgresClip(name1, n1)
	if name2 != "" {
		name += "_" + postgresClip(name2, n2)
	}
	return name + "_" + label
}

// postgresClip cuts s to at most n bytes without splitting a UTF-8 character.
func postgresClip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && n < len(s) && s[n]&0xC0 == 0x80 {
		n--
	}
	return s[:n]
}

var postgresPlainIdent = regexp.MustCompile(`^[a-z_][a-z0-9_$]*$`)

// postgresQuoteIdent is quote_ident: a name is written bare when it is lower
// case and not a keyword PostgreSQL reserves in that position, and in double
// quotes otherwise.
func postgresQuoteIdent(name string) string {
	if postgresPlainIdent.MatchString(name) && !postgresKeywords[name] {
		return name
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// postgresKeywords are PostgreSQL's keywords other than the unreserved ones:
// the reserved, the type or function name and the column name keywords,
// which quote_ident quotes.
var postgresKeywords = func() map[string]bool {
	m := map[string]bool{}
	for _, w := range strings.Fields(`
		all analyse analyze and any array as asc asymmetric both case cast check collate column
		constraint create current_catalog current_date current_role current_time current_timestamp
		current_user default deferrable desc distinct do else end except false fetch for foreign
		from grant group having in initially intersect into lateral leading limit localtime
		localtimestamp not null offset on only or order placing primary references returning
		select session_user some symmetric system_user table then to trailing true union unique
		user using variadic when where window with
		authorization binary collation concurrently cross current_schema freeze full ilike inner
		is isnull join left like natural notnull outer overlaps right similar tablesample verbose
		between bigint bit boolean char character coalesce dec decimal exists extract float
		greatest grouping inout int integer interval json json_array json_arrayagg json_exists
		json_object json_objectagg json_query json_scalar json_serialize json_table json_value
		least merge_action national nchar none normalize nullif numeric out overlay position
		precision real row setof smallint substring time timestamp treat trim values varchar
		xmlattributes xmlconcat xmlelement xmlexists xmlforest xmlnamespaces xmlparse xmlpi
		xmlroot xmlserialize xmltable`) {
		m[w] = true
	}
	return m
}()

// ============================================================================
// jsonb_pretty layout
// ============================================================================

// jsonValue is one of: string, bool, nil, jsonObject, []jsonValue.
type jsonValue any

type jsonMember struct {
	key   string
	value jsonValue
}

type jsonObject []jsonMember

func jsonStrings(s []string) jsonValue {
	if s == nil {
		return nil
	}
	list := make([]jsonValue, len(s))
	for i, v := range s {
		list[i] = v
	}
	return list
}

func jsonStringOrNull(s *string) jsonValue {
	if s == nil {
		return nil
	}
	return *s
}

// writeJSONBPretty writes v as jsonb_pretty does: four spaces a level, an
// object's keys shortest first and then in byte order (jsonb's own order),
// and an empty list or object still opened on one line and closed on the
// next.
func writeJSONBPretty(sb *strings.Builder, v jsonValue, indent int) {
	pad := strings.Repeat(" ", indent)
	inner := strings.Repeat(" ", indent+4)
	switch v := v.(type) {
	case nil:
		sb.WriteString("null")
	case bool:
		sb.WriteString(strconv.FormatBool(v))
	case string:
		writeJSONBString(sb, v)
	case jsonObject:
		members := append(jsonObject(nil), v...)
		sort.SliceStable(members, func(i, j int) bool {
			if len(members[i].key) != len(members[j].key) {
				return len(members[i].key) < len(members[j].key)
			}
			return members[i].key < members[j].key
		})
		sb.WriteString("{\n")
		for i, m := range members {
			if i > 0 {
				sb.WriteString(",\n")
			}
			sb.WriteString(inner)
			writeJSONBString(sb, m.key)
			sb.WriteString(": ")
			writeJSONBPretty(sb, m.value, indent+4)
		}
		if len(members) > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(pad + "}")
	case []jsonValue:
		sb.WriteString("[\n")
		for i, item := range v {
			if i > 0 {
				sb.WriteString(",\n")
			}
			sb.WriteString(inner)
			writeJSONBPretty(sb, item, indent+4)
		}
		if len(v) > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(pad + "]")
	default:
		panic(fmt.Sprintf("writeJSONBPretty: %T", v))
	}
}

// writeJSONBString escapes as PostgreSQL's escape_json: the quote, the
// backslash and the control characters, with every other character as it is.
func writeJSONBString(sb *strings.Builder, s string) {
	sb.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			sb.WriteString(`\"`)
		case '\\':
			sb.WriteString(`\\`)
		case '\b':
			sb.WriteString(`\b`)
		case '\f':
			sb.WriteString(`\f`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		default:
			if r < 0x20 {
				fmt.Fprintf(sb, `\u%04x`, r)
			} else {
				sb.WriteRune(r)
			}
		}
	}
	sb.WriteByte('"')
}
