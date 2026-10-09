package models

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/donnyhardyanto/dxlib/base"
	"github.com/donnyhardyanto/dxlib/types"
)

const (
	catalogueFixturePath   = "examples/library/model"
	catalogueFixtureCommit = "0123456789abcdef0123456789abcdef01234567"
)

// catalogueFixture is a small model touching every part of the document:
// serial and bigserial keys, literal and function defaults, a unique column
// with a reserved word for a name, foreign keys within and across schemas, a
// table with the _t suffix, explicit indexes, names long enough to be cut,
// and two views.
func catalogueFixture() *ModelDB {
	db := NewModelDB("library", nil)
	public := NewModelDBSchema(db, "public", 1)
	stock := NewModelDBSchema(db, "stock", 2)

	NewModelDBTable(public, "authors", 1, map[string]*ModelDBField{
		"id":         {Order: 1, Type: types.DataTypeBigSerial, IsPrimaryKey: true},
		"name":       {Order: 2, Type: types.DataTypeString255, IsNotNull: true, IsUnique: true},
		"status":     {Order: 3, Type: types.DataTypeString50, IsNotNull: true, DefaultValue: "active"},
		"is_deleted": {Order: 4, Type: types.DataTypeBool, IsNotNull: true, DefaultValue: false},
		"created_at": {Order: 5, Type: types.DataTypeISO8601, DefaultValue: "now()"},
		"tags":       {Order: 6, Type: types.DataTypeArrayString},
		"fee":        {Order: 7, Type: types.DataTypeMoney, DefaultValue: 0},
	}, ModelDBTDEConfig{})

	books := NewModelDBTable(public, "books", 2, map[string]*ModelDBField{
		"id":        {Order: 1, Type: types.DataTypeSerial, IsPrimaryKey: true},
		"author_id": {Order: 2, Type: types.DataTypeInt64, IsNotNull: true, References: "public.authors.id"},
		"order":     {Order: 3, Type: types.DataTypeString20, IsUnique: true},
		"title":     {Order: 4, Type: types.DataTypeString, IsNotNull: true},
		"opened_on": {Order: 5, Type: types.DataTypeDate},
		"opens_at":  {Order: 6, Type: types.DataTypeTime},
		"meta":      {Order: 7, Type: types.DataTypeJSON},
		"cover":     {Order: 8, Type: types.DataTypeBlob},
		"weight":    {Order: 9, Type: types.DataTypeFloat64},
	}, ModelDBTDEConfig{})
	books.UseTableSuffix = true
	NewModelDBIndexForTable(books, "books_t_author_title_idx", 1, []ModelDBIndexColumn{{Name: "author_id"}, {Name: "title", Order: "DESC"}}, false)
	NewModelDBIndexForTable(books, "books_t_title_uidx", 2, []ModelDBIndexColumn{{Name: "title"}}, true)

	NewModelDBTable(stock, "shelf_locations_of_the_central_reading_room", 1, map[string]*ModelDBField{
		"id":                                {Order: 1, Type: types.DataTypeBigSerial, IsPrimaryKey: true},
		"author_id":                         {Order: 2, Type: types.DataTypeInt64, References: "public.authors.id"},
		"position_label_printed_on_the_tag": {Order: 3, Type: types.DataTypeString100, IsUnique: true},
	}, ModelDBTDEConfig{})

	NewModelDBViewRawSQL(public, "author_names", "SELECT id, name FROM public.authors")
	NewModelDBViewRawSQL(stock, "Shelf_Count", "SELECT count(*) AS n FROM stock.shelf_locations_of_the_central_reading_room")
	return db
}

// The fixture's document is the one PostgreSQL's catalogue gives after the
// model's DDL is applied: testdata/catalogue.json was printed by the catalogue
// query against PostgreSQL 18 with that DDL, and the model must write the
// same bytes. TestCatalogueFixtureDDL writes the DDL to run it again.
func TestCatalogueMatchesPostgreSQL(t *testing.T) {
	want, err := os.ReadFile(filepath.Join("testdata", "catalogue.json"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := catalogueFixture().CatalogueAsJSON(catalogueFixturePath, catalogueFixtureCommit)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Errorf("the model's catalogue differs from PostgreSQL's:\n%s", got)
	}
}

// TestCatalogueFixtureDDL writes the fixture's PostgreSQL DDL to the file
// DXLIB_CATALOGUE_FIXTURE_DDL names, to apply it to a disposable PostgreSQL
// and print testdata/catalogue.json again with the catalogue query.
func TestCatalogueFixtureDDL(t *testing.T) {
	out := os.Getenv("DXLIB_CATALOGUE_FIXTURE_DDL")
	if out == "" {
		t.Skip("DXLIB_CATALOGUE_FIXTURE_DDL not set")
	}
	ddl, err := catalogueFixture().CreateDDL(base.DXDatabaseTypePostgreSQL)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out, []byte(ddl), 0o644); err != nil {
		t.Fatal(err)
	}
}

// The members a reader of catalogue dumps reads, with their JSON types; a
// member of the wrong type fails to decode here.
type catalogueReaderShape struct {
	Catalogue string `json:"catalogue"`
	Path      string `json:"path"`
	Commit    string `json:"commit"`
	Tables    []struct {
		Schema  string  `json:"schema"`
		Name    string  `json:"name"`
		Comment *string `json:"comment"`
		Columns []struct {
			Name      string  `json:"name"`
			Type      string  `json:"type"`
			NotNull   bool    `json:"notNull"`
			Default   *string `json:"default"`
			Identity  *string `json:"identity"`
			Generated *string `json:"generated"`
			Comment   *string `json:"comment"`
		} `json:"columns"`
		Constraints []struct {
			Name              string   `json:"name"`
			Kind              string   `json:"kind"`
			Columns           []string `json:"columns"`
			Definition        string   `json:"definition"`
			ReferencesSchema  *string  `json:"referencesSchema"`
			ReferencesTable   *string  `json:"referencesTable"`
			ReferencesColumns []string `json:"referencesColumns"`
			OnDelete          *string  `json:"onDelete"`
		} `json:"constraints"`
		Indexes []struct {
			Name            string `json:"name"`
			Unique          bool   `json:"unique"`
			Primary         bool   `json:"primary"`
			BacksConstraint bool   `json:"backsConstraint"`
			Definition      string `json:"definition"`
		} `json:"indexes"`
	} `json:"tables"`
	Views []struct {
		Schema       string `json:"schema"`
		Name         string `json:"name"`
		Materialized bool   `json:"materialized"`
	} `json:"views"`
	Enums []struct {
		Schema string   `json:"schema"`
		Name   string   `json:"name"`
		Values []string `json:"values"`
	} `json:"enums"`
}

func TestCatalogueDecodesAsTheReadersShape(t *testing.T) {
	b, err := catalogueFixture().CatalogueAsJSON(catalogueFixturePath, catalogueFixtureCommit)
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	var c catalogueReaderShape
	if err := dec.Decode(&c); err != nil {
		t.Fatal(err)
	}
	if c.Catalogue != CatalogueFormat || c.Path != catalogueFixturePath || c.Commit != catalogueFixtureCommit {
		t.Errorf("header: %q %q %q", c.Catalogue, c.Path, c.Commit)
	}
	if len(c.Tables) != 3 || len(c.Views) != 2 || c.Enums == nil || len(c.Enums) != 0 {
		t.Errorf("%d tables, %d views, enums %v", len(c.Tables), len(c.Views), c.Enums)
	}
}

// The same model gives the same bytes, however its maps iterate, and two
// fields with the same Order fall back to their names.
func TestCatalogueIsDeterministic(t *testing.T) {
	first, err := catalogueFixture().CatalogueAsJSON("", "")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		again, err := catalogueFixture().CatalogueAsJSON("", "")
		if err != nil {
			t.Fatal(err)
		}
		if string(again) != string(first) {
			t.Fatalf("run %d differs:\n%s\n---\n%s", i, first, again)
		}
	}

	db := NewModelDB("tied", nil)
	NewModelDBTable(NewModelDBSchema(db, "public", 1), "tied", 1, map[string]*ModelDBField{
		"b": {Order: 1, Type: types.DataTypeInt64},
		"a": {Order: 1, Type: types.DataTypeInt64},
		"c": {Order: 0, Type: types.DataTypeInt64},
	}, ModelDBTDEConfig{})
	b, err := db.CatalogueAsJSON("", "")
	if err != nil {
		t.Fatal(err)
	}
	var c catalogueReaderShape
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, col := range c.Tables[0].Columns {
		names = append(names, col.Name)
	}
	if strings.Join(names, ",") != "c,a,b" {
		t.Errorf("columns %v, want c,a,b", names)
	}
}

// What the fixture leaves out because PostgreSQL rewrites it: a generated
// column is written with its expression as declared and "s", unquoted and so
// folded to lower case, and is never NOT NULL; a function default stays as
// declared; a table with no schema is in public.
func TestCatalogueGeneratedColumnsAndExpressions(t *testing.T) {
	db := NewModelDB("misc", nil)
	NewModelDBTable(NewModelDBSchema(db, "", 1), "people", 1, map[string]*ModelDBField{
		"uid":        {Order: 1, Type: types.DataTypeUID, IsAutoIncrement: true, IsNotNull: true},
		"full_name":  {Order: 2, Type: types.DataTypeString255},
		"Name_Lower": {Order: 3, Type: types.DataTypeString255, IsGenerated: true, IsNotNull: true, GeneratedExpression: "lower(full_name)"},
	}, ModelDBTDEConfig{})
	b, err := db.CatalogueAsJSON("", "")
	if err != nil {
		t.Fatal(err)
	}
	var c catalogueReaderShape
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	tbl := c.Tables[0]
	if tbl.Schema != "public" || tbl.Name != "people" {
		t.Errorf("table %s.%s, want public.people", tbl.Schema, tbl.Name)
	}
	uid := tbl.Columns[0]
	if uid.Default == nil || *uid.Default != types.UIDDefaultExprPostgreSQL || !uid.NotNull {
		t.Errorf("uid: %+v", uid)
	}
	gen := tbl.Columns[2]
	if gen.Name != "name_lower" || gen.Generated == nil || *gen.Generated != "s" || gen.Default == nil || *gen.Default != "lower(full_name)" || gen.NotNull {
		t.Errorf("generated column: %+v", gen)
	}
}

func TestCatalogueRefusesWhatPostgreSQLWouldRefuse(t *testing.T) {
	db := NewModelDB("bad", nil)
	NewModelDBTable(NewModelDBSchema(db, "public", 1), "pairs", 1, map[string]*ModelDBField{
		"a": {Order: 1, Type: types.DataTypeInt64, IsPrimaryKey: true},
		"b": {Order: 2, Type: types.DataTypeInt64, IsPrimaryKey: true},
	}, ModelDBTDEConfig{})
	if _, err := db.CatalogueAsJSON("", ""); err == nil || !strings.Contains(err.Error(), "CATALOGUE_MULTIPLE_PRIMARY_KEYS") {
		t.Errorf("two primary key fields: %v", err)
	}

	db = NewModelDB("twice", nil)
	s := NewModelDBSchema(db, "public", 1)
	NewModelDBTable(s, "same", 1, map[string]*ModelDBField{"id": {Order: 1, Type: types.DataTypeInt64}}, ModelDBTDEConfig{})
	NewModelDBTable(s, "same", 2, map[string]*ModelDBField{"id": {Order: 1, Type: types.DataTypeInt64}}, ModelDBTDEConfig{})
	if _, err := db.CatalogueAsJSON("", ""); err == nil || !strings.Contains(err.Error(), "CATALOGUE_DUPLICATE_TABLE") {
		t.Errorf("one table declared twice: %v", err)
	}
}

func TestPostgresFormatType(t *testing.T) {
	for declared, want := range map[string]string{
		"VARCHAR(255)":                "character varying(255)",
		"varchar":                     "character varying",
		"CHAR(3)":                     "character(3)",
		"CHAR":                        "character(1)",
		"INT":                         "integer",
		"BIGSERIAL":                   "bigint",
		"NUMERIC(23,4)":               "numeric(23,4)",
		"DECIMAL( 10 )":               "numeric(10,0)",
		"TIME":                        "time without time zone",
		"TIMESTAMP(3) WITH TIME ZONE": "timestamp(3) with time zone",
		"TIMESTAMPTZ(6)":              "timestamp(6) with time zone",
		"TEXT[]":                      "text[]",
		"BIGINT[]":                    "bigint[]",
		"double  precision":           "double precision",
		"geometry(Point, 4326)":       "geometry(point, 4326)",
	} {
		if got := postgresFormatType(declared); got != want {
			t.Errorf("%q: %q, want %q", declared, got, want)
		}
	}
}

func TestPostgresObjectNameCutsAsPostgreSQLDoes(t *testing.T) {
	long := strings.Repeat("a", 40)
	got := postgresObjectName(long, strings.Repeat("b", 40), "key")
	if len(got) != 63 || got != strings.Repeat("a", 29)+"_"+strings.Repeat("b", 29)+"_key" {
		t.Errorf("%q (%d bytes)", got, len(got))
	}
	if got := postgresObjectName(strings.Repeat("x", 70), "", "pkey"); len(got) != 63 || !strings.HasSuffix(got, "_pkey") {
		t.Errorf("%q", got)
	}
	// A multi-byte character is not split.
	if got := postgresObjectName(strings.Repeat("é", 40), "", "pkey"); !strings.HasSuffix(got, "é_pkey") || len(got) > 63 {
		t.Errorf("%q", got)
	}
}
