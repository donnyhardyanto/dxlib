package utils

import (
	"strings"
	"testing"
)

// The three provisioning functions in this package put a database name into
// statements whose verbs are DROP DATABASE and CREATE USER, across four
// engines. These tests cover the two pure pieces that decide whether that is
// safe: what names are allowed in at all, and how an allowed name is quoted
// for each engine. The statement construction itself needs a live server and
// is not exercised here.

func TestValidateDatabaseNameAcceptsTheNamesThisProjectUses(t *testing.T) {
	for _, name := range []string{
		"dcc_system",
		"myorganization1contactcenter",
		"pushnotification",
		"dcc_pushnotification",
		"postgres",
		"a",
		"_leading_underscore",
		"with-hyphen",
		"with$dollar",
		"with#hash",
		"MixedCase123",
		// A per-tenant database is named after an app-instance nameid, and
		// those carry dots in practice.
		"dcc.mareca.vc-development",
		"with.dots.and-hyphens",
		strings.Repeat("a", 63),
	} {
		if err := ValidateDatabaseName(name); err != nil {
			t.Errorf("%q was refused and should not be: %v", name, err)
		}
	}
}

// TestValidateDatabaseNameRefusesEveryQuoteBreakout is the point of the
// allowlist. Each of these carries a character that would end the quoting of
// at least one of the four engines, or terminate the statement outright.
func TestValidateDatabaseNameRefusesEveryQuoteBreakout(t *testing.T) {
	for _, c := range []struct{ name, why string }{
		{`x"; DROP DATABASE postgres; --`, "postgres and oracle double-quote breakout"},
		{`x"`, "bare double quote"},
		{"x`", "mariadb backtick breakout"},
		{"x]", "sqlserver bracket breakout"},
		{"x[", "sqlserver bracket open"},
		{`x'`, "single quote, which ends a string-literal context"},
		{`x';DROP DATABASE y;--'`, "single-quote statement injection"},
		{"x;y", "statement separator"},
		{`x\y`, "backslash"},
		{"x y", "whitespace"},
		{"x\ty", "tab"},
		{"x\ny", "newline, which could forge a log line as well as SQL"},
		{"x\x00y", "NUL, which some quoters truncate at rather than reject"},
		{"1leading_digit", "must start with a letter or underscore"},
		{"-leading_hyphen", "must start with a letter or underscore"},
		{"", "empty"},
		{strings.Repeat("a", 64), "over PostgreSQL's 63-character identifier limit"},
		{"naïve", "non-ASCII, legal in some engines but not accepted here"},
	} {
		if err := ValidateDatabaseName(c.name); err == nil {
			t.Errorf("%q was accepted (%s)", c.name, c.why)
		}
	}
}

// TestQuoteDatabaseIdentifierPerEngine pins the quoting rule for each of the
// four engines. Getting one wrong is silent rather than loud: MariaDB without
// ANSI_QUOTES reads "name" as a string literal, not an identifier, and Oracle
// treats a quoted lower-case name as a distinct, non-existent object.
func TestQuoteDatabaseIdentifierPerEngine(t *testing.T) {
	for _, c := range []struct {
		driver string
		in     string
		want   string
	}{
		{"postgres", "dcc_system", `"dcc_system"`},
		{"sqlserver", "dcc_system", "[dcc_system]"},
		{"mariadb", "dcc_system", "`dcc_system`"},
		// Oracle folds to upper case before quoting, because its DDL creates
		// upper-case objects and a quoted lower-case name never resolves.
		{"oracle", "dcc_system", `"DCC_SYSTEM"`},
		{"godror", "dcc_system", `"DCC_SYSTEM"`},
		{"oracle", "MixedCase", `"MIXEDCASE"`},
		{"postgres", "MixedCase", `"MixedCase"`},
	} {
		got, err := quoteDatabaseIdentifier(c.driver, c.in)
		if err != nil {
			t.Errorf("%s/%q: %v", c.driver, c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s/%q quoted as %s, want %s", c.driver, c.in, got, c.want)
		}
	}
}

func TestQuoteDatabaseIdentifierRefusesAnUnknownDriver(t *testing.T) {
	if _, err := quoteDatabaseIdentifier("db2", "dcc_system"); err == nil {
		t.Error("an unsupported driver was quoted rather than refused")
	}
}

// TestQuoteDatabaseIdentifierRevalidates covers the redundancy that is there
// on purpose: a caller that skips the entry check must not be the reason a
// quote character reaches a DROP statement.
func TestQuoteDatabaseIdentifierRevalidates(t *testing.T) {
	for _, driver := range []string{"postgres", "sqlserver", "mariadb", "oracle", "godror"} {
		if _, err := quoteDatabaseIdentifier(driver, `x"; DROP DATABASE postgres; --`); err == nil {
			t.Errorf("%s: quoted a name the validator refuses", driver)
		}
	}
}

// TestQuotedIdentifierCannotEscapeItsQuoting is the property that matters,
// stated directly: for every engine, an accepted name produces exactly one
// balanced quoted token containing no unescaped delimiter.
func TestQuotedIdentifierCannotEscapeItsQuoting(t *testing.T) {
	names := []string{"dcc_system", "with-hyphen", "with$dollar", "with#hash", "A", strings.Repeat("z", 63)}
	for _, driver := range []string{"postgres", "sqlserver", "mariadb", "oracle"} {
		var open, close string
		switch driver {
		case "postgres", "oracle":
			open, close = `"`, `"`
		case "sqlserver":
			open, close = "[", "]"
		case "mariadb":
			open, close = "`", "`"
		}
		for _, n := range names {
			got, err := quoteDatabaseIdentifier(driver, n)
			if err != nil {
				t.Fatalf("%s/%q: %v", driver, n, err)
			}
			if !strings.HasPrefix(got, open) || !strings.HasSuffix(got, close) {
				t.Errorf("%s/%q: %s is not wrapped in %s%s", driver, n, got, open, close)
			}
			// Strip the wrapper; nothing delimiter-like may remain.
			inner := got[len(open) : len(got)-len(close)]
			if strings.Contains(inner, close) {
				t.Errorf("%s/%q: %s carries an unescaped %s inside its quoting", driver, n, got, close)
			}
			if strings.ContainsAny(inner, "'\";`[]\\\x00\n\t ") {
				t.Errorf("%s/%q: %s carries a delimiter inside its quoting", driver, n, got)
			}
		}
	}
}
