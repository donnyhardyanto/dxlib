package sqlfile

import "testing"

// removeComments tracked comment state but carried no quote state, so a comment
// marker inside a string literal was taken for a comment and the rest of the
// value was deleted. Every schema file, migration and embedded SQL string this
// library applies goes through here, and the corruption is silent -- the
// statement still parses, it just carries a truncated value.
func TestRemoveCommentsPreservesMarkersInsideLiterals(t *testing.T) {
	for _, c := range []struct{ name, in, want string }{
		{
			name: "double dash inside a single-quoted literal",
			in:   `INSERT INTO t (note) VALUES ('range 2026-01-01 -- 2026-12-31');`,
			want: `INSERT INTO t (note) VALUES ('range 2026-01-01 -- 2026-12-31');`,
		},
		{
			name: "block comment opener inside a literal",
			in:   `INSERT INTO t (pattern) VALUES ('/* not a comment */');`,
			want: `INSERT INTO t (pattern) VALUES ('/* not a comment */');`,
		},
		{
			name: "escaped quote does not end the literal",
			in:   `INSERT INTO t (s) VALUES ('it''s -- fine');`,
			want: `INSERT INTO t (s) VALUES ('it''s -- fine');`,
		},
		{
			name: "double-quoted identifier",
			in:   `SELECT "a--b" FROM t;`,
			want: `SELECT "a--b" FROM t;`,
		},
		{
			name: "a real line comment is still removed",
			in:   "SELECT 1; -- trailing note\nSELECT 2;",
			want: "SELECT 1; \nSELECT 2;",
		},
		{
			name: "a real block comment is still removed",
			in:   `SELECT /* note */ 1;`,
			want: `SELECT  1;`,
		},
		{
			name: "apostrophe inside a block comment does not open a literal",
			in:   "/* SQL Server does not have jsonb */\nSELECT 1;",
			want: "\nSELECT 1;",
		},
	} {
		if got := removeComments(c.in); got != c.want {
			t.Errorf("%s:\n  in:   %q\n  got:  %q\n  want: %q", c.name, c.in, got, c.want)
		}
	}
}

// The end-to-end shape: a file whose literal contains a double dash must still
// split into the same number of statements, with the value intact.
func TestRemoveCommentsThenSplitKeepsStatementCount(t *testing.T) {
	in := `CREATE TABLE t (a int);
INSERT INTO t (note) VALUES ('a -- b');
CREATE VIEW v AS SELECT * FROM t;`

	stmts := splitSQLStatements(removeComments(in))
	if len(stmts) != 3 {
		t.Fatalf("expected 3 statements, got %d: %#v", len(stmts), stmts)
	}
	if !contains(stmts[1], "a -- b") {
		t.Errorf("literal was truncated: %q", stmts[1])
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
