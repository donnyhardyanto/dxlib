package types

import (
	"regexp"
	"testing"
)

// GenerateUID must mirror UIDDefaultExprPostgreSQL: lowercase hex of Unix
// microseconds + a dashed lowercase UUIDv4.
var uidRe = regexp.MustCompile(`^[0-9a-f]+[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func TestGenerateUIDFormat(t *testing.T) {
	uid := GenerateUID()
	if !uidRe.MatchString(uid) {
		t.Fatalf("GenerateUID() = %q, does not match hex-micros + dashed-uuid", uid)
	}
	if len(uid) < 44 || len(uid) > 56 { // ~11-13 hex + 36 uuid; fits VARCHAR(255) with room
		t.Errorf("GenerateUID() length = %d, want ~45-55", len(uid))
	}
}

func TestGenerateUIDUnique(t *testing.T) {
	seen := make(map[string]bool, 10000)
	for i := 0; i < 10000; i++ {
		u := GenerateUID()
		if seen[u] {
			t.Fatalf("GenerateUID() collision after %d iterations: %q", i, u)
		}
		seen[u] = true
	}
}
