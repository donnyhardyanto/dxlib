package login_system

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSessionTTLFallsBackToTheInstanceTTL(t *testing.T) {
	l := &LoginSystem{ExpiredTimeDuration: 24 * time.Hour}
	if got := l.sessionTTL(map[string]any{}); got != 24*time.Hour {
		t.Fatalf("no field: %v", got)
	}
	if got := l.sessionTTL(map[string]any{SessionTTLField: int64(0)}); got != 24*time.Hour {
		t.Fatalf("zero field: %v", got)
	}
}

// The field has to survive the JSON round trip every store puts session data
// through, where the number comes back as float64 and fixJsonNumbers turns it
// into int64.
func TestSessionTTLSurvivesTheJSONRoundTrip(t *testing.T) {
	l := &LoginSystem{ExpiredTimeDuration: 24 * time.Hour}
	in := map[string]any{SessionTTLField: (10 * time.Minute).Milliseconds()}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err = json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if got := l.sessionTTL(out); got != 10*time.Minute {
		t.Fatalf("float64: %v", got)
	}
	l.fixJsonNumbers(out)
	if got := l.sessionTTL(out); got != 10*time.Minute {
		t.Fatalf("int64: %v", got)
	}
}
