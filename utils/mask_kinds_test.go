package utils

import (
	"encoding/json"
	"testing"

	"github.com/donnyhardyanto/dxlib"
)

// The three rule kinds a host can pick per field, besides Front+Back: e-mail, name initials and
// location. Each has to reach a field three levels down under "params", in an object and in an
// array, because that is where a typed operation puts its body.
func kindRules() map[string]MaskRule {
	return map[string]MaskRule{
		"email":     {Kind: MaskKindEmail},
		"full_name": {Kind: MaskKindInitials},
		"location":  {Kind: MaskKindLocation},
		"latitude":  {Kind: MaskKindLocation},
		"longitude": {Kind: MaskKindLocation},
	}
}

func TestMaskKind_Email(t *testing.T) {
	resetMaskState(t)
	SetMaskRules(kindRules())

	cases := map[string]any{
		"adi.darma@example.co.id": "ad***ma@ex***.co.id",
		"budi@mail.com":           "b***@ma***.com",
		"a@b.co":                  "a***@b***.co",
		"üñíçø@dömain.com":        "üñ***çø@dö***.com",
		"not-an-email":            "***",
		"":                        "***",
	}
	for in, want := range cases {
		if got := MaskSensitiveValue("email", in); got != want {
			t.Errorf("email %q: got %v, want %v", in, got, want)
		}
	}
}

func TestMaskKind_Initials(t *testing.T) {
	resetMaskState(t)
	SetMaskRules(kindRules())

	// Each word is its first letter and a fixed "***", whatever its length, so the mask gives
	// away neither the word nor its length; runs of whitespace are one space; a hyphenated
	// name is one word; an empty or blank value is fully masked.
	cases := map[string]any{
		"Budi Santoso":      "B*** S***",
		"  Siti   Aminah  ": "S*** A***",
		"Émile Zola":        "É*** Z***",
		"Ali":               "A***",
		"A":                 "A***",
		"Jean-Luc Picard":   "J*** P***",
		"":                  "********",
		"   ":               "********",
	}
	for in, want := range cases {
		if got := MaskSensitiveValue("full_name", in); got != want {
			t.Errorf("initials %q: got %v, want %v", in, got, want)
		}
	}
}

func TestMaskKind_Location(t *testing.T) {
	resetMaskState(t)
	SetMaskRules(kindRules())

	cases := []struct {
		in, want any
	}{
		{-6.914744, -6.91},                        // float64, as a plain unmarshal yields
		{json.Number("107.609810"), 107.61},       // json.Number, as the response dump yields
		{int64(107), float64(107)},                // a whole-number coordinate
		{"-6.914744, 107.609810", "-6.91,107.61"}, // "lat,lng" string
		{"-6.914744", "-6.91"},                    // a single coordinate as text
		{"Jl. Merdeka 1", "********"},             // not a coordinate
		{json.Number("abc"), "********"},          // unreadable number
		{true, "********"},                        // not a coordinate
	}
	for _, c := range cases {
		if got := MaskSensitiveValue("location", c.in); got != c.want {
			t.Errorf("location %v: got %v (%T), want %v", c.in, got, got, c.want)
		}
	}
}

func TestMaskKind_LocationObjectIsWalkedAndRounded(t *testing.T) {
	resetMaskState(t)
	SetMaskRules(map[string]MaskRule{"location": {Kind: MaskKindLocation}})

	out := MaskForLog(JSON{
		"location": JSON{
			"lat":      -6.914744,
			"lng":      json.Number("107.609810"),
			"accuracy": 12.3456,
			"label":    "home",
		},
		"locations": []any{JSON{"lat": -6.914744}, "-6.914744,107.609810"},
	})

	loc := out["location"].(JSON)
	if loc["lat"] != -6.91 || loc["lng"] != 107.61 || loc["accuracy"] != 12.35 {
		t.Errorf("numeric leaves of a location object should be rounded: got %v", loc)
	}
	if loc["label"] != "home" {
		t.Errorf("a text leaf of a location object follows the default posture: got %v", loc["label"])
	}
	locs := out["locations"].([]any)
	if locs[0].(JSON)["lat"] != -6.91 || locs[1] != "-6.91,107.61" {
		t.Errorf("location array: got %v", locs)
	}
}

func TestMaskKind_ReachDepthThreeUnderParams(t *testing.T) {
	resetMaskState(t)
	SetMaskRules(kindRules())

	out := MaskForLog(JSON{
		"params": JSON{
			"applicant": JSON{
				"email":     "adi.darma@example.co.id",
				"full_name": "Budi Santoso",
				"latitude":  -6.914744,
				"longitude": json.Number("107.609810"),
			},
			"relatives": []any{
				JSON{"email": "siti@mail.com", "full_name": "Siti Aminah", "location": "-6.914744,107.609810"},
			},
		},
	})

	applicant := out["params"].(JSON)["applicant"].(JSON)
	want := JSON{"email": "ad***ma@ex***.co.id", "full_name": "B*** S***", "latitude": -6.91, "longitude": 107.61}
	for k, w := range want {
		if applicant[k] != w {
			t.Errorf("depth-3 %s: got %v, want %v", k, applicant[k], w)
		}
	}
	relative := out["params"].(JSON)["relatives"].([]any)[0].(JSON)
	want = JSON{"email": "s***@ma***.com", "full_name": "S*** A***", "location": "-6.91,107.61"}
	for k, w := range want {
		if relative[k] != w {
			t.Errorf("depth-3 in array %s: got %v, want %v", k, relative[k], w)
		}
	}
}

func TestMaskKind_StrictMasksEveryKindInFull(t *testing.T) {
	resetMaskState(t)
	SetMaskRules(kindRules())
	SetMaskStrict(true)

	for field, v := range map[string]any{"email": "budi@mail.com", "full_name": "Budi Santoso", "location": -6.914744} {
		if got := MaskSensitiveValue(field, v); got != "********" {
			t.Errorf("strict %s: got %v, want ********", field, got)
		}
	}
	out := MaskForLog(JSON{"location": JSON{"lat": -6.914744}})
	if out["location"].(JSON)["lat"] != "********" {
		t.Errorf("strict should reach a location object's leaves: got %v", out)
	}
}

func TestMaskKind_DebugOverrideShowsEveryKindRaw(t *testing.T) {
	resetMaskState(t)
	SetMaskRules(kindRules())
	prevDebug, prevOverride := dxlib.IsDebug, OverrideShowPasswordOnLog
	t.Cleanup(func() { dxlib.IsDebug, OverrideShowPasswordOnLog = prevDebug, prevOverride })
	dxlib.IsDebug, OverrideShowPasswordOnLog = true, true

	for field, v := range map[string]any{"email": "budi@mail.com", "full_name": "Budi Santoso", "location": -6.914744} {
		if got := MaskSensitiveValue(field, v); got != v {
			t.Errorf("debug override %s: got %v, want the raw value", field, got)
		}
	}
}

// MaskForAnswer is MaskForLog for data that leaves the process in an answer: the debug override
// that shows raw values in a log does not reach it. A credential leaf, a credential container, a
// PII-rule leaf and a PII-rule object are masked as they are with the override off, at depth,
// while MaskForLog on the same data shows them raw.
func TestMaskForAnswer_IgnoresDebugOverride(t *testing.T) {
	resetMaskState(t)
	SetMaskRules(kindRules())
	prevDebug, prevOverride := dxlib.IsDebug, OverrideShowPasswordOnLog
	t.Cleanup(func() { dxlib.IsDebug, OverrideShowPasswordOnLog = prevDebug, prevOverride })
	dxlib.IsDebug, OverrideShowPasswordOnLog = true, true

	in := JSON{
		"password": "hunter2",
		"token":    JSON{"access": "abc", "refresh": "def"},
		"params": JSON{
			"email":     "budi@mail.com",
			"full_name": JSON{"first": "Budi"},
			"unique_id": "dev-1",
		},
	}
	out := MaskForAnswer(in)
	if out["password"] != "***REDACTED***" {
		t.Errorf("credential leaf: got %v, want it redacted", out["password"])
	}
	if out["token"] != "***REDACTED***" {
		t.Errorf("credential container: got %v, want it redacted whole", out["token"])
	}
	params, _ := out["params"].(JSON)
	if params["email"] != "b***@ma***.com" {
		t.Errorf("PII leaf at depth: got %v, want it masked by its rule", params["email"])
	}
	if params["full_name"] != "********" {
		t.Errorf("PII object at depth: got %v, want it masked whole", params["full_name"])
	}
	if params["unique_id"] != "dev-1" {
		t.Errorf("plain field: got %v, want it kept", params["unique_id"])
	}
	if in["password"] != "hunter2" {
		t.Error("MaskForAnswer changed its input")
	}

	// The log walk on the same data is the override's own path and shows the values raw.
	logged := MaskForLog(in)
	if logged["password"] != "hunter2" {
		t.Errorf("MaskForLog under the debug override: got %v, want the raw value", logged["password"])
	}
}

// The zero Kind is the original Front+Back rule, so a host's existing rules keep their meaning.
func TestMaskKind_ZeroValueIsPartial(t *testing.T) {
	resetMaskState(t)
	SetMaskRules(map[string]MaskRule{"nik": {Front: 5, Back: 2}})
	if got := MaskSensitiveValue("nik", "3175012345678901"); got != "31750***01" {
		t.Errorf("got %v, want 31750***01", got)
	}
}
