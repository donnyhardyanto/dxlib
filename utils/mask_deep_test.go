package utils

import "testing"

// MaskForLog is the walker every log dump goes through. The credential keywords, the host's PII
// rules and the default-deny switch must reach a field at any depth, inside objects and arrays
// alike, because a typed operation sends its whole body under one "params" key.
func TestMaskForLog_HostRulesApplyAtEveryDepth(t *testing.T) {
	resetMaskState(t)
	SetMaskRules(map[string]MaskRule{"national_id": {Front: 4, Back: 2}, "full_name": {Front: 1, Back: 0}})

	in := JSON{
		"status_code": 201,
		"params": JSON{
			"applicant": JSON{
				"full_name":   "Budi Santoso",
				"national_id": "3175012345678901",
				"password":    "hunter2",
				"branch_code": "0231",
			},
			"relatives": []any{
				JSON{"full_name": "Siti Aminah", "national_id": "3175019876543210"},
				JSON{"full_name": "Al", "api_token": "tok-123"},
			},
		},
	}

	out := MaskForLog(in)

	applicant := out["params"].(JSON)["applicant"].(JSON)
	if got := applicant["full_name"]; got != "B***" {
		t.Errorf("depth-3 PII rule: got %v, want B***", got)
	}
	if got := applicant["national_id"]; got != "3175***01" {
		t.Errorf("depth-3 PII rule: got %v, want 3175***01", got)
	}
	if got := applicant["password"]; got != "***REDACTED***" {
		t.Errorf("depth-3 credential: got %v, want ***REDACTED***", got)
	}
	if got := applicant["branch_code"]; got != "0231" {
		t.Errorf("unmatched field under default-ALLOW should pass: got %v", got)
	}

	relatives := out["params"].(JSON)["relatives"].([]any)
	if got := relatives[0].(JSON)["national_id"]; got != "3175***10" {
		t.Errorf("object inside array: got %v, want 3175***10", got)
	}
	if got := relatives[1].(JSON)["full_name"]; got != "A***" {
		t.Errorf("short value under a partial rule keeps only its first rune: got %v", got)
	}
	if got := relatives[1].(JSON)["api_token"]; got != "***REDACTED***" {
		t.Errorf("credential inside array: got %v, want ***REDACTED***", got)
	}
	if got := out["status_code"]; got != 201 {
		t.Errorf("top-level unmatched field: got %v, want 201", got)
	}

	// The input is left as it was: the caller may still send it to the client.
	if in["params"].(JSON)["applicant"].(JSON)["national_id"] != "3175012345678901" {
		t.Error("MaskForLog changed its input")
	}
}

func TestMaskForLog_ArrayElementsTakeTheArraysKey(t *testing.T) {
	resetMaskState(t)
	SetMaskRules(map[string]MaskRule{"phone": {Front: 3, Back: 2}})

	out := MaskForLog(JSON{
		"phone_numbers": []any{"081234567890", "0813"},
		"notes":         []any{"first", "second"},
	})

	phones := out["phone_numbers"].([]any)
	if phones[0] != "081***90" || phones[1] != "0***" {
		t.Errorf("array leaves should be masked under the array's key: got %v", phones)
	}
	notes := out["notes"].([]any)
	if notes[0] != "first" || notes[1] != "second" {
		t.Errorf("array under no rule should pass: got %v", notes)
	}
}

func TestMaskForLog_ContainerUnderAMatchingKeyIsMaskedWhole(t *testing.T) {
	resetMaskState(t)
	SetMaskRules(map[string]MaskRule{"address": {Front: 2, Back: 2}})

	out := MaskForLog(JSON{
		"address":     JSON{"street": "Jl. Merdeka 1", "city": "Bandung"},
		"credentials": []any{JSON{"user": "a", "pass": "b"}},
		"token":       JSON{"value": "t"},
	})

	for k, want := range map[string]string{"address": "********", "credentials": "***REDACTED***", "token": "***REDACTED***"} {
		if got := out[k]; got != want {
			t.Errorf("%s holds a container under a matching key and must be masked whole as %s, got %v", k, want, got)
		}
	}
}

// An array under a PII-rule key is walked element by element: a string gets the rule, an object
// is masked whole.
func TestMaskForLog_ArrayUnderARuleKeyMasksEachElement(t *testing.T) {
	resetMaskState(t)
	SetMaskRules(map[string]MaskRule{"address": {Front: 2, Back: 2}})

	out := MaskForLog(JSON{
		"addresses": []any{"Jl. Merdeka 1, Bandung", JSON{"street": "Jl. Merdeka 1"}},
	})
	got := out["addresses"].([]any)
	if got[0] != "Jl***ng" || got[1] != "********" {
		t.Errorf("got %v, want [Jl***ng ********]", got)
	}
}

func TestMaskForLog_StrictAndDefaultDenyReachNestedFields(t *testing.T) {
	resetMaskState(t)
	SetMaskRules(map[string]MaskRule{"national_id": {Front: 4, Back: 2}})
	SetMaskStrict(true)
	SetMaskDefaultDeny(true)
	SetLogAllowedFields([]string{"response_code"})

	out := MaskForLog(JSON{
		"params": JSON{
			"national_id":   "3175012345678901",
			"anything_else": "value",
			"response_code": "00",
			"items":         []any{JSON{"response_code": "01", "amount": 10}},
		},
	})

	params := out["params"].(JSON)
	if got := params["national_id"]; got != "********" {
		t.Errorf("strict should force a full mask at depth: got %v", got)
	}
	if got := params["anything_else"]; got != "********" {
		t.Errorf("default-deny should mask an unnamed field at depth: got %v", got)
	}
	if got := params["response_code"]; got != "00" {
		t.Errorf("allowlisted field at depth should stay readable: got %v", got)
	}
	item := params["items"].([]any)[0].(JSON)
	if item["response_code"] != "01" || item["amount"] != "********" {
		t.Errorf("allowlist and deny should apply inside arrays too: got %v", item)
	}
}

func TestMaskForLog_TypedSliceOfObjects(t *testing.T) {
	resetMaskState(t)
	SetMaskRules(map[string]MaskRule{"national_id": {Front: 4, Back: 2}})

	out := MaskForLog(JSON{
		"rows": []map[string]any{{"national_id": "3175012345678901"}},
	})
	rows := out["rows"].([]any)
	if got := rows[0].(JSON)["national_id"]; got != "3175***01" {
		t.Errorf("[]map[string]any should be walked: got %v", got)
	}
}
